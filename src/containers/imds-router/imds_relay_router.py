#!/usr/bin/env python3
"""IMDS Relay Router — proxies `az login --identity` token requests to Azure Relay.

Serves the App Service / IMDS managed-identity contract on localhost so that a
container whose `IDENTITY_ENDPOINT` points here can `az login --identity` and
fetch real tokens for a fake UAMI. Token requests are relayed (SAS-signed) over
an Azure Relay hybrid connection to the identity that actually holds the UAMI.
"""

import base64
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LISTEN_PORT = int(os.environ.get("IMDS_ROUTER_PORT", "8080"))
RELAY_URL = os.environ.get("IMDS_RELAY_URL", "")
RELAY_SENDER_KEY = os.environ.get("IMDS_RELAY_SENDER_KEY", "")
RELAY_KEY_NAME = os.environ.get("IMDS_RELAY_KEY_NAME", "Send")
IDENTITY_HEADER_VALUE = os.environ.get("IDENTITY_HEADER", "local-dev-secret")
TOKEN_MAX_ATTEMPTS = int(os.environ.get("IMDS_TOKEN_MAX_ATTEMPTS", "3"))
TOKEN_EXPIRY_SKEW_SEC = int(os.environ.get("IMDS_TOKEN_EXPIRY_SKEW_SEC", "300"))
# The background refresher re-mints a cached token once it is within this many
# seconds of expiry — deliberately larger than the serve-time skew so a token is
# refreshed off the request path well before any synchronous `az` call would
# find it stale. Keeps the slow relay round-trip out of the hot path entirely.
TOKEN_REFRESH_LEAD_SEC = int(os.environ.get("IMDS_TOKEN_REFRESH_LEAD_SEC", "900"))
REFRESHER_INTERVAL_SEC = int(os.environ.get("IMDS_REFRESHER_INTERVAL_SEC", "60"))


def _relay_sas_uri(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.rstrip("/").removesuffix("/token")
    return f"http://{parsed.hostname}{path}"


def _generate_sas_token(uri: str, key: str, key_name: str, expiry_seconds: int = 3600) -> str:
    expiry = int(time.time()) + expiry_seconds
    sts = f"{urllib.parse.quote(uri, safe='')}\n{expiry}"
    sig = hmac.new(key.encode(), sts.encode(), hashlib.sha256).digest()
    sig_b64 = urllib.parse.quote(base64.b64encode(sig).decode(), safe="")
    return f"SharedAccessSignature sr={urllib.parse.quote(uri, safe='')}&sig={sig_b64}&se={expiry}&skn={key_name}"


class TokenCache:
    """Route-/scope-aware token cache.

    Keyed by resource (+ client_id), so different scopes never share an entry. A
    token within ``skew_sec`` of expiry is treated as stale so callers refetch
    ahead of expiry rather than serving a token about to expire.
    """

    def __init__(self, skew_sec, now=lambda: int(time.time())):
        self._entries = {}
        self._skew_sec = skew_sec
        self._now = now
        self._lock = threading.Lock()

    def is_fresh(self, entry):
        return entry["expires_on"] - self._now() > self._skew_sec

    def get(self, key):
        with self._lock:
            hit = self._entries.get(key)
            if hit is None:
                return None
            if self.is_fresh(hit):
                return hit
            del self._entries[key]
            return None

    def set(self, key, entry):
        with self._lock:
            self._entries[key] = entry

    def snapshot(self):
        with self._lock:
            return list(self._entries.items())


def _parse_expires_on(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_CACHE = TokenCache(TOKEN_EXPIRY_SKEW_SEC)

# Per-cache-key locks so concurrent requests (or the refresher) for the SAME
# scope collapse onto a single relay round-trip instead of stampeding.
_fetch_locks = {}
_fetch_locks_guard = threading.Lock()


def _fetch_lock(cache_key):
    with _fetch_locks_guard:
        lock = _fetch_locks.get(cache_key)
        if lock is None:
            lock = threading.Lock()
            _fetch_locks[cache_key] = lock
        return lock


def _relay_fetch(resource, client_id):
    """Single SAS-signed relay round-trip. Returns (body_dict, error_str)."""
    relay_uri = f"{RELAY_URL}?resource={urllib.parse.quote_plus(resource)}"
    if client_id:
        relay_uri += f"&client_id={urllib.parse.quote_plus(client_id)}"
    sas = _generate_sas_token(_relay_sas_uri(RELAY_URL), RELAY_SENDER_KEY, RELAY_KEY_NAME)
    req = urllib.request.Request(relay_uri, headers={"ServiceBusAuthorization": sas})
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode())
    if not body.get("access_token"):
        return None, "relay returned empty access_token"
    return body, ""


def _fetch_token(resource, client_id, cache_key, log=lambda *a: None):
    """Return a fresh token dict for (resource, client_id), retrying the relay.

    Dedupes concurrent callers on a per-key lock; a caller that blocked on the
    lock re-checks the cache first so only one relay round-trip happens.
    """
    with _fetch_lock(cache_key):
        cached = _CACHE.get(cache_key)
        if cached:
            return cached, ""
        last_error = "unknown error"
        for attempt in range(1, TOKEN_MAX_ATTEMPTS + 1):
            try:
                body, err = _relay_fetch(resource, client_id)
                if body is not None:
                    expires_on = _parse_expires_on(body.get("expires_on"))
                    entry = {
                        "access_token": body["access_token"],
                        "expires_on": expires_on if expires_on is not None else 0,
                        "resource": resource,
                        "client_id": client_id,
                    }
                    if expires_on is not None:
                        _CACHE.set(cache_key, entry)
                    return entry, ""
                last_error = err
            except urllib.error.HTTPError as e:
                detail = e.read().decode() if e.fp else ""
                last_error = f"Relay {e.code}: {detail[:200]}"
            except Exception as e:  # noqa: BLE001
                last_error = str(e)
            log("Token attempt %d/%d failed: %s", attempt, TOKEN_MAX_ATTEMPTS, last_error)
            if attempt < TOKEN_MAX_ATTEMPTS:
                time.sleep(min(2**attempt, 10))
        return None, last_error


def _refresher_loop():
    """Proactively re-mint cached tokens well before expiry so synchronous `az`
    requests always land on a fresh cache entry and never pay the relay latency.
    """
    while True:
        time.sleep(REFRESHER_INTERVAL_SEC)
        now = int(time.time())
        for cache_key, entry in _CACHE.snapshot():
            if entry.get("expires_on", 0) - now > TOKEN_REFRESH_LEAD_SEC:
                continue
            # Force a re-fetch by evicting, then minting under the per-key lock.
            with _fetch_lock(cache_key):
                still = _CACHE.get(cache_key)
                if still and still["expires_on"] - int(time.time()) > TOKEN_REFRESH_LEAD_SEC:
                    continue
                try:
                    body, _ = _relay_fetch(entry["resource"], entry.get("client_id"))
                    if body is not None:
                        expires_on = _parse_expires_on(body.get("expires_on"))
                        if expires_on is not None:
                            _CACHE.set(cache_key, {
                                "access_token": body["access_token"],
                                "expires_on": expires_on,
                                "resource": entry["resource"],
                                "client_id": entry.get("client_id"),
                            })
                            print(f"[imds-router] refreshed {cache_key} (expires_on={expires_on})", flush=True)
                except Exception as e:  # noqa: BLE001
                    print(f"[imds-router] refresh {cache_key} failed: {e}", flush=True)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"[imds-router] {fmt % args}", flush=True)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/healthz":
            return self._json(200, {"Healthy": True})
        if parsed.path in ("/metadata/identity/oauth2/token", "/token"):
            return self._handle_token(parsed)
        if parsed.path.startswith("/metadata/instance"):
            return self._json(
                200,
                {
                    "compute": {
                        "subscriptionId": os.environ.get(
                            "IMDS_SUBSCRIPTION_ID", "00000000-0000-0000-0000-000000000000"
                        ),
                        "resourceGroupName": "ivm-bench",
                        "name": "ivm-bench-fabric",
                    }
                },
            )
        self._json(404, {"error": "Not found"})

    def _handle_token(self, parsed):
        if self.headers.get("X-IDENTITY-HEADER", "") != IDENTITY_HEADER_VALUE:
            return self._json(403, {"error": "Invalid X-IDENTITY-HEADER"})

        qs = urllib.parse.parse_qs(parsed.query)
        resource = qs.get("resource", ["https://management.azure.com/"])[0]

        if not RELAY_URL or not RELAY_SENDER_KEY:
            return self._json(500, {"error": "IMDS_RELAY_URL or IMDS_RELAY_SENDER_KEY not set"})

        client_id = qs.get("client_id", [None])[0]
        cache_key = resource if not client_id else f"{resource}|client_id={client_id}"

        cached = _CACHE.get(cache_key)
        if cached:
            self.log_message("Cache hit: %s (expires_on=%s)", cache_key, cached["expires_on"])
            return self._json(
                200,
                {
                    "access_token": cached["access_token"],
                    "expires_on": str(cached["expires_on"]),
                    "resource": resource,
                    "token_type": "Bearer",
                },
            )

        entry, err = _fetch_token(resource, client_id, cache_key, self.log_message)
        if entry is not None:
            return self._json(
                200,
                {
                    "access_token": entry["access_token"],
                    "expires_on": str(entry["expires_on"]),
                    "resource": resource,
                    "token_type": "Bearer",
                },
            )
        self._json(502, {"error": "Relay token request failed", "detail": err[:500]})

    def _json(self, status: int, body: dict):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    threading.Thread(target=_refresher_loop, name="token-refresher", daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    server.daemon_threads = True
    print(
        f"[imds-router] Listening on 0.0.0.0:{LISTEN_PORT} "
        f"(threaded; refresh lead {TOKEN_REFRESH_LEAD_SEC}s)",
        flush=True,
    )
    server.serve_forever()
