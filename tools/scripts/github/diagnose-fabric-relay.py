"""Read-only, secret-safe connectivity probe for the existing Fabric identity relay."""

import importlib.util
import json
import os
from pathlib import Path
import socket
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request


def record(stage, start, **fields):
    print(json.dumps({"stage": stage, "seconds": round(time.monotonic() - start, 3), **fields}), flush=True)


def main():
    relay_url = os.environ.get("IMDS_RELAY_URL", "")
    parsed = urllib.parse.urlsplit(relay_url)
    record("configuration", time.monotonic(), url_set=bool(relay_url),
           sender_key_set=bool(os.environ.get("IMDS_RELAY_SENDER_KEY")),
           scheme=parsed.scheme, explicit_port=parsed.port,
           proxy_configured=bool(urllib.request.getproxies()))
    if parsed.scheme != "https" or not parsed.hostname:
        return 1
    start = time.monotonic()
    try:
        addresses = socket.getaddrinfo(parsed.hostname, parsed.port or 443, type=socket.SOCK_STREAM)
        record("dns", start, success=True, address_count=len(addresses),
               families=sorted({item[0].name for item in addresses}))
    except OSError as exc:
        record("dns", start, success=False, error_type=type(exc).__name__)
        return 1
    start = time.monotonic()
    try:
        with socket.create_connection((parsed.hostname, parsed.port or 443), timeout=10) as sock:
            record("tcp", start, success=True)
            start = time.monotonic()
            with ssl.create_default_context().wrap_socket(sock, server_hostname=parsed.hostname) as tls:
                record("tls", start, success=True, version=tls.version())
    except OSError as exc:
        record("connect_or_tls", start, success=False, error_type=type(exc).__name__, errno=exc.errno)
        return 1
    # No response bodies or exception strings are logged: they can contain credentials.
    start = time.monotonic()
    try:
        with urllib.request.urlopen(relay_url, timeout=10) as response:
            record("unsigned_http", start, status=response.status)
    except urllib.error.HTTPError as exc:
        record("unsigned_http", start, status=exc.code)
    except Exception as exc:
        record("unsigned_http", start, error_type=type(exc).__name__)
    source = Path(__file__).resolve().parents[3] / "src/containers/imds-router/imds_relay_router.py"
    spec = importlib.util.spec_from_file_location("relay_router", source)
    router = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(router)
    resource = "https://management.core.windows.net/"
    client_id = os.environ.get("UAMI_CLIENT_ID")
    uri = relay_url + "?" + urllib.parse.urlencode({"resource": resource, **({"client_id": client_id} if client_id else {})})
    sas = router._generate_sas_token(router._relay_sas_uri(relay_url), router.RELAY_SENDER_KEY, router.RELAY_KEY_NAME)
    request = urllib.request.Request(uri, headers={"ServiceBusAuthorization": sas})
    start = time.monotonic()
    try:
        # Azure Relay can return its own diagnostic response at 60s. A single
        # 65s diagnostic request distinguishes it from our router's 30s timeout.
        with urllib.request.urlopen(request, timeout=65) as response:
            body = json.load(response)
            valid = bool(body.get("access_token"))
            record("signed_token", start, status=response.status, access_token_present=valid,
                   expiry_present=bool(body.get("expires_on")))
            return 0 if valid else 1
    except urllib.error.HTTPError as exc:
        body = exc.read(4096).lower()
        record("signed_token", start, status=exc.code,
               no_listener=b"no listener" in body or b"nolistener" in body,
               listener_timeout=b"listener" in body and b"time" in body,
               unauthorized=b"unauthor" in body)
    except Exception as exc:
        record("signed_token", start, error_type=type(exc).__name__)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
