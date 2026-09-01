"""Microsoft Fabric backend for the ``fabric-openivm-jvm-35`` / ``fabric-jvm-35``
engines.

Consolidates everything the dbt-server does *around* the dbt-fabricspark build:

* auth — ``az login --identity`` (brokered by the imds-router sidecar) + token
  minting for the Fabric REST, OneLake storage, and Power BI (Livy) audiences;
* Environment manager — upload the openivm JAR + push a fresh Spark-config set
  into Fabric Environment "35", then publish (openivm engine only);
* shared cache — stage the locally-generated TPC-DI Delta dirs into the
  lakehouse ``Files/_shared_cache/tpcdi_raw_cache/sf=<N>/batch<M>_pct=<P>/``
  area via azcopy (mirrors the databricks-enzyme UC-Volume cache);
* blow-up — drop the lakehouse ``Tables/`` contents + the openivm state
  (``Files/_openivm``) between runs so each experiment starts clean.

The source-table CREATE/INSERT SQL itself runs inside the dbt Livy session via
the ``load_fabric_sources`` on-run-start macro (so it shares the openivm
extension + RocksDB state); this module only moves bytes and manages the
Environment.
"""

import json
import logging
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from services.source_cache import batch_cache_root

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config (from the fabric-* compose environment)
# ---------------------------------------------------------------------------

FABRIC_API_BASE = os.environ.get("FABRIC_API_BASE", "https://api.fabric.microsoft.com").rstrip("/")
WORKSPACE_ID = os.environ.get("FABRIC_WORKSPACE_ID", "")
ONELAKE_HOST = os.environ.get("FABRIC_ONELAKE_HOST", "msit-onelake.dfs.fabric.microsoft.com")
UAMI_CLIENT_ID = os.environ.get("UAMI_CLIENT_ID", "")
OPENIVM_JAR_PATH = os.environ.get("OPENIVM_JAR_PATH", "/data/bin/openivm-extension.jar")
RAW_DELTA_DIR = os.environ.get("RAW_DELTA_DIR", "/data/raw/delta")

# ── Dynamic per-run provisioning ────────────────────────────────────────────
# The compute Lakehouse + Environment are created FRESH per run (named
# ``<BASE_NAME>_<RUN_ID>``) and torn down at the end; orphaned ones (from crashed
# runs) are swept by age. Only the shared CACHE lakehouse (raw TPC-DI sources) is
# persistent — resolved find-or-create by a stable name so nothing but the
# workspace needs to be pinned in ``.env``.
CACHE_LAKEHOUSE_NAME = os.environ.get("FABRIC_CACHE_LAKEHOUSE_NAME", "ivmbench_cache")
# Per-flavor compute base name; the compose files map the per-flavor .env value.
BASE_NAME = os.environ.get("FABRIC_BASE_NAME", "openivm_jvm_35")
# Minted per-experiment by the orchestrator (mirrors DATABRICKS_EXPERIMENT_ID).
RUN_ID = os.environ.get("FABRIC_RUN_ID", "")
STALE_MAX_AGE_S = int(os.environ.get("FABRIC_STALE_RESOURCE_MAX_AGE_SECONDS", "") or "86400")
# Per-run resolved IDs (compute + cache), persisted so dbt_runner can inject them
# into the dbt subprocess env and teardown can find what to delete.
_RESOLVED_PATH = os.environ.get("FABRIC_RESOLVED_PATH", "/tmp/fabric-resolved.json")

# Token audiences.
FABRIC_RESOURCE = "https://api.fabric.microsoft.com"
STORAGE_RESOURCE = "https://storage.azure.com"
# dbt-fabricspark's CLI auth scope (Fabric Livy). Pre-warmed into az's token
# cache so the adapter's AzureCliCredential (10s subprocess timeout) never pays
# the cold imds-router relay round-trip mid-build.
LIVY_SCOPE = "https://analysis.windows.net/powerbi/api/.default"

# OneLake cache + state layout (under the lakehouse Files/ area).
CACHE_ROOT = "Files/_shared_cache/tpcdi_raw_cache"
# RocksDB state mirror — matches spark.openivm.stateSync.uri (Files/_openivm).
STATE_ROOT = "Files/_openivm"
# openivm MV Delta warehouse — matches spark.sql.warehouse.dir pushed to Env "35"
# (Files/_ivm-warehouse). Holds the MV tables + DML staging Delta; blown away
# between runs so a fresh CREATE MATERIALIZED VIEW never hits a stale path.
WAREHOUSE_ROOT = "Files/_ivm-warehouse"

# TPC-DI source-table groups (mirror databricks_enzyme_sources).
BATCH1_TABLES: List[str] = [
    "customer_mgmt", "date", "finwire", "hr", "industry",
    "status_type", "tax_rate", "trade_history", "trade_type",
]
STAGING_TABLES: List[str] = [
    "cash_transaction", "daily_market", "holding_history", "prospect",
    "trade", "watch_history", "account", "customer", "batch_date",
]

_HTTP_TIMEOUT = 300

# Continuous token keep-warm. dbt-fabricspark's AzureCliCredential shells
# ``az account get-access-token`` per token acquisition with a ~10s subprocess
# timeout; a 30-thread dbt build fires many of these across a ~15 min batch. If
# az's MSAL cache lapses mid-build the refresh pays a cold imds-router relay
# round-trip that can exceed 10s → "Failed to invoke the Azure CLI" fails the
# node. A daemon thread re-mints all three audiences every _KEEPWARM_INTERVAL s
# so az always answers from a warm cache (<1s) and any slow relay refresh lands
# on the background thread, never on dbt's hot path.
_KEEPWARM_INTERVAL = 30
_KEEPWARM_SCOPES = (LIVY_SCOPE,)
_KEEPWARM_RESOURCES = (STORAGE_RESOURCE, FABRIC_RESOURCE)
_keepwarm_started = False
_keepwarm_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _warm_livy_token(timeout: Optional[float] = None) -> None:
    """Synchronously mint the Livy (Power BI) token into az's cache so the very
    first dbt statement of a batch reads a warm token."""
    subprocess.run(
        ["az", "account", "get-access-token", "--scope", LIVY_SCOPE, "-o", "none"],
        capture_output=True, text=True, timeout=timeout,
    )


def _bounded_timeout(
    deadline: Optional[float], timeout: Optional[float]
) -> Optional[float]:
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Fabric storage collection deadline exceeded")
    return remaining if timeout is None else min(timeout, remaining)


def _keepwarm_loop() -> None:
    while True:
        for scope in _KEEPWARM_SCOPES:
            try:
                subprocess.run(
                    ["az", "account", "get-access-token", "--scope", scope, "-o", "none"],
                    capture_output=True, text=True, timeout=60,
                )
            except Exception as e:  # noqa: BLE001 — never let keep-warm die
                logger.warning("[fabric] keep-warm scope %s failed: %s", scope, e)
        for resource in _KEEPWARM_RESOURCES:
            try:
                subprocess.run(
                    ["az", "account", "get-access-token", "--resource", resource, "-o", "none"],
                    capture_output=True, text=True, timeout=60,
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("[fabric] keep-warm resource %s failed: %s", resource, e)
        time.sleep(_KEEPWARM_INTERVAL)


def _start_keepwarm() -> None:
    """Start the token keep-warm daemon exactly once."""
    global _keepwarm_started
    with _keepwarm_lock:
        if _keepwarm_started:
            return
        threading.Thread(target=_keepwarm_loop, name="fabric-token-keepwarm", daemon=True).start()
        _keepwarm_started = True
        logger.info("[fabric] token keep-warm daemon started (every %ss)", _KEEPWARM_INTERVAL)


def ensure_az_login(
    *,
    timeout: Optional[float] = None,
    deadline: Optional[float] = None,
    warm_livy: bool = True,
    force: bool = False,
) -> None:
    """Idempotently ``az login --identity`` through the imds-router sidecar, then
    pre-warm the Livy token and start the keep-warm daemon.

    dbt-fabricspark's ``authentication: CLI`` and azcopy's AZCLI auth both shell
    out to ``az account get-access-token``, which requires a logged-in ``az``.
    Safe to call repeatedly — a live login short-circuits.
    """
    logged_in = False
    if not force:
        probe = subprocess.run(
            ["az", "account", "show"],
            capture_output=True,
            text=True,
            timeout=_bounded_timeout(deadline, timeout),
        )
        logged_in = probe.returncode == 0
    if not logged_in:
        cmd = ["az", "login", "--identity", "--allow-no-subscriptions"]
        if UAMI_CLIENT_ID:
            cmd += ["--client-id", UAMI_CLIENT_ID]
        res = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=_bounded_timeout(deadline, timeout),
        )
        if res.returncode != 0:
            raise RuntimeError(f"`az login --identity` failed: {res.stderr[:500]}")
        logger.info("[fabric] az login --identity OK")
    if warm_livy:
        _warm_livy_token(timeout=_bounded_timeout(deadline, timeout))
    _start_keepwarm()


def get_token(
    resource: str,
    *,
    timeout: Optional[float] = None,
    deadline: Optional[float] = None,
    warm_livy: bool = True,
) -> str:
    ensure_az_login(
        timeout=timeout,
        deadline=deadline,
        warm_livy=warm_livy,
    )
    for attempt in range(2):
        res = subprocess.run(
            ["az", "account", "get-access-token", "--resource", resource,
             "--query", "accessToken", "-o", "tsv"],
            capture_output=True,
            text=True,
            timeout=_bounded_timeout(deadline, timeout),
        )
        if res.returncode == 0:
            return res.stdout.strip()
        if attempt == 0:
            ensure_az_login(timeout=timeout, deadline=deadline, warm_livy=warm_livy, force=True)
    raise RuntimeError(f"az token for {resource} failed: {res.stderr[:300]}")


def _fabric_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {get_token(FABRIC_RESOURCE)}"}


def _storage_headers(deadline: Optional[float] = None) -> Dict[str, str]:
    return {
        "Authorization": (
            f"Bearer {get_token(STORAGE_RESOURCE, deadline=deadline, warm_livy=False)}"
        ),
        "x-ms-version": "2021-10-04",
    }


# ---------------------------------------------------------------------------
# Per-run resolved IDs (compute lakehouse/env + shared cache lakehouse)
# ---------------------------------------------------------------------------

_resolved_cache: Dict[str, object] = {}


def _load_resolved() -> Dict[str, object]:
    global _resolved_cache
    if _resolved_cache:
        return _resolved_cache
    try:
        with open(_RESOLVED_PATH) as f:
            _resolved_cache = json.load(f)
    except (OSError, ValueError):
        _resolved_cache = {}
    return _resolved_cache


def _save_resolved(d: Dict[str, object]) -> None:
    global _resolved_cache
    _resolved_cache = dict(d)
    with open(_RESOLVED_PATH, "w") as f:
        json.dump(_resolved_cache, f)


def _compute_lakehouse_id() -> str:
    lid = str(_load_resolved().get("lakehouse_id", "") or "")
    if not lid:
        raise RuntimeError(
            "compute lakehouse not provisioned — call POST /environment/fabric/provision first"
        )
    return lid


def _compute_environment_id() -> str:
    eid = str(_load_resolved().get("environment_id", "") or "")
    if not eid:
        raise RuntimeError(
            "compute environment not provisioned — call POST /environment/fabric/provision first"
        )
    return eid


# ---------------------------------------------------------------------------
# Fabric Items REST — create / list / delete lakehouses + environments
# ---------------------------------------------------------------------------

def _workspace_base() -> str:
    return f"{FABRIC_API_BASE}/v1/workspaces/{WORKSPACE_ID}"


def _fabric_json_headers() -> Dict[str, str]:
    return {**_fabric_headers(), "Content-Type": "application/json"}


def _fabric_req(method: str, url: str, *, retries: int = 8, **kwargs) -> requests.Response:
    """Fabric REST call with 429/503 retry honoring ``Retry-After``. Fabric
    throttles bursts of item/environment operations, so provisioning (which fires
    create + list + publish + poll in quick succession) would otherwise 429."""
    kwargs.setdefault("timeout", 120)
    backoff = 5.0
    r = requests.request(method, url, **kwargs)
    for attempt in range(1, retries + 1):
        if r.status_code not in (429, 503) or attempt == retries:
            return r
        try:
            wait = float(r.headers.get("Retry-After", ""))
        except ValueError:
            wait = backoff
        logger.warning(
            "[fabric] %s ...%s -> HTTP %s, retry %d/%d in %.0fs",
            method, url[-48:], r.status_code, attempt, retries, min(max(wait, 1.0), 60),
        )
        time.sleep(min(max(wait, 1.0), 60))
        backoff = min(backoff * 2, 60)
        r = requests.request(method, url, **kwargs)
    return r


def _lro_poll(location: str, timeout_s: int = 900, interval_s: int = 10) -> None:
    """Poll a Fabric long-running-operation Location URL until it succeeds."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = _fabric_req("GET", location, headers=_fabric_headers())
        r.raise_for_status()
        status = (r.json() or {}).get("status", "").lower()
        if status == "succeeded":
            return
        if status in ("failed", "cancelled"):
            raise RuntimeError(f"Fabric operation {status}: {r.text[:300]}")
        time.sleep(interval_s)
    raise RuntimeError("Fabric operation timed out")


def _create_item(item_type: str, name: str) -> str:
    """Create a workspace item (``lakehouses`` | ``environments``); return its id.

    Handles the sync (201) and async LRO (202 + Location poll) responses; the id
    is resolved by re-listing by displayName (robust across both paths)."""
    r = _fabric_req(
        "POST",
        f"{_workspace_base()}/{item_type}",
        headers=_fabric_json_headers(),
        json={"displayName": name},
        timeout=180,
    )
    if r.status_code in (200, 201):
        body = r.json() if r.text else {}
        if body.get("id"):
            return str(body["id"])
    elif r.status_code == 202:
        loc = r.headers.get("Location")
        if loc:
            _lro_poll(loc)
    else:
        raise RuntimeError(
            f"create {item_type} '{name}' failed: HTTP {r.status_code} {r.text[:300]}"
        )
    found = _find_item_by_name(item_type, name)
    if not found:
        raise RuntimeError(f"create {item_type} '{name}' — item not found after create")
    return found["id"]


def _list_items(item_type: str) -> List[Dict[str, str]]:
    """List all workspace items of a type, following continuation tokens."""
    items: List[Dict[str, str]] = []
    url = f"{_workspace_base()}/{item_type}"
    params: Dict[str, str] = {}
    for _ in range(50):  # hard page cap
        r = _fabric_req("GET", url, headers=_fabric_headers(), params=params)
        if r.status_code != 200:
            break
        body = r.json() or {}
        items += [
            {"id": str(it.get("id", "")), "displayName": str(it.get("displayName", ""))}
            for it in (body.get("value") or [])
            if it.get("id")
        ]
        token = body.get("continuationToken")
        if not token:
            break
        params = {"continuationToken": token}
    return items


def _find_item_by_name(item_type: str, name: str) -> Optional[Dict[str, str]]:
    for it in _list_items(item_type):
        if it["displayName"] == name:
            return it
    return None


def _delete_item(item_type: str, item_id: str) -> bool:
    r = _fabric_req(
        "DELETE", f"{_workspace_base()}/{item_type}/{item_id}", headers=_fabric_headers(), timeout=180
    )
    return r.status_code in (200, 202, 404)


def resolve_cache_lakehouse() -> str:
    """Find-or-create the persistent shared cache lakehouse by name; return id."""
    cached = str(_load_resolved().get("cache_lakehouse_id", "") or "")
    if cached:
        return cached
    found = _find_item_by_name("lakehouses", CACHE_LAKEHOUSE_NAME)
    lid = found["id"] if found else _create_item("lakehouses", CACHE_LAKEHOUSE_NAME)
    logger.info("[fabric] cache lakehouse '%s' id=%s", CACHE_LAKEHOUSE_NAME, lid)
    return lid


# ---------------------------------------------------------------------------
# Per-run provisioning + orphan sweep
# ---------------------------------------------------------------------------

def _run_ts_us(name: str, base: str) -> Optional[float]:
    """Extract the microsecond epoch from a ``<base>_<microsec>_<rand>`` item name."""
    prefix = f"{base}_"
    if not name.startswith(prefix):
        return None
    ts_str = name[len(prefix):].split("_", 1)[0]
    if not ts_str.isdigit() or len(ts_str) < 15:
        return None
    return float(ts_str)


def sweep_stale_resources(max_age_s: Optional[int] = None) -> dict:
    """Delete this flavor's compute lakehouses + environments (named
    ``<BASE_NAME>_<microsec>_<rand>``) whose run-id timestamp is older than
    ``max_age_s`` (default ``FABRIC_STALE_RESOURCE_MAX_AGE_SECONDS``). The age
    guard keeps a concurrent runner's live resources (< threshold) safe."""
    max_age = max_age_s if max_age_s is not None else STALE_MAX_AGE_S
    now_us = time.time() * 1_000_000
    deleted: List[str] = []
    for item_type in ("lakehouses", "environments"):
        for it in _list_items(item_type):
            name = it["displayName"]
            ts = _run_ts_us(name, BASE_NAME)
            if ts is None:
                continue
            if RUN_ID and name == f"{BASE_NAME}_{RUN_ID}":
                continue  # never sweep our own
            if (now_us - ts) / 1_000_000 <= max_age:
                continue
            if _delete_item(item_type, it["id"]):
                deleted.append(f"{item_type}/{name}")
    if deleted:
        logger.info("[fabric] swept %d stale resource(s): %s", len(deleted), deleted)
    return {"status": "ok", "deleted": deleted, "max_age_s": max_age}


def provision_run(openivm: bool) -> dict:
    """Batch-1 provisioning: sweep orphans -> resolve the shared cache lakehouse
    -> create a FRESH compute lakehouse + environment named ``<BASE_NAME>_<RUN_ID>``
    -> (openivm) publish the JAR + Spark config into that environment; (baseline)
    publish the empty environment so it is attachable. Persists the resolved IDs
    for the dbt run + teardown."""
    if not RUN_ID:
        raise RuntimeError("FABRIC_RUN_ID not set (the orchestrator must mint it per experiment)")
    swept = sweep_stale_resources()
    cache_id = resolve_cache_lakehouse()
    name = f"{BASE_NAME}_{RUN_ID}"
    lakehouse_id = _create_item("lakehouses", name)
    environment_id = _create_item("environments", name)
    resolved = {
        "run_id": RUN_ID,
        "cache_lakehouse_id": cache_id,
        "cache_lakehouse_name": CACHE_LAKEHOUSE_NAME,
        "lakehouse_id": lakehouse_id,
        "lakehouse_name": name,
        "environment_id": environment_id,
        "environment_name": name,
        "openivm": bool(openivm),
    }
    _save_resolved(resolved)
    logger.info(
        "[fabric] provisioned %s: lakehouse=%s environment=%s (cache=%s)",
        name, lakehouse_id, environment_id, cache_id,
    )
    publish = refresh_environment() if openivm else publish_empty_environment()
    return {"status": "ok", "resolved": resolved, "swept": swept["deleted"], "publish": publish}


def teardown_run() -> dict:
    """End-of-run: delete this run's compute lakehouse + environment (the shared
    cache lakehouse is preserved). Best-effort; orphan-sweep is the backstop."""
    r = _load_resolved()
    deleted: List[str] = []
    for item_type, id_key, name_key in (
        ("environments", "environment_id", "environment_name"),
        ("lakehouses", "lakehouse_id", "lakehouse_name"),
    ):
        iid = str(r.get(id_key, "") or "")
        if iid and _delete_item(item_type, iid):
            deleted.append(f"{item_type}/{r.get(name_key, iid)}")
    try:
        if os.path.exists(_RESOLVED_PATH):
            os.unlink(_RESOLVED_PATH)
    except OSError:
        pass
    global _resolved_cache
    _resolved_cache = {}
    logger.info("[fabric] torn down: %s", deleted)
    return {"status": "ok", "deleted": deleted}


# ---------------------------------------------------------------------------
# OneLake path helpers
# ---------------------------------------------------------------------------

def onelake_dfs_url(rel_path: str, lakehouse_id: Optional[str] = None) -> str:
    """HTTPS DFS URL for a path under a lakehouse (defaults to the compute one)."""
    lid = lakehouse_id or _compute_lakehouse_id()
    return f"https://{ONELAKE_HOST}/{WORKSPACE_ID}/{lid}/{rel_path.lstrip('/')}"


def onelake_abfss(rel_path: str, lakehouse_id: Optional[str] = None) -> str:
    """ABFSS URI (for Spark ``delta.`...``` reads) for a path under a lakehouse."""
    lid = lakehouse_id or _compute_lakehouse_id()
    return f"abfss://{WORKSPACE_ID}@{ONELAKE_HOST}/{lid}/{rel_path.lstrip('/')}"


def cache_section_abfss(sf: int, batch_num: int, section: str) -> str:
    """ABFSS URI of a cache section (``batch1/<t>``, ``staging/<t>``,
    ``staging_batch<N>/<t>``, ``audit``) — read from the shared CACHE lakehouse."""
    root = batch_cache_root(CACHE_ROOT, sf, batch_num)
    return onelake_abfss(
        f"{root}/{section}", lakehouse_id=resolve_cache_lakehouse()
    )


# ---------------------------------------------------------------------------
# OneLake DFS (marker checks + blow-up)
# ---------------------------------------------------------------------------

def _dfs_exists(rel_path: str, lakehouse_id: Optional[str] = None) -> bool:
    r = requests.head(onelake_dfs_url(rel_path, lakehouse_id), headers=_storage_headers(), timeout=60)
    return r.status_code == 200


def _dfs_put_marker(rel_path: str, lakehouse_id: Optional[str] = None) -> None:
    url = onelake_dfs_url(rel_path, lakehouse_id)
    hdr = _storage_headers()
    requests.put(f"{url}?resource=file", headers=hdr, timeout=60)
    requests.patch(
        f"{url}?action=append&position=0",
        headers={**hdr, "Content-Type": "application/octet-stream"},
        data=b"ok\n", timeout=60,
    )
    requests.patch(
        f"{url}?action=flush&position=3",
        headers=hdr, timeout=60,
    )


def _dfs_delete(rel_path: str, recursive: bool = True, lakehouse_id: Optional[str] = None) -> bool:
    r = requests.delete(
        f"{onelake_dfs_url(rel_path, lakehouse_id)}?recursive={'true' if recursive else 'false'}",
        headers=_storage_headers(), timeout=_HTTP_TIMEOUT,
    )
    return r.status_code in (200, 202, 404)


def _dfs_list(rel_dir: str, lakehouse_id: Optional[str] = None) -> List[str]:
    """List immediate paths under a lakehouse directory (relative paths)."""
    lid = lakehouse_id or _compute_lakehouse_id()
    url = f"https://{ONELAKE_HOST}/{WORKSPACE_ID}"
    params = {
        "resource": "filesystem",
        "recursive": "false",
        "directory": f"{lid}/{rel_dir.lstrip('/')}",
    }
    r = requests.get(url, headers=_storage_headers(), params=params, timeout=_HTTP_TIMEOUT)
    if r.status_code != 200:
        return []
    return [p["name"] for p in (r.json().get("paths") or []) if p.get("name")]


def list_storage_paths(deadline: Optional[float] = None) -> List[Dict[str, object]]:
    """List durable files owned by the active compute lakehouse.

    The persistent shared raw cache is deliberately excluded: it is reusable
    benchmark input, not storage created by this experiment.  ADLS pagination
    and the caller's collection deadline are honored for every root.
    """
    lid = _compute_lakehouse_id()
    base_url = f"https://{ONELAKE_HOST}/{WORKSPACE_ID}"
    headers = _storage_headers(deadline)
    out: List[Dict[str, object]] = []
    for rel_dir in ("Tables", WAREHOUSE_ROOT, STATE_ROOT):
        continuation: Optional[str] = None
        for _ in range(1000):
            remaining = _HTTP_TIMEOUT if deadline is None else deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Fabric storage collection deadline exceeded")
            params = {
                "resource": "filesystem",
                "recursive": "true",
                "directory": f"{lid}/{rel_dir.lstrip('/')}",
            }
            if continuation:
                params["continuation"] = continuation
            response = requests.get(
                base_url,
                headers=headers,
                params=params,
                timeout=max(1.0, min(float(_HTTP_TIMEOUT), remaining)),
            )
            if response.status_code == 404:
                break
            response.raise_for_status()
            for record in (response.json().get("paths") or []):
                if record.get("isDirectory") in (True, "true", "True"):
                    continue
                name = str(record.get("name") or "")
                prefix = f"{lid}/"
                if name.startswith(prefix):
                    name = name[len(prefix):]
                if name:
                    out.append({
                        "path": name,
                        "bytes": int(record.get("contentLength") or 0),
                    })
            continuation = response.headers.get("x-ms-continuation")
            if not continuation:
                break
        else:
            raise RuntimeError(f"Fabric storage listing exceeded page cap for {rel_dir}")
    return out


# ---------------------------------------------------------------------------
# azcopy — stage local Delta dirs into the OneLake Files cache
# ---------------------------------------------------------------------------

# azcopy's AZCLI auto-login spawns its own `az account get-access-token` for the
# storage audience. When two Fabric stacks run in parallel and mint tokens against
# the shared relay/UAMI at once, that az can be killed mid-flight ("failed to
# perform Auto-login: AzureCLICredential: ... killed"). Pre-warm the storage token
# and retry the transient auth failures with backoff.
_AZCOPY_TRANSIENT_SIGNATURES = (
    "failed to perform auto-login",
    "azureclicredential",
    "please authenticate",
    "killed",
    "context deadline exceeded",
    "authenticationfailed",
    "no cached token",
)


def _warm_storage_token() -> None:
    """Synchronously mint the storage token so azcopy's internal AZCLI login is a
    warm cache hit (<1s) rather than a cold relay round-trip that can be killed."""
    try:
        subprocess.run(
            ["az", "account", "get-access-token", "--resource", STORAGE_RESOURCE, "-o", "none"],
            capture_output=True, text=True, timeout=60,
        )
    except Exception as e:  # noqa: BLE001 — a warm failure just means the retry pays the cold path
        logger.warning("[fabric] storage token warm failed: %s", e)


def _run_azcopy(args: List[str], what: str, attempts: int = 5, backoff_s: float = 6.0) -> None:
    """Run ``azcopy <args>`` with AZCLI auto-login, retrying on ANY failure with
    exponential backoff. azcopy failures are almost always transient — either an
    auth token killed under parallel token contention (see
    ``_AZCOPY_TRANSIENT_SIGNATURES``) or a single flaky file transfer out of many
    (more likely at high SF) — and the copy is idempotent (``--overwrite=true``),
    so a whole-command retry is safe."""
    env = {
        **os.environ,
        "AZCOPY_AUTO_LOGIN_TYPE": "AZCLI",
        "AZCOPY_LOG_LOCATION": "/tmp/azcopy",
        "AZCOPY_JOB_PLAN_LOCATION": "/tmp/azcopy",
    }
    last = ""
    delay = backoff_s
    for attempt in range(1, attempts + 1):
        ensure_az_login()
        _warm_storage_token()
        res = subprocess.run(["azcopy", *args], capture_output=True, text=True, env=env)
        if res.returncode == 0:
            return
        last = res.stderr or res.stdout or ""
        if attempt == attempts:
            break
        kind = "auth" if any(s in last.lower() for s in _AZCOPY_TRANSIENT_SIGNATURES) else "transfer"
        logger.warning(
            "[fabric] azcopy %s %s failure (attempt %d/%d), retrying in %.0fs: %s",
            what, kind, attempt, attempts, delay, last[-200:],
        )
        time.sleep(delay)
        delay = min(delay * 2, 60)
    raise RuntimeError(f"azcopy {what} failed: {last[-500:]}")


def _azcopy(local_dir: Path, dest_rel: str, lakehouse_id: Optional[str] = None) -> int:
    """Copy a local Delta dir into the lakehouse so it lands exactly at
    ``dest_rel`` (whose last segment must equal ``local_dir.name``). Returns
    file count.

    azcopy places a source dir *under* the destination as ``<dest>/<dirname>``,
    so we target the PARENT of ``dest_rel`` and let azcopy recreate the leaf.
    """
    files = sum(1 for _ in local_dir.rglob("*") if _.is_file())
    parent_url = onelake_dfs_url(dest_rel, lakehouse_id).rsplit("/", 1)[0]
    _run_azcopy(
        ["copy", str(local_dir), f"{parent_url}/",
         "--recursive", "--overwrite=true", "--output-type=text",
         "--trusted-microsoft-suffixes=*.fabric.microsoft.com;*.dfs.fabric.microsoft.com"],
        what=f"{local_dir} -> {dest_rel}",
    )
    return files


def _all_init_sections() -> List[Tuple[str, str]]:
    """(local_subdir, cache_section) pairs for the batch-1 seed."""
    out = [(f"batch1/{t}", f"batch1/{t}") for t in BATCH1_TABLES]
    out += [(f"staging/{t}", f"staging/{t}") for t in STAGING_TABLES]
    out.append(("audit", "audit"))
    return out


def seed_cache_init(sf: int) -> dict:
    """Idempotently stage the batch-1 + initial-staging + audit Delta dirs into
    a percentage-keyed path in the shared CACHE lakehouse. Marker-guarded."""
    cache_lh = resolve_cache_lakehouse()
    root = batch_cache_root(CACHE_ROOT, sf, 1)
    marker = f"{root}/_UPLOADED_INIT"
    if _dfs_exists(marker, lakehouse_id=cache_lh):
        return {"status": "ok", "files_uploaded": 0, "already_seeded": True}
    total = 0
    for subdir, section in _all_init_sections():
        local = Path(RAW_DELTA_DIR) / subdir
        if not local.is_dir():
            continue
        total += _azcopy(
            local, f"{root}/{section}", lakehouse_id=cache_lh
        )
    _dfs_put_marker(marker, lakehouse_id=cache_lh)
    return {"status": "ok", "files_uploaded": total, "already_seeded": False}


def seed_cache_batch(sf: int, batch_num: int) -> dict:
    """Idempotently stage the per-batch staging Delta into the shared CACHE
    lakehouse's percentage-keyed ``staging_batch<N>/``. Marker-guarded."""
    if batch_num not in (2, 3):
        raise ValueError(f"seed_cache_batch supports batch 2/3, got {batch_num}")
    cache_lh = resolve_cache_lakehouse()
    root = batch_cache_root(CACHE_ROOT, sf, batch_num)
    marker = f"{root}/_UPLOADED_BATCH{batch_num}"
    if _dfs_exists(marker, lakehouse_id=cache_lh):
        return {"status": "ok", "files_uploaded": 0, "already_seeded": True}
    total = 0
    for t in STAGING_TABLES:
        local_batch = Path(RAW_DELTA_DIR) / f"batch{batch_num}" / t
        local = local_batch if local_batch.is_dir() else Path(RAW_DELTA_DIR) / "staging" / t
        if not local.is_dir():
            continue
        total += _azcopy(
            local,
            f"{root}/staging_batch{batch_num}/{t}",
            lakehouse_id=cache_lh,
        )
    _dfs_put_marker(marker, lakehouse_id=cache_lh)
    return {"status": "ok", "files_uploaded": total, "already_seeded": False}


# ---------------------------------------------------------------------------
# Blow-up — drop lakehouse Tables/ + openivm state between runs
# ---------------------------------------------------------------------------

def cleanup_tables_and_state() -> dict:
    """Delete every table under ``Tables/``, the openivm RocksDB state mirror
    (``Files/_openivm``) and the openivm MV Delta warehouse
    (``Files/_ivm-warehouse``) so the next experiment starts clean. The source
    cache (``Files/_shared_cache``) is preserved (re-used across same-SF runs)."""
    dropped_tables = 0
    for path in _dfs_list("Tables"):
        # path is "<lakehouseId>/Tables/<table>"; keep the tail after Tables/.
        tail = path.split("Tables/", 1)[-1]
        if tail and _dfs_delete(f"Tables/{tail}"):
            dropped_tables += 1
    state_deleted = _dfs_delete(STATE_ROOT)
    warehouse_deleted = _dfs_delete(WAREHOUSE_ROOT)
    return {
        "status": "ok",
        "dropped_tables": dropped_tables,
        "state_deleted": state_deleted,
        "warehouse_deleted": warehouse_deleted,
    }


def cleanup_cache_for_sf(sf: int) -> dict:
    ok = _dfs_delete(f"{CACHE_ROOT}/sf={sf}", lakehouse_id=resolve_cache_lakehouse())
    return {"status": "ok", "sf": sf, "deleted": ok}


# ---------------------------------------------------------------------------
# Environment "35" — openivm JAR + Spark config
# ---------------------------------------------------------------------------

def default_openivm_spark_properties() -> Dict[str, str]:
    """Fresh Spark-config set pushed to the openivm Fabric Environment. Mirrors
    the local spark-openivm ``spark-defaults.conf.tmpl`` openivm keys, adapted
    for Fabric:

    * RocksDB state lives on LOCAL driver disk (``spark.openivm.statePath``) —
      Fabric Livy sessions have no ``/lakehouse/default`` FUSE mount and RocksDB
      needs a real POSIX filesystem;
    * ``spark.openivm.stateSync.uri`` mirrors that local state into OneLake
      ``Files/_openivm`` (via the Hadoop FS) so it survives session recycling;
    * ``spark.sql.warehouse.dir`` is an OneLake path so MV/staging Delta land in
      durable object storage.

    The DuckDB CLI + extension the compile bridge needs are baked into the
    openivm JAR (see MaterializedViewCommands.extractBundledAssets) — no
    ``compiler.assetsUri`` / OneLake staging is required.
    """
    def _b(env_key: str) -> str:
        return "true" if os.environ.get(env_key, "0") == "1" else "false"

    add_opens = " ".join(
        f"--add-opens=java.base/{m}=ALL-UNNAMED"
        for m in (
            "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io",
            "java.net", "java.nio", "java.util", "java.util.concurrent",
            "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs",
            "sun.security.action", "sun.util.calendar",
        )
    ) + " --add-exports=java.base/sun.nio.ch=ALL-UNNAMED"

    state_sync_uri = onelake_abfss(STATE_ROOT)
    warehouse_uri = onelake_abfss(WAREHOUSE_ROOT)

    return {
        "spark.sql.extensions": (
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.openivm.spark.OpenIvmSparkExtensions"
        ),
        "spark.openivm.enabled": "true",
        "spark.openivm.changeFeed.mode": "cdf",
        # RocksDB on local driver disk; mirrored to OneLake for durability.
        "spark.openivm.statePath": "/tmp/openivm-state",
        "spark.openivm.stateSync.uri": state_sync_uri,
        # MV / staging Delta land in durable OneLake, not the ephemeral driver.
        "spark.sql.warehouse.dir": warehouse_uri,
        "spark.openivm.rocksdb.multiProcess": "false",
        "spark.openivm.delta.enableDeletionVectors": "true",
        "spark.openivm.refresh.semiJoinPrune.enabled": "true",
        "spark.openivm.refresh.scd2RangeAccel.enabled": "true",
        "spark.databricks.delta.schema.autoMerge.enabled": "true",
        "spark.openivm.profile.refresh": _b("OPENIVM_PROFILE_REFRESH"),
        "spark.openivm.queryLog.enabled": _b("OPENIVM_QUERY_LOG"),
        "spark.driver.extraJavaOptions": add_opens,
        "spark.executor.extraJavaOptions": add_opens,
    }


def _env_base() -> str:
    return f"{FABRIC_API_BASE}/v1/workspaces/{WORKSPACE_ID}/environments/{_compute_environment_id()}"


def publish_empty_environment() -> dict:
    """Publish the freshly-created (empty) compute Environment so the baseline
    Livy session can attach it. A brand-new env may have nothing to publish; a
    non-2xx here is treated as already-usable (no-op)."""
    pub = _fabric_req("POST", f"{_env_base()}/staging/publish", headers=_fabric_headers())
    if pub.status_code in (200, 202):
        return {"publish_state": _poll_publish()}
    logger.info(
        "[fabric] empty env publish HTTP %s (treating as no-op): %s",
        pub.status_code, pub.text[:200],
    )
    return {"publish_state": "skipped"}


def _staged_library_names(hdr: Dict[str, str]) -> List[str]:
    r = _fabric_req("GET", f"{_env_base()}/staging/libraries", headers=hdr)
    if r.status_code != 200:
        return []
    names: List[str] = []
    cl = (r.json() or {}).get("customLibraries") or {}
    for v in cl.values():
        if isinstance(v, list):
            names += [x for x in v if isinstance(x, str)]
    return names


def _delete_staged_libraries(hdr: Dict[str, str]) -> List[str]:
    names = _staged_library_names(hdr)
    for name in names:
        _fabric_req(
            "DELETE", f"{_env_base()}/staging/libraries",
            headers=hdr, params={"libraryToDelete": name},
        )
    return names


def _poll_publish(timeout_s: int = 1800, interval_s: int = 15) -> str:
    import time
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        r = _fabric_req("GET", _env_base(), headers=_fabric_headers())
        r.raise_for_status()
        state = (
            (r.json().get("properties") or {})
            .get("publishDetails", {})
            .get("state", "")
        ).lower()
        if state == "success":
            return state
        if state in ("failed", "cancelled"):
            raise RuntimeError(f"Environment publish {state}")
        time.sleep(interval_s)
    raise RuntimeError("Environment publish timed out")


def upload_jar_to_lib() -> str:
    """azcopy the openivm assembly JAR into OneLake ``Files/_openivm-lib`` so the
    dbt profile's ``spark.jars`` can put it on the Fabric Livy DRIVER classpath.

    openivm runs entirely driver-side (parser/analyzer/optimizer/strategy rewrite
    + RocksDB catalog + the DuckDB compile bridge, whose native binaries are now
    BAKED INTO the JAR). Fabric Environment libraries reach executors only, and
    executors run vanilla Spark/Delta — so no separate DuckDB upload and no
    Environment JAR library are needed. Overwrites any prior copy. Returns the
    JAR's abfss path."""
    jar = Path(OPENIVM_JAR_PATH)
    if not jar.exists():
        raise RuntimeError(f"openivm JAR not found at {OPENIVM_JAR_PATH}")
    dest = onelake_dfs_url("Files/_openivm-lib")
    _run_azcopy(
        ["copy", str(jar), f"{dest}/", "--overwrite=true", "--output-type=text",
         "--trusted-microsoft-suffixes=*.fabric.microsoft.com;*.dfs.fabric.microsoft.com"],
        what=f"{jar.name} -> lib",
    )
    return onelake_abfss(f"Files/_openivm-lib/{jar.name}")


def refresh_environment(spark_properties: Optional[Dict[str, str]] = None) -> dict:
    """Refresh the openivm Fabric Environment: remove any stale custom library,
    stage the freshly-built openivm JAR into OneLake (for the DRIVER classpath),
    PATCH a fresh Spark config, publish, and block until published.

    The JAR is deliberately NOT uploaded as an Environment library: Fabric
    attaches those to EXECUTORS only, and openivm runs entirely driver-side
    (plan rewrite + RocksDB catalog + DuckDB compile bridge) — executors run
    vanilla Spark/Delta. The driver loads the JAR via the profile's
    ``spark.jars`` (the OneLake copy staged here)."""
    _compute_environment_id()  # raises if not provisioned
    jar = Path(OPENIVM_JAR_PATH)
    if not jar.exists():
        raise RuntimeError(f"openivm JAR not found at {OPENIVM_JAR_PATH}")

    import hashlib
    sha = hashlib.sha256(jar.read_bytes()).hexdigest()
    logger.info(
        "[fabric] openivm JAR %s sha256=%s size=%d",
        jar.name, sha, jar.stat().st_size,
    )

    hdr = _fabric_headers()
    base = _env_base()

    # Remove any stale custom library (e.g. a JAR left by the earlier
    # executor-upload design) so the published env carries config only.
    deleted = _delete_staged_libraries(hdr)

    # Stage the fresh JAR to OneLake for the profile's spark.jars (driver
    # classpath). azcopy --overwrite=true replaces any prior copy.
    jar_abfss = upload_jar_to_lib()

    props = spark_properties or default_openivm_spark_properties()
    patch = _fabric_req(
        "PATCH",
        f"{base}/staging/sparkcompute",
        headers={**hdr, "Content-Type": "application/json"},
        json={"sparkProperties": props},
    )
    if patch.status_code not in (200, 202):
        raise RuntimeError(
            f"sparkcompute PATCH failed: HTTP {patch.status_code} {patch.text[:300]}"
        )

    pub = _fabric_req("POST", f"{base}/staging/publish", headers=hdr)
    if pub.status_code not in (200, 202):
        raise RuntimeError(f"publish failed: HTTP {pub.status_code} {pub.text[:300]}")

    state = _poll_publish()
    remaining = _staged_library_names(hdr)
    if remaining:
        logger.warning(
            "[fabric] Environment still lists custom libraries after publish: %s",
            remaining,
        )
    return {
        "status": "ok",
        "deleted_libraries": deleted,
        "remaining_libraries": remaining,
        "jar": jar.name,
        "jar_sha256": sha,
        "jar_abfss": jar_abfss,
        "spark_properties": len(props),
        "publish_state": state,
    }
