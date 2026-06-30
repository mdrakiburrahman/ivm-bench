"""databricks-enzyme source-table management (per-experiment isolated).

Each OAT experiment runs in its own **shared-nothing** namespace inside the
Unity Catalog so multiple experiments — sequential or concurrent — can
share the same workspace without stomping each other:

  * The orchestrator mints a microsecond-resolution ``experiment_id`` per
    OAT experiment row and propagates it as the ``DATABRICKS_EXPERIMENT_ID``
    env var into the dbt-server container.
  * Every Databricks artifact created during an experiment lives under one
    of these five per-experiment schemas (all dropped CASCADE at the end of
    the experiment):

        <catalog>.exp_<ts>_data        # source-table CTAS-from-cache
        <catalog>.exp_<ts>_bronze      # dbt bronze MVs
        <catalog>.exp_<ts>_silver      # dbt silver MVs
        <catalog>.exp_<ts>_gold        # dbt gold MVs
        <catalog>.exp_<ts>_work        # dbt work (ephemeral) artifacts

  * Raw TPC-DI Delta files are uploaded ONCE per SF into a persistent
    shared **read-only** cache volume:

        /Volumes/<catalog>/_shared_cache/tpcdi_raw_cache/sf=<N>/{
            batch1/<table>/...,            (init only)
            staging_batch1/<table>/...,
            staging_batch2/<table>/...,
            staging_batch3/<table>/...,
            audit/...,
        }

    Idempotent via per-section ``_UPLOADED`` marker files. Per-experiment
    source tables are then created server-side as **CTAS managed Delta**
    from those cache paths — no client-side re-upload past the first run
    at a given SF.

  * The VIEW-over-delta-path strategy is no longer probed: Enzyme requires
    row-tracking on its source tables, which only managed Delta tables
    have, so we always CTAS.

  * Crash-recovery: at the start of every experiment the orchestrator
    calls ``sweep_stale_schemas`` which lists every ``exp_*`` schema in
    the catalog, parses the embedded microsecond timestamp, and drops
    CASCADE anything older than ``OAT_STALE_SCHEMA_MAX_AGE_SECONDS``
    (default ``86400`` = 1 day). A freshly-minted experiment ID is
    always < 1 day old, so the active experiment is never targeted.

The benchmark-server's engine runner calls these routes:

  POST /sources/databricks-enzyme/sweep-stale            (start of every exp)
  POST /sources/databricks-enzyme/init/<sf>              (before batch 1)
  POST /sources/databricks-enzyme/append/<batch_num>/<sf> (before batch 2/3)
  POST /sources/databricks-enzyme/cleanup-schema         (end of every exp)
  POST /sources/databricks-enzyme/cleanup-all            (end of sweep)
"""

from __future__ import annotations

import io
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)

RAW_DELTA_DIR = os.environ.get("RAW_DELTA_DIR", "/data/raw/delta")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "ivmbenchdbrx")

# Shared READ-ONLY cache for raw TPC-DI Delta files. ONE schema + ONE
# volume per workspace, idempotently populated per-SF, then read by every
# subsequent experiment's CTAS source-table creation. Never cleaned up
# automatically — keep your raw data around to avoid expensive re-uploads.
CACHE_SCHEMA = os.environ.get("DATABRICKS_CACHE_SCHEMA", "_shared_cache")
CACHE_VOLUME = os.environ.get("DATABRICKS_CACHE_VOLUME", "tpcdi_raw_cache")

# Per-experiment schemas all share the literal prefix ``exp_<ts>_``. This
# is the SOLE convention the sweeper uses to discover stale schemas; do
# not change it without also updating the regex below.
_EXP_PREFIX = "exp_"
_EXP_SCHEMA_PATTERN = re.compile(r"^exp_(\d+)_([A-Za-z0-9_]+)$")
_EXP_LAYER_NAMES = ("data", "bronze", "silver", "gold", "work")

# Stale-schema retention (seconds). 1 day by default. Sweeper drops any
# `exp_<ts>_*` schemas whose timestamp is older than this. Overridable
# per environment for short-lived CI runs.
_STALE_MAX_AGE_S = int(
    os.environ.get("DATABRICKS_STALE_SCHEMA_MAX_AGE_SECONDS", str(24 * 60 * 60))
)


# ---------------------------------------------------------------------------
# Per-experiment ID + schema helpers (lazy — env is read at call time so
# the module loads cleanly in non-databricks-enzyme containers, which all
# share the same dbt-server image but never call into this service).
# ---------------------------------------------------------------------------


def _experiment_id() -> str:
    """Return the active per-experiment microsecond timestamp.

    Raises ``RuntimeError`` if ``DATABRICKS_EXPERIMENT_ID`` is unset or
    malformed — every public function in this module needs it to compute
    the per-experiment schema names, so we refuse to operate without it
    rather than silently fall back to a shared namespace.
    """
    eid = os.environ.get("DATABRICKS_EXPERIMENT_ID", "").strip()
    if not eid:
        raise RuntimeError(
            "DATABRICKS_EXPERIMENT_ID env var is required for databricks-enzyme "
            "operations (per-experiment isolation). The benchmark-server "
            "orchestrator should set this when starting the engine container."
        )
    if not eid.isdigit():
        raise RuntimeError(
            f"DATABRICKS_EXPERIMENT_ID must be a microsecond integer; got {eid!r}"
        )
    return eid


def _schema_for(layer: str) -> str:
    return f"{_EXP_PREFIX}{_experiment_id()}_{layer}"


def data_schema() -> str:
    """Schema holding the per-experiment CTAS source tables."""
    return _schema_for("data")


def bronze_schema() -> str: return _schema_for("bronze")
def silver_schema() -> str: return _schema_for("silver")
def gold_schema() -> str: return _schema_for("gold")
def work_schema() -> str: return _schema_for("work")


def layer_schemas() -> List[str]:
    """The 4 per-experiment layer schemas dbt-databricks materialises into."""
    return [bronze_schema(), silver_schema(), gold_schema(), work_schema()]


def all_experiment_schemas() -> List[str]:
    """All 5 per-experiment schemas (data + 4 layers). Used by cleanup."""
    return [data_schema(), *layer_schemas()]


# ---------------------------------------------------------------------------
# Back-compat shims — older code imports `DBT_SCHEMA`, `SOURCE_SCHEMA`,
# `LAYER_SCHEMAS` directly. Keep the names but make them callable so any
# caller that did `src.DBT_SCHEMA` (string) still works without code changes.
# All such call sites have been migrated to the function forms above; these
# shims exist purely to surface a clear error if anything still references
# the old constants.
# ---------------------------------------------------------------------------


def _deprecated_constant_error(name: str) -> str:
    return (
        f"databricks_enzyme_sources.{name} was removed when per-experiment "
        f"isolation landed. Use the function form instead "
        f"(data_schema / bronze_schema / silver_schema / gold_schema / "
        f"work_schema / layer_schemas / all_experiment_schemas)."
    )


def __getattr__(name: str):  # PEP 562 module __getattr__
    if name in ("DBT_SCHEMA", "SOURCE_SCHEMA", "VOLUME", "LAYER_SCHEMAS"):
        raise AttributeError(_deprecated_constant_error(name))
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

BATCH1_TABLES: List[str] = [
    "customer_mgmt",
    "date",
    "finwire",
    "hr",
    "industry",
    "status_type",
    "tax_rate",
    "trade_history",
    "trade_type",
]

STAGING_TABLES: List[str] = [
    "cash_transaction",
    "daily_market",
    "holding_history",
    "prospect",
    "trade",
    "watch_history",
    "account",
    "customer",
    "batch_date",
]


def _all_init_tables() -> List[Tuple[str, str, str]]:
    """Yield (group, local_subdir, source_table_name) for the batch1 load.

    The ``staging`` group represents the *batch 1 slice* of staging tables
    (in the local Delta layout it's the full ``staging/`` dir at init
    time, since batch 2/3 haven't appended yet).  Subsequent batches land
    in ``staging_batch<N>/`` directories in the shared cache and are
    INSERTed into the per-experiment ``exp_<ts>_data.staging_<t>`` tables.
    """
    out: List[Tuple[str, str, str]] = []
    for t in BATCH1_TABLES:
        out.append(("batch1", f"batch1/{t}", f"batch1_{t}"))
    for t in STAGING_TABLES:
        out.append(("staging", f"staging/{t}", f"staging_{t}"))
    out.append(("audit", "audit", "audit"))
    return out


# ---------------------------------------------------------------------------
# Volume / path helpers
# ---------------------------------------------------------------------------


def _cache_volume_root() -> str:
    """Persistent, per-workspace shared cache volume root (read-only after
    first seed at each SF)."""
    return f"/Volumes/{CATALOG}/{CACHE_SCHEMA}/{CACHE_VOLUME}"


def _cache_sf_root(sf: int) -> str:
    return f"{_cache_volume_root()}/sf={sf}"


def _cache_init_marker(sf: int) -> str:
    return f"{_cache_sf_root(sf)}/_UPLOADED_INIT"


def _cache_batch_marker(sf: int, batch_num: int) -> str:
    return f"{_cache_sf_root(sf)}/_UPLOADED_BATCH{batch_num}"


def _cache_section_path(sf: int, section: str) -> str:
    """Cache subpath for an ``_all_init_tables`` section. ``section`` is
    one of ``batch1/<t>``, ``staging/<t>``, ``audit``, or
    ``staging_batch<N>/<t>``."""
    return f"{_cache_sf_root(sf)}/{section}"


def _databricks_config() -> Config:
    host = os.environ["DATABRICKS_HOST"]
    if "://" not in host:
        host = f"https://{host}"
    return Config(
        host=host,
        client_id=os.environ["DATABRICKS_SPN_CLIENT_ID"],
        client_secret=os.environ["DATABRICKS_SPN_CLIENT_SECRET"],
    )


def _server_hostname() -> str:
    host = os.environ["DATABRICKS_HOST"]
    parsed = urlparse(host if "://" in host else f"https://{host}")
    return parsed.netloc or parsed.path


def _http_path() -> str:
    return os.environ["DATABRICKS_SQL_HTTP_PATH"]


def _credentials_provider():
    return oauth_service_principal(_databricks_config())


_conn_lock = threading.Lock()
_conn = None


def _get_connection():
    global _conn
    with _conn_lock:
        if _conn is None:
            logger.info(
                "[databricks-enzyme] opening Databricks SQL connection host=%s",
                _server_hostname(),
            )
            _conn = dbsql.connect(
                server_hostname=_server_hostname(),
                http_path=_http_path(),
                credentials_provider=_credentials_provider,
            )
        return _conn


def _drop_connection():
    global _conn
    with _conn_lock:
        if _conn is not None:
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None


def _execute(sql_text: str) -> Optional[pd.DataFrame]:
    conn = _get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql_text)
        if cursor.description is None:
            return None
        try:
            return cursor.fetchall_arrow().to_pandas()
        except Exception:
            rows = cursor.fetchall()
            cols = [d[0] for d in (cursor.description or [])]
            return pd.DataFrame.from_records(rows, columns=cols)
    finally:
        cursor.close()


def _workspace_client() -> WorkspaceClient:
    return WorkspaceClient(config=_databricks_config())


# ---------------------------------------------------------------------------
# Volume / Files helpers
# ---------------------------------------------------------------------------


def _file_exists(ws: WorkspaceClient, path: str) -> bool:
    try:
        ws.files.get_metadata(file_path=path)
        return True
    except NotFound:
        return False
    except Exception:
        return False


def _upload_bytes(ws: WorkspaceClient, path: str, payload: bytes) -> None:
    ws.files.upload(file_path=path, contents=io.BytesIO(payload), overwrite=True)


def _upload_dir(
    ws: WorkspaceClient, local_dir: Path, remote_dir: str
) -> int:
    if not local_dir.is_dir():
        return 0
    count = 0
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_dir).as_posix()
        remote = f"{remote_dir.rstrip('/')}/{rel}"
        with path.open("rb") as fh:
            ws.files.upload(file_path=remote, contents=fh, overwrite=True)
        count += 1
    return count


def _sync_dir(
    ws: WorkspaceClient, local_dir: Path, remote_dir: str
) -> Tuple[int, int]:
    """Upload only files that are missing or whose size differs remotely.

    Cheapest "did anything new appear?" check that doesn't require hashing
    every file. Returns (files_uploaded, files_skipped).
    """
    if not local_dir.is_dir():
        return (0, 0)
    uploaded = 0
    skipped = 0
    for path in sorted(local_dir.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(local_dir).as_posix()
        remote = f"{remote_dir.rstrip('/')}/{rel}"
        local_size = path.stat().st_size
        remote_size: Optional[int] = None
        try:
            meta = ws.files.get_metadata(file_path=remote)
            remote_size = getattr(meta, "content_length", None) or getattr(
                meta, "size", None
            )
        except NotFound:
            remote_size = None
        except Exception:
            remote_size = None
        if remote_size == local_size:
            skipped += 1
            continue
        with path.open("rb") as fh:
            ws.files.upload(file_path=remote, contents=fh, overwrite=True)
        uploaded += 1
    return (uploaded, skipped)


def _delete_volume_dir(ws: WorkspaceClient, path: str) -> None:
    """Best-effort recursive delete of a UC Volume directory."""
    try:
        entries = list(ws.files.list_directory_contents(directory_path=path))
    except NotFound:
        return
    except Exception:
        return
    for entry in entries:
        entry_path = getattr(entry, "path", None)
        is_dir = getattr(entry, "is_directory", False)
        if entry_path is None:
            continue
        if is_dir:
            _delete_volume_dir(ws, entry_path)
        else:
            try:
                ws.files.delete(file_path=entry_path)
            except Exception:
                pass
    try:
        ws.files.delete_directory(directory_path=path)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Schema / cache lifecycle
# ---------------------------------------------------------------------------


def _ensure_cache_schema(ws: WorkspaceClient) -> None:
    """Idempotent: create the shared cache schema + volume if they don't
    already exist. Safe to call from every experiment."""
    _execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{CACHE_SCHEMA}`")
    _execute(
        f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{CACHE_SCHEMA}`.`{CACHE_VOLUME}`"
    )


def _ensure_experiment_schemas() -> None:
    """Create all 5 per-experiment schemas for the active experiment_id.

    These are the schemas dbt-databricks materialises MVs into PLUS the
    ``exp_<ts>_data`` schema that holds the CTAS source tables. The
    materialized_view materialization needs them to exist before
    ``CREATE MATERIALIZED VIEW`` runs (dbt-databricks does not
    auto-create custom schemas for MV materializations), and the
    pre-flight stub-VIEW registration in `databricks_enzyme_explain`
    also needs them to exist before its `CREATE OR REPLACE VIEW
    exp_<ts>_<layer>.<model>` calls (otherwise it warns
    `SCHEMA_NOT_FOUND` per model).
    """
    for schema in all_experiment_schemas():
        _execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{schema}`")


def sweep_stale_schemas(
    max_age_seconds: Optional[int] = None,
) -> dict:
    """Drop every ``exp_<microsec_ts>_<layer>`` schema older than
    ``max_age_seconds`` (default = 1 day).

    Discovery uses ``SHOW SCHEMAS LIKE 'exp_*'`` (cheap, no
    information_schema dependency). For each matching schema name we
    parse the ``exp_(\\d+)_<layer>`` form, group by timestamp, and drop
    CASCADE every group whose timestamp is older than the cutoff.

    Crash-safety: a freshly-minted experiment ID is always < 1 day old
    so the active experiment is never targeted. ``DROP SCHEMA IF EXISTS
    ... CASCADE`` is idempotent so concurrent sweepers can't error out
    on each other.

    Returns ``{status, scanned, kept, dropped, errors, cutoff_age_s}``.
    """
    cutoff_age = max_age_seconds if max_age_seconds is not None else _STALE_MAX_AGE_S
    now_us = int(time.time() * 1_000_000)
    cutoff_us = now_us - int(cutoff_age) * 1_000_000

    scanned: List[str] = []
    by_ts: Dict[int, List[str]] = {}
    try:
        df = _execute(
            f"SHOW SCHEMAS IN `{CATALOG}` LIKE '{_EXP_PREFIX}*'"
        )
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/sweep] SHOW SCHEMAS LIKE '%s*' failed: %s",
            _EXP_PREFIX, exc,
        )
        return {
            "status": "error",
            "scanned": 0,
            "kept": [],
            "dropped": [],
            "errors": [str(exc)],
            "cutoff_age_s": cutoff_age,
        }

    if df is None or df.empty:
        return {
            "status": "ok",
            "scanned": 0,
            "kept": [],
            "dropped": [],
            "errors": [],
            "cutoff_age_s": cutoff_age,
        }

    # The first column of SHOW SCHEMAS holds the schema name. Different
    # Databricks/Spark versions name it differently (namespace /
    # databaseName / schema_name), so just read column 0 positionally.
    name_col = df.columns[0]
    for raw in df[name_col].tolist():
        if raw is None:
            continue
        name = str(raw).strip()
        if not name:
            continue
        scanned.append(name)
        m = _EXP_SCHEMA_PATTERN.match(name)
        if not m:
            continue
        ts = int(m.group(1))
        by_ts.setdefault(ts, []).append(name)

    kept: List[str] = []
    dropped: List[str] = []
    errors: List[str] = []
    for ts in sorted(by_ts):
        if ts >= cutoff_us:
            kept.extend(by_ts[ts])
            continue
        for schema in sorted(by_ts[ts]):
            try:
                _execute(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{schema}` CASCADE")
                dropped.append(schema)
            except Exception as exc:
                logger.warning(
                    "[databricks-enzyme/sweep] DROP SCHEMA %s failed: %s",
                    schema, exc,
                )
                errors.append(f"{schema}: {exc}")

    logger.info(
        "[databricks-enzyme/sweep] scanned=%d kept=%d dropped=%d errors=%d cutoff_age_s=%d",
        len(scanned), len(kept), len(dropped), len(errors), cutoff_age,
    )
    return {
        "status": "ok" if not errors else "partial",
        "scanned": len(scanned),
        "kept": kept,
        "dropped": dropped,
        "errors": errors,
        "cutoff_age_s": cutoff_age,
    }


# ---------------------------------------------------------------------------
# Shared-cache seeding (idempotent per SF + per batch)
# ---------------------------------------------------------------------------


def _seed_cache_init(ws: WorkspaceClient, sf: int) -> Tuple[int, bool]:
    """Idempotent: upload the batch-1 + initial-staging + audit Delta dirs
    into the shared cache for ``sf``. Returns (files_uploaded, already_seeded).

    The marker file at ``_UPLOADED_INIT`` indicates a successful seed; if
    present, this is a no-op.
    """
    marker = _cache_init_marker(sf)
    if _file_exists(ws, marker):
        return (0, True)

    logger.info(
        "[databricks-enzyme/cache] seeding init sf=%d into %s",
        sf, _cache_sf_root(sf),
    )
    total = 0
    for _group, subdir, tname in _all_init_tables():
        local = Path(RAW_DELTA_DIR) / subdir
        remote = _cache_section_path(sf, subdir)
        n = _upload_dir(ws, local, remote)
        total += n
        logger.info(
            "[databricks-enzyme/cache] uploaded %d files for %s -> %s",
            n, tname, remote,
        )

    _upload_bytes(ws, marker, b"ok\n")
    return (total, False)


def _seed_cache_batch(
    ws: WorkspaceClient, sf: int, batch_num: int,
) -> Tuple[int, bool]:
    """Idempotent: upload the per-batch staging Delta dirs (and any
    optional ``batch<N>/<t>`` per-batch dir) into the shared cache for
    ``sf`` under ``staging_batch<N>/``. Returns (files_uploaded,
    already_seeded).
    """
    if batch_num not in (2, 3):
        raise ValueError(
            f"_seed_cache_batch only supports batch 2 or 3, got {batch_num}"
        )
    marker = _cache_batch_marker(sf, batch_num)
    if _file_exists(ws, marker):
        return (0, True)

    logger.info(
        "[databricks-enzyme/cache] seeding batch=%d sf=%d into %s",
        batch_num, sf, _cache_sf_root(sf),
    )
    total = 0
    for t in STAGING_TABLES:
        # Source for batch-2/3 increments: prefer the per-batch dir if
        # the batch_loader populated one; else use staging/<t> (which
        # for incremental loaders is overwritten between batches).
        local_batch = Path(RAW_DELTA_DIR) / f"batch{batch_num}" / t
        if local_batch.is_dir():
            remote = _cache_section_path(sf, f"staging_batch{batch_num}/{t}")
            n = _upload_dir(ws, local_batch, remote)
            total += n
            continue
        local_staging = Path(RAW_DELTA_DIR) / "staging" / t
        if local_staging.is_dir():
            remote = _cache_section_path(sf, f"staging_batch{batch_num}/{t}")
            n = _upload_dir(ws, local_staging, remote)
            total += n

    _upload_bytes(ws, marker, b"ok\n")
    return (total, False)


def seed_shared_cache(sf: int) -> dict:
    """Public helper: seed init + (optionally) any pre-existing per-batch
    staging dirs for ``sf`` into the cache. Safe to call multiple times.
    """
    ws = _workspace_client()
    _ensure_cache_schema(ws)
    init_files, init_skipped = _seed_cache_init(ws, sf)
    batch_results: Dict[int, dict] = {}
    for bn in (2, 3):
        local_batch_root = Path(RAW_DELTA_DIR) / f"batch{bn}"
        if not local_batch_root.is_dir():
            continue
        files, skipped = _seed_cache_batch(ws, sf, bn)
        batch_results[bn] = {"files_uploaded": files, "already_seeded": skipped}
    return {
        "status": "ok",
        "scale_factor": sf,
        "init": {"files_uploaded": init_files, "already_seeded": init_skipped},
        "batches": batch_results,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_sources(sf: int) -> dict:
    """Prepare a per-experiment source-table set for batch 1.

    Steps:
      1. Ensure shared cache (``_shared_cache.tpcdi_raw_cache``) exists.
      2. Idempotently seed the cache with raw Delta files for ``sf`` if
         the ``_UPLOADED_INIT`` marker is missing (first run at this SF).
      3. Create the 5 per-experiment schemas
         (``exp_<ts>_{data,bronze,silver,gold,work}``).
      4. CTAS every source table in ``exp_<ts>_data`` *server-side* from
         the cache's Delta paths with ``delta.enableRowTracking=true``
         (Enzyme's INCREMENTAL STRICT requires row tracking on sources).

    No client-side data upload happens here past the first run at this
    SF — the cache is hit instead. Idempotent across re-runs.
    """
    ws = _workspace_client()

    _ensure_cache_schema(ws)
    files_uploaded, already_seeded = _seed_cache_init(ws, sf)
    if already_seeded:
        logger.info(
            "[databricks-enzyme] sf=%d cache already seeded; skipping upload", sf,
        )
    else:
        logger.info(
            "[databricks-enzyme] sf=%d cache seeded with %d files", sf, files_uploaded,
        )

    _ensure_experiment_schemas()
    ds = data_schema()

    tables_registered = 0
    for _group, subdir, tname in _all_init_tables():
        remote = _cache_section_path(sf, subdir)
        fq = f"`{CATALOG}`.`{ds}`.`{tname}`"
        # CTAS managed Delta with row tracking + CDF — both required by
        # Enzyme to incrementalize downstream MVs. DROP first because
        # CREATE OR REPLACE TABLE does not always reset table-level
        # TBLPROPERTIES across re-runs.
        _execute(f"DROP TABLE IF EXISTS {fq}")
        _execute(
            f"CREATE TABLE {fq} "
            f"TBLPROPERTIES ('delta.enableRowTracking' = 'true', "
            f"                'delta.enableChangeDataFeed' = 'true') AS "
            f"SELECT * FROM delta.`{remote}`"
        )
        tables_registered += 1

    logger.info(
        "[databricks-enzyme] init complete sf=%d experiment_id=%s "
        "data_schema=%s files_uploaded=%d tables=%d",
        sf, _experiment_id(), ds, files_uploaded, tables_registered,
    )
    return {
        "status": "ok",
        "scale_factor": sf,
        "experiment_id": _experiment_id(),
        "data_schema": ds,
        "strategy": "ctas",
        "files_uploaded": files_uploaded,
        "tables_created": tables_registered,
        "skipped_upload": already_seeded,
    }


def append_sources(batch_num: int, sf: int) -> dict:
    """Per-experiment batch append (batch 2 or 3).

    Steps:
      1. Idempotently seed the per-batch staging dirs into the shared
         cache (``_UPLOADED_BATCH<N>`` marker).
      2. ``INSERT INTO exp_<ts>_data.staging_<t> SELECT * FROM
         delta.\`/Volumes/_shared_cache/.../sf=<N>/staging_batch<N>/<t>\``
         for each staging table — server-side, no client bytes.

    The next dbt-databricks REFRESH on the MVs will see the new rows
    via the standard Delta change-feed path.
    """
    if batch_num not in (2, 3):
        raise ValueError(
            f"append_sources only supports batch 2 or 3, got {batch_num}"
        )

    ws = _workspace_client()
    _ensure_cache_schema(ws)
    files_uploaded, already_seeded = _seed_cache_batch(ws, sf, batch_num)
    if already_seeded:
        logger.info(
            "[databricks-enzyme] batch=%d sf=%d cache already seeded; skipping upload",
            batch_num, sf,
        )
    else:
        logger.info(
            "[databricks-enzyme] batch=%d sf=%d cache seeded with %d files",
            batch_num, sf, files_uploaded,
        )

    ds = data_schema()
    tables_inserted = 0
    for t in STAGING_TABLES:
        # Only INSERT for tables that actually have a per-batch dir in
        # the cache — some staging tables (e.g. ``batch_date``) may not
        # appear in every batch.
        remote = _cache_section_path(sf, f"staging_batch{batch_num}/{t}")
        if not _file_exists(ws, remote):
            # The marker file exists but no per-batch dir for THIS table
            # means there was no new data for it in this batch. Skip.
            continue
        fq = f"`{CATALOG}`.`{ds}`.`staging_{t}`"
        try:
            _execute(
                f"INSERT INTO {fq} "
                f"SELECT * FROM delta.`{remote}`"
            )
            tables_inserted += 1
        except Exception as exc:
            logger.warning(
                "[databricks-enzyme] INSERT INTO %s from %s failed: %s",
                fq, remote, exc,
            )
            raise

    logger.info(
        "[databricks-enzyme] append batch=%d sf=%d experiment_id=%s "
        "files_uploaded=%d tables_inserted=%d",
        batch_num, sf, _experiment_id(), files_uploaded, tables_inserted,
    )
    return {
        "status": "ok",
        "scale_factor": sf,
        "batch_num": batch_num,
        "experiment_id": _experiment_id(),
        "strategy": "ctas",
        "files_uploaded": files_uploaded,
        "files_skipped": 0,
        "tables_appended": tables_inserted,
    }


def cleanup_schemas() -> dict:
    """Drop every per-experiment schema (5 of them) CASCADE so background
    REFRESH on MVs stops accruing Serverless SQL bills.

    Does NOT touch the shared cache (``_shared_cache.tpcdi_raw_cache``)
    or any other workspace state.
    """
    dropped: List[str] = []
    errors: List[str] = []
    try:
        targets = all_experiment_schemas()
    except RuntimeError as exc:
        # No active DATABRICKS_EXPERIMENT_ID — nothing to drop for "this"
        # experiment. Defer to sweep_stale_schemas for crash recovery.
        logger.warning(
            "[databricks-enzyme] cleanup-schema called without experiment_id: %s",
            exc,
        )
        return {"status": "ok", "dropped": [], "errors": [str(exc)]}
    for s in targets:
        try:
            _execute(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{s}` CASCADE")
            dropped.append(s)
        except Exception as exc:
            logger.warning(
                "[databricks-enzyme] DROP SCHEMA %s failed: %s", s, exc,
            )
            errors.append(f"{s}: {exc}")
    return {"status": "ok" if not errors else "partial",
            "dropped": dropped, "errors": errors}


def cleanup_volume_for_sf(sf: int) -> dict:
    """Drop the *cache* subdir for ``sf`` — back-compat hook so old
    orchestrator code that called this when SF changed still has a no-op
    target. Forces a re-seed on the next ``init_sources(sf)``."""
    ws = _workspace_client()
    _delete_volume_dir(ws, _cache_sf_root(sf))
    return {"status": "ok", "scale_factor": sf}


def warmup(max_attempts: int = 6, sleep_s: float = 5.0) -> dict:
    """Warm the Serverless SQL warehouse before the benchmark timer starts.

    Runs `SELECT 1` until it succeeds so the cold-start cost of resuming a
    suspended warehouse is not charged against the engine's measured batch
    latency. Returns the total wall-clock spent warming and the number of
    attempts needed (typically 1 if the warehouse is already running, more
    if it had to be resumed from suspend).
    """
    started = time.time()
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        attempt_started = time.time()
        try:
            df = _execute("SELECT 1 AS warmup_probe")
            elapsed = time.time() - started
            value = None
            if df is not None and not df.empty:
                value = int(df.iloc[0, 0])
            return {
                "status": "ok",
                "attempts": attempt,
                "elapsed_s": round(elapsed, 3),
                "last_attempt_s": round(time.time() - attempt_started, 3),
                "value": value,
            }
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "[databricks-enzyme] warmup attempt %d/%d failed: %s",
                attempt,
                max_attempts,
                exc,
            )
            _drop_connection()
            time.sleep(sleep_s)
    raise RuntimeError(
        f"databricks-enzyme warmup failed after {max_attempts} attempts: {last_exc}"
    )


def cleanup_all() -> dict:
    """End-of-sweep teardown: sweeper-style drop of every ``exp_*`` schema
    in the catalog regardless of age, plus best-effort wipe of every
    ``sf=*`` subdir in the shared cache.

    Use sparingly — this nukes the cache too, so the next experiment
    pays the full re-upload cost.
    """
    # 1) Drop EVERY exp_* schema in the catalog (age=0 cutoff drops all).
    sweep = sweep_stale_schemas(max_age_seconds=0)

    # 2) Best-effort: wipe every sf=* subdir in the shared cache volume.
    ws = _workspace_client()
    cache_root = _cache_volume_root()
    deleted = 0
    try:
        entries = list(
            ws.files.list_directory_contents(directory_path=cache_root)
        )
    except NotFound:
        return {"status": "ok", "sweep": sweep, "cache_subdirs_deleted": 0}
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme] list %s failed: %s", cache_root, exc,
        )
        return {"status": "ok", "sweep": sweep, "cache_subdirs_deleted": 0}
    for entry in entries:
        if getattr(entry, "is_directory", False):
            _delete_volume_dir(ws, entry.path)
            deleted += 1
    return {"status": "ok", "sweep": sweep, "cache_subdirs_deleted": deleted}
