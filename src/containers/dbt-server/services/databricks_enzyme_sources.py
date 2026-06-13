"""databricks-enzyme source-table management.

Routes batch1 init + batch{2,3} append through:

  1) **Files upload** of the local Delta directories
     (`/data/raw/delta/{batch1,staging,audit}/<table>/...`) into the
     Databricks Unity Catalog Volume at
     `/Volumes/<catalog>/<schema>/<volume>/sf=<N>/{batch1,staging,audit}/<table>/`
     via the Databricks SDK Files API (`ws.files.upload`).

  2) **Source-table registration** in `<catalog>.<source_schema>`. Two
     strategies, decided once per `init_sources(sf)` call by probing
     Databricks Enzyme:

       a) **VIEW over delta-path** — `CREATE OR REPLACE VIEW <catalog>.
          <source_schema>.<t> AS SELECT * FROM delta.`<volume_path>` `.
          Cheapest, no double-storage. Used when the Enzyme `EXPLAIN
          CREATE MATERIALIZED VIEW ... REFRESH POLICY INCREMENTAL STRICT
          AS SELECT * FROM <view>` probe accepts a view source.

       b) **CTAS managed Delta** — `CREATE OR REPLACE TABLE <catalog>.
          <source_schema>.<t> ... TBLPROPERTIES(delta.enableRowTracking
          = true) AS SELECT * FROM delta.`<volume_path>` `. Used when
          the probe rejects the VIEW path (row-tracking / change-data-
          feed required by Enzyme is only present on managed Delta
          tables, not external paths).

       The chosen strategy is recorded in a small marker file at
       `/Volumes/.../sf=<N>/_STRATEGY` so `append_sources` knows whether
       to INSERT INTO the managed table or just re-upload new Delta
       files.

  3) **Idempotence** via `/Volumes/.../sf=<N>/_UPLOADED` marker. Once
     written, `init_sources(sf)` short-circuits to a no-op (returns the
     cached strategy) — re-running an experiment at the same SF stays
     cheap.

The benchmark-server's engine runner calls these:

  POST /sources/databricks-enzyme/init/<sf>              (before batch 1)
  POST /sources/databricks-enzyme/append/<batch_num>/<sf> (before batch 2/3)
  POST /sources/databricks-enzyme/cleanup-schema          (start of every exp)
  POST /sources/databricks-enzyme/cleanup-volume/<sf>     (when SF changes)
  POST /sources/databricks-enzyme/cleanup-all             (end of sweep)
"""

from __future__ import annotations

import io
import logging
import os
import threading
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)

RAW_DELTA_DIR = os.environ.get("RAW_DELTA_DIR", "/data/raw/delta")
CATALOG = os.environ.get("DATABRICKS_CATALOG", "ivmbenchdbrx")
DBT_SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "tpcdi_bench")
SOURCE_SCHEMA = os.environ.get("DATABRICKS_SOURCE_SCHEMA", "tpcdi_src")
VOLUME = os.environ.get("DATABRICKS_VOLUME", "tpcdi_raw")
LAYER_SCHEMAS = [
    s.strip()
    for s in os.environ.get(
        "DATABRICKS_LAYER_SCHEMAS", "bronze,silver,gold,work"
    ).split(",")
    if s.strip()
]

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
    """Yield (group, local_subdir, source_table_name) for the batch1 load."""
    out: List[Tuple[str, str, str]] = []
    for t in BATCH1_TABLES:
        out.append(("batch1", f"batch1/{t}", f"batch1_{t}"))
    for t in STAGING_TABLES:
        out.append(("staging", f"staging/{t}", f"staging_{t}"))
    out.append(("audit", "audit", "audit"))
    return out


def _volume_root() -> str:
    return f"/Volumes/{CATALOG}/{DBT_SCHEMA}/{VOLUME}"


def _sf_root(sf: int) -> str:
    return f"{_volume_root()}/sf={sf}"


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
# Source-strategy probe
# ---------------------------------------------------------------------------


def _ensure_schemas(ws: WorkspaceClient) -> None:
    _execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{DBT_SCHEMA}`")
    _execute(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SOURCE_SCHEMA}`")
    _execute(
        f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{DBT_SCHEMA}`.`{VOLUME}`"
    )


def _probe_view_strategy(sample_volume_path: str) -> bool:
    """Probe whether INCREMENTAL STRICT MV accepts a VIEW-over-delta-path.

    Returns True if the strategy is acceptable to Databricks Enzyme.

    NB: an `EXPLAIN CREATE MATERIALIZED VIEW ... REFRESH POLICY INCREMENTAL
    STRICT` does NOT validate row tracking — only syntax — so it can pass
    even when the actual CREATE will fail with
    `MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE: Tables do not have row
    tracking enabled`. Therefore the only reliable probe is a real CREATE
    MV against a one-row table sample, executed then dropped.

    The override env var DATABRICKS_ENZYME_STRATEGY may pin this without
    a network round-trip ("view" | "ctas" | "auto" [default]).
    """
    pin = os.environ.get("DATABRICKS_ENZYME_STRATEGY", "auto").strip().lower()
    if pin == "view":
        logger.info("[databricks-enzyme] strategy pinned to 'view' via env")
        return True
    if pin == "ctas":
        logger.info("[databricks-enzyme] strategy pinned to 'ctas' via env")
        return False

    probe_view = (
        f"`{CATALOG}`.`{SOURCE_SCHEMA}`._probe_view_src"
    )
    probe_mv = f"`{CATALOG}`.`{SOURCE_SCHEMA}`._probe_mv"
    try:
        _execute(f"DROP MATERIALIZED VIEW IF EXISTS {probe_mv}")
        _execute(f"DROP VIEW IF EXISTS {probe_view}")
        _execute(
            f"CREATE VIEW {probe_view} AS "
            f"SELECT * FROM delta.`{sample_volume_path}`"
        )
        _execute(
            f"CREATE MATERIALIZED VIEW {probe_mv} "
            f"REFRESH POLICY INCREMENTAL STRICT "
            f"AS SELECT * FROM {probe_view}"
        )
        return True
    except Exception as exc:
        logger.info(
            "[databricks-enzyme] VIEW probe rejected, falling back to CTAS: %s",
            exc,
        )
        return False
    finally:
        try:
            _execute(f"DROP MATERIALIZED VIEW IF EXISTS {probe_mv}")
        except Exception:
            pass
        try:
            _execute(f"DROP VIEW IF EXISTS {probe_view}")
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_sources(sf: int) -> dict:
    """Idempotent: upload Delta source dirs into UC Volume for `sf`, then
    register source tables/views in `<catalog>.<source_schema>`.

    Re-running with the same SF after a successful run is a no-op: we
    just re-register the source relations (cheap CREATE OR REPLACE) so
    they survive a `cleanup_schemas()` between experiments.
    """
    ws = _workspace_client()
    _ensure_schemas(ws)

    sf_root = _sf_root(sf)
    uploaded_marker = f"{sf_root}/_UPLOADED"
    strategy_marker = f"{sf_root}/_STRATEGY"

    already_uploaded = _file_exists(ws, uploaded_marker)

    if already_uploaded:
        logger.info(
            "[databricks-enzyme] sf=%d already uploaded; re-registering sources",
            sf,
        )
        files_uploaded = 0
    else:
        logger.info(
            "[databricks-enzyme] sf=%d not uploaded; uploading Delta sources",
            sf,
        )
        files_uploaded = 0
        for _group, subdir, tname in _all_init_tables():
            local = Path(RAW_DELTA_DIR) / subdir
            remote = f"{sf_root}/{subdir}"
            n = _upload_dir(ws, local, remote)
            files_uploaded += n
            logger.info(
                "[databricks-enzyme] uploaded %d files for %s -> %s",
                n, tname, remote,
            )

    sample_tname = "audit"
    sample_remote = f"{sf_root}/audit"
    use_view = _probe_view_strategy(sample_remote)
    strategy = "view" if use_view else "ctas"

    tables_registered = 0
    for _group, subdir, tname in _all_init_tables():
        remote = f"{sf_root}/{subdir}"
        fq = f"`{CATALOG}`.`{SOURCE_SCHEMA}`.`{tname}`"
        if use_view:
            _execute(f"DROP TABLE IF EXISTS {fq}")
            _execute(
                f"CREATE OR REPLACE VIEW {fq} AS "
                f"SELECT * FROM delta.`{remote}`"
            )
        else:
            _execute(f"DROP VIEW IF EXISTS {fq}")
            _execute(
                f"CREATE OR REPLACE TABLE {fq} "
                f"TBLPROPERTIES ('delta.enableRowTracking' = 'true') AS "
                f"SELECT * FROM delta.`{remote}`"
            )
        tables_registered += 1

    if not already_uploaded:
        _upload_bytes(ws, uploaded_marker, b"ok\n")
        _upload_bytes(ws, strategy_marker, strategy.encode("utf-8") + b"\n")

    logger.info(
        "[databricks-enzyme] init complete sf=%d strategy=%s files=%d tables=%d",
        sf, strategy, files_uploaded, tables_registered,
    )
    return {
        "status": "ok",
        "scale_factor": sf,
        "strategy": strategy,
        "files_uploaded": files_uploaded,
        "tables_created": tables_registered,
        "skipped_upload": already_uploaded,
    }


def _read_strategy(ws: WorkspaceClient, sf: int) -> str:
    strategy_marker = f"{_sf_root(sf)}/_STRATEGY"
    try:
        f = ws.files.download(file_path=strategy_marker)
        body = f.contents.read().decode("utf-8", errors="replace").strip()
        if body in ("view", "ctas"):
            return body
    except Exception:
        pass
    return "view"


def append_sources(batch_num: int, sf: int) -> dict:
    """For batch 2/3, sync any new local staging Delta files up to the
    UC Volume, then (when strategy=ctas) `INSERT INTO` the new rows.
    For strategy=view nothing else is needed — the next REFRESH reads
    the freshly-uploaded Delta files via the existing view definition.
    """
    if batch_num not in (2, 3):
        raise ValueError(
            f"append_sources only supports batch 2 or 3, got {batch_num}"
        )

    ws = _workspace_client()
    strategy = _read_strategy(ws, sf)

    sf_root = _sf_root(sf)

    tables_synced = 0
    tables_inserted = 0
    files_uploaded = 0
    files_skipped = 0

    for t in STAGING_TABLES:
        local_staging = Path(RAW_DELTA_DIR) / "staging" / t
        remote_staging = f"{sf_root}/staging/{t}"
        if local_staging.is_dir():
            up, sk = _sync_dir(ws, local_staging, remote_staging)
            files_uploaded += up
            files_skipped += sk
            if up:
                tables_synced += 1

        # The batch-loader also writes a per-batch dir at `batchN/<t>` —
        # mirror those up too so the source view sees the new rows when
        # they are union-ed into staging. (In practice the spark-batch-
        # loader's "append" mode also appends INTO staging/<t>, so this
        # is belt-and-braces.)
        local_batch = Path(RAW_DELTA_DIR) / f"batch{batch_num}" / t
        if local_batch.is_dir():
            remote_batch = f"{sf_root}/batch{batch_num}/{t}"
            up, sk = _sync_dir(ws, local_batch, remote_batch)
            files_uploaded += up
            files_skipped += sk

    if strategy == "ctas":
        for t in STAGING_TABLES:
            local_batch = Path(RAW_DELTA_DIR) / f"batch{batch_num}" / t
            if not local_batch.is_dir():
                continue
            remote_batch = f"{sf_root}/batch{batch_num}/{t}"
            fq = f"`{CATALOG}`.`{SOURCE_SCHEMA}`.`staging_{t}`"
            _execute(
                f"INSERT INTO {fq} "
                f"SELECT * FROM delta.`{remote_batch}`"
            )
            tables_inserted += 1

    logger.info(
        "[databricks-enzyme] append batch=%d sf=%d strategy=%s "
        "uploaded=%d skipped=%d inserted=%d",
        batch_num, sf, strategy, files_uploaded, files_skipped, tables_inserted,
    )
    return {
        "status": "ok",
        "scale_factor": sf,
        "batch_num": batch_num,
        "strategy": strategy,
        "files_uploaded": files_uploaded,
        "files_skipped": files_skipped,
        "tables_appended": tables_synced if strategy == "view" else tables_inserted,
    }


def cleanup_schemas() -> dict:
    """Drop every schema dbt-databricks writes into (CASCADE) so background
    REFRESH on MVs stops accruing Serverless SQL bills.

    `DBT_SCHEMA` and `SOURCE_SCHEMA` are dropped for legacy compatibility;
    the actual MV output lands in the per-layer schemas declared in
    `dbt_project.yml` (`bronze`, `silver`, `gold`, `work` by default —
    overridable via `DATABRICKS_LAYER_SCHEMAS=foo,bar`).
    """
    dropped: List[str] = []
    targets = [DBT_SCHEMA, SOURCE_SCHEMA, *LAYER_SCHEMAS]
    seen: set[str] = set()
    for s in targets:
        if not s or s in seen:
            continue
        seen.add(s)
        try:
            _execute(f"DROP SCHEMA IF EXISTS `{CATALOG}`.`{s}` CASCADE")
            dropped.append(s)
        except Exception as exc:
            logger.warning(
                "[databricks-enzyme] DROP SCHEMA %s failed: %s", s, exc
            )
    return {"status": "ok", "dropped": dropped}


def cleanup_volume_for_sf(sf: int) -> dict:
    ws = _workspace_client()
    _delete_volume_dir(ws, _sf_root(sf))
    return {"status": "ok", "scale_factor": sf}


def warmup(max_attempts: int = 6, sleep_s: float = 5.0) -> dict:
    """Warm the Serverless SQL warehouse before the benchmark timer starts.

    Runs `SELECT 1` until it succeeds so the cold-start cost of resuming a
    suspended warehouse is not charged against the engine's measured batch
    latency. Returns the total wall-clock spent warming and the number of
    attempts needed (typically 1 if the warehouse is already running, more
    if it had to be resumed from suspend).
    """
    import time

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
    """End-of-sweep teardown: drop schemas, wipe every sf=* in the Volume."""
    cleanup_schemas()
    ws = _workspace_client()
    volume_root = _volume_root()
    try:
        entries = list(
            ws.files.list_directory_contents(directory_path=volume_root)
        )
    except NotFound:
        return {"status": "ok", "deleted": 0}
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme] list %s failed: %s", volume_root, exc
        )
        return {"status": "ok", "deleted": 0}
    deleted = 0
    for entry in entries:
        if getattr(entry, "is_directory", False):
            _delete_volume_dir(ws, entry.path)
            deleted += 1
    return {"status": "ok", "deleted": deleted}
