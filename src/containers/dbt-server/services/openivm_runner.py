"""OpenIVM runner — executes TPC-DI benchmark via DuckLake materialized views.

Replaces the standalone openivm-benchmark.py script.  Runs inside dbt-server
so that results are stored in SQLite and telemetry (progress, SQL analysis,
lineage) is identical to Spark / DuckDB / Feldera.
"""

import json
import logging
import os
import shutil
import subprocess
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.db import DB_LOCK, get_db
from services.progress import LIVE_PROGRESS, PROGRESS_LOCK

logger = logging.getLogger(__name__)

OPENIVM_BIN = os.environ.get("OPENIVM_BIN", "/data/bin/openivm/duckdb")
RAW_DELTA_DIR = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
OPENIVM_WORK_DIR = Path(os.environ.get("OPENIVM_WORK_DIR", "/data/processed/openivm"))

# ── Source table classification (mirrors openivm-benchmark.py) ──────────────

BATCH1_ONLY_TABLES = [
    "customer_mgmt", "date", "finwire", "hr", "industry",
    "status_type", "tax_rate", "trade_history", "trade_type",
]

STAGING_CATEGORY_A = [
    "cash_transaction", "daily_market", "holding_history",
    "prospect", "trade", "watch_history",
]

STAGING_CATEGORY_B = ["account", "customer", "batch_date"]

CDC_TABLES = {
    "cash_transaction", "daily_market", "holding_history",
    "trade", "watch_history",
}

CATEGORY_B_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "account": [
        ("cdc_flag", "VARCHAR"), ("cdc_dsn", "BIGINT"),
        ("accountid", "BIGINT"), ("ca_b_id", "BIGINT"), ("ca_c_id", "BIGINT"),
        ("accountdesc", "VARCHAR"), ("taxstatus", "TINYINT"), ("ca_st_id", "VARCHAR"),
    ],
    "customer": [
        ("cdc_flag", "VARCHAR"), ("cdc_dsn", "BIGINT"),
        ("customerid", "BIGINT"), ("taxid", "VARCHAR"), ("status", "VARCHAR"),
        ("lastname", "VARCHAR"), ("firstname", "VARCHAR"), ("middleinitial", "VARCHAR"),
        ("gender", "VARCHAR"), ("tier", "TINYINT"), ("dob", "DATE"),
        ("addressline1", "VARCHAR"), ("addressline2", "VARCHAR"),
        ("postalcode", "VARCHAR"), ("city", "VARCHAR"),
        ("stateprov", "VARCHAR"), ("country", "VARCHAR"),
        ("c_ctry_1", "VARCHAR"), ("c_area_1", "VARCHAR"),
        ("c_local_1", "VARCHAR"), ("c_ext_1", "VARCHAR"),
        ("c_ctry_2", "VARCHAR"), ("c_area_2", "VARCHAR"),
        ("c_local_2", "VARCHAR"), ("c_ext_2", "VARCHAR"),
        ("c_ctry_3", "VARCHAR"), ("c_area_3", "VARCHAR"),
        ("c_local_3", "VARCHAR"), ("c_ext_3", "VARCHAR"),
        ("email1", "VARCHAR"), ("email2", "VARCHAR"),
        ("lcl_tx_id", "VARCHAR"), ("nat_tx_id", "VARCHAR"),
    ],
    "batch_date": [("batchdate", "DATE")],
}

# ── SQL helpers ─────────────────────────────────────────────────────────────


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _relation(catalog: str, schema: str, name: str) -> str:
    return ".".join(_quote_ident(p) for p in [catalog, schema, name])


def _strip_semicolon(sql: str) -> str:
    sql = sql.strip()
    while sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql


def _parquet_files(path: Path) -> list[Path]:
    files = sorted(
        f for f in path.rglob("*.parquet")
        if "_delta_log" not in f.parts
    )
    if not files:
        raise FileNotFoundError(f"No parquet files under {path}")
    return files


def _read_parquet_sql(path: Path) -> str:
    files = ", ".join(_quote_literal(str(f)) for f in _parquet_files(path))
    return f"read_parquet([{files}], union_by_name = true)"


def _attach_sql(meta_path: str, data_path: str) -> str:
    return f"""
LOAD openivm;
SET ivm_cascade_refresh='off';
INSTALL icu;
LOAD icu;
INSTALL ducklake;
LOAD ducklake;
ATTACH {_quote_literal(meta_path)} AS ducklake (
    TYPE ducklake,
    DATA_PATH {_quote_literal(data_path)}
);
"""


def _run_duckdb(sql: str, label: str) -> str:
    """Execute SQL via the OpenIVM DuckDB binary."""
    prefix = ".bail on\n.timer off\n"
    db_file = str(OPENIVM_WORK_DIR / "openivm.duckdb")

    proc = subprocess.run(
        [OPENIVM_BIN, db_file],
        input=prefix + sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{proc.stdout}")
    return proc.stdout


# ── Source table management ─────────────────────────────────────────────────


def _source_path(batch: int, table: str) -> Path:
    return RAW_DELTA_DIR / f"batch{batch}" / table


def _source_init_sql() -> str:
    """Create DuckLake schemas and batch-1 source tables."""
    stmts = [
        "CREATE SCHEMA IF NOT EXISTS ducklake.tpcdi;",
        "CREATE SCHEMA IF NOT EXISTS ducklake.bronze;",
        "CREATE SCHEMA IF NOT EXISTS ducklake.silver;",
        "CREATE SCHEMA IF NOT EXISTS ducklake.gold;",
    ]

    for table in BATCH1_ONLY_TABLES:
        path = _source_path(1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', f'batch1_{table}')} AS "
            f"SELECT * FROM {_read_parquet_sql(path)};"
        )

    audit_path = RAW_DELTA_DIR / "audit"
    if audit_path.exists():
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', 'audit')} AS "
            f"SELECT * FROM {_read_parquet_sql(audit_path)};"
        )

    for table in STAGING_CATEGORY_A:
        path = _source_path(1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        prefix = ""
        if table in CDC_TABLES:
            prefix = "NULL::VARCHAR AS cdc_flag, NULL::BIGINT AS cdc_dsn, "
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', f'staging_{table}')} AS "
            f"SELECT {prefix}* FROM {_read_parquet_sql(path)};"
        )

    for table in STAGING_CATEGORY_B:
        cols = ", ".join(f"{_quote_ident(n)} {t}" for n, t in CATEGORY_B_SCHEMAS[table])
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', f'staging_{table}')} ({cols});"
        )

    return "\n".join(stmts)


def _source_append_sql(batch_num: int) -> str:
    """INSERT INTO staging tables from new batch parquet data."""
    stmts = []
    for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
        path = _source_path(batch_num, table)
        if not path.exists():
            continue
        stmts.append(
            f"INSERT INTO {_relation('ducklake', 'tpcdi', f'staging_{table}')} "
            f"SELECT * FROM {_read_parquet_sql(path)};"
        )
    return "\n".join(stmts)


# ── Model helpers ───────────────────────────────────────────────────────────


def _model_schema_map(models_dir: str) -> dict[str, str]:
    """Map model name → schema (bronze/silver/gold/work) from file paths."""
    result: dict[str, str] = {}
    for path in Path(models_dir).rglob("*.sql"):
        rel = path.relative_to(models_dir)
        top = rel.parts[0]
        if top in {"bronze", "silver", "gold", "work"}:
            result[path.stem] = top
    return result


def _load_model_nodes(duckdb_manifest: dict[str, Any], models_dir: str) -> list[dict]:
    """Extract model nodes from a dbt manifest, topologically sorted."""
    schemas = _model_schema_map(models_dir)
    nodes = []

    for uid, node in duckdb_manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        name = node.get("name", uid.split(".")[-1])
        schema = schemas.get(name)
        if not schema or schema == "work":
            continue
        compiled_sql = _strip_semicolon(
            node.get("compiled_code") or node.get("compiled_sql", "")
        )
        if not compiled_sql:
            continue
        nodes.append({
            "unique_id": uid,
            "name": name,
            "schema": schema,
            "compiled_sql": compiled_sql,
            "depends_on": node.get("depends_on", {}).get("nodes", []),
        })

    if not nodes:
        raise ValueError("No model nodes found in DuckDB manifest")
    return _topological_sort(nodes)


def _topological_sort(nodes: list[dict]) -> list[dict]:
    by_id = {n["unique_id"]: n for n in nodes}
    indegree: dict[str, int] = {uid: 0 for uid in by_id}
    children: dict[str, list[str]] = defaultdict(list)

    for node in nodes:
        for dep in node.get("depends_on", []):
            if dep in by_id:
                indegree[node["unique_id"]] += 1
                children[dep].append(node["unique_id"])

    queue = deque(sorted(uid for uid, deg in indegree.items() if deg == 0))
    ordered = []
    while queue:
        uid = queue.popleft()
        ordered.append(by_id[uid])
        for child in sorted(children[uid]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(nodes):
        raise ValueError("Cycle detected in dbt model graph")
    return ordered


def _create_mv_sql(node: dict) -> str:
    target = _relation("ducklake", node["schema"], node["name"])
    return f"CREATE MATERIALIZED VIEW {target} AS (\n{node['compiled_sql']}\n);"


def _refresh_mv_sql(node: dict) -> str:
    return (
        f"PRAGMA ivm_options("
        f"{_quote_literal('ducklake')}, "
        f"{_quote_literal(node['schema'])}, "
        f"{_quote_literal(node['name'])}"
        f");"
    )


def _compare_sql(node: dict) -> str:
    target = _relation("ducklake", node["schema"], node["name"])
    query = node["compiled_sql"]
    name_esc = node["name"].replace("'", "''")
    schema_esc = node["schema"].replace("'", "''")
    return f"""
CREATE TEMP TABLE openivm_actual AS SELECT * FROM {target};
CREATE TEMP TABLE openivm_expected AS {query};
CREATE TEMP TABLE openivm_diff AS
    (SELECT * FROM openivm_actual EXCEPT ALL SELECT * FROM openivm_expected)
    UNION ALL
    (SELECT * FROM openivm_expected EXCEPT ALL SELECT * FROM openivm_actual);
SELECT CASE
    WHEN (SELECT COUNT(*) FROM openivm_diff) = 0 THEN 'ok'
    ELSE error('OpenIVM comparison failed for {schema_esc}.{name_esc}')
END AS verification;
"""


# ── Progress reporting ──────────────────────────────────────────────────────


def _report_start(run_id: str, node: dict, idx: int, total: int):
    """Report model start to the live progress system."""
    with PROGRESS_LOCK:
        prog = LIVE_PROGRESS.get(run_id)
        if not prog:
            return
        prog["events"][node["unique_id"]] = {
            "unique_id": node["unique_id"],
            "name": node["name"],
            "resource_type": "model",
            "status": "running",
            "execution_time_s": None,
            "rows_affected": None,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "message": f"Running {node['schema']}.{node['name']}",
        }


def _report_finish(run_id: str, node: dict, duration_s: float, status: str = "success"):
    """Report model completion to the live progress system."""
    with PROGRESS_LOCK:
        prog = LIVE_PROGRESS.get(run_id)
        if not prog:
            return
        existing = prog["events"].get(node["unique_id"], {})
        prog["events"][node["unique_id"]] = {
            **existing,
            "unique_id": node["unique_id"],
            "name": node["name"],
            "resource_type": "model",
            "status": status,
            "execution_time_s": round(duration_s, 2),
            "rows_affected": None,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "message": f"{node['schema']}.{node['name']} [{duration_s:.2f}s]",
        }


# ── DB helpers ──────────────────────────────────────────────────────────────


def _save_node(run_id: str, node: dict, duration_s: float, status: str = "success"):
    """Persist a single node result to SQLite."""
    with DB_LOCK:
        conn = get_db()
        conn.execute(
            """INSERT OR REPLACE INTO run_nodes
               (run_id, unique_id, name, resource_type, execution_time_s, status, compiled_sql, depends_on, rows_affected)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                run_id,
                node["unique_id"],
                node["name"],
                "model",
                round(duration_s, 4),
                status,
                node["compiled_sql"],
                json.dumps(node.get("depends_on", [])),
                None,
            ),
        )
        conn.commit()
        conn.close()


def _update_run(run_id: str, status: str, duration_s: float | None = None, error: str | None = None):
    completed_at = datetime.now(timezone.utc).isoformat()
    with DB_LOCK:
        conn = get_db()
        conn.execute(
            "UPDATE runs SET status=?, completed_at=?, duration_s=?, error=? WHERE run_id=?",
            (status, completed_at, round(duration_s, 2) if duration_s else None, error, run_id),
        )
        conn.commit()
        conn.close()


# ── Main runner ─────────────────────────────────────────────────────────────


def run_openivm(run_id: str, scale_factor: int, full_refresh: bool, batch_num: int = 1):
    """Execute the OpenIVM benchmark for one batch.

    Parameters
    ----------
    run_id : str
        UUID for this run (already inserted in ``runs`` table as "queued").
    scale_factor : int
        TPC-DI scale factor.
    full_refresh : bool
        True for batch 1 (CREATE MATERIALIZED VIEW), False for batch 2/3 (PRAGMA ivm).
    batch_num : int
        Which batch (1, 2, or 3).  Needed for source append on incremental runs.
    """
    from services.dbt_compiler import get_manifest
    from services.progress import cleanup_progress, init_progress

    now_iso = datetime.now(timezone.utc).isoformat()
    with DB_LOCK:
        conn = get_db()
        conn.execute(
            "UPDATE runs SET status='running', started_at=? WHERE run_id=?",
            (now_iso, run_id),
        )
        conn.commit()
        conn.close()

    init_progress(run_id)

    try:
        # Validate binary
        if not os.path.isfile(OPENIVM_BIN):
            raise FileNotFoundError(
                f"OpenIVM binary not found at {OPENIVM_BIN}. "
                "Run docker-compose.openivm-build.yml first."
            )

        # Load DuckDB manifest for compiled SQL + DAG.
        # The persisted manifest (saved by benchmark.sh after the DuckDB phase)
        # is guaranteed to have compiled_code. The dbt project target/ may have
        # an uncompiled manifest, so prefer the persisted copy.
        manifest = None
        fallback = os.path.join(str(OPENIVM_WORK_DIR), "manifest-duckdb.json")
        if os.path.exists(fallback):
            with open(fallback) as f:
                manifest = json.load(f)
            logger.info("[openivm] Loaded DuckDB manifest from %s", fallback)
        if not manifest:
            manifest = get_manifest("duckdb")
        if not manifest:
            raise RuntimeError(
                "Could not load DuckDB dbt manifest. "
                "Run the DuckDB benchmark first, or ensure manifest-duckdb.json is in STATE_DIR."
            )

        duckdb_models_dir = "/app/dbt-projects/duckdb/models"
        models = _load_model_nodes(manifest, duckdb_models_dir)
        total = len(models)

        with PROGRESS_LOCK:
            prog = LIVE_PROGRESS.get(run_id)
            if prog:
                prog["total"] = total

        if full_refresh:
            # Wipe previous state for a clean batch 1.
            # Can't rmtree the mount root inside Docker, so clear contents.
            # Preserve manifest-duckdb.json (copied from DuckDB engine run).
            if OPENIVM_WORK_DIR.exists():
                for child in OPENIVM_WORK_DIR.iterdir():
                    if child.name == "manifest-duckdb.json":
                        continue
                    if child.is_dir():
                        shutil.rmtree(child)
                    else:
                        child.unlink()
            OPENIVM_WORK_DIR.mkdir(parents=True, exist_ok=True)
        else:
            # Incremental: verify batch 1 state exists
            OPENIVM_WORK_DIR.mkdir(parents=True, exist_ok=True)
            ducklake_meta = OPENIVM_WORK_DIR / "openivm.ducklake"
            if not ducklake_meta.exists():
                raise RuntimeError(
                    f"No DuckLake state found at {ducklake_meta}. "
                    "Run batch 1 (full_refresh=true) first."
                )

        meta_path = str(OPENIVM_WORK_DIR / "openivm.ducklake")
        data_path = str(OPENIVM_WORK_DIR / "data")
        attach = _attach_sql(meta_path, data_path)

        start_ts = time.monotonic()

        if full_refresh:
            # Batch 1: init sources + create materialized views
            logger.info("[openivm] Initialising DuckLake sources from batch 1 data")
            _run_duckdb(attach + _source_init_sql(), "openivm source init")

            for idx, node in enumerate(models):
                _report_start(run_id, node, idx, total)
                label = f"create {node['schema']}.{node['name']}"
                t0 = time.monotonic()
                _run_duckdb(attach + _create_mv_sql(node), label)
                dur = time.monotonic() - t0

                # Validate
                _run_duckdb(attach + _compare_sql(node), f"compare {node['schema']}.{node['name']}")

                _report_finish(run_id, node, dur)
                _save_node(run_id, node, dur)
                logger.info("[openivm]   %s: %.4fs", label, dur)
        else:
            # Batch 2/3: append sources + PRAGMA ivm refresh
            logger.info("[openivm] Appending batch %d sources", batch_num)
            append_sql = _source_append_sql(batch_num)
            if append_sql.strip():
                _run_duckdb(attach + append_sql, f"openivm source append batch {batch_num}")

            for idx, node in enumerate(models):
                _report_start(run_id, node, idx, total)
                label = f"refresh {node['schema']}.{node['name']}"
                t0 = time.monotonic()
                _run_duckdb(attach + _refresh_mv_sql(node), label)
                dur = time.monotonic() - t0

                # Validate
                _run_duckdb(attach + _compare_sql(node), f"compare {node['schema']}.{node['name']}")

                _report_finish(run_id, node, dur)
                _save_node(run_id, node, dur)
                logger.info("[openivm]   %s: %.4fs", label, dur)

        elapsed = time.monotonic() - start_ts
        _update_run(run_id, "completed", elapsed)
        cleanup_progress(run_id)
        logger.info("[openivm] batch %d completed in %.2fs", batch_num, elapsed)

    except Exception as e:
        elapsed = time.monotonic() - start_ts if 'start_ts' in locals() else None
        logger.exception("[openivm] batch %d failed: %s", batch_num, e)
        _update_run(run_id, "failed", elapsed, str(e))
        cleanup_progress(run_id)
