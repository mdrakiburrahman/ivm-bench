"""DuckDB-OpenIVM source table management.

Manages DuckLake source tables independently of the dbt project.
Called by the benchmark-server via REST endpoints before dbt build runs.
Uses the Python duckdb module (with OpenIVM extension baked in via wheel).
"""

import logging
import os
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

RAW_DELTA_DIR = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
WORK_DIR = Path(os.environ.get("DUCKDB_OPENIVM_WORK_DIR", "/data/processed/duckdb-openivm"))

# ── Source table classification ─────────────────────────────────────────────

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


def _source_path(batch: int, table: str) -> Path:
    return RAW_DELTA_DIR / f"batch{batch}" / table


OPENIVM_BIN = os.environ.get("DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")


def _run_openivm_sql(sql: str, label: str) -> str:
    """Execute SQL via the OpenIVM DuckDB binary (subprocess)."""
    db_file = str(WORK_DIR / "openivm.duckdb")
    meta_path = str(WORK_DIR / "openivm.ducklake")
    data_path = str(WORK_DIR / "data")

    prefix = ".bail on\n.timer off\n"
    attach = (
        f"LOAD openivm;\n"
        f"SET ivm_cascade_refresh='off';\n"
        f"INSTALL icu; LOAD icu;\n"
        f"INSTALL ducklake; LOAD ducklake;\n"
        f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
        f"(DATA_PATH {_quote_literal(data_path)}, data_inlining_row_limit 0);\n"
    )

    import subprocess
    proc = subprocess.run(
        [OPENIVM_BIN, db_file],
        input=prefix + attach + sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{proc.stdout}")
    return proc.stdout


# ── Public API ──────────────────────────────────────────────────────────────


def init_sources() -> dict:
    """Create DuckLake schemas and batch-1 source tables.

    Wipes previous state for a clean batch 1. Uses the OpenIVM CLI binary.
    Returns a summary dict with table counts.
    """
    # Clean previous state
    if WORK_DIR.exists():
        for child in WORK_DIR.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    (WORK_DIR / "data").mkdir(exist_ok=True)

    stmts = []

    # Create schemas
    for schema in ("tpcdi", "bronze", "silver", "gold"):
        stmts.append(f"CREATE SCHEMA IF NOT EXISTS ducklake.{schema};")

    created = []

    # Batch1-only reference tables
    for table in BATCH1_ONLY_TABLES:
        path = _source_path(1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', f'batch1_{table}')} AS "
            f"SELECT * FROM {_read_parquet_sql(path)};"
        )
        created.append(f"batch1_{table}")

    # Audit table
    audit_path = RAW_DELTA_DIR / "audit"
    if audit_path.exists():
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', 'audit')} AS "
            f"SELECT * FROM {_read_parquet_sql(audit_path)};"
        )
        created.append("audit")

    # Staging category A (with optional CDC prefix columns)
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
        created.append(f"staging_{table}")

    # Staging category B (empty tables with explicit schema)
    for table in STAGING_CATEGORY_B:
        cols = ", ".join(
            f"{_quote_ident(n)} {t}" for n, t in CATEGORY_B_SCHEMAS[table]
        )
        stmts.append(
            f"CREATE TABLE {_relation('ducklake', 'tpcdi', f'staging_{table}')} ({cols});"
        )
        created.append(f"staging_{table}")

    _run_openivm_sql("\n".join(stmts), "source init")

    logger.info("[duckdb-openivm] Source init complete: %d tables", len(created))
    return {"status": "ok", "tables_created": len(created), "tables": created}


def append_sources(batch_num: int) -> dict:
    """INSERT new batch data into staging tables.

    Returns a summary dict with tables appended.
    """
    if not (WORK_DIR / "openivm.ducklake").exists():
        raise RuntimeError(
            "No DuckLake state found. Run init_sources (batch 1) first."
        )

    stmts = []
    appended = []
    for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
        path = _source_path(batch_num, table)
        if not path.exists():
            continue
        stmts.append(
            f"INSERT INTO {_relation('ducklake', 'tpcdi', f'staging_{table}')} "
            f"SELECT * FROM {_read_parquet_sql(path)};"
        )
        appended.append(f"staging_{table}")

    if stmts:
        _run_openivm_sql("\n".join(stmts), f"source append batch {batch_num}")

    logger.info(
        "[duckdb-openivm] Batch %d append: %d tables", batch_num, len(appended)
    )
    return {"status": "ok", "batch_num": batch_num, "tables_appended": len(appended), "tables": appended}
