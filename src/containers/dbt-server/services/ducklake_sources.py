"""Shared DuckLake source table management for DuckDB-based engines."""

import shutil
from pathlib import Path
from typing import Callable

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


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def relation(catalog: str, schema: str, name: str) -> str:
    return ".".join(quote_ident(p) for p in [catalog, schema, name])


def _parquet_files(path: Path) -> list[Path]:
    files = sorted(
        f for f in path.rglob("*.parquet")
        if "_delta_log" not in f.parts
    )
    if not files:
        raise FileNotFoundError(f"No parquet files under {path}")
    return files


def _read_parquet_sql(path: Path) -> str:
    files = ", ".join(quote_literal(str(f)) for f in _parquet_files(path))
    return f"read_parquet([{files}], union_by_name = true)"


def _source_path(raw_delta_dir: Path, batch: int, table: str) -> Path:
    return raw_delta_dir / f"batch{batch}" / table


def _reset_work_dir(work_dir: Path) -> None:
    if work_dir.exists():
        for child in work_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()
    work_dir.mkdir(parents=True, exist_ok=True)
    (work_dir / "data").mkdir(exist_ok=True)


def init_sources(
    raw_delta_dir: Path,
    work_dir: Path,
    execute_sql: Callable[[str, str], str],
    label: str,
) -> dict:
    """Create DuckLake schemas and batch-1 source tables.

    The benchmark generator still emits the raw batch payload under a
    ``delta`` directory. DuckDB-based engines read only the Parquet data files
    from that artifact and load them into DuckLake source tables.
    """
    _reset_work_dir(work_dir)

    stmts = []
    for schema in ("tpcdi", "bronze", "silver", "gold"):
        stmts.append(f"CREATE SCHEMA IF NOT EXISTS ducklake.{schema};")

    created = []
    for table in BATCH1_ONLY_TABLES:
        path = _source_path(raw_delta_dir, 1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        stmts.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', f'batch1_{table}')} AS "
            f"SELECT * FROM {_read_parquet_sql(path)};"
        )
        created.append(f"batch1_{table}")

    audit_path = raw_delta_dir / "audit"
    if audit_path.exists():
        stmts.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', 'audit')} AS "
            f"SELECT * FROM {_read_parquet_sql(audit_path)};"
        )
        created.append("audit")

    for table in STAGING_CATEGORY_A:
        path = _source_path(raw_delta_dir, 1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        prefix = ""
        if table in CDC_TABLES:
            prefix = "NULL::VARCHAR AS cdc_flag, NULL::BIGINT AS cdc_dsn, "
        stmts.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', f'staging_{table}')} AS "
            f"SELECT {prefix}* FROM {_read_parquet_sql(path)};"
        )
        created.append(f"staging_{table}")

    for table in STAGING_CATEGORY_B:
        cols = ", ".join(
            f"{quote_ident(n)} {t}" for n, t in CATEGORY_B_SCHEMAS[table]
        )
        stmts.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', f'staging_{table}')} ({cols});"
        )
        created.append(f"staging_{table}")

    execute_sql("\n".join(stmts), f"{label} source init")
    return {"status": "ok", "tables_created": len(created), "tables": created}


def append_sources(
    raw_delta_dir: Path,
    work_dir: Path,
    batch_num: int,
    execute_sql: Callable[[str, str], str],
    label: str,
    state_files: tuple[Path, ...],
) -> dict:
    """Append batch data into DuckLake staging source tables."""
    if state_files and not any(path.exists() for path in state_files):
        raise RuntimeError("No DuckLake state found. Run source init first.")

    stmts = []
    appended = []
    for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
        path = _source_path(raw_delta_dir, batch_num, table)
        if not path.exists():
            continue
        stmts.append(
            f"INSERT INTO {relation('ducklake', 'tpcdi', f'staging_{table}')} "
            f"SELECT * FROM {_read_parquet_sql(path)};"
        )
        appended.append(f"staging_{table}")

    if stmts:
        execute_sql("\n".join(stmts), f"{label} source append batch {batch_num}")
    return {"status": "ok", "batch_num": batch_num, "tables_appended": len(appended), "tables": appended}
