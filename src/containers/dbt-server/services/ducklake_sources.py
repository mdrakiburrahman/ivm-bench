"""Shared DuckLake source table management for DuckDB-based engines."""

import os
import shutil
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Callable

from sql_catalog import load_sql

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

SCORE_MODULUS = 1_000_000


@dataclass(frozen=True)
class MutationSpec:
    table: str


MUTATION_SPECS = [
    MutationSpec("cash_transaction"),
    MutationSpec("daily_market"),
    MutationSpec("holding_history"),
    MutationSpec("prospect"),
    MutationSpec("trade"),
    MutationSpec("watch_history"),
    MutationSpec("account"),
    MutationSpec("customer"),
]


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


def _read_parquet_sql(path: Path, engine: str) -> str:
    files = ", ".join(quote_literal(str(f)) for f in _parquet_files(path))
    return load_sql(engine, "inline", "dbt_server/services_ducklake_sources/read_parquet", files=files)


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


def _mutation_pct(batch_num: int, op: str) -> Decimal:
    raw = os.environ.get(f"BATCH_{batch_num}_{op.upper()}_PCT", "0")
    try:
        value = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Invalid BATCH_{batch_num}_{op.upper()}_PCT={raw!r}") from exc
    if value < 0 or value > 100:
        raise ValueError(f"BATCH_{batch_num}_{op.upper()}_PCT must be between 0 and 100")
    return value


def _mutation_buckets(batch_num: int) -> dict[str, tuple[int, int, Decimal]]:
    if batch_num == 1:
        return {}

    pcts = {
        "update": _mutation_pct(batch_num, "update"),
        "delete": _mutation_pct(batch_num, "delete"),
    }
    total = sum(pcts.values(), Decimal("0"))
    if total > 100:
        raise ValueError(f"Batch {batch_num} mutation percentages sum to {total}; max is 100")

    start = 0
    buckets = {}
    for op in ("update", "delete"):
        width = int(
            (pcts[op] * SCORE_MODULUS / Decimal("100")).to_integral_value(
                rounding=ROUND_HALF_UP
            )
        )
        buckets[op] = (start, start + width, pcts[op])
        start += width
    return buckets


def _mutation_sql(batch_num: int, engine: str) -> tuple[list[str], list[dict]]:
    buckets = _mutation_buckets(batch_num)
    if not buckets or all(start == end for start, end, _ in buckets.values()):
        return [], []

    stmts: list[str] = []
    summaries: list[dict] = []
    for spec in MUTATION_SPECS:
        table_rel = relation("ducklake", "tpcdi", f"staging_{spec.table}")
        update_start, update_end, update_pct = buckets["update"]
        delete_start, delete_end, delete_pct = buckets["delete"]

        for operation, start, end in (
            ("update", update_start, update_end),
            ("delete", delete_start, delete_end),
        ):
            if start != end:
                stmts.append(load_sql(
                    engine,
                    "inline",
                    f"dbt_server/services_ducklake_sources/mutation_{spec.table}_{operation}",
                    table_relation=table_rel,
                    batch_offset=batch_num * 7919,
                    start=start,
                    end=end,
                ))

        summaries.append({
            "table": f"staging_{spec.table}",
            "update_pct": str(update_pct),
            "delete_pct": str(delete_pct),
        })

    return stmts, summaries

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
        stmts.append(load_sql(
            label,
            "inline",
            "dbt_server/services_ducklake_sources/create_schema",
            schema=schema,
        ))

    created = []
    for table in BATCH1_ONLY_TABLES:
        path = _source_path(raw_delta_dir, 1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        stmts.append(load_sql(
            label,
            "inline",
            "dbt_server/services_ducklake_sources/create_table",
            relation=relation("ducklake", "tpcdi", f"batch1_{table}"),
            source=_read_parquet_sql(path, label),
        ))
        created.append(f"batch1_{table}")

    audit_path = raw_delta_dir / "audit"
    if audit_path.exists():
        stmts.append(load_sql(
            label,
            "inline",
            "dbt_server/services_ducklake_sources/create_table",
            relation=relation("ducklake", "tpcdi", "audit"),
            source=_read_parquet_sql(audit_path, label),
        ))
        created.append("audit")

    for table in STAGING_CATEGORY_A:
        path = _source_path(raw_delta_dir, 1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        template = "create_staging_cdc" if table in CDC_TABLES else "create_table"
        stmts.append(load_sql(
            label,
            "inline",
            f"dbt_server/services_ducklake_sources/{template}",
            relation=relation("ducklake", "tpcdi", f"staging_{table}"),
            source=_read_parquet_sql(path, label),
        ))
        created.append(f"staging_{table}")

    for table in STAGING_CATEGORY_B:
        cols = ", ".join(
            f"{quote_ident(n)} {t}" for n, t in CATEGORY_B_SCHEMAS[table]
        )
        stmts.append(load_sql(
            label,
            "inline",
            "dbt_server/services_ducklake_sources/create_empty_staging",
            relation=relation("ducklake", "tpcdi", f"staging_{table}"),
            columns=cols,
        ))
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
    """Mutate existing staging rows, then append batch data into DuckLake tables."""
    if state_files and not any(path.exists() for path in state_files):
        raise RuntimeError("No DuckLake state found. Run source init first.")

    stmts = []
    mutation_stmts, mutations = _mutation_sql(batch_num, label)
    stmts.extend(mutation_stmts)

    appended = []
    for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
        path = _source_path(raw_delta_dir, batch_num, table)
        if not path.exists():
            continue
        stmts.append(load_sql(
            label,
            "inline",
            "dbt_server/services_ducklake_sources/insert_batch",
            relation=relation("ducklake", "tpcdi", f"staging_{table}"),
            source=_read_parquet_sql(path, label),
        ))
        appended.append(f"staging_{table}")

    if stmts:
        execute_sql("\n".join(stmts), f"{label} source append batch {batch_num}")
    return {
        "status": "ok",
        "batch_num": batch_num,
        "tables_appended": len(appended),
        "tables": appended,
        "mutations": mutations,
    }
