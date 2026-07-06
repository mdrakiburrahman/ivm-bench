"""Shared DuckLake source table management for DuckDB-based engines."""

import os
import shutil
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
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

SCORE_MODULUS = 1_000_000


@dataclass(frozen=True)
class MutationSpec:
    table: str
    score_expr: str
    update_assignments: str


MUTATION_SPECS = [
    MutationSpec(
        "cash_transaction",
        "(coalesce(ct_ca_id, 0) % 1000000) * 2654435761",
        "ct_amt = coalesce(ct_amt, 0) + 0.01",
    ),
    MutationSpec(
        "daily_market",
        "date_diff('day', DATE '1970-01-01', dm_date) * 2654435761 + coalesce(ascii(substr(dm_s_symb, 1, 1)), 0) * 9176 + coalesce(ascii(substr(dm_s_symb, 2, 1)), 0)",
        "dm_close = dm_close + 0.01, dm_high = dm_high + 0.01, dm_vol = dm_vol + 1",
    ),
    MutationSpec(
        "holding_history",
        "(coalesce(hh_h_t_id, 0) % 1000000) * 2654435761 + (coalesce(hh_t_id, 0) % 1000000)",
        "hh_after_qty = hh_after_qty + 1",
    ),
    MutationSpec(
        "prospect",
        "coalesce(age, 0) * 2654435761 + coalesce(creditrating, 0) * 9176 + coalesce(numbercars, 0) * 271 + coalesce(numberchildren, 0)",
        "networth = networth + 1",
    ),
    MutationSpec(
        "trade",
        "(coalesce(t_id, 0) % 1000000) * 2654435761",
        "t_qty = t_qty + 1",
    ),
    MutationSpec(
        "watch_history",
        "(coalesce(w_c_id, 0) % 1000000) * 2654435761",
        "w_dts = w_dts + INTERVAL 1 SECOND",
    ),
    MutationSpec(
        "account",
        "(coalesce(accountid, 0) % 1000000) * 2654435761",
        "accountdesc = accountdesc || ' upd'",
    ),
    MutationSpec(
        "customer",
        "(coalesce(customerid, 0) % 1000000) * 2654435761",
        "tier = CASE WHEN tier IS NULL THEN NULL ELSE CAST(((CAST(tier AS INTEGER) + 1) % 100) AS TINYINT) END",
    ),
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


def _score_predicate(spec: MutationSpec, batch_num: int, start: int, end: int) -> str:
    if start == end:
        return "false"
    score = f"abs(({spec.score_expr} + {batch_num * 7919}) % {SCORE_MODULUS})"
    return f"{score} >= {start} AND {score} < {end}"


def _mutation_sql(batch_num: int) -> tuple[list[str], list[dict]]:
    buckets = _mutation_buckets(batch_num)
    if not buckets or all(start == end for start, end, _ in buckets.values()):
        return [], []

    stmts: list[str] = []
    summaries: list[dict] = []
    for spec in MUTATION_SPECS:
        table_rel = relation("ducklake", "tpcdi", f"staging_{spec.table}")
        update_start, update_end, update_pct = buckets["update"]
        delete_start, delete_end, delete_pct = buckets["delete"]

        if update_start != update_end:
            pred = _score_predicate(spec, batch_num, update_start, update_end)
            stmts.append(f"UPDATE {table_rel} SET {spec.update_assignments} WHERE {pred};")
        if delete_start != delete_end:
            pred = _score_predicate(spec, batch_num, delete_start, delete_end)
            stmts.append(f"DELETE FROM {table_rel} WHERE {pred};")

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
    """Mutate existing staging rows, then append batch data into DuckLake tables."""
    if state_files and not any(path.exists() for path in state_files):
        raise RuntimeError("No DuckLake state found. Run source init first.")

    stmts = []
    mutation_stmts, mutations = _mutation_sql(batch_num)
    stmts.extend(mutation_stmts)

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
    return {
        "status": "ok",
        "batch_num": batch_num,
        "tables_appended": len(appended),
        "tables": appended,
        "mutations": mutations,
    }
