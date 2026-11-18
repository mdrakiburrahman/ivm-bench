"""Load immutable Iceberg batch snapshots into mutable RisingWave tables."""

import logging
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

from sql_catalog import load_sql

from services.ducklake_sources import (
    BATCH1_ONLY_TABLES,
    CATEGORY_B_SCHEMAS,
    CDC_TABLES,
    MUTATION_SPECS,
    STAGING_CATEGORY_A,
    STAGING_CATEGORY_B,
    _mutation_buckets,
    quote_ident,
    quote_literal,
)

logger = logging.getLogger(__name__)
RAW_DELTA_DIR = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
RW_HOST = os.environ.get("RISINGWAVE_HOST", "risingwave")
RW_PORT = int(os.environ.get("RISINGWAVE_PORT", "4566"))
RW_USER = os.environ.get("RISINGWAVE_USER", "root")
RW_PASSWORD = os.environ.get("RISINGWAVE_PASSWORD", "")
RW_DATABASE = os.environ.get("RISINGWAVE_DATABASE", "dev")
RW_SCHEMA = os.environ.get("RISINGWAVE_SOURCE_SCHEMA", "tpcdi")
RW_LOAD_WORKERS = max(1, int(os.environ.get("RISINGWAVE_LOAD_WORKERS", "4")))
RW_BATCH_PARALLELISM = max(1, int(os.environ.get("RISINGWAVE_BATCH_PARALLELISM", "32")))
S3_ENDPOINT = os.environ.get("RISINGWAVE_S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("RISINGWAVE_S3_ACCESS_KEY", "ivmbench")
S3_SECRET_KEY = os.environ.get("RISINGWAVE_S3_SECRET_KEY", "ivmbench-secret")
S3_BUCKET = os.environ.get("RISINGWAVE_S3_BUCKET", "ivm-bench")
S3_REGION = os.environ.get("RISINGWAVE_S3_REGION", "us-east-1")

_TYPE_MAP = {
    "BOOLEAN": "BOOLEAN", "TINYINT": "SMALLINT", "SMALLINT": "SMALLINT",
    "INTEGER": "INT", "BIGINT": "BIGINT", "HUGEINT": "DECIMAL",
    "FLOAT": "REAL", "DOUBLE": "DOUBLE PRECISION", "DATE": "DATE",
    "TIME": "TIME", "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ", "VARCHAR": "VARCHAR",
    "BLOB": "BYTEA",
}

# Spark XML nested fields are projected to the existing bronze source schema.
CUSTOMER_FIELDS = (
    (("_ActionTS",), "action_ts"),
    (("_ActionType",), "action_type"),
    (("Customer", "_C_ID"), "c_id"),
    (("Customer", "_C_TAX_ID"), "c_tax_id"),
    (("Customer", "_C_GNDR"), "c_gndr"),
    (("Customer", "_C_TIER"), "c_tier"),
    (("Customer", "_C_DOB"), "c_dob"),
    (("Customer", "Name", "C_L_NAME"), "c_l_name"),
    (("Customer", "Name", "C_F_NAME"), "c_f_name"),
    (("Customer", "Name", "C_M_NAME"), "c_m_name"),
    (("Customer", "Address", "C_ADLINE1"), "c_adline1"),
    (("Customer", "Address", "C_ADLINE2"), "c_adline2"),
    (("Customer", "Address", "C_ZIPCODE"), "c_zipcode"),
    (("Customer", "Address", "C_CITY"), "c_city"),
    (("Customer", "Address", "C_STATE_PROV"), "c_state_prov"),
    (("Customer", "Address", "C_CTRY"), "c_ctry"),
    (("Customer", "ContactInfo", "C_PRIM_EMAIL"), "c_prim_email"),
    (("Customer", "ContactInfo", "C_ALT_EMAIL"), "c_alt_email"),
    (("Customer", "ContactInfo", "C_PHONE_1", "C_CTRY_CODE"), "c_phone_1_ctry"),
    (("Customer", "ContactInfo", "C_PHONE_1", "C_AREA_CODE"), "c_phone_1_area"),
    (("Customer", "ContactInfo", "C_PHONE_1", "C_LOCAL"), "c_phone_1_local"),
    (("Customer", "ContactInfo", "C_PHONE_1", "C_EXT"), "c_phone_1_ext"),
    (("Customer", "ContactInfo", "C_PHONE_2", "C_CTRY_CODE"), "c_phone_2_ctry"),
    (("Customer", "ContactInfo", "C_PHONE_2", "C_AREA_CODE"), "c_phone_2_area"),
    (("Customer", "ContactInfo", "C_PHONE_2", "C_LOCAL"), "c_phone_2_local"),
    (("Customer", "ContactInfo", "C_PHONE_2", "C_EXT"), "c_phone_2_ext"),
    (("Customer", "ContactInfo", "C_PHONE_3", "C_CTRY_CODE"), "c_phone_3_ctry"),
    (("Customer", "ContactInfo", "C_PHONE_3", "C_AREA_CODE"), "c_phone_3_area"),
    (("Customer", "ContactInfo", "C_PHONE_3", "C_LOCAL"), "c_phone_3_local"),
    (("Customer", "ContactInfo", "C_PHONE_3", "C_EXT"), "c_phone_3_ext"),
    (("Customer", "TaxInfo", "C_LCL_TX_ID"), "c_lcl_tx_id"),
    (("Customer", "TaxInfo", "C_NAT_TX_ID"), "c_nat_tx_id"),
    (("Customer", "Account", "_CA_ID"), "ca_id"),
    (("Customer", "Account", "_CA_TAX_ST"), "ca_tax_st"),
    (("Customer", "Account", "CA_B_ID"), "ca_b_id"),
    (("Customer", "Account", "CA_NAME"), "ca_name"),
)


def _connect():
    import psycopg

    dsn = (
        f"postgresql://{RW_USER}:{RW_PASSWORD}@{RW_HOST}:{RW_PORT}/{RW_DATABASE}"
        if RW_PASSWORD
        else f"postgresql://{RW_USER}@{RW_HOST}:{RW_PORT}/{RW_DATABASE}"
    )
    return psycopg.connect(dsn, autocommit=True)


def _rel(name: str) -> str:
    return f"{quote_ident(RW_SCHEMA)}.{quote_ident(name)}"


def _rw_type(source_type: str) -> str:
    base = str(source_type).upper()
    if base.startswith("DECIMAL") or base.startswith("NUMERIC"):
        return "DECIMAL"
    if base.startswith("VARCHAR") or base.startswith("CHAR"):
        return "VARCHAR"
    mapped = _TYPE_MAP.get(base)
    if mapped is None:
        raise ValueError(f"no RisingWave mapping for source type {source_type!r}")
    return mapped


def _source_path(batch: int, table: str) -> Path:
    return RAW_DELTA_DIR / f"batch{batch}" / table


@dataclass(frozen=True)
class LoadSpec:
    table: str
    namespace: str
    source_table: str
    extra_cols: tuple[tuple[str, str], ...] = ()
    customer_projection: bool = False


def _execute(cur, name: str, **params):
    cur.execute(load_sql("risingwave", "inline", f"dbt_server/services_risingwave_sources/{name}", **params))


def _create_table_from_columns(cur, table: str, columns: Sequence[tuple[str, str]]) -> None:
    ddl = ", ".join(f"{quote_ident(name)} {data_type}" for name, data_type in columns)
    _execute(cur, "drop_table", relation=_rel(table))
    _execute(cur, "create_table", relation=_rel(table), columns=ddl)


def _customer_projection() -> list[str]:
    result = []
    for path, alias in CUSTOMER_FIELDS:
        expression = quote_ident(path[0])
        for field in path[1:]:
            expression = f"({expression}).{quote_ident(field)}"
        if path == ("_ActionTS",):
            expression = f"({expression} AT TIME ZONE 'UTC')"
        result.append(f"{expression} AS {quote_ident(alias)}")
    return result


def _source_projection(cur, source: str, customer: bool) -> list[str]:
    if customer:
        return _customer_projection()
    _execute(cur, "source_columns", schema=quote_literal(RW_SCHEMA), table=quote_literal(source))
    columns = cur.fetchall()
    if not columns:
        raise RuntimeError(f"Iceberg source {source} has no columns")
    result = []
    for name, data_type in columns:
        column = quote_ident(name)
        if data_type.lower() == "timestamp with time zone":
            result.append(f"({column} AT TIME ZONE 'UTC') AS {column}")
        else:
            result.append(column)
    return result


def _open_source(cur, spec: LoadSpec, snapshot: int) -> tuple[str, int, list[str]]:
    source = f"ivm_iceberg_{uuid.uuid4().hex}"
    properties = ", ".join([
        "connector = 'iceberg'",
        "catalog.type = 'storage'",
        f"warehouse.path = {quote_literal(f's3a://{S3_BUCKET}/warehouse')}",
        f"database.name = {quote_literal(spec.namespace)}",
        f"table.name = {quote_literal(spec.source_table)}",
        f"s3.endpoint = {quote_literal(S3_ENDPOINT)}",
        f"s3.region = {quote_literal(S3_REGION)}",
        f"s3.access.key = {quote_literal(S3_ACCESS_KEY)}",
        f"s3.secret.key = {quote_literal(S3_SECRET_KEY)}",
        "s3.path.style.access = 'true'",
    ])
    _execute(cur, "create_iceberg_source", relation=_rel(source), properties=properties)
    try:
        _execute(cur, "count_snapshot", relation=_rel(source), snapshot=snapshot)
        rows = int(cur.fetchone()[0])
        projection = _source_projection(cur, source, spec.customer_projection)
        return source, rows, projection
    except Exception:
        try:
            _execute(cur, "drop_source", relation=_rel(source))
        except Exception:
            logger.exception("Failed to remove Iceberg source %s after initialization failure", source)
        raise


def _prepare_session(cur) -> None:
    _execute(cur, "set_batch_parallelism", value=RW_BATCH_PARALLELISM)
    _execute(cur, "set_statement_timeout", timeout=quote_literal("4h"))
    _execute(cur, "set_timezone_utc")


def _validate_snapshot(spec: LoadSpec, expected: dict | None = None) -> tuple[int, dict]:
    import boto3

    with (RAW_DELTA_DIR / ".iceberg-snapshots.json").open(encoding="utf-8") as handle:
        manifest = json.load(handle)
    key = f"{spec.namespace}.{spec.source_table}"
    if key not in manifest:
        raise RuntimeError(f"Missing immutable Iceberg snapshot identity for {key}")
    current = manifest[key]
    if expected is not None and current != expected:
        raise RuntimeError(f"Iceberg snapshot manifest changed for {key}: {current} != {expected}")
    expected = current if expected is None else expected
    client = boto3.client(
        "s3", endpoint_url=S3_ENDPOINT, region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY,
    )
    prefix = f"warehouse/{spec.namespace}/{spec.source_table}/metadata"
    version = client.get_object(Bucket=S3_BUCKET, Key=f"{prefix}/version-hint.text")["Body"].read().decode().strip()
    metadata = json.loads(client.get_object(
        Bucket=S3_BUCKET, Key=f"{prefix}/v{version}.metadata.json",
    )["Body"].read())
    actual = {
        "table_uuid": str(metadata["table-uuid"]),
        "snapshot_id": str(metadata.get("current-snapshot-id") or ""),
        "schema_id": str(metadata["current-schema-id"]),
        "delta_id": metadata["properties"].get("ivm.delta.id"),
        "delta_version": metadata["properties"].get("ivm.delta.version"),
    }
    if actual != expected:
        raise RuntimeError(f"Iceberg snapshot identity changed for {key}: {actual} != {expected}")
    snapshot = int(expected["snapshot_id"])
    if snapshot <= 0:
        raise RuntimeError(f"Invalid Iceberg snapshot id for {key}: {snapshot}")
    return snapshot, expected


def _load_one(spec: LoadSpec, create: bool, cur=None) -> tuple[str, int]:
    own_connection = cur is None
    conn = _connect() if own_connection else None
    source = None
    failed = False
    try:
        if own_connection:
            cur = conn.cursor()
            _prepare_session(cur)
        snapshot, expected = _validate_snapshot(spec)
        source, rows, projection = _open_source(cur, spec, snapshot)
        if create:
            prefix = [
                f"CAST(NULL AS {data_type}) AS {quote_ident(name)}"
                for name, data_type in spec.extra_cols
            ]
            _execute(cur, "drop_table", relation=_rel(spec.table))
            _execute(cur, "create_table_as", relation=_rel(spec.table),
                     projection=", ".join(prefix + projection), source_relation=_rel(source),
                     snapshot=snapshot)
        else:
            _execute(cur, "list_columns", schema=quote_literal(RW_SCHEMA), table=quote_literal(spec.table))
            columns = [row[0] for row in cur.fetchall()]
            if len(columns) != len(projection):
                raise ValueError(f"{spec.table}: input has {len(projection)} columns, table has {len(columns)}")
            _execute(cur, "insert_iceberg", target_relation=_rel(spec.table),
                     columns=", ".join(map(quote_ident, columns)),
                     projection=", ".join(projection), source_relation=_rel(source),
                     snapshot=snapshot)
        _validate_snapshot(spec, expected)
        return spec.table, rows
    except BaseException:
        failed = True
        raise
    finally:
        cleanup_errors = []
        if source is not None:
            try:
                _execute(cur, "drop_source", relation=_rel(source))
            except Exception as exc:
                logger.exception("Failed to remove Iceberg source %s", source)
                cleanup_errors.append(exc)
        if conn is not None:
            try:
                conn.close()
            except Exception as exc:
                logger.exception("Failed to close Iceberg loader connection for %s", spec.table)
                cleanup_errors.append(exc)
        if cleanup_errors and not failed:
            raise cleanup_errors[0]


def _parallel_load(specs: Sequence[LoadSpec], loader) -> list:
    if not specs:
        return []
    with ThreadPoolExecutor(max_workers=min(RW_LOAD_WORKERS, len(specs))) as pool:
        futures = [pool.submit(loader, spec) for spec in specs]
        return [future.result() for future in as_completed(futures)]


def _verify_loaded_counts(expected: Sequence[tuple[str, int]]) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        for table, expected_rows in expected:
            _execute(cur, "count_rows", relation=_rel(table))
            actual = int(cur.fetchone()[0])
            if actual != expected_rows:
                raise RuntimeError(f"{table}: Iceberg load expected {expected_rows} rows, got {actual}")
    finally:
        conn.close()


def init_sources() -> dict:
    conn = _connect()
    try:
        cur = conn.cursor()
        _execute(cur, "create_schema", schema=quote_ident(RW_SCHEMA))
        for table in STAGING_CATEGORY_B:
            _create_table_from_columns(cur, f"staging_{table}", [
                (column, _rw_type(data_type)) for column, data_type in CATEGORY_B_SCHEMAS[table]
            ])
    finally:
        conn.close()
    specs = []
    for table in BATCH1_ONLY_TABLES:
        if not _source_path(1, table).exists():
            raise FileNotFoundError(f"Missing batch1/{table}")
        specs.append(LoadSpec(f"batch1_{table}", "batch1", table,
                              customer_projection=table == "customer_mgmt"))
    if (RAW_DELTA_DIR / "audit").exists():
        specs.append(LoadSpec("audit", "common", "audit"))
    for table in STAGING_CATEGORY_A:
        if not _source_path(1, table).exists():
            raise FileNotFoundError(f"Missing batch1/{table}")
        extra = (("cdc_flag", "VARCHAR"), ("cdc_dsn", "BIGINT")) if table in CDC_TABLES else ()
        specs.append(LoadSpec(f"staging_{table}", "batch1", table, extra_cols=extra))

    def load(spec):
        started = time.monotonic()
        table, rows = _load_one(spec, create=True)
        return table, rows, round(time.monotonic() - started, 3)

    loaded = _parallel_load(specs, load)
    conn = _connect()
    try:
        _execute(conn.cursor(), "flush")
    finally:
        conn.close()
    _verify_loaded_counts([(table, rows) for table, rows, _ in loaded])
    created = [spec.table for spec in specs] + [f"staging_{table}" for table in STAGING_CATEGORY_B]
    return {
        "status": "ok", "tables_created": len(created), "tables": created,
        "load_modes": ["iceberg"], "load_workers": min(RW_LOAD_WORKERS, len(specs)),
        "load_metrics": [
            {"table": table, "rows": rows, "mode": "iceberg", "duration_s": elapsed}
            for table, rows, elapsed in sorted(loaded)
        ],
    }


def _mutation_statements(batch_num: int) -> tuple[list[str], list[dict]]:
    buckets = _mutation_buckets(batch_num)
    if not buckets or all(start == end for start, end, _ in buckets.values()):
        return [], []
    stmts, summaries = [], []
    for spec in MUTATION_SPECS:
        table_relation = _rel(f"staging_{spec.table}")
        update_start, update_end, update_pct = buckets["update"]
        delete_start, delete_end, delete_pct = buckets["delete"]
        for operation, start, end in (
            ("update", update_start, update_end), ("delete", delete_start, delete_end),
        ):
            if start != end:
                stmts.append(load_sql(
                    "risingwave", "inline",
                    f"dbt_server/services_ducklake_sources/mutation_{spec.table}_{operation}",
                    table_relation=table_relation, batch_offset=batch_num * 7919, start=start, end=end,
                ))
        summaries.append({
            "table": f"staging_{spec.table}",
            "update_pct": str(update_pct), "delete_pct": str(delete_pct),
        })
    return stmts, summaries


def append_sources(batch_num: int, flush: bool = True) -> dict:
    specs = [
        LoadSpec(f"staging_{table}", f"batch{batch_num}", table)
        for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B
        if _source_path(batch_num, table).exists()
    ]
    mutation_stmts, mutations = _mutation_statements(batch_num)
    if mutation_stmts:
        conn = _connect()
        try:
            cur = conn.cursor()
            _prepare_session(cur)
            for stmt in mutation_stmts:
                cur.execute(stmt)
            appended = [_load_one(spec, create=False, cur=cur) for spec in specs]
            if flush:
                _execute(cur, "flush")
        finally:
            conn.close()
    else:
        appended = _parallel_load(specs, lambda spec: _load_one(spec, create=False))
        if flush:
            conn = _connect()
            try:
                _execute(conn.cursor(), "flush")
            finally:
                conn.close()
    return {
        "status": "ok", "batch_num": batch_num, "tables_appended": len(appended),
        "tables": [table for table, _ in appended], "mutations": mutations, "flushed": flush,
    }
