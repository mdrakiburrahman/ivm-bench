"""Bounded Delta snapshot ingestion for Materialize Emulator native tables."""

import io
import json
import math
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable
from psycopg2 import sql

from sql_catalog import load_sql
from services.ducklake_sources import (
    BATCH1_ONLY_TABLES,
    CATEGORY_B_SCHEMAS,
    CDC_TABLES,
    MUTATION_SPECS,
    STAGING_CATEGORY_A,
    STAGING_CATEGORY_B,
    _mutation_buckets,
)

COPY_CHUNK_BYTES = 16 * 1024 * 1024
ARROW_BATCH_ROWS = 256


def _materialize_type(value):
    if pa.types.is_boolean(value):
        return "boolean"
    if pa.types.is_int8(value) or pa.types.is_int16(value) or pa.types.is_uint8(value):
        return "smallint"
    if pa.types.is_int32(value) or pa.types.is_uint16(value):
        return "integer"
    if pa.types.is_int64(value) or pa.types.is_uint32(value):
        return "bigint"
    if pa.types.is_float32(value):
        return "real"
    if pa.types.is_float64(value):
        return "double precision"
    if pa.types.is_decimal(value):
        return f"numeric({value.precision},{value.scale})"
    if pa.types.is_string(value) or pa.types.is_large_string(value):
        return "text"
    if pa.types.is_binary(value) or pa.types.is_large_binary(value):
        return "bytea"
    if pa.types.is_date(value):
        return "date"
    if pa.types.is_timestamp(value):
        if value.unit == "ns":
            raise ValueError(f"Nanosecond timestamps cannot be copied without loss: {value}")
        return "timestamp with time zone" if value.tz else "timestamp"
    if pa.types.is_struct(value) or pa.types.is_list(value) or pa.types.is_large_list(value):
        return "jsonb"
    raise TypeError(f"Unsupported Delta type for Materialize: {value}")


def _create_table(connection, schema, table, columns):
    normalized = [name.lower() for name, _ in columns]
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"Case-insensitive source column collision for {schema}.{table}")
    relation = sql.Identifier(schema, table).as_string(connection)
    column_sql = sql.SQL(", ").join(
        sql.SQL("{} {}").format(sql.Identifier(name.lower()), sql.SQL(data_type))
        for name, data_type in columns
    ).as_string(connection)
    statement = load_sql(
        "materialize-emulator", "inline", "dbt_server/services_materialize_sources/create_table",
        relation=relation, columns=column_sql,
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)


def init_source_tables(connection, raw_delta_dir: Path):
    """Create and load batch-one source tables on a fresh Emulator."""
    if not connection.autocommit:
        raise ValueError("Materialize source loading requires autocommit for the replay guard")
    statement = load_sql(
        "materialize-emulator", "inline", "dbt_server/services_materialize_sources/create_schema",
        schema=sql.Identifier("tpcdi").as_string(connection),
    )
    with connection.cursor() as cursor:
        cursor.execute(statement)

    loaded = {}
    for source, target in (
        [(name, f"batch1_{name}") for name in BATCH1_ONLY_TABLES]
        + [(name, f"staging_{name}") for name in STAGING_CATEGORY_A]
    ):
        path = raw_delta_dir / "batch1" / source
        dataset = DeltaTable(str(path)).to_pyarrow_dataset()
        columns = [(field.name, _materialize_type(field.type)) for field in dataset.schema]
        if source in CDC_TABLES and target.startswith("staging_"):
            columns = [("cdc_flag", "text"), ("cdc_dsn", "bigint")] + columns
        _create_table(connection, "tpcdi", target, columns)
        loaded[target] = copy_delta_snapshot(connection, path, "tpcdi", target)

    for name in STAGING_CATEGORY_B:
        columns = [
            (column, {"VARCHAR": "text", "TINYINT": "smallint", "BIGINT": "bigint", "DATE": "date"}[kind])
            for column, kind in CATEGORY_B_SCHEMAS[name]
        ]
        _create_table(connection, "tpcdi", f"staging_{name}", columns)
        loaded[f"staging_{name}"] = 0

    audit = raw_delta_dir / "audit"
    if audit.exists():
        dataset = DeltaTable(str(audit)).to_pyarrow_dataset()
        _create_table(connection, "tpcdi", "audit", [
            (field.name, _materialize_type(field.type)) for field in dataset.schema
        ])
        loaded["audit"] = copy_delta_snapshot(connection, audit, "tpcdi", "audit")
    relation = sql.Identifier("tpcdi", "_benchmark_batches").as_string(connection)
    with connection.cursor() as cursor:
        cursor.execute(load_sql(
            "materialize-emulator", "inline", "dbt_server/services_materialize_sources/create_batch_state",
            relation=relation,
        ))
        cursor.execute(load_sql(
            "materialize-emulator", "inline", "dbt_server/services_materialize_sources/claim_batch",
            relation=relation,
        ), (1,))
        cursor.execute(load_sql(
            "materialize-emulator", "inline", "dbt_server/services_materialize_sources/finish_batch",
            relation=relation,
        ), (1,))
    return loaded


def append_source_tables(connection, raw_delta_dir: Path, batch_num: int):
    """Apply one batch once; a failed or uncertain write remains claimed."""
    if not connection.autocommit:
        raise ValueError("Materialize source loading requires autocommit for the replay guard")
    if batch_num not in (2, 3):
        raise ValueError(f"Unsupported Materialize batch: {batch_num}")
    buckets = _mutation_buckets(batch_num)
    paths = {
        name: raw_delta_dir / f"batch{batch_num}" / name
        for name in STAGING_CATEGORY_A + STAGING_CATEGORY_B
    }
    available = {name: path for name, path in paths.items() if path.exists()}
    if not available:
        raise FileNotFoundError(f"No source tables found for batch {batch_num} in {raw_delta_dir}")

    relation = sql.Identifier("tpcdi", "_benchmark_batches").as_string(connection)
    with connection.cursor() as cursor:
        cursor.execute(load_sql(
            "materialize-emulator", "inline", "dbt_server/services_materialize_sources/read_batch_state",
            relation=relation,
        ))
        state = cursor.fetchall()
        if state != [(i, "applied") for i in range(1, batch_num)]:
            raise RuntimeError(f"Cannot apply batch {batch_num}; existing batch state: {state}")
        cursor.execute(load_sql(
            "materialize-emulator", "inline", "dbt_server/services_materialize_sources/claim_batch",
            relation=relation,
        ), (batch_num,))
        for spec in MUTATION_SPECS:
            table = sql.Identifier("tpcdi", f"staging_{spec.table}").as_string(connection)
            for operation in ("update", "delete"):
                start, end, _ = buckets[operation]
                if start == end:
                    continue
                cursor.execute(load_sql(
                    "materialize-emulator", "inline",
                    f"dbt_server/services_ducklake_sources/mutation_{spec.table}_{operation}",
                    table_relation=table, batch_offset=batch_num * 7919, start=start, end=end,
                ))

    loaded = {
        f"staging_{name}": copy_delta_snapshot(connection, path, "tpcdi", f"staging_{name}")
        for name, path in available.items()
    }
    with connection.cursor() as cursor:
        cursor.execute(load_sql(
            "materialize-emulator", "inline", "dbt_server/services_materialize_sources/finish_batch",
            relation=relation,
        ), (batch_num,))
    return {"tables": loaded, "absent_inputs": sorted(set(paths) - set(available))}


def _json_value(value):
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    raise TypeError(f"Unsupported nested value for Materialize JSONB: {type(value).__name__}")


def _copy_field(value):
    if value is None:
        return r"\N"
    if isinstance(value, (dict, list)):
        value = json.dumps(value, default=_json_value, allow_nan=False, ensure_ascii=False)
    elif isinstance(value, bool):
        value = "true" if value else "false"
    elif isinstance(value, bytes):
        value = r"\x" + value.hex()
    elif isinstance(value, (datetime, date, time)):
        value = value.isoformat()
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Non-finite floating point values cannot be copied safely")
        value = str(value)
    elif isinstance(value, (str, int, Decimal)):
        value = str(value)
    else:
        raise TypeError(f"Unsupported Materialize COPY value: {type(value).__name__}")
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def copy_delta_snapshot(connection, delta_path: Path, schema: str, table: str) -> int:
    """COPY active Delta snapshot rows without retrying uncertain writes."""
    dataset = DeltaTable(str(delta_path)).to_pyarrow_dataset()
    names = dataset.schema.names
    if not names or len({name.lower() for name in names}) != len(names):
        raise ValueError(f"Invalid source columns for {delta_path}: {names}")
    relation = sql.Identifier(schema, table).as_string(connection)
    columns = sql.SQL(", ").join(sql.Identifier(name.lower()) for name in names).as_string(connection)
    statement = load_sql(
        "materialize-emulator", "inline", "dbt_server/services_materialize_sources/copy_from_stdin",
        relation=relation, columns=columns,
    )
    count = 0
    chunk = io.BytesIO()

    def flush(cursor):
        if chunk.tell():
            chunk.seek(0)
            cursor.copy_expert(statement, chunk)
            chunk.seek(0)
            chunk.truncate()

    with connection.cursor() as cursor:
        for batch in dataset.scanner(batch_size=ARROW_BATCH_ROWS).to_batches():
            for row in batch.to_pylist():
                line = ("\t".join(_copy_field(row[name]) for name in names) + "\n").encode("utf-8")
                if len(line) > COPY_CHUNK_BYTES:
                    raise ValueError(f"Delta row exceeds COPY chunk size in {delta_path}")
                if chunk.tell() + len(line) > COPY_CHUNK_BYTES:
                    flush(cursor)
                chunk.write(line)
                count += 1
        flush(cursor)
    return count
