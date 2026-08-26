"""RisingWave source table management for the TPC-DI benchmark.

RisingWave cannot read the generated Delta tables itself: it has no Delta
connector and its file readers require object storage.  For batch 1, DuckDB
therefore resolves each Delta snapshot into Parquet, uploads the files to the
benchmark's local MinIO service, and RisingWave bulk-copies them into ordinary
mutable tables.  MinIO is only a transport: the benchmark still reads and
mutates RisingWave-owned tables, with the exact same SQL and rows as every other
engine.

The pgwire multi-row INSERT path remains as a fallback and for the much smaller
batch-2/3 deltas. Independent tables are loaded in parallel in both paths.

Batch semantics match `ducklake_sources`: batch 1 creates and fills, batches 2
and 3 first mutate existing staging rows (the UPDATE/DELETE mix) and then append.
"""

import logging
import os
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence

from services.ducklake_sources import (
    BATCH1_ONLY_TABLES,
    CATEGORY_B_SCHEMAS,
    CDC_TABLES,
    MUTATION_SPECS,
    STAGING_CATEGORY_A,
    STAGING_CATEGORY_B,
    _mutation_buckets,
    _score_predicate,
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
RW_BULK_LOAD = os.environ.get("RISINGWAVE_BULK_LOAD", "1") != "0"
RW_BULK_FALLBACK = os.environ.get("RISINGWAVE_BULK_FALLBACK", "1") != "0"
RW_STAGE_THREADS = max(1, int(os.environ.get("RISINGWAVE_STAGE_THREADS", "4")))
RW_BATCH_PARALLELISM = max(1, int(os.environ.get("RISINGWAVE_BATCH_PARALLELISM", "32")))
RW_SOURCE_PARALLELISM = max(1, int(os.environ.get("RISINGWAVE_SOURCE_PARALLELISM", "4")))

S3_ENDPOINT = os.environ.get("RISINGWAVE_S3_ENDPOINT", "http://minio:9000")
S3_ACCESS_KEY = os.environ.get("RISINGWAVE_S3_ACCESS_KEY", "ivmbench")
S3_SECRET_KEY = os.environ.get("RISINGWAVE_S3_SECRET_KEY", "ivmbench-secret")
S3_BUCKET = os.environ.get("RISINGWAVE_S3_BUCKET", "ivm-bench")
S3_REGION = os.environ.get("RISINGWAVE_S3_REGION", "us-east-1")
S3_CONNECTOR = os.environ.get("RISINGWAVE_S3_CONNECTOR", "s3")
S3_READY_TIMEOUT_S = max(1, int(os.environ.get("RISINGWAVE_S3_READY_TIMEOUT_S", "120")))

_S3_READY = False
_S3_READY_LOCK = threading.Lock()
_DUCKDB_EXTENSION_LOCK = threading.Lock()

#: RisingWave panics and drops the session on a multi-row INSERT carrying ~42k
#: bound parameters ("index out of bounds: the len is 0 but the index is 0");
#: 21k was stable in the real-engine probe. Stay just below that measured bound.
MAX_BIND_PARAMS = int(os.environ.get("RISINGWAVE_MAX_BIND_PARAMS", "20000"))

#: DuckDB type -> RisingWave type. RisingWave rejects every length- and
#: precision-parameterised type at the parser ("unsupported data type:
#: NUMERIC(p,s)", parser error on VARCHAR(n)), and has no TINYINT.
_TYPE_MAP = {
    "BOOLEAN": "BOOLEAN",
    "TINYINT": "SMALLINT",
    "SMALLINT": "SMALLINT",
    "INTEGER": "INT",
    "BIGINT": "BIGINT",
    "HUGEINT": "DECIMAL",
    "FLOAT": "REAL",
    "DOUBLE": "DOUBLE PRECISION",
    "DATE": "DATE",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
    "VARCHAR": "VARCHAR",
    "BLOB": "BYTEA",
}

#: batch1_customer_mgmt arrives as one deeply nested struct from spark-xml.
#: RisingWave has STRUCT, but building nested struct literals for every INSERT is
#: fragile, so the struct is flattened here instead. The bronze model consumes
#: these flat names; its own output schema is unchanged, so cross-engine
#: comparisons still line up.
CUSTOMER_MGMT_PROJECTION = """
    "_ActionTS"                                  AS action_ts,
    "_ActionType"                                AS action_type,
    Customer._C_ID                               AS c_id,
    Customer._C_TAX_ID                           AS c_tax_id,
    Customer._C_GNDR                             AS c_gndr,
    Customer._C_TIER                             AS c_tier,
    Customer._C_DOB                              AS c_dob,
    Customer."Name".C_L_NAME                     AS c_l_name,
    Customer."Name".C_F_NAME                     AS c_f_name,
    Customer."Name".C_M_NAME                     AS c_m_name,
    Customer.Address.C_ADLINE1                   AS c_adline1,
    Customer.Address.C_ADLINE2                   AS c_adline2,
    Customer.Address.C_ZIPCODE                   AS c_zipcode,
    Customer.Address.C_CITY                      AS c_city,
    Customer.Address.C_STATE_PROV                AS c_state_prov,
    Customer.Address.C_CTRY                      AS c_ctry,
    Customer.ContactInfo.C_PRIM_EMAIL            AS c_prim_email,
    Customer.ContactInfo.C_ALT_EMAIL             AS c_alt_email,
    Customer.ContactInfo.C_PHONE_1.C_CTRY_CODE   AS c_phone_1_ctry,
    Customer.ContactInfo.C_PHONE_1.C_AREA_CODE   AS c_phone_1_area,
    Customer.ContactInfo.C_PHONE_1.C_LOCAL       AS c_phone_1_local,
    Customer.ContactInfo.C_PHONE_1.C_EXT         AS c_phone_1_ext,
    Customer.ContactInfo.C_PHONE_2.C_CTRY_CODE   AS c_phone_2_ctry,
    Customer.ContactInfo.C_PHONE_2.C_AREA_CODE   AS c_phone_2_area,
    Customer.ContactInfo.C_PHONE_2.C_LOCAL       AS c_phone_2_local,
    Customer.ContactInfo.C_PHONE_2.C_EXT         AS c_phone_2_ext,
    Customer.ContactInfo.C_PHONE_3.C_CTRY_CODE   AS c_phone_3_ctry,
    Customer.ContactInfo.C_PHONE_3.C_AREA_CODE   AS c_phone_3_area,
    Customer.ContactInfo.C_PHONE_3.C_LOCAL       AS c_phone_3_local,
    Customer.ContactInfo.C_PHONE_3.C_EXT         AS c_phone_3_ext,
    Customer.TaxInfo.C_LCL_TX_ID                 AS c_lcl_tx_id,
    Customer.TaxInfo.C_NAT_TX_ID                 AS c_nat_tx_id,
    Customer.Account._CA_ID                      AS ca_id,
    Customer.Account._CA_TAX_ST                  AS ca_tax_st,
    Customer.Account.CA_B_ID                     AS ca_b_id,
    Customer.Account.CA_NAME                     AS ca_name
"""


# ---------------------------------------------------------------------------
# connections
# ---------------------------------------------------------------------------
def _connect():
    import psycopg

    dsn = (
        f"postgresql://{RW_USER}:{RW_PASSWORD}@{RW_HOST}:{RW_PORT}/{RW_DATABASE}"
        if RW_PASSWORD
        else f"postgresql://{RW_USER}@{RW_HOST}:{RW_PORT}/{RW_DATABASE}"
    )
    return psycopg.connect(dsn, autocommit=True)


def _duckdb():
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"SET threads={RW_STAGE_THREADS}")
    con.execute("SET preserve_insertion_order=false")
    try:
        con.execute("LOAD delta")
    except Exception:
        # The image preinstalls delta. Keep this fallback for local development
        # and for old cached images, but never make every worker install it.
        with _DUCKDB_EXTENSION_LOCK:
            try:
                con.execute("LOAD delta")
            except Exception:
                con.execute("INSTALL delta; LOAD delta")
    return con


def _rel(name: str) -> str:
    return f"{quote_ident(RW_SCHEMA)}.{quote_ident(name)}"


def _rw_type(duck_type: str) -> str:
    base = str(duck_type).upper()
    if base.startswith("DECIMAL") or base.startswith("NUMERIC"):
        return "DECIMAL"
    if base.startswith("VARCHAR") or base.startswith("CHAR"):
        return "VARCHAR"
    mapped = _TYPE_MAP.get(base)
    if mapped is None:
        raise ValueError(f"no RisingWave mapping for DuckDB type {duck_type!r}")
    return mapped


def _source_path(batch: int, table: str) -> Path:
    return RAW_DELTA_DIR / f"batch{batch}" / table


def _scan_sql(path: Path, projection: str = "*") -> str:
    return f"SELECT {projection} FROM delta_scan('{path}')"


@dataclass(frozen=True)
class LoadSpec:
    table: str
    scan_sql: str
    extra_cols: tuple[tuple[str, str], ...] = ()
    prefix_values: tuple = ()


def _bulk_scan_sql(spec: LoadSpec) -> str:
    """Put batch-1 CDC nulls in Parquet instead of synthesising them rowwise."""
    if not spec.extra_cols:
        return spec.scan_sql
    prefix = ", ".join(
        f"CAST(NULL AS {data_type}) AS {quote_ident(name)}"
        for name, data_type in spec.extra_cols
    )
    return f"SELECT {prefix}, source.* FROM ({spec.scan_sql}) AS source"


def _columns_from_scan(duck, scan_sql: str) -> list[tuple[str, str]]:
    described = duck.sql(f"{scan_sql} LIMIT 0")
    return [(c, _rw_type(t)) for c, t in zip(described.columns, described.types)]


# ---------------------------------------------------------------------------
# create + load
# ---------------------------------------------------------------------------
def _create_from_scan(rw_cur, duck, table: str, scan_sql: str, extra_cols: Sequence = ()) -> list:
    """CREATE TABLE in RisingWave with a schema taken from the DuckDB scan."""
    columns = list(extra_cols) + _columns_from_scan(duck, scan_sql)
    return _create_table_from_columns(rw_cur, table, columns)


def _insert_rows(rw_cur, table: str, columns: Sequence[str], rows: Iterable[Sequence]) -> int:
    ncols = len(columns)
    batch = max(1, MAX_BIND_PARAMS // max(1, ncols))
    placeholder = "(" + ",".join(["%s"] * ncols) + ")"
    collist = ",".join(quote_ident(c) for c in columns)
    total = 0
    pending: list = []
    for row in rows:
        pending.append(row)
        if len(pending) >= batch:
            total += _flush(rw_cur, table, collist, placeholder, pending)
            pending = []
    if pending:
        total += _flush(rw_cur, table, collist, placeholder, pending)
    return total


def _flush(rw_cur, table: str, collist: str, placeholder: str, rows: list) -> int:
    sql = (
        f"INSERT INTO {_rel(table)} ({collist}) VALUES "
        + ",".join([placeholder] * len(rows))
    )
    rw_cur.execute(sql, [v for r in rows for v in r])
    return len(rows)


#: Rows pulled out of DuckDB per round trip. Deliberately fetched in chunks
#: rather than all at once so peak memory stays flat at SF100+.
FETCH_CHUNK = 20000


def _load(rw_cur, duck, table: str, scan_sql: str, columns: Sequence[str],
          prefix_values: Sequence = ()) -> int:
    """Stream a DuckDB scan into an existing RisingWave table.

    Uses DuckDB's own fetchmany rather than Arrow: pyarrow is not a dependency
    of the dbt-server image and this needs no conversion layer.
    """
    cursor = duck.execute(scan_sql)
    prefix = tuple(prefix_values)
    total = 0
    while True:
        chunk = cursor.fetchmany(FETCH_CHUNK)
        if not chunk:
            break
        rows = [prefix + tuple(r) for r in chunk] if prefix else chunk
        total += _insert_rows(rw_cur, table, columns, rows)
    return total


def _create_table_from_columns(rw_cur, table: str, columns: Sequence[tuple[str, str]]) -> list[str]:
    ddl_cols = ", ".join(f"{quote_ident(name)} {data_type}" for name, data_type in columns)
    rw_cur.execute(f"DROP TABLE IF EXISTS {_rel(table)} CASCADE")
    rw_cur.execute(f"CREATE TABLE {_rel(table)} ({ddl_cols})")
    return [name for name, _ in columns]


def _s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            retries={"max_attempts": 8, "mode": "standard"},
        ),
    )


def _ensure_s3_bucket(client) -> None:
    """Create the private staging bucket once, waiting for MinIO startup."""
    global _S3_READY
    if _S3_READY:
        return
    with _S3_READY_LOCK:
        if _S3_READY:
            return
        deadline = time.monotonic() + S3_READY_TIMEOUT_S
        last_error = None
        while time.monotonic() < deadline:
            try:
                client.head_bucket(Bucket=S3_BUCKET)
                _S3_READY = True
                return
            except Exception as exc:
                last_error = exc
            try:
                client.create_bucket(Bucket=S3_BUCKET)
                _S3_READY = True
                return
            except Exception as exc:
                last_error = exc
                time.sleep(1)
        raise RuntimeError(
            f"MinIO bucket {S3_BUCKET!r} was not ready after {S3_READY_TIMEOUT_S}s"
        ) from last_error


def _upload_parquet(client, files: Sequence[Path], prefix: str) -> list[str]:
    from boto3.s3.transfer import TransferConfig

    transfer = TransferConfig(
        multipart_threshold=16 * 1024 * 1024,
        multipart_chunksize=16 * 1024 * 1024,
        max_concurrency=2,
        use_threads=True,
    )
    keys = []
    for idx, path in enumerate(files):
        key = f"{prefix}/part-{idx:05d}.parquet"
        client.upload_file(str(path), S3_BUCKET, key, Config=transfer)
        keys.append(key)
    return keys


def _delete_s3_keys(client, keys: Sequence[str]) -> None:
    for start in range(0, len(keys), 1000):
        chunk = keys[start:start + 1000]
        if chunk:
            client.delete_objects(
                Bucket=S3_BUCKET,
                Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
            )


def _export_parquet(duck, scan_sql: str, directory: Path) -> tuple[list[tuple[str, str]], list[Path], int]:
    """Materialize a resolved Delta snapshot as parallel-readable Parquet."""
    columns = _columns_from_scan(duck, scan_sql)
    output = directory / "parts"
    result = duck.execute(
        f"COPY ({scan_sql}) TO {quote_literal(str(output))} "
        "(FORMAT PARQUET, COMPRESSION SNAPPY, PER_THREAD_OUTPUT TRUE, "
        "ROW_GROUP_SIZE 122880)"
    ).fetchone()
    row_count = int(result[0]) if result else 0
    files = sorted(output.glob("*.parquet")) if output.is_dir() else []
    if row_count and not files:
        raise RuntimeError(f"DuckDB exported {row_count} rows but produced no Parquet files")
    return columns, files, row_count


def _external_source_sql(source_name: str, columns: Sequence[tuple[str, str]], prefix: str) -> str:
    ddl_cols = ", ".join(f"{quote_ident(name)} {data_type}" for name, data_type in columns)
    properties = ", ".join([
        f"connector = {quote_literal(S3_CONNECTOR)}",
        f"match_pattern = {quote_literal(prefix + '/*.parquet')}",
        f"s3.region_name = {quote_literal(S3_REGION)}",
        f"s3.bucket_name = {quote_literal(S3_BUCKET)}",
        f"s3.credentials.access = {quote_literal(S3_ACCESS_KEY)}",
        f"s3.credentials.secret = {quote_literal(S3_SECRET_KEY)}",
        f"s3.endpoint_url = {quote_literal(S3_ENDPOINT)}",
        "refresh.interval.sec = '1'",
    ])
    return (
        f"CREATE SOURCE {_rel(source_name)} ({ddl_cols}) "
        f"WITH ({properties}) FORMAT PLAIN ENCODE PARQUET"
    )


def _bulk_load_spec(spec: LoadSpec) -> int:
    """Bulk-load one resolved snapshot through MinIO into a mutable table."""
    duck = _duckdb()
    conn = _connect()
    cur = conn.cursor()
    client = _s3_client()
    source_name = f"openivm_load_{spec.table[:30]}_{uuid.uuid4().hex[:8]}"
    prefix = f"tpcdi/{spec.table}/{uuid.uuid4().hex}"
    keys: list[str] = []
    try:
        _ensure_s3_bucket(client)
        with tempfile.TemporaryDirectory(prefix=f"rw-{spec.table}-") as directory:
            columns, files, row_count = _export_parquet(
                duck, _bulk_scan_sql(spec), Path(directory)
            )
            names = _create_table_from_columns(cur, spec.table, columns)
            if not row_count:
                return 0
            keys = _upload_parquet(client, files, prefix)
            cur.execute(f"SET batch_parallelism = {RW_BATCH_PARALLELISM}")
            cur.execute(f"SET streaming_parallelism_for_source = {RW_SOURCE_PARALLELISM}")
            cur.execute("SET statement_timeout = '4h'")
            cur.execute(_external_source_sql(source_name, columns, prefix))
            collist = ", ".join(quote_ident(name) for name in names)
            cur.execute(
                f"INSERT INTO {_rel(spec.table)} ({collist}) "
                f"SELECT {collist} FROM {_rel(source_name)}"
            )
            return row_count
    finally:
        try:
            cur.execute(f"DROP SOURCE IF EXISTS {_rel(source_name)}")
        except Exception as exc:
            logger.warning("[risingwave] failed to drop staging source %s: %s", source_name, exc)
        if keys:
            try:
                _delete_s3_keys(client, keys)
            except Exception as exc:
                logger.warning("[risingwave] failed to clean MinIO prefix %s: %s", prefix, exc)
        conn.close()
        duck.close()


def _pgwire_load_spec(spec: LoadSpec, *, create: bool = True) -> int:
    duck = _duckdb()
    conn = _connect()
    cur = conn.cursor()
    try:
        if create:
            columns = _create_from_scan(
                cur, duck, spec.table, spec.scan_sql, extra_cols=spec.extra_cols
            )
        else:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (RW_SCHEMA, spec.table),
            )
            columns = [row[0] for row in cur.fetchall()]
            scan_columns = list(duck.sql(f"{spec.scan_sql} LIMIT 0").columns)
            if len(scan_columns) != len(columns):
                raise ValueError(
                    f"{spec.table}: input has {len(scan_columns)} columns, "
                    f"table has {len(columns)}"
                )
        return _load(
            cur, duck, spec.table, spec.scan_sql, columns,
            prefix_values=spec.prefix_values,
        )
    finally:
        conn.close()
        duck.close()


def _initial_load_spec(spec: LoadSpec) -> tuple[str, int, str, float]:
    started = time.monotonic()
    if RW_BULK_LOAD:
        try:
            rows = _bulk_load_spec(spec)
            mode = "minio-parquet"
        except Exception:
            if not RW_BULK_FALLBACK:
                raise
            logger.exception(
                "[risingwave] bulk load failed for %s; falling back to pgwire",
                spec.table,
            )
            rows = _pgwire_load_spec(spec)
            mode = "pgwire"
    else:
        rows = _pgwire_load_spec(spec)
        mode = "pgwire"
    elapsed = time.monotonic() - started
    logger.info(
        "[risingwave] loaded %s: %d rows via %s in %.2fs",
        spec.table, rows, mode, elapsed,
    )
    return spec.table, rows, mode, elapsed


def _parallel_load(specs: Sequence[LoadSpec], loader) -> list:
    if not specs:
        return []
    workers = min(RW_LOAD_WORKERS, len(specs))
    if workers == 1:
        return [loader(spec) for spec in specs]
    results = []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="rw-load") as pool:
        futures = {pool.submit(loader, spec): spec.table for spec in specs}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _verify_loaded_counts(expected: Sequence[tuple[str, int]]) -> None:
    conn = _connect()
    cur = conn.cursor()
    try:
        for table, expected_rows in expected:
            cur.execute(f"SELECT COUNT(*) FROM {_rel(table)}")
            actual = int(cur.fetchone()[0])
            if actual != expected_rows:
                raise RuntimeError(
                    f"{table}: bulk-load verification expected {expected_rows} rows, got {actual}"
                )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# public API — mirrors ducklake_sources
# ---------------------------------------------------------------------------
def init_sources() -> dict:
    """Create the `tpcdi` source tables and load batch 1."""
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(RW_SCHEMA)}")
        # These tables are empty at batch 1 and populated by later CDC batches.
        for table in STAGING_CATEGORY_B:
            name = f"staging_{table}"
            _create_table_from_columns(
                cur, name,
                [(column, _rw_type(data_type)) for column, data_type in CATEGORY_B_SCHEMAS[table]],
            )
    finally:
        conn.close()

    specs: list[LoadSpec] = []
    for table in BATCH1_ONLY_TABLES:
        path = _source_path(1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        projection = CUSTOMER_MGMT_PROJECTION if table == "customer_mgmt" else "*"
        specs.append(LoadSpec(f"batch1_{table}", _scan_sql(path, projection)))

    audit_path = RAW_DELTA_DIR / "audit"
    if audit_path.exists():
        specs.append(LoadSpec("audit", _scan_sql(audit_path)))

    for table in STAGING_CATEGORY_A:
        path = _source_path(1, table)
        if not path.exists():
            raise FileNotFoundError(f"Missing batch1/{table}: {path}")
        extra = (("cdc_flag", "VARCHAR"), ("cdc_dsn", "BIGINT")) if table in CDC_TABLES else ()
        prefix = (None, None) if table in CDC_TABLES else ()
        specs.append(
            LoadSpec(
                f"staging_{table}", _scan_sql(path),
                extra_cols=extra, prefix_values=prefix,
            )
        )

    loaded = _parallel_load(specs, _initial_load_spec)
    conn = _connect()
    try:
        conn.cursor().execute("FLUSH")
    finally:
        conn.close()
    _verify_loaded_counts([(table, rows) for table, rows, _, _ in loaded])

    created = [spec.table for spec in specs] + [f"staging_{table}" for table in STAGING_CATEGORY_B]

    logger.info("[risingwave] Source init complete: %d tables", len(created))
    modes = sorted({mode for _, _, mode, _ in loaded})
    return {
        "status": "ok",
        "tables_created": len(created),
        "tables": created,
        "load_modes": modes,
        "load_workers": min(RW_LOAD_WORKERS, len(specs)),
        "load_metrics": [
            {"table": table, "rows": rows, "mode": mode, "duration_s": round(elapsed, 3)}
            for table, rows, mode, elapsed in sorted(loaded)
        ],
    }


def _mutation_statements(batch_num: int) -> tuple[list[str], list[dict]]:
    """Same score-bucketed UPDATE/DELETE mix as the DuckLake engines."""
    buckets = _mutation_buckets(batch_num)
    if not buckets or all(start == end for start, end, _ in buckets.values()):
        return [], []

    stmts: list[str] = []
    summaries: list[dict] = []
    for spec in MUTATION_SPECS:
        rel = _rel(f"staging_{spec.table}")
        update_start, update_end, update_pct = buckets["update"]
        delete_start, delete_end, delete_pct = buckets["delete"]
        if update_start != update_end:
            pred = _score_predicate(spec, batch_num, update_start, update_end)
            stmts.append(f"UPDATE {rel} SET {spec.update_assignments} WHERE {pred}")
        if delete_start != delete_end:
            pred = _score_predicate(spec, batch_num, delete_start, delete_end)
            stmts.append(f"DELETE FROM {rel} WHERE {pred}")
        summaries.append({
            "table": f"staging_{spec.table}",
            "update_pct": str(update_pct),
            "delete_pct": str(delete_pct),
        })
    return stmts, summaries


def append_sources(batch_num: int, flush: bool = True) -> dict:
    """Mutate existing staging rows, then append this batch's rows.

    ``flush=False`` returns as soon as the writes are submitted, without waiting
    for them to become visible downstream. That split matters for the benchmark:
    the interesting number is how long the MV graph takes to absorb the delta,
    and folding the FLUSH into the append hides it. The caller then times the
    FLUSH separately. The default stays True so a caller that just wants the
    rows loaded gets a consistent read afterwards.
    """
    specs = []
    for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
        path = _source_path(batch_num, table)
        if path.exists():
            specs.append(LoadSpec(f"staging_{table}", _scan_sql(path)))

    mutation_stmts, mutations = _mutation_statements(batch_num)

    if mutation_stmts:
        # Keep mutations and appends on one session and in program order. Across
        # independent connections, an append could otherwise overtake an UPDATE
        # and make new batch rows part of the mutation selection.
        duck = _duckdb()
        conn = _connect()
        cur = conn.cursor()
        appended_rows = []
        try:
            for stmt in mutation_stmts:
                cur.execute(stmt)
            for spec in specs:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                    (RW_SCHEMA, spec.table),
                )
                columns = [row[0] for row in cur.fetchall()]
                scan_columns = list(duck.sql(f"{spec.scan_sql} LIMIT 0").columns)
                if len(scan_columns) != len(columns):
                    raise ValueError(
                        f"{spec.table}: input has {len(scan_columns)} columns, "
                        f"table has {len(columns)}"
                    )
                rows = _load(cur, duck, spec.table, spec.scan_sql, columns)
                appended_rows.append((spec.table, rows))
            if flush:
                cur.execute("FLUSH")
        finally:
            conn.close()
            duck.close()
    else:
        # Insert-only benchmark batches are independent and safe to submit on
        # separate connections. This is the SF100 path.
        def append_one(spec: LoadSpec) -> tuple[str, int]:
            started = time.monotonic()
            rows = _pgwire_load_spec(spec, create=False)
            logger.info(
                "[risingwave] appended %s: %d rows via pgwire in %.2fs",
                spec.table, rows, time.monotonic() - started,
            )
            return spec.table, rows

        appended_rows = _parallel_load(specs, append_one)

        if flush:
            conn = _connect()
            try:
                conn.cursor().execute("FLUSH")
            finally:
                conn.close()

    if mutation_stmts:
        for table, rows in appended_rows:
            logger.info("[risingwave] appended %s: %d rows via serial pgwire", table, rows)

    appended = [table for table, _ in appended_rows]

    return {
        "status": "ok",
        "batch_num": batch_num,
        "tables_appended": len(appended),
        "tables": appended,
        "mutations": mutations,
        "flushed": flush,
    }
