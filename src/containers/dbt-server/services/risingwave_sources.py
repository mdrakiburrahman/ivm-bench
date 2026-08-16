"""RisingWave source table management for the TPC-DI benchmark.

RisingWave cannot read the generated Delta tables itself: `file_scan` supports
only s3/gcs/azblob (no local filesystem) and there is no Delta source connector,
so unlike the DuckDB engines there is no `CREATE TABLE AS SELECT read_parquet()`
path. This module therefore reads the Delta payload with DuckDB in-process and
streams it into RisingWave over the PostgreSQL wire protocol.

Row counts stay modest for that to be sane: SF10 is ~16M rows across all
sources, and measured INSERT throughput is ~45k rows/s.

Batch semantics match `ducklake_sources`: batch 1 creates and fills, batches 2
and 3 first mutate existing staging rows (the UPDATE/DELETE mix) and then append.
"""

import logging
import os
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
)

logger = logging.getLogger(__name__)

RAW_DELTA_DIR = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
RW_HOST = os.environ.get("RISINGWAVE_HOST", "risingwave")
RW_PORT = int(os.environ.get("RISINGWAVE_PORT", "4566"))
RW_USER = os.environ.get("RISINGWAVE_USER", "root")
RW_PASSWORD = os.environ.get("RISINGWAVE_PASSWORD", "")
RW_DATABASE = os.environ.get("RISINGWAVE_DATABASE", "dev")
RW_SCHEMA = os.environ.get("RISINGWAVE_SOURCE_SCHEMA", "tpcdi")

#: RisingWave panics and drops the session on a multi-row INSERT carrying ~42k
#: bound parameters ("index out of bounds: the len is 0 but the index is 0");
#: 21k is fine. Cap well below the observed cliff.
MAX_BIND_PARAMS = 10000

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


# ---------------------------------------------------------------------------
# create + load
# ---------------------------------------------------------------------------
def _create_from_scan(rw_cur, duck, table: str, scan_sql: str, extra_cols: Sequence = ()) -> list:
    """CREATE TABLE in RisingWave with a schema taken from the DuckDB scan."""
    described = duck.sql(f"{scan_sql} LIMIT 0")
    cols = [(c, _rw_type(t)) for c, t in zip(described.columns, described.types)]
    cols = list(extra_cols) + cols
    ddl_cols = ", ".join(f"{quote_ident(c)} {t}" for c, t in cols)
    rw_cur.execute(f"DROP TABLE IF EXISTS {_rel(table)} CASCADE")
    rw_cur.execute(f"CREATE TABLE {_rel(table)} ({ddl_cols})")
    return [c for c, _ in cols]


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


# ---------------------------------------------------------------------------
# public API — mirrors ducklake_sources
# ---------------------------------------------------------------------------
def init_sources() -> dict:
    """Create the `tpcdi` source tables and load batch 1."""
    duck = _duckdb()
    conn = _connect()
    cur = conn.cursor()
    created: list[str] = []
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(RW_SCHEMA)}")

        for table in BATCH1_ONLY_TABLES:
            path = _source_path(1, table)
            if not path.exists():
                raise FileNotFoundError(f"Missing batch1/{table}: {path}")
            name = f"batch1_{table}"
            projection = (
                CUSTOMER_MGMT_PROJECTION if table == "customer_mgmt" else "*"
            )
            scan = _scan_sql(path, projection)
            cols = _create_from_scan(cur, duck, name, scan)
            n = _load(cur, duck, name, scan, cols)
            created.append(name)
            logger.info("[risingwave] loaded %s: %d rows", name, n)

        audit_path = RAW_DELTA_DIR / "audit"
        if audit_path.exists():
            scan = _scan_sql(audit_path)
            cols = _create_from_scan(cur, duck, "audit", scan)
            _load(cur, duck, "audit", scan, cols)
            created.append("audit")

        for table in STAGING_CATEGORY_A:
            path = _source_path(1, table)
            if not path.exists():
                raise FileNotFoundError(f"Missing batch1/{table}: {path}")
            name = f"staging_{table}"
            scan = _scan_sql(path)
            # CDC tables carry two extra leading columns that batch 1 leaves null.
            extra = (("cdc_flag", "VARCHAR"), ("cdc_dsn", "BIGINT")) if table in CDC_TABLES else ()
            cols = _create_from_scan(cur, duck, name, scan, extra_cols=extra)
            prefix = (None, None) if table in CDC_TABLES else ()
            n = _load(cur, duck, name, scan, cols, prefix_values=prefix)
            created.append(name)
            logger.info("[risingwave] loaded %s: %d rows", name, n)

        for table in STAGING_CATEGORY_B:
            name = f"staging_{table}"
            ddl_cols = ", ".join(
                f"{quote_ident(n)} {_rw_type(t)}" for n, t in CATEGORY_B_SCHEMAS[table]
            )
            cur.execute(f"DROP TABLE IF EXISTS {_rel(name)} CASCADE")
            cur.execute(f"CREATE TABLE {_rel(name)} ({ddl_cols})")
            created.append(name)

        cur.execute("FLUSH")
    finally:
        conn.close()
        duck.close()

    logger.info("[risingwave] Source init complete: %d tables", len(created))
    return {"status": "ok", "tables_created": len(created), "tables": created}


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
    duck = _duckdb()
    conn = _connect()
    cur = conn.cursor()
    appended: list[str] = []
    try:
        mutation_stmts, mutations = _mutation_statements(batch_num)
        for stmt in mutation_stmts:
            cur.execute(stmt)

        for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
            path = _source_path(batch_num, table)
            if not path.exists():
                continue
            name = f"staging_{table}"
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
                (RW_SCHEMA, name),
            )
            cols = [r[0] for r in cur.fetchall()]
            scan = _scan_sql(path)
            described = duck.sql(f"{scan} LIMIT 0")
            scan_cols = list(described.columns)
            # Batch 2/3 payloads carry the CDC columns; batch 1 did not.
            if len(scan_cols) != len(cols):
                raise ValueError(
                    f"{name}: batch{batch_num} has {len(scan_cols)} columns, "
                    f"table has {len(cols)}"
                )
            n = _load(cur, duck, name, scan, cols)
            appended.append(name)
            logger.info("[risingwave] appended %s: %d rows", name, n)

        if flush:
            cur.execute("FLUSH")
    finally:
        conn.close()
        duck.close()

    return {
        "status": "ok",
        "batch_num": batch_num,
        "tables_appended": len(appended),
        "tables": appended,
        "mutations": mutations,
        "flushed": flush,
    }
