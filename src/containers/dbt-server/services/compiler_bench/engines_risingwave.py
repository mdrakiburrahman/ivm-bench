"""compiler-bench adapter for RisingWave.

RisingWave has no full-recompute mode: a MATERIALIZED VIEW is a streaming
dataflow, so a query the planner accepts is maintained incrementally and one it
rejects is not maintained at all. Classification is therefore constant, the same
shape as Feldera — the interesting number is what fraction of the corpus the
planner accepts.

Two adaptations are required, both measured against RisingWave 3.0.2 and both
about SQL surface rather than incrementalization power:

* the schema and any cast must drop length/precision parameters — RisingWave
  rejects ``VARCHAR(n)``, ``CHAR(n)`` and ``DECIMAL(p,s)`` outright;
* the MV needs an explicit positional column list, because a corpus query that
  does ``SELECT *`` over a join repeats output names and RisingWave refuses
  ``column "w_id" specified more than once``.

Without the first, 55/499 corpus queries fail before the MV is even attempted;
without the second, a further 60 fail at MV creation.
"""

from __future__ import annotations

import logging
import os
import re
from typing import List, Optional, Sequence

from services.compiler_bench.corpus import Corpus
from services.compiler_bench.engines import (
    INCREMENTAL,
    UNKNOWN,
    EngineAdapter,
    EngineCrashed,
    EngineTimeout,
    QueryFailed,
)

logger = logging.getLogger(__name__)

# RisingWave's parser rejects every length- and precision-parameterised type,
# in DDL and in casts alike. Its VARCHAR is unbounded and its DECIMAL carries 28
# significant digits, so dropping the parameters only widens the domain.
_PARAM_VARCHAR = re.compile(r"\b(VARCHAR|CHAR|CHARACTER VARYING)\s*\(\s*\d+\s*\)", re.I)
_PARAM_DECIMAL = re.compile(r"\b(DECIMAL|NUMERIC)\s*\(\s*\d+\s*,\s*\d+\s*\)", re.I)


def strip_type_params(sql: str) -> str:
    sql = _PARAM_VARCHAR.sub("VARCHAR", sql)
    return _PARAM_DECIMAL.sub("DECIMAL", sql)


class RisingWaveAdapter(EngineAdapter):
    name = "risingwave"
    supports_verify = True

    #: RisingWave panics and drops the session on a multi-row INSERT with ~42k
    #: bound parameters; 21k is fine. Cap well below the observed cliff.
    MAX_BIND_PARAMS = 10000

    def __init__(self) -> None:
        self._host = os.environ.get("RISINGWAVE_HOST", "risingwave")
        self._port = int(os.environ.get("RISINGWAVE_PORT", "4566"))
        self._user = os.environ.get("RISINGWAVE_USER", "root")
        self._password = os.environ.get("RISINGWAVE_PASSWORD", "")
        self._database = os.environ.get("RISINGWAVE_DATABASE", "dev")
        self._conn = None
        self._corpus: Optional[Corpus] = None
        self._last_verification_error = ""

    # ----- connection -----

    @property
    def _dsn(self) -> str:
        auth = f"{self._user}:{self._password}" if self._password else self._user
        return f"postgresql://{auth}@{self._host}:{self._port}/{self._database}"

    def _connect(self):
        import psycopg

        try:
            return psycopg.connect(self._dsn, autocommit=True)
        except Exception as exc:
            raise EngineCrashed(f"{self.name}: cannot connect: {exc}") from exc

    def _cursor(self):
        if self._conn is None or self._conn.closed:
            self._conn = self._connect()
        return self._conn.cursor()

    def reset(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        self._conn = None

    def teardown(self) -> None:
        self.reset()

    def _execute(self, sql: str, *, timeout_s: float, params=None):
        """Run one statement, mapping RisingWave's failure modes onto the runner's."""
        cur = self._cursor()
        try:
            cur.execute(f"SET statement_timeout = '{max(1, int(timeout_s))}s'")
        except Exception:
            pass
        try:
            cur.execute(sql, params)
            return cur
        except Exception as exc:
            message = " ".join(str(exc).split())
            # A panic takes the session down with it; the next call reconnects.
            if "Panicked" in message or "connection is lost" in message:
                self.reset()
                raise EngineCrashed(f"{self.name}: {message[:600]}") from exc
            if "timed out" in message.lower() or "statement_timeout" in message.lower():
                raise EngineTimeout(f"{self.name}: {message[:400]}") from exc
            raise QueryFailed(f"{self.name}: {message[:800]}") from exc

    # ----- phases -----

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        cur = self._cursor()
        for stmt in corpus.schema_ddl:
            table = re.search(r"CREATE TABLE\s+([A-Za-z0-9_]+)", stmt, re.I)
            if table:
                try:
                    cur.execute(f"DROP TABLE IF EXISTS {table.group(1)} CASCADE")
                except Exception:
                    self.reset()
                    cur = self._cursor()
            self._execute(strip_type_params(stmt.rstrip(";")), timeout_s=120)
        self._load_tpcc(cur)

    def _load_tpcc(self, cur) -> None:
        """Stream the corpus Parquet into RisingWave.

        RisingWave's `file_scan` covers only s3/gcs/azblob and there is no COPY
        FROM STDIN, so the rows go in as bound-parameter INSERTs read through
        DuckDB — the same path services/risingwave_sources.py uses.
        """
        import duckdb

        from services.compiler_bench.engines import _tpcc_data_dir, _tpcc_table_names

        assert self._corpus is not None
        data_dir = _tpcc_data_dir(self._corpus)
        duck = duckdb.connect(":memory:")
        try:
            for table in _tpcc_table_names(self._corpus.schema_ddl):
                path = f"{data_dir}/{table}.parquet"
                if not os.path.exists(path):
                    continue
                described = duck.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0")
                ncols = len(described.description or ())
                if not ncols:
                    continue
                batch = max(1, self.MAX_BIND_PARAMS // ncols)
                placeholder = "(" + ",".join(["%s"] * ncols) + ")"
                reader = duck.execute(f"SELECT * FROM read_parquet('{path}')")
                while True:
                    chunk = reader.fetchmany(20000)
                    if not chunk:
                        break
                    for i in range(0, len(chunk), batch):
                        rows = chunk[i : i + batch]
                        sql = (
                            f"INSERT INTO {table} VALUES "
                            + ",".join([placeholder] * len(rows))
                        )
                        self._execute(
                            sql, timeout_s=600, params=[v for r in rows for v in r]
                        )
        finally:
            duck.close()
        # INSERT is asynchronous: without this the rows are not yet readable.
        self._execute("FLUSH", timeout_s=600)

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        cur = self._execute(
            f"SELECT * FROM ({strip_type_params(sql)}) __cb LIMIT 0", timeout_s=timeout_s
        )
        cur.fetchall()

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        sql = strip_type_params(sql)
        cur = self._execute(f"SELECT * FROM ({sql}) __cb LIMIT 0", timeout_s=timeout_s)
        cur.fetchall()
        ncols = len(cur.description or ())
        columns = f" ({','.join(f'c{i}' for i in range(ncols))})" if ncols else ""
        self._execute(
            f"CREATE MATERIALIZED VIEW {mv_name}{columns} AS {sql}", timeout_s=timeout_s
        )

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        # An accepted MV is a streaming dataflow; there is no full-recompute mode
        # to fall back to, so acceptance IS the verdict.
        return INCREMENTAL

    def observed_classification(self, mv_name: str, *, timeout_s: float) -> str:
        return UNKNOWN

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        for stmt in statements:
            try:
                self._execute(strip_type_params(stmt), timeout_s=timeout_s)
            except EngineCrashed:
                raise
            except Exception:
                # Individual deltas may legitimately fail (duplicate key), as in
                # the C++ benchmark; that is not a verdict on the query.
                logger.debug("[%s] delta failed: %s", self.name, stmt[:120])

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        # Maintenance is continuous. FLUSH returns once every pending barrier has
        # been applied, which is the point the view is up to date.
        self._execute("FLUSH", timeout_s=timeout_s)

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        """Compare the view against a re-run of the query, positionally.

        RisingWave has EXCEPT but not EXCEPT ALL, so the runner's SQL probe
        cannot be used; the comparison is done here as a multiset instead.
        """
        self._last_verification_error = ""
        cur = self._execute(f"SELECT * FROM {mv_name}", timeout_s=timeout_s)
        got = self._multiset(cur.fetchall())
        cur = self._execute(
            f"SELECT * FROM ({strip_type_params(sql)}) __q", timeout_s=timeout_s
        )
        want = self._multiset(cur.fetchall())
        if got == want:
            return True
        only_mv = sum((got - want).values())
        only_query = sum((want - got).values())
        self._last_verification_error = (
            f"multiset differs: {only_mv} row(s) only in view, "
            f"{only_query} row(s) only in query"
        )
        return False

    @staticmethod
    def _multiset(rows: Sequence[Sequence]):
        import datetime
        import decimal
        from collections import Counter

        def norm(value):
            # Float tolerance mirrors the runner's SQL probe: an incrementally
            # maintained SUM/COUNT cannot reproduce a batch AVG bit-for-bit.
            if isinstance(value, float):
                return round(value, 10)
            if isinstance(value, decimal.Decimal):
                return round(float(value), 10)
            if isinstance(value, (datetime.datetime, datetime.date)):
                return value.isoformat()
            # RisingWave returns ARRAY columns as Python lists and composite
            # types as dicts, neither of which is hashable — Counter then raises
            # "unhashable type: 'list'", which the runner reports as an engine
            # CRASH even though the engine answered fine. Recurse so nested
            # arrays normalise too.
            if isinstance(value, (list, tuple)):
                return tuple(norm(v) for v in value)
            if isinstance(value, dict):
                return tuple(sorted((k, norm(v)) for k, v in value.items()))
            return value

        return Counter(tuple(norm(c) for c in row) for row in rows)

    def verification_error(self) -> str:
        return self._last_verification_error

    def drop_mv(self, mv_name: str) -> None:
        try:
            self._execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}", timeout_s=120)
        except Exception:
            self.reset()
