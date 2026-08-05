"""Per-engine adapters for the compiler benchmark.

Error mapping matters for the reported percentages:
  QueryFailed    bad/unsupported query — run continues
  EngineTimeout  outlived its budget — run continues
  EngineCrashed  engine/session died — runner resets before the next query

Classification is two-stage: `classify()` is the engine's up-front verdict
(OpenIVM's catalog, Databricks' EXPLAIN), `observed_classification()` is what it
actually did (query log, pipeline events). The runner prefers the observed one.
`unknown` means we could not interrogate the engine — never `full`.
"""

from __future__ import annotations

import logging
import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional, Sequence

from services.compiler_bench.corpus import Corpus

logger = logging.getLogger(__name__)

INCREMENTAL = "incremental"
FULL = "full"
UNKNOWN = "unknown"


class QueryFailed(Exception):
    """The engine rejected this query."""


class EngineTimeout(Exception):
    """The query exceeded its time budget."""


class EngineCrashed(Exception):
    """The engine process or session died."""


# Narrow on purpose: over-matching would turn ordinary SQL errors into crashes.
_FATAL_PATTERNS = (
    "segmentation fault", "signal 11", "signal 6", "core dumped",
    "assertion failed", "internal error", "out of memory",
    "database has been invalidated", "connection refused",
    "connection reset", "broken pipe",
)


def _is_fatal(message: str) -> bool:
    lowered = message.lower()
    return any(pattern in lowered for pattern in _FATAL_PATTERNS)


class EngineAdapter:
    name = "base"
    #: False when comparing the view to a re-run of the query is tautological
    #: (full-recompute engines) — the runner then records is_correct as
    #: "not determined" rather than claiming a pass.
    supports_verify = True

    def setup(self, corpus: Corpus) -> None:
        raise NotImplementedError

    def teardown(self) -> None:
        return None

    def reset(self) -> None:
        return None

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        raise NotImplementedError

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        raise NotImplementedError

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        return UNKNOWN

    def observed_classification(self, mv_name: str, *, timeout_s: float) -> str:
        return UNKNOWN

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        raise NotImplementedError

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        raise NotImplementedError

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        raise NotImplementedError

    def drop_mv(self, mv_name: str) -> None:
        return None


def _tpcc_table_names(schema_ddl: Sequence[str]) -> List[str]:
    names = []
    for stmt in schema_ddl:
        match = re.search(r"CREATE TABLE\s+([A-Za-z0-9_]+)", stmt, re.IGNORECASE)
        if match:
            names.append(match.group(1))
    return names


#: Types whose values can drift between a view and a re-run of the query.
#: DuckDB's native AVG uses compensated arithmetic that an incremental
#: SUM/COUNT cannot reproduce bit-for-bit, so an AVG over DECIMAL differs in the
#: last places. DECIMAL itself is exact and is NOT rounded.
_FLOAT_TYPES = ("DOUBLE", "FLOAT", "REAL")


def _is_float_type(sql_type: str) -> bool:
    upper = (sql_type or "").upper()
    return any(t in upper for t in _FLOAT_TYPES)


def _verify_probe(
    mv_name: str, sql: str, columns: Optional[Sequence[tuple]] = None
) -> str:
    """Symmetric EXCEPT ALL, compared by position with float tolerance.

    The view's column names are engine-sanitised and no longer match the query's
    output names, so both sides are aliased to synthetic names (c0, c1, …) and
    compared positionally. Float columns are rounded to 10 decimals — without
    that, every AVG-over-DECIMAL query reports a spurious correctness failure
    (the C++ benchmark applies the same tolerance).

    Falls back to a plain positional comparison when column types are unknown.
    """
    if not columns:
        return (
            f"SELECT (SELECT count(*) FROM ((SELECT * FROM {mv_name}) "
            f"EXCEPT ALL (SELECT * FROM ({sql}) __q)) __d1) "
            f"+ (SELECT count(*) FROM ((SELECT * FROM ({sql}) __q2) "
            f"EXCEPT ALL (SELECT * FROM {mv_name})) __d2) AS diff"
        )

    alias = ", ".join(f"c{i}" for i in range(len(columns)))
    projection = ", ".join(
        f"round(CAST(c{i} AS DOUBLE), 10) AS c{i}" if _is_float_type(col_type)
        else f"c{i}"
        for i, (_, col_type) in enumerate(columns)
    )
    left = f"SELECT {projection} FROM (SELECT * FROM {mv_name}) AS __m({alias})"
    right = f"SELECT {projection} FROM ({sql}) AS __q({alias})"
    return (
        f"SELECT (SELECT count(*) FROM (({left}) EXCEPT ALL ({right})) __d1) "
        f"+ (SELECT count(*) FROM (({right}) EXCEPT ALL ({left})) __d2) AS diff"
    )


# ---------------------------------------------------------------------------
# DuckDB family — a CLI subprocess per phase
# ---------------------------------------------------------------------------


class _DuckDBCliAdapter(EngineAdapter):
    """One CLI process per phase against a persistent database file.

    Slower than holding a connection open, but it is what buys crash isolation:
    a query that segfaults DuckDB takes down only its own process, as the C++
    benchmark's fork-per-query does. The database file is this benchmark's own,
    never the main benchmark's, so corruption cannot damage a real run.
    """

    binary_env = "DUCKDB_OPENIVM_BIN"
    binary_default = "/data/bin/duckdb-openivm/duckdb"
    work_dir_env = "DUCKDB_OPENIVM_WORK_DIR"
    work_dir_default = "/data/processed/duckdb-openivm"
    load_extension: Optional[str] = "openivm"

    def __init__(self) -> None:
        self._binary = os.environ.get(self.binary_env, self.binary_default)
        work_dir = Path(os.environ.get(self.work_dir_env, self.work_dir_default))
        self._db_path = work_dir / "compiler-bench.duckdb"
        self._temp_dir = work_dir / "_compiler_bench_tmp"
        # DuckLake storage mode. The real duckdb / duckdb-openivm engines are
        # DuckLake-backed, so this is the configuration the timed benchmark
        # actually uses; plain DuckDB tables are the simpler comparison point.
        # The translated corpus is storage-agnostic (unqualified table names), so
        # the same queries run either way — only where the tables live changes.
        self._ducklake = os.environ.get("COMPILER_BENCH_DUCKLAKE", "0").strip().lower() in (
            "1", "true", "yes", "on",
        )
        self._ducklake_meta = work_dir / "compiler-bench.ducklake"
        self._ducklake_data = work_dir / "compiler-bench-ducklake-data"
        prefix = self.name.upper().replace("-", "_")
        self._mem_limit = os.environ.get(f"{prefix}_MEM_LIMIT", "")
        self._threads = os.environ.get(f"{prefix}_THREADS", "")
        self._corpus: Optional[Corpus] = None

    @property
    def storage_label(self) -> str:
        return "ducklake" if self._ducklake else "duckdb"

    def _preamble(self, *, bail: bool = True) -> List[str]:
        # icu is loaded best-effort ahead of `.bail on`: a few corpus queries need
        # it, but it must not abort the run when the extension is unavailable.
        lines = [".bail off", "INSTALL icu;", "LOAD icu;"]
        lines.append(".bail on" if bail else ".bail off")
        lines.append(f"SET temp_directory='{self._temp_dir}';")
        if self._mem_limit:
            lines.append(f"SET memory_limit='{self._mem_limit}';")
        if self._threads:
            lines.append(f"SET threads={int(self._threads)};")
        if self.load_extension:
            lines.append(f"LOAD {self.load_extension};")
        if self._ducklake:
            # Every phase runs in its own process, so the attach is repeated
            # rather than held open.
            #
            # DuckDB-backed metadata, NOT the `ducklake:sqlite:` the timed engine
            # uses (services/duckdb_openivm_sources.py). Reopening a SQLite
            # metadata catalog once per phase races on its lock: the refresh dies
            # with `Failed to commit DuckLake transaction ... database is locked`
            # for roughly half the corpus. The metadata backend is not what this
            # benchmark measures — the DuckLake storage and scan path are
            # identical either way — so it trades that for a stable run.
            lines += [
                "INSTALL ducklake;",
                "LOAD ducklake;",
                f"ATTACH IF NOT EXISTS 'ducklake:{self._ducklake_meta}' AS ducklake "
                f"(DATA_PATH '{self._ducklake_data}', data_inlining_row_limit 0);",
                "USE ducklake.main;",
            ]
        return lines

    def _run(
        self, statements: Sequence[str], *, timeout_s: float, json_out: bool = False
    ) -> str:
        script = self._preamble()
        if json_out:
            script.append(".mode json")
        script += [s.strip().rstrip(";") + ";" for s in statements if s.strip()]
        try:
            proc = subprocess.run(
                [self._binary, "-unsigned", str(self._db_path)],
                input="\n".join(script) + "\n",
                text=True,
                capture_output=True,
                timeout=max(1.0, timeout_s),
            )
        except subprocess.TimeoutExpired:
            raise EngineTimeout(f"{self.name}: CLI exceeded {timeout_s:.0f}s")
        except OSError as exc:
            raise EngineCrashed(f"{self.name}: cannot launch CLI: {exc}") from exc

        if proc.returncode != 0:
            message = (proc.stderr or proc.stdout or "").strip()
            if proc.returncode < 0 or _is_fatal(message):
                raise EngineCrashed(
                    f"{self.name}: CLI died (rc={proc.returncode}): {message[:800]}"
                )
            raise QueryFailed(f"{self.name}: {message[:800]}")
        return proc.stdout

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        # Start from a clean slate so a previous run's views cannot influence
        # this one's verdicts.
        for suffix in ("", ".wal"):
            path = Path(str(self._db_path) + suffix)
            if path.exists():
                path.unlink()
        if self._ducklake:
            for path in (self._ducklake_meta, Path(str(self._ducklake_meta) + "-wal")):
                if path.exists():
                    path.unlink()
            shutil.rmtree(self._ducklake_data, ignore_errors=True)
            self._ducklake_data.mkdir(parents=True, exist_ok=True)

        statements = list(corpus.schema_ddl)
        data_dir = _tpcc_data_dir(corpus)
        for table in _tpcc_table_names(corpus.schema_ddl):
            statements.append(
                f"INSERT INTO {table} SELECT * FROM read_parquet('{data_dir}/{table}.parquet')"
            )
        if statements:
            self._run(statements, timeout_s=1800)

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        self._run([f"SELECT * FROM ({sql}) __cb LIMIT 0"], timeout_s=timeout_s)

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        if not statements:
            return
        # Individual deltas may legitimately fail (duplicate key), as in the C++
        # benchmark, so run with bail off and ignore per-statement errors.
        script = self._preamble(bail=False)
        script += [s.strip().rstrip(";") + ";" for s in statements]
        try:
            subprocess.run(
                [self._binary, "-unsigned", str(self._db_path)],
                input="\n".join(script) + "\n",
                text=True,
                capture_output=True,
                timeout=max(1.0, timeout_s),
            )
        except subprocess.TimeoutExpired:
            raise EngineTimeout(f"{self.name}: delta batch exceeded {timeout_s:.0f}s")

    def _describe(self, mv_name: str, *, timeout_s: float) -> List[tuple]:
        """(name, type) per column, in order. Empty when it cannot be read."""
        try:
            out = self._run(
                [f"DESCRIBE SELECT * FROM {mv_name}"], timeout_s=timeout_s, json_out=True
            )
        except (QueryFailed, EngineTimeout):
            return []
        columns = []
        for name, col_type in re.findall(
            r'"column_name":"(.*?)","column_type":"(.*?)"', out
        ):
            columns.append((name, col_type))
        return columns

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        columns = self._describe(mv_name, timeout_s=timeout_s)
        out = self._run(
            [_verify_probe(mv_name, sql, columns)], timeout_s=timeout_s, json_out=True
        )
        match = re.search(r'"diff":\s*(-?\d+)', out)
        if not match:
            raise QueryFailed(f"{self.name}: verification produced no comparable result")
        return int(match.group(1)) == 0


def _tpcc_data_dir(corpus: Corpus) -> str:
    default = f"/data/compiler-bench/data/sf{corpus.meta.get('scale_factor', 3)}"
    return os.environ.get("COMPILER_BENCH_TPCC_DATA_DIR", default)


class DuckDBOpenIVMAdapter(_DuckDBCliAdapter):
    name = "duckdb-openivm"

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._run([f"CREATE MATERIALIZED VIEW {mv_name} AS {sql}"], timeout_s=timeout_s)

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        # openivm_views.type 3 is FULL_REFRESH; other values are incremental
        # strategies. Same test the C++ benchmark makes.
        catalog = self._db_path.stem
        out = self._run(
            [
                f'SELECT type FROM "{catalog}".main.openivm_views '
                f"WHERE view_name = '{mv_name}'"
            ],
            timeout_s=timeout_s,
            json_out=True,
        )
        match = re.search(r'"type":\s*(\d+)', out)
        if not match:
            return UNKNOWN
        return FULL if int(match.group(1)) == 3 else INCREMENTAL

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._run([f"PRAGMA refresh('{mv_name}')"], timeout_s=timeout_s)

    def drop_mv(self, mv_name: str) -> None:
        try:
            self._run([f"DROP MATERIALIZED VIEW IF EXISTS {mv_name}"], timeout_s=120)
        except (QueryFailed, EngineTimeout, EngineCrashed):
            pass


class DuckDBAdapter(_DuckDBCliAdapter):
    """Vanilla DuckDB — the full-recompute baseline.

    Separates "engine cannot express this query" from "engine cannot
    incrementalize it" for the OpenIVM variant.
    """

    name = "duckdb"
    binary_env = "DUCKDB_BIN"
    binary_default = "/data/bin/duckdb-openivm/duckdb"
    work_dir_env = "DUCKDB_WORK_DIR"
    work_dir_default = "/data/processed/duckdb"
    load_extension = None
    supports_verify = False

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._run([f"CREATE OR REPLACE TABLE {mv_name} AS {sql}"], timeout_s=timeout_s)

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        # A fact about the engine, not a failure to interrogate it.
        return FULL

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._run([f"CREATE OR REPLACE TABLE {mv_name} AS {sql}"], timeout_s=timeout_s)

    def drop_mv(self, mv_name: str) -> None:
        try:
            self._run([f"DROP TABLE IF EXISTS {mv_name}"], timeout_s=120)
        except (QueryFailed, EngineTimeout, EngineCrashed):
            pass


# ---------------------------------------------------------------------------
# Spark family — Livy session
# ---------------------------------------------------------------------------


class _LivyAdapter(EngineAdapter):
    """Livy-driven Spark engines.

    No process to fork: the session is remote and shared, so crash detection
    keys on transport failure and session loss, and reset() reopens the session.
    """

    name = "spark-base"
    schema = "compiler_bench"
    #: Each Spark-family engine runs its own Livy, so the endpoint is per-engine —
    #: the vanilla engine's is `spark`, not `spark-openivm`.
    livy_url_env = "SPARK_OPENIVM_LIVY_URL"
    livy_url_default = "http://spark-openivm:8998"

    def __init__(self) -> None:
        from services.spark_openivm_sources import LivyClient

        self._client_factory = LivyClient
        self._livy_url = os.environ.get(self.livy_url_env, self.livy_url_default)
        self._client = None
        self._corpus: Optional[Corpus] = None

    def _ensure_client(self):
        if self._client is None:
            self._client = self._client_factory(base_url=self._livy_url)
            self._client.open()
            self._client.execute(f"CREATE DATABASE IF NOT EXISTS {self.schema}")
            self._client.execute(f"USE {self.schema}")
        return self._client

    def reset(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                logger.debug("[%s] closing dead session failed", self.name, exc_info=True)
        self._client = None
        self._ensure_client()

    def teardown(self) -> None:
        if self._client is None:
            return
        try:
            self._client.execute(f"DROP DATABASE IF EXISTS {self.schema} CASCADE")
        except Exception:
            logger.debug("[%s] dropping benchmark schema failed", self.name, exc_info=True)
        try:
            self._client.close()
        except Exception:
            pass
        self._client = None

    def _execute(self, sql: str, *, timeout_s: float) -> dict:
        import requests

        client = self._ensure_client()
        try:
            return client.execute(sql)
        except TimeoutError as exc:
            raise EngineTimeout(f"{self.name}: {exc}") from exc
        except requests.RequestException as exc:
            raise EngineCrashed(f"{self.name}: Livy transport failure: {exc}") from exc
        except RuntimeError as exc:
            message = str(exc)
            lowered = message.lower()
            if _is_fatal(message) or ("session" in lowered and "dead" in lowered):
                raise EngineCrashed(f"{self.name}: {message[:800]}") from exc
            raise QueryFailed(f"{self.name}: {message[:800]}") from exc

    @staticmethod
    def _rows(result: dict) -> List[list]:
        payload = ((result or {}).get("data") or {}).get("application/json") or {}
        if isinstance(payload, dict):
            return [list(r) for r in (payload.get("data") or [])]
        return []

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        self._ensure_client()
        data_dir = _tpcc_data_dir(corpus)
        for stmt in corpus.schema_ddl:
            table = _tpcc_table_names([stmt])[0]
            self._execute(f"DROP TABLE IF EXISTS {table}", timeout_s=300)
            # Delta everywhere in this family: Databricks needs it for
            # incremental maintenance, so all Spark engines see one format.
            self._execute(f"{stmt} USING DELTA", timeout_s=300)
            self._execute(
                f"INSERT INTO {table} SELECT * FROM parquet.`{data_dir}/{table}.parquet`",
                timeout_s=1800,
            )

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        self._execute(f"SELECT * FROM ({sql}) __cb LIMIT 0", timeout_s=timeout_s)

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        for stmt in statements:
            try:
                self._execute(stmt, timeout_s=timeout_s)
            except QueryFailed:
                continue

    def _describe(self, mv_name: str, *, timeout_s: float) -> List[tuple]:
        try:
            rows = self._rows(self._execute(f"DESCRIBE {mv_name}", timeout_s=timeout_s))
        except (QueryFailed, EngineTimeout):
            return []
        # Spark's DESCRIBE appends partition/metadata sections after a blank row.
        columns = []
        for row in rows:
            if not row or not str(row[0]).strip() or str(row[0]).startswith("#"):
                break
            columns.append((str(row[0]), str(row[1]) if len(row) > 1 else ""))
        return columns

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        columns = self._describe(mv_name, timeout_s=timeout_s)
        rows = self._rows(
            self._execute(_verify_probe(mv_name, sql, columns), timeout_s=timeout_s)
        )
        if not rows or not rows[0]:
            raise QueryFailed(f"{self.name}: verification produced no comparable result")
        return int(rows[0][0]) == 0


class SparkOpenIVMAdapter(_LivyAdapter):
    name = "spark-openivm"

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._execute(f"CREATE MATERIALIZED VIEW {mv_name} AS ({sql})", timeout_s=timeout_s)

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._execute(f"REFRESH MATERIALIZED VIEW {mv_name}", timeout_s=timeout_s)

    def observed_classification(self, mv_name: str, *, timeout_s: float) -> str:
        # SHOW OPENIVM QUERY LOG carries a `mode` per view per refresh;
        # FULL_REFRESH means the extension gave up on incremental maintenance
        # (including when per-view compilation failed).
        try:
            rows = self._rows(self._execute("SHOW OPENIVM QUERY LOG", timeout_s=timeout_s))
        except (QueryFailed, EngineTimeout):
            return UNKNOWN
        mode = None
        for row in rows:
            if len(row) > 5 and str(row[1]).lower().endswith(mv_name.lower()):
                mode = str(row[5])
        if mode is None:
            return UNKNOWN
        return FULL if "full" in mode.lower() else INCREMENTAL

    def drop_mv(self, mv_name: str) -> None:
        try:
            self._execute(
                f"DROP MATERIALIZED VIEW IF EXISTS {mv_name} CASCADE", timeout_s=300
            )
        except Exception:
            pass


class SparkAdapter(_LivyAdapter):
    name = "spark"
    supports_verify = False
    livy_url_env = "SPARK_LIVY_URL"
    livy_url_default = "http://spark:8998"

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._execute(
            f"CREATE OR REPLACE TABLE {mv_name} USING DELTA AS {sql}", timeout_s=timeout_s
        )

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        return FULL

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._execute(
            f"CREATE OR REPLACE TABLE {mv_name} USING DELTA AS {sql}", timeout_s=timeout_s
        )

    def drop_mv(self, mv_name: str) -> None:
        try:
            self._execute(f"DROP TABLE IF EXISTS {mv_name}", timeout_s=300)
        except Exception:
            pass


_LOCAL_ADAPTERS = {
    "duckdb-openivm": DuckDBOpenIVMAdapter,
    "duckdb": DuckDBAdapter,
    "spark-openivm": SparkOpenIVMAdapter,
    "spark": SparkAdapter,
}


def get_adapter(engine: str) -> EngineAdapter:
    if engine in _LOCAL_ADAPTERS:
        return _LOCAL_ADAPTERS[engine]()
    from services.compiler_bench import engines_cloud

    adapter = engines_cloud.get_adapter(engine)
    if adapter is None:
        raise ValueError(f"no compiler-bench adapter for engine {engine!r}")
    return adapter
