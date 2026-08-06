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

import json
import logging
import os
import queue
import re
import shutil
import subprocess
import threading
import time
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
# DuckDB family — one persistent CLI worker driven over pipes
# ---------------------------------------------------------------------------


class _DuckDBSession:
    """A long-lived DuckDB CLI process, one statement at a time.

    Mirrors the C++ benchmark's worker: start once, hold ONE connection across
    every phase of every query, re-spawn only after a crash. A process per phase
    would instead race on whatever lock the connection holds — with DuckLake's
    SQLite metadata catalog that surfaces as `Failed to commit DuckLake
    transaction ... database is locked`.

    Protocol: each statement is bracketed by marker SELECTs on stdout so the end
    of its output is unambiguous, while stderr is drained concurrently. Whatever
    stderr produced during a statement is that statement's error; `.bail off`
    keeps the session alive so the next query still runs.
    """

    _POLL_S = 0.25
    #: stdout and stderr are separate pipes, so a failing statement's stderr can
    #: trail its closing stdout marker by a hair.
    _STDERR_GRACE_S = 0.05

    def __init__(self, binary: str, db_path: str, preamble: Sequence[str]) -> None:
        self._binary = binary
        self._db_path = db_path
        self._preamble = list(preamble)
        self._proc: Optional[subprocess.Popen] = None
        self._stdout: "queue.Queue" = queue.Queue()
        self._stderr: "queue.Queue" = queue.Queue()
        self._seq = 0

    def start(self) -> None:
        self.close()
        try:
            self._proc = subprocess.Popen(
                [self._binary, "-unsigned", self._db_path],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            raise EngineCrashed(f"cannot launch {self._binary}: {exc}") from exc

        self._stdout = queue.Queue()
        self._stderr = queue.Queue()
        threading.Thread(target=self._drain, args=(self._proc.stdout, self._stdout, True),
                         daemon=True).start()
        threading.Thread(target=self._drain, args=(self._proc.stderr, self._stderr, False),
                         daemon=True).start()

        # `.bail off` keeps one bad query from ending the session; markers plus the
        # error stream say what happened. JSON mode makes results parseable.
        self._write(".bail off\n.mode json\n")
        for statement in self._preamble:
            self._write(statement.rstrip(";") + ";\n")
        # Preamble errors are tolerated (icu may be unavailable) and its output is
        # drained here so it cannot be mistaken for the first statement's.
        self.execute("SELECT 1 AS __ready", timeout_s=300, tolerate_errors=True)

    def close(self) -> None:
        proc, self._proc = self._proc, None
        if proc is None:
            return
        try:
            if proc.stdin:
                proc.stdin.close()
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass

    @property
    def alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    @staticmethod
    def _drain(stream, sink: "queue.Queue", mark_eof: bool) -> None:
        for line in stream:
            sink.put(line)
        if mark_eof:
            sink.put(None)  # the process is gone

    def _write(self, text: str) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise EngineCrashed("session is not running")
        try:
            self._proc.stdin.write(text)
            self._proc.stdin.flush()
        except (BrokenPipeError, ValueError) as exc:
            raise EngineCrashed(f"session died while writing: {exc}") from exc

    def _take_stderr(self) -> str:
        lines = []
        while True:
            try:
                lines.append(self._stderr.get_nowait())
            except queue.Empty:
                break
        return "".join(lines).strip()

    def execute(
        self, sql: str, *, timeout_s: float, tolerate_errors: bool = False
    ) -> str:
        """Run one statement, returning its stdout.

        Raises QueryFailed on a SQL error, EngineTimeout when it outlives the
        budget (the process is killed — the CLI offers no statement-level
        cancel), EngineCrashed when the process died.
        """
        if not self.alive:
            raise EngineCrashed("session is not running")

        self._take_stderr()  # drop anything left over from an earlier statement
        self._seq += 1
        begin, end = f"<<<B{self._seq}>>>", f"<<<E{self._seq}>>>"
        self._write(
            f"SELECT '{begin}' AS __m;\n{sql.rstrip().rstrip(';')};\n"
            f"SELECT '{end}' AS __m;\n"
        )

        deadline = time.monotonic() + max(1.0, timeout_s)
        out: List[str] = []
        started = False
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.close()
                raise EngineTimeout(f"statement exceeded {timeout_s:.0f}s")
            try:
                line = self._stdout.get(timeout=min(remaining, self._POLL_S))
            except queue.Empty:
                continue
            if line is None:
                error = self._take_stderr()
                self._proc = None
                raise EngineCrashed(f"session died: {error[:800] or 'no output'}")
            if end in line:
                break
            if begin in line:
                started = True
                continue
            if started:
                out.append(line)

        time.sleep(self._STDERR_GRACE_S)
        error = self._take_stderr()
        if error and not tolerate_errors:
            if _is_fatal(error):
                raise EngineCrashed(error[:800])
            raise QueryFailed(error[:800])
        return "".join(out)


class _DuckDBCliAdapter(EngineAdapter):
    """DuckDB-family engines, driven through one persistent CLI worker.

    Crash isolation comes from the worker being a separate process: a query that
    segfaults DuckDB takes the worker down rather than the dbt-server, and the
    runner calls reset() to spawn a fresh one — the same fork-and-respawn
    arrangement as the C++ benchmark. The database file is this benchmark's own,
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
        self._ducklake_meta = work_dir / "compiler-bench.ducklake.db"
        self._ducklake_data = work_dir / "compiler-bench-ducklake-data"
        prefix = self.name.upper().replace("-", "_")
        self._mem_limit = os.environ.get(f"{prefix}_MEM_LIMIT", "")
        self._threads = os.environ.get(f"{prefix}_THREADS", "")
        self._corpus: Optional[Corpus] = None
        self._session: Optional[_DuckDBSession] = None

    @property
    def storage_label(self) -> str:
        return "ducklake" if self._ducklake else "duckdb"

    def _preamble(self) -> List[str]:
        # Deliberately NOT loading icu, even though the translation step does.
        # icu changes string collation, which changes what EXCEPT ALL considers
        # equal: with it loaded the verification probe reports differences for
        # queries whose results are in fact identical (measured: 10 of 60 became
        # spurious verify_failed). The C++ benchmark does not load it either, so
        # leaving it out also keeps verdicts comparable with the reference. A
        # query that genuinely needs icu to run fails as base_query_failed, which
        # is an honest verdict.
        lines = [f"SET temp_directory='{self._temp_dir}'"]
        if self._mem_limit:
            lines.append(f"SET memory_limit='{self._mem_limit}'")
        if self._threads:
            lines.append(f"SET threads={int(self._threads)}")
        if self.load_extension:
            lines.append(f"LOAD {self.load_extension}")
        if self._ducklake:
            # Byte-for-byte the attach the C++ benchmark uses (rewriter_benchmark
            # .cpp: `ATTACH IF NOT EXISTS '<db>.ducklake.db' AS dl (TYPE
            # ducklake)`), so DuckLake results stay comparable with the reference.
            #
            # Metadata is DuckDB-backed, NOT the `ducklake:sqlite:` the timed
            # duckdb-openivm engine uses. That is not a shortcut: OpenIVM's
            # refresh cannot commit against DuckLake with SQLite metadata at all
            # — `PRAGMA refresh` dies with `Failed to commit DuckLake transaction
            # ... database is locked` on a single connection in a single process,
            # regardless of openivm_cascade_refresh. The C++ benchmark avoids it
            # the same way.
            lines += [
                "INSTALL ducklake",
                "LOAD ducklake",
                f"ATTACH IF NOT EXISTS '{self._ducklake_meta}' AS dl (TYPE ducklake, "
                f"DATA_PATH '{self._ducklake_data}', data_inlining_row_limit 0)",
                "USE dl.main",
            ]
        return lines

    def _session_or_start(self) -> _DuckDBSession:
        if self._session is None or not self._session.alive:
            self._session = _DuckDBSession(
                self._binary, str(self._db_path), self._preamble()
            )
            self._session.start()
        return self._session

    def _run(
        self,
        statements: Sequence[str],
        *,
        timeout_s: float,
        json_out: bool = False,
        tolerate_errors: bool = False,
    ) -> str:
        """Run statements on the persistent worker, returning the last output.

        ``json_out`` is kept for call-site clarity; the session is always in JSON
        mode, so it is a no-op.
        """
        session = self._session_or_start()
        deadline = time.monotonic() + max(1.0, timeout_s)
        out = ""
        for statement in statements:
            if not statement.strip():
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise EngineTimeout(f"{self.name}: exceeded {timeout_s:.0f}s")
            try:
                out = session.execute(
                    statement, timeout_s=remaining, tolerate_errors=tolerate_errors
                )
            except (QueryFailed, EngineTimeout, EngineCrashed) as exc:
                # Prefix the engine so the CSV error column names its source.
                exc.args = (f"{self.name}: {exc.args[0] if exc.args else exc}",)
                raise
        return out

    def reset(self) -> None:
        # After a crash or timeout the worker is gone or wedged; a fresh one
        # re-establishes the settings and any DuckLake attach via the preamble.
        if self._session is not None:
            self._session.close()
        self._session = None
        self._session_or_start()

    def teardown(self) -> None:
        if self._session is not None:
            self._session.close()
            self._session = None

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
        # benchmark, so errors are tolerated: a delta failure is not a verdict on
        # the query.
        self._run(statements, timeout_s=timeout_s, tolerate_errors=True)

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
        """Rows out of a Livy statement response.

        The table payload sits at result["output"]["data"]["application/json"],
        NOT result["data"] — LivyClient.execute returns the whole statement
        object, whose "output" holds the result. Dropping that level makes every
        row-returning statement silently yield nothing, which shows up as
        "classification unknown" and "verification produced no comparable
        result" rather than as an error. Same shape
        spark_openivm_profile._extract_rows consumes.
        """
        output = (result or {}).get("output") or {}
        payload = (output.get("data") or {}).get("application/json")
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
            # Delta with the change data feed on. The spark-openivm engine runs
            # with `spark.openivm.changeFeed.mode=cdf`, which refuses to create a
            # view over any source lacking `delta.enableChangeDataFeed` — without
            # it every CREATE MATERIALIZED VIEW fails. Vanilla Spark does not need
            # it, but both engines get the same table properties so the two are
            # comparing views, not storage configurations.
            self._execute(
                f"{stmt} USING DELTA "
                "TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
                timeout_s=300,
            )
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

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        """Ask the extension up front, via its own dry-run verdict.

        `EXPLAIN CREATE MATERIALIZED VIEW` compiles and classifies exactly as a
        real CREATE would but materialises nothing, returning one JSON row with
        `eligible` and `refresh_type` (refresh_type 3 is FULL_REFRESH, mirroring
        openivm's DuckDB catalog). Asking here rather than only reading the
        post-refresh query log means a view still gets a verdict when the log is
        unavailable — and it is the same shape as the databricks-enzyme probe.

        Uses a throwaway name: the command registers the explained view in the
        session's dry-run registry, which would otherwise collide with the real
        CREATE that follows.
        """
        rows = self._rows(
            self._execute(
                f"EXPLAIN CREATE MATERIALIZED VIEW {mv_name}_explain AS ({sql})",
                timeout_s=timeout_s,
            )
        )
        if not rows or not rows[0]:
            return UNKNOWN
        try:
            verdict = json.loads(str(rows[0][0]))
        except (json.JSONDecodeError, TypeError):
            return UNKNOWN
        if "eligible" in verdict:
            return INCREMENTAL if verdict["eligible"] else FULL
        refresh_type = verdict.get("refresh_type")
        if refresh_type is None:
            return UNKNOWN
        return FULL if int(refresh_type) == 3 else INCREMENTAL

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
