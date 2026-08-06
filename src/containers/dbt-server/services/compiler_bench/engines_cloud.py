"""Cloud/managed engine adapters: databricks-enzyme, feldera, fabric.

Split from engines.py because these need live credentials to exercise, so they
are validated by a GCI run rather than on a dev box.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeout
from typing import List, Optional, Sequence

from services.compiler_bench.corpus import Corpus
from services.compiler_bench.engines import (
    FULL,
    INCREMENTAL,
    UNKNOWN,
    EngineAdapter,
    EngineCrashed,
    EngineTimeout,
    QueryFailed,
    _is_fatal,
    _tpcc_data_dir,
    _tpcc_table_names,
    _verify_probe,
)

logger = logging.getLogger(__name__)

_NOT_INCREMENTALIZABLE = "MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE"


class DatabricksEnzymeAdapter(EngineAdapter):
    """Databricks SQL materialized views with REFRESH POLICY INCREMENTAL STRICT.

    Every MV here is backed by its own Lakeflow pipeline, so a full corpus run
    creates one pipeline per query. That is the dominant cost and the reason
    `drop_mv` is not best-effort optional — the runner drops after each query.

    The up-front verdict comes from EXPLAIN against a throwaway name, which is
    planner-only and creates nothing; the observed verdict would require reading
    pipeline events per update, which costs an API round trip per query, so it is
    only consulted when EXPLAIN was inconclusive.
    """

    name = "databricks-enzyme"

    def __init__(self) -> None:
        from services import databricks_enzyme_sources as src

        self._src = src
        self._schema = f"{src.data_schema()}_compiler_bench"
        self._corpus: Optional[Corpus] = None

    def _fq(self, name: str) -> str:
        return f"`{self._src.CATALOG}`.`{self._schema}`.`{name}`"

    def _execute(self, sql: str, *, timeout_s: float):
        # The module-level connection, NOT execute_isolated: the corpus queries
        # are unqualified and rely on the `USE CATALOG` / `USE SCHEMA` issued in
        # setup(), and an isolated connection per statement drops that session
        # state — every query then fails with TABLE_OR_VIEW_NOT_FOUND. Safe here
        # because compiler-bench runs with storage metrics off and no concurrent
        # dbt run, which is what execute_isolated exists to avoid colliding with.
        #
        # That connection has no per-statement timeout, so the query budget is
        # enforced here. A timed-out statement keeps running server-side; the
        # session is reset so the next query does not inherit its state.
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                future = pool.submit(self._src._execute, sql)
                try:
                    return future.result(timeout=max(5.0, timeout_s))
                except FuturesTimeout:
                    self._reset_connection()
                    raise EngineTimeout(
                        f"{self.name}: statement exceeded {timeout_s:.0f}s"
                    )
        except (EngineTimeout, EngineCrashed):
            raise
        except Exception as exc:
            message = str(exc)
            if isinstance(exc, TimeoutError) or "timeout" in message.lower():
                raise EngineTimeout(f"{self.name}: {message[:600]}") from exc
            if _is_fatal(message):
                raise EngineCrashed(f"{self.name}: {message[:800]}") from exc
            raise QueryFailed(f"{self.name}: {message[:800]}") from exc

    def _reset_connection(self) -> None:
        """Drop the shared connection so the next statement gets a fresh one.

        Also re-issues the session default, which does not survive the drop.
        """
        try:
            self._src._drop_connection()
        except Exception:
            logger.debug("[%s] dropping connection failed", self.name, exc_info=True)
        try:
            self._src._execute(f"USE `{self._src.CATALOG}`.`{self._schema}`")
        except Exception:
            logger.warning("[%s] could not restore session schema", self.name, exc_info=True)

    def _upload_tpcc_data(self, corpus: Corpus) -> str:
        """Copy the TPC-C Parquet into a UC volume and return the volume path.

        Databricks Serverless SQL has no access to the benchmark host's
        filesystem, so reading `parquet.'<local path>'` fails with
        FAILED_TO_CREATE_PLAN_FOR_DIRECT_QUERY. The data has to live in a volume
        the warehouse can read. Reuses the same shared cache volume (and its
        size-aware sync) that init_sources uses for the TPC-DI sources, so a
        repeat run at the same scale re-uploads nothing.
        """
        from pathlib import Path

        local = Path(_tpcc_data_dir(corpus))
        if not local.is_dir():
            raise EngineCrashed(
                f"{self.name}: TPC-C Parquet not found at {local} — corpus prep "
                "must generate it before the engine runs"
            )
        scale_factor = corpus.meta.get("scale_factor", 3)
        remote = f"{self._src._cache_volume_root()}/compiler-bench/sf{scale_factor}"
        try:
            ws = self._src._workspace_client()
            self._src._ensure_cache_schema(ws)
            uploaded, skipped = self._src._sync_dir(ws, local, remote)
        except Exception as exc:
            raise EngineCrashed(
                f"{self.name}: uploading TPC-C data to {remote} failed: {exc}"
            ) from exc
        logger.info(
            "[%s] TPC-C data in volume %s (%d uploaded, %d already current)",
            self.name, remote, uploaded, skipped,
        )
        return remote

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        self._execute(
            f"CREATE SCHEMA IF NOT EXISTS `{self._src.CATALOG}`.`{self._schema}`",
            timeout_s=120,
        )
        self._execute(f"USE `{self._src.CATALOG}`.`{self._schema}`", timeout_s=60)
        data_dir = self._upload_tpcc_data(corpus)
        for stmt in corpus.schema_ddl:
            table = _tpcc_table_names([stmt])[0]
            self._execute(f"DROP TABLE IF EXISTS {self._fq(table)}", timeout_s=300)
            # Row tracking is what makes Enzyme able to maintain a view
            # incrementally over this table at all.
            self._execute(
                f"{stmt.replace(f'CREATE TABLE {table}', f'CREATE TABLE {self._fq(table)}', 1)} "
                "USING DELTA TBLPROPERTIES ("
                "'delta.enableRowTracking' = 'true', "
                "'delta.enableChangeDataFeed' = 'true')",
                timeout_s=600,
            )
            self._execute(
                f"INSERT INTO {self._fq(table)} "
                f"SELECT * FROM parquet.`{data_dir}/{table}.parquet`",
                timeout_s=1800,
            )

    def teardown(self) -> None:
        try:
            self._execute(
                f"DROP SCHEMA IF EXISTS `{self._src.CATALOG}`.`{self._schema}` CASCADE",
                timeout_s=600,
            )
        except Exception:
            logger.warning("[%s] schema cleanup failed", self.name, exc_info=True)

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        self._execute(f"SELECT * FROM ({sql}) __cb LIMIT 0", timeout_s=timeout_s)

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        probe = f"{self._fq(mv_name)}_explain"
        try:
            self._execute(
                f"EXPLAIN CREATE MATERIALIZED VIEW {probe} "
                f"REFRESH POLICY INCREMENTAL STRICT AS {sql}",
                timeout_s=timeout_s,
            )
        except QueryFailed as exc:
            if _NOT_INCREMENTALIZABLE in str(exc):
                return FULL
            raise
        return INCREMENTAL

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._execute(
            f"CREATE MATERIALIZED VIEW {self._fq(mv_name)} AS {sql}", timeout_s=timeout_s
        )

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        for stmt in statements:
            try:
                self._execute(stmt, timeout_s=timeout_s)
            except QueryFailed:
                continue

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        self._execute(
            f"REFRESH MATERIALIZED VIEW {self._fq(mv_name)}", timeout_s=timeout_s
        )

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        frame = self._execute(
            _verify_probe(self._fq(mv_name), sql), timeout_s=timeout_s
        )
        if frame is None or frame.empty:
            raise QueryFailed(f"{self.name}: verification produced no comparable result")
        return int(frame.iloc[0, 0]) == 0

    def drop_mv(self, mv_name: str) -> None:
        for stmt in (
            f"DROP MATERIALIZED VIEW IF EXISTS {self._fq(mv_name)}",
            f"DROP MATERIALIZED VIEW IF EXISTS {self._fq(mv_name)}_explain",
        ):
            try:
                self._execute(stmt, timeout_s=300)
            except Exception:
                pass


class FelderaAdapter(EngineAdapter):
    """Feldera (DBSP), run as ONE batched pipeline.

    Feldera has no per-view DDL: a pipeline is a single program, and deploying it
    compiles Rust — minutes. One program per query is therefore intractable at
    corpus scale, which is why this batches every view into one program, pays the
    compile once, ingests the data once, and then verifies each view.

    Consequences of batching, and how they are handled:
      * One view that fails to compile fails the WHOLE program. So each view is
        first probed alone through the SQL-compilation stage (seconds, no Rust)
        and only the accepted ones go into the batch. A view rejected there is
        reported as mv_creation_failed with its compiler message, which is the
        same verdict a per-view run would give.
      * Classification stays constant: DBSP maintains everything it accepts
        incrementally, with no full-recompute mode to fall back to.
      * Refresh is not a per-view operation — the pipeline maintains continuously
        — so `refresh` waits for the pipeline to quiesce after the deltas.
    """

    name = "feldera"
    supports_verify = True

    _COMPILE_POLL_S = 5.0
    _SQL_PROBE_PIPELINE = "compiler_bench_probe"

    def __init__(self) -> None:
        self._base_url = os.environ.get("FELDERA_URL", "http://pipeline-manager:8080")
        self._pipeline = os.environ.get("COMPILER_BENCH_FELDERA_PIPELINE", "compiler_bench")
        self._corpus: Optional[Corpus] = None
        self._schema_sql: List[str] = []
        #: views that survived the per-view SQL probe and are in the batch program
        self._accepted: dict = {}
        self._rejected: dict = {}
        self._deployed = False

    # ----- HTTP -----

    def _request(self, method: str, path: str, *, json_body=None, data=None,
                 params=None, timeout_s: float = 60.0):
        import requests

        url = f"{self._base_url.rstrip('/')}{path}"
        try:
            response = requests.request(
                method, url, json=json_body, data=data, params=params,
                timeout=max(5.0, timeout_s),
            )
        except requests.RequestException as exc:
            raise EngineCrashed(f"{self.name}: pipeline-manager unreachable: {exc}") from exc
        if response.status_code >= 500:
            raise EngineCrashed(
                f"{self.name}: pipeline-manager {response.status_code}: {response.text[:400]}"
            )
        return response

    @staticmethod
    def _program_status(body: dict) -> str:
        status = (body or {}).get("program_status")
        if isinstance(status, str):
            return status
        if isinstance(status, dict) and status:
            return next(iter(status))
        return ""

    def _await_program(self, pipeline: str, *, until: Sequence[str], timeout_s: float) -> dict:
        """Poll until program_status hits one of `until`, or an error status."""
        deadline = time.monotonic() + timeout_s
        while True:
            if time.monotonic() > deadline:
                raise EngineTimeout(
                    f"{self.name}: compilation exceeded {timeout_s:.0f}s"
                )
            body = self._request("GET", f"/v0/pipelines/{pipeline}", timeout_s=30).json()
            status = self._program_status(body)
            if status in ("SqlError", "RustError", "SystemError"):
                raise QueryFailed(
                    f"{self.name}: {str(body.get('program_error') or status)[:800]}"
                )
            if status in until:
                return body
            time.sleep(self._COMPILE_POLL_S)

    def _await_deployment(self, *, timeout_s: float) -> None:
        """Poll deployment_status until the pipeline can accept data."""
        deadline = time.monotonic() + timeout_s
        last = ""
        while True:
            if time.monotonic() > deadline:
                raise EngineTimeout(
                    f"{self.name}: pipeline not deployed within {timeout_s:.0f}s "
                    f"(last deployment_status={last!r})"
                )
            body = self._request(
                "GET", f"/v0/pipelines/{self._pipeline}", timeout_s=30
            ).json()
            status = body.get("deployment_status")
            last = status if isinstance(status, str) else str(status)
            if last in ("Running", "Provisioned", "Initializing", "Paused"):
                if last in ("Running", "Paused"):
                    return
            if last in ("Failed", "Stopping"):
                raise EngineCrashed(
                    f"{self.name}: pipeline deployment {last}: "
                    f"{str(body.get('deployment_error'))[:600]}"
                )
            time.sleep(self._COMPILE_POLL_S)

    # ----- program assembly -----

    def _program(self, views: Sequence[tuple]) -> str:
        """Table DDL plus one MATERIALIZED VIEW per (name, sql).

        MATERIALIZED so the ad-hoc query endpoint can read them back for
        verification; a plain VIEW in Feldera is not queryable after the fact.
        """
        # Input tables must be materialized too, not just the views: the verify
        # probe re-runs the base query through the ad-hoc endpoint, which refuses
        # to "SELECT from a non-materialized source".
        parts = [
            f"{stmt.rstrip(';')} WITH ('materialized' = 'true');"
            for stmt in self._schema_sql
        ]
        for name, sql in views:
            parts.append(f"CREATE MATERIALIZED VIEW {name} AS {sql};")
        return "\n".join(parts)

    # ----- phases -----

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        self._schema_sql = [stmt.rstrip(";") for stmt in corpus.schema_ddl]
        # Clear any pipeline left by an earlier run so its program cannot be
        # mistaken for this one's.
        for pipeline in (self._pipeline, self._SQL_PROBE_PIPELINE):
            try:
                self._request("DELETE", f"/v0/pipelines/{pipeline}", timeout_s=60)
            except Exception:
                logger.debug("[%s] no pipeline %s to clear", self.name, pipeline)

    #: Views per probe program. Chunking is what makes the pre-filter scale: a
    #: per-view probe costs one full SQL compilation (~5s measured), so 2186
    #: queries would be ~3h serial. A chunk that compiles accepts every view in
    #: it from one round trip; only failing chunks are split.
    _PROBE_CHUNK = 50

    def _chunk_compiles(self, views: Sequence[tuple], *, timeout_s: float) -> Optional[str]:
        """None if every view in `views` compiles, else the compiler's message."""
        try:
            self._request(
                "PUT",
                f"/v0/pipelines/{self._SQL_PROBE_PIPELINE}",
                json_body={
                    "name": self._SQL_PROBE_PIPELINE,
                    "program_code": self._program(list(views)),
                },
                timeout_s=timeout_s,
            )
            self._await_program(
                self._SQL_PROBE_PIPELINE,
                until=("SqlCompiled", "CompilingRust", "Success"),
                timeout_s=timeout_s,
            )
            return None
        except (QueryFailed, EngineTimeout) as exc:
            return str(exc)

    def _partition(self, views: Sequence[tuple], *, timeout_s: float) -> None:
        """Accept/reject each view, splitting only what fails to compile.

        Bisecting costs ~log2(chunk) extra compiles per bad view, which beats a
        per-view probe whenever most views compile — the measured rejection rate
        is a few percent.
        """
        if not views:
            return
        error = self._chunk_compiles(views, timeout_s=timeout_s)
        if error is None:
            for name, sql in views:
                self._accepted[name] = sql
            return
        if len(views) == 1:
            self._rejected[views[0][0]] = error
            return
        middle = len(views) // 2
        self._partition(views[:middle], timeout_s=timeout_s)
        self._partition(views[middle:], timeout_s=timeout_s)

    def build_batch(self, queries: Sequence[tuple], *, timeout_s: float) -> None:
        """Pre-filter by chunked probing, then deploy ONE program.

        Called by the runner before the per-query loop; see runner.py.
        """
        probe_budget = min(timeout_s, 600)
        for start in range(0, len(queries), self._PROBE_CHUNK):
            self._partition(
                list(queries[start : start + self._PROBE_CHUNK]), timeout_s=probe_budget
            )
        logger.info(
            "[%s] batch: %d views accepted, %d rejected by the SQL compiler",
            self.name, len(self._accepted), len(self._rejected),
        )
        if not self._accepted:
            return
        self._request(
            "PUT",
            f"/v0/pipelines/{self._pipeline}",
            json_body={
                "name": self._pipeline,
                "program_code": self._program(list(self._accepted.items())),
            },
            timeout_s=timeout_s,
        )
        # The Rust stage is the expensive one and is paid exactly once here.
        self._await_program(
            self._pipeline, until=("Success",), timeout_s=max(timeout_s, 1800)
        )
        self._request("POST", f"/v0/pipelines/{self._pipeline}/start", timeout_s=300)
        # Feldera tracks compilation and deployment separately: program_status
        # reaching Success only means the binary is built. Ingesting before
        # deployment_status leaves Stopped/Provisioning fails with
        # "PipelineInteractionNotDeployed".
        self._await_deployment(timeout_s=900)
        self._ingest(timeout_s=1800)
        self._deployed = True

    def _ingest(self, *, timeout_s: float) -> None:
        """Push the TPC-C CSV into each input table over HTTP."""
        from pathlib import Path

        data_dir = Path(_tpcc_data_dir(self._corpus))
        for table in _tpcc_table_names(self._corpus.schema_ddl):
            csv_path = data_dir / f"{table}.csv"
            if not csv_path.exists():
                raise EngineCrashed(
                    f"{self.name}: {csv_path} missing — corpus prep must emit CSV "
                    "for Feldera ingestion"
                )
            self._request(
                "POST",
                f"/v0/pipelines/{self._pipeline}/ingress/{table}",
                params={"format": "csv"},
                data=csv_path.read_bytes(),
                timeout_s=timeout_s,
            )

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        # Acceptance is decided by the SQL probe in build_batch, so there is no
        # separate base-query phase to run.
        return None

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        if mv_name in self._rejected:
            raise QueryFailed(self._rejected[mv_name])
        if mv_name not in self._accepted:
            raise QueryFailed(f"{self.name}: {mv_name} was not part of the batch program")
        if not self._deployed:
            raise EngineCrashed(f"{self.name}: batch program was never deployed")

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        # Constant by construction: DBSP has no full-recompute mode.
        return INCREMENTAL

    # The delta pool is SQL DML aimed at engines with an UPDATE statement.
    # Feldera takes changes as insert/delete records on its ingress endpoint, so
    # each statement is translated into that form. Shapes come from
    # compiler_bench_corpus.tpcc_delta_pool, which generates exactly these three.
    _RE_UPDATE = re.compile(
        r"^UPDATE\s+(?P<table>\w+)\s+SET\s+(?P<sets>.+?)\s+WHERE\s+(?P<where>.+)$",
        re.IGNORECASE | re.DOTALL,
    )
    _RE_DELETE = re.compile(
        r"^DELETE\s+FROM\s+(?P<table>\w+)\s+WHERE\s+(?P<where>.+)$",
        re.IGNORECASE | re.DOTALL,
    )
    _RE_INSERT = re.compile(
        r"^INSERT\s+INTO\s+(?P<table>\w+)\s+VALUES\s*\((?P<values>.+)\)\s*$",
        re.IGNORECASE | re.DOTALL,
    )

    @staticmethod
    def _split_top_level(text: str, sep: str) -> List[str]:
        """Split on `sep` outside quotes, so values containing it stay intact."""
        parts, buf, quote = [], [], None
        for ch in text:
            if quote:
                buf.append(ch)
                if ch == quote:
                    quote = None
                continue
            if ch in "'\"":
                quote = ch
                buf.append(ch)
                continue
            if text[len(parts) : ] and ch == sep:
                parts.append("".join(buf))
                buf = []
                continue
            buf.append(ch)
        parts.append("".join(buf))
        return [p.strip() for p in parts if p.strip()]

    @staticmethod
    def _literal(text: str):
        """SQL literal -> Python value, for JSON ingress records."""
        text = text.strip()
        if text.upper() == "NULL":
            return None
        if len(text) >= 2 and text[0] == "'" and text[-1] == "'":
            return text[1:-1].replace("''", "'")
        if text.upper().startswith("TIMESTAMP'") and text.endswith("'"):
            return text[len("TIMESTAMP'"):-1]
        try:
            return int(text)
        except ValueError:
            pass
        try:
            return float(text)
        except ValueError:
            return text

    @classmethod
    def parse_delta(cls, statement: str) -> Optional[dict]:
        """Break one DML statement into the parts an ingress translation needs.

        Returns None for anything unrecognised, so an unexpected shape is
        skipped rather than silently applied as something else.
        """
        stmt = statement.strip().rstrip(";")
        match = cls._RE_UPDATE.match(stmt)
        if match:
            assignments = {}
            for item in cls._split_top_level(match.group("sets"), ","):
                if "=" not in item:
                    return None
                column, _, value = item.partition("=")
                assignments[column.strip().lower()] = cls._literal(value)
            return {"op": "update", "table": match.group("table"),
                    "set": assignments, "where": match.group("where").strip()}
        match = cls._RE_DELETE.match(stmt)
        if match:
            return {"op": "delete", "table": match.group("table"),
                    "where": match.group("where").strip()}
        match = cls._RE_INSERT.match(stmt)
        if match:
            values = [cls._literal(v) for v in cls._split_top_level(match.group("values"), ",")]
            return {"op": "insert", "table": match.group("table"), "values": values}
        return None

    def _columns_of(self, table: str) -> List[str]:
        for stmt in self._schema_sql:
            match = re.match(r"CREATE TABLE\s+(\w+)\s*\((.*)\)\s*$", stmt.strip(),
                             re.IGNORECASE | re.DOTALL)
            if match and match.group(1).lower() == table.lower():
                return [c.split()[0].lower()
                        for c in self._split_top_level(match.group(2), ",")]
        return []

    def _select_rows(self, table: str, where: str, *, timeout_s: float) -> List[dict]:
        response = self._request(
            "GET",
            f"/v0/pipelines/{self._pipeline}/query",
            params={"sql": f"SELECT * FROM {table} WHERE {where}", "format": "json"},
            timeout_s=timeout_s,
        )
        if response.status_code >= 400:
            raise QueryFailed(f"{self.name}: {response.text[:400]}")
        return [r for r in self._ndjson_rows(response.text) if isinstance(r, dict)]

    def _push(self, table: str, records: Sequence[dict], *, timeout_s: float) -> None:
        if not records:
            return
        body = "\n".join(json.dumps(r) for r in records)
        self._request(
            "POST",
            f"/v0/pipelines/{self._pipeline}/ingress/{table}",
            params={"format": "json", "update_format": "insert_delete"},
            data=body.encode(),
            timeout_s=timeout_s,
        )

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        """Apply the delta batch as ingress records.

        An UPDATE becomes delete-old + insert-new, which needs the current rows —
        read back from the input tables, which are materialized. Reading them
        from Feldera itself (rather than replaying the DML elsewhere) keeps the
        change set consistent with what this pipeline actually holds.
        """
        if not self._deployed:
            return
        for statement in statements:
            parsed = self.parse_delta(statement)
            if not parsed:
                logger.debug("[%s] skipping unrecognised delta: %s", self.name, statement[:120])
                continue
            table = parsed["table"]
            try:
                if parsed["op"] == "insert":
                    columns = self._columns_of(table)
                    values = parsed["values"]
                    if len(columns) != len(values):
                        continue
                    self._push(table, [{"insert": dict(zip(columns, values))}],
                               timeout_s=timeout_s)
                    continue
                rows = self._select_rows(table, parsed["where"], timeout_s=timeout_s)
                if not rows:
                    continue
                records = [{"delete": row} for row in rows]
                if parsed["op"] == "update":
                    for row in rows:
                        records.append({"insert": {**row, **parsed["set"]}})
                self._push(table, records, timeout_s=timeout_s)
            except QueryFailed:
                # As in the C++ benchmark, a delta that does not apply is not a
                # verdict on the query.
                continue

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        # Continuous maintenance — the only meaningful wait is for the pipeline
        # to finish processing what has been ingested.
        self._request("GET", f"/v0/pipelines/{self._pipeline}/stats", timeout_s=timeout_s)

    @staticmethod
    def _ndjson_rows(text: str) -> List[dict]:
        """Parse the ad-hoc endpoint's newline-delimited JSON.

        It returns one JSON object per line — NOT a JSON array and not
        {"rows": [...]}. Observed against a live pipeline:
            {"a":1,"s":30}
            {"a":2,"s":5}
        response.json() parses only a single-row body and then finds no "rows"
        key, which surfaced as "verification produced no comparable result" for
        every query instead of as an error.
        """
        rows = []
        for line in (text or "").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows

    def _adhoc(self, sql: str, *, timeout_s: float, label: str = "query") -> List[dict]:
        response = self._request(
            "GET",
            f"/v0/pipelines/{self._pipeline}/query",
            params={"sql": sql, "format": "json"},
            timeout_s=timeout_s,
        )
        if response.status_code >= 400:
            raise QueryFailed(f"{self.name}: [{label}] {response.text[:600]}")
        rows = self._ndjson_rows(response.text)
        for row in rows:
            if isinstance(row, dict) and "error" in row and len(row) == 1:
                raise QueryFailed(f"{self.name}: [{label}] {str(row['error'])[:600]}")
        return [r for r in rows if isinstance(r, dict)]

    @staticmethod
    def _normalize(rows: Sequence[dict]) -> List[tuple]:
        """Row multiset, comparable across the two sides.

        Compares by position after sorting each row's columns by name, because
        the view's column names are engine-sanitised and need not match the
        query's. Floats are rounded to the same 10 decimals the SQL probes use.
        """
        out = []
        for row in rows:
            values = []
            for _, value in sorted(row.items()):
                if isinstance(value, float):
                    value = round(value, 10)
                values.append(value)
            out.append(tuple(values))
        return sorted(out, key=repr)

    def _view_columns(self, mv_name: str, *, timeout_s: float) -> List[str]:
        """Column names of a view, from a zero-row read."""
        rows = self._adhoc(
            f"SELECT * FROM {mv_name} LIMIT 1", timeout_s=timeout_s, label="describe"
        )
        return list(rows[0].keys()) if rows else []

    #: Hex digit -> value via strpos, because DataFusion cannot cast '0xabc' to
    #: an integer ("Cannot cast string '0xf19fabb5' to value of Int64 type") and
    #: exposes no hash-to-number function. 8 hex digits fit in a BIGINT.
    _HEX_DIGITS = "0123456789abcdef"
    _HEX_PLACE_VALUES = (268435456, 16777216, 1048576, 65536, 4096, 256, 16, 1)

    @classmethod
    def _hex_prefix_to_int(cls, hex_expr: str) -> str:
        terms = [
            f"(strpos('{cls._HEX_DIGITS}', substr({hex_expr}, {i + 1}, 1)) - 1) * {place}"
            for i, place in enumerate(cls._HEX_PLACE_VALUES)
        ]
        return "(" + " + ".join(terms) + ")"

    @classmethod
    def bag_digest_sql(cls, relation: str, columns: Sequence[str]) -> str:
        """One row summarising a relation as a bag — no join, no set operation.

        Groups by every column, then reduces the groups to three numbers: how
        many distinct groups, the total row count, and a checksum mixing each
        group's multiplicity with a hash of its values. Multiplicity is inside
        the checksum, so a row present twice on one side and once on the other
        changes it — the property a set-based probe loses.

        Why no join: the group-and-count comparison ran, but its FULL JOIN still
        hit "Unexpected record with negative weight" on the million-row join
        views, and the labelled error placed the failure inside that comparison
        rather than in either read. Aggregates over a Z-set view were measured to
        survive retractions, so each side is reduced independently here and the
        two digests are compared outside the engine.

        NULLs are coalesced to a sentinel that cannot collide with the string
        'NULL', so a NULL column and a literal 'NULL' hash differently.
        """
        cols = ", ".join(columns)
        parts = ", ".join(
            f"coalesce(cast({c} AS VARCHAR), '\\x00NULL')" for c in columns
        )
        hex_expr = f"substr(md5(concat_ws(chr(1), {parts})), 1, 8)"
        row_hash = cls._hex_prefix_to_int(hex_expr)
        return (
            "SELECT count(*) AS groups, sum(__n) AS rows_total, "
            "sum(__n * __h) AS checksum FROM "
            f"(SELECT count(*) AS __n, {row_hash} AS __h "
            f"FROM {relation} GROUP BY {cols}) __g"
        )

    @staticmethod
    def bag_compare_sql(mv_name: str, sql: str, columns: Sequence[str]) -> str:
        """One query returning the number of rows whose multiplicity differs.

        Group both sides by every column and compare the per-group counts, so
        this is BAG equality: a row present twice on one side and once on the
        other is a difference. A plain anti-join would be set equality and would
        miss exactly that, which matters most for the join-heavy queries where
        the same row legitimately repeats.

        Deliberately no set operation: a Feldera view is a Z-set and after
        deltas contains negative-weight records, which the ad-hoc engine refuses
        inside EXCEPT/INTERSECT. Grouping and joining are fine.

        FULL JOIN so a group missing from either side counts, and NULL-safe
        equality so grouping columns that are NULL still match each other —
        plain `=` would leave those groups unjoined and report them as
        differences.
        """
        cols = ", ".join(columns)
        # Each predicate parenthesised: IS NOT DISTINCT FROM binds looser than
        # AND in DataFusion, so without them this parses as
        # `v.a IS NOT DISTINCT FROM (q.a AND ...)` and fails type coercion with
        # "Cannot infer common argument type for logical boolean operation".
        on = " AND ".join(
            f"(v.{c} IS NOT DISTINCT FROM q.{c})" for c in columns
        )
        return (
            "SELECT count(*) AS diff FROM "
            f"(SELECT {cols}, count(*) AS __n FROM {mv_name} GROUP BY {cols}) v "
            "FULL JOIN "
            f"(SELECT {cols}, count(*) AS __n FROM ({sql}) __cb GROUP BY {cols}) q "
            f"ON {on} "
            "WHERE v.__n IS DISTINCT FROM q.__n"
        )

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        """Compare view against query inside Feldera, returning one row.

        The corpus contains partial-key joins that produce ~1e6 rows at SF3;
        reading both sides out and diffing them in Python transferred all of
        that per query and was slow and fragile. This keeps the comparison in
        the engine and transfers a single count.

        Falls back to reading both sides only when the column list cannot be
        established, since the grouped form needs the names.
        """
        columns = self._view_columns(mv_name, timeout_s=timeout_s)
        if not columns:
            view_rows = self._adhoc(
                f"SELECT * FROM {mv_name}", timeout_s=timeout_s, label="view-read"
            )
            expected_rows = self._adhoc(
                f"SELECT * FROM ({sql}) __cb", timeout_s=timeout_s, label="base-query"
            )
            return self._normalize(view_rows) == self._normalize(expected_rows)

        view = self._adhoc(
            self.bag_digest_sql(mv_name, columns),
            timeout_s=timeout_s,
            label="view-digest",
        )
        expected = self._adhoc(
            self.bag_digest_sql(f"({sql}) __cb", columns),
            timeout_s=timeout_s,
            label="query-digest",
        )
        if not view or not expected:
            raise QueryFailed(f"{self.name}: digest comparison produced no result")
        keys = ("groups", "rows_total", "checksum")
        if any(k not in view[0] or k not in expected[0] for k in keys):
            raise QueryFailed(
                f"{self.name}: digest missing fields; got {view[0]!r} / {expected[0]!r}"
            )
        return all(view[0][k] == expected[0][k] for k in keys)

    @staticmethod
    def _diff_value(row):
        """Read the probe's single count out of a result row.

        The alias survives as `diff` in every shape observed, but a single-column
        row is unambiguous regardless of what the engine called the column, so
        fall back to its only value rather than failing on a naming difference.
        """
        if not isinstance(row, dict):
            return row[0] if row else None
        for key in ("diff", "DIFF", "Diff"):
            if key in row and row[key] is not None:
                return row[key]
        values = [v for v in row.values() if v is not None]
        if len(row) == 1 and values and isinstance(values[0], (int, float)):
            # Numeric only: a single-column row holding an error string would
            # otherwise be returned and then blow up on int(), turning an engine
            # message into an unattributable harness error.
            return values[0]
        return None

    def drop_mv(self, mv_name: str) -> None:
        # Views live in the batch program; dropping one would mean recompiling.
        return None

    def teardown(self) -> None:
        try:
            self._request("DELETE", f"/v0/pipelines/{self._pipeline}", timeout_s=120)
        except Exception:
            logger.debug("[%s] pipeline cleanup failed", self.name, exc_info=True)


class _FabricAdapter(EngineAdapter):
    """Fabric Spark engines.

    Not wired yet: the repo drives Fabric through dbt over Livy with an AAD
    token (services/fabric.py provisions the workspace and mints tokens) and has
    no direct statement-submission helper for ad-hoc SQL. The compiler bench needs
    one — a Fabric equivalent of spark_openivm_sources.LivyClient — before these
    engines can run. Raising here keeps a Fabric run from silently reporting
    zeros that would read as "no queries incrementalizable".
    """

    def setup(self, corpus: Corpus) -> None:
        raise EngineCrashed(
            f"{self.name}: compiler-bench needs a direct Fabric Livy SQL client; "
            "services/fabric.py only provisions the workspace and mints tokens. "
            "Wire a Fabric statement-submit helper before enabling this engine."
        )


class FabricOpenIVMAdapter(_FabricAdapter):
    name = "fabric-openivm-jvm-35"


class FabricVanillaAdapter(_FabricAdapter):
    name = "fabric-jvm-35"


_CLOUD_ADAPTERS = {
    "databricks-enzyme": DatabricksEnzymeAdapter,
    "feldera": FelderaAdapter,
    "fabric-openivm-jvm-35": FabricOpenIVMAdapter,
    "fabric-jvm-35": FabricVanillaAdapter,
}


def get_adapter(engine: str) -> Optional[EngineAdapter]:
    factory = _CLOUD_ADAPTERS.get(engine)
    return factory() if factory else None
