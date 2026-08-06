"""Cloud/managed engine adapters: databricks-enzyme, feldera, fabric.

Split from engines.py because these need live credentials to exercise, so they
are validated by a GCI run rather than on a dev box.
"""

from __future__ import annotations

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
        parts = [stmt.rstrip(";") + ";" for stmt in self._schema_sql]
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

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        # Deltas are DML text aimed at SQL engines; Feldera takes changes through
        # its ingress endpoint instead. Left out deliberately rather than faked:
        # the views are verified against the ingested data as-is.
        return None

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        # Continuous maintenance — the only meaningful wait is for the pipeline
        # to finish processing what has been ingested.
        self._request("GET", f"/v0/pipelines/{self._pipeline}/stats", timeout_s=timeout_s)

    def verify(self, mv_name: str, sql: str, *, timeout_s: float) -> bool:
        """Compare the maintained view against the query, via ad-hoc SQL."""
        response = self._request(
            "GET",
            f"/v0/pipelines/{self._pipeline}/query",
            params={"sql": _verify_probe(mv_name, sql), "format": "json"},
            timeout_s=timeout_s,
        )
        if response.status_code >= 400:
            raise QueryFailed(f"{self.name}: {response.text[:600]}")
        try:
            payload = response.json()
        except ValueError:
            raise QueryFailed(f"{self.name}: ad-hoc query returned no JSON")
        rows = payload if isinstance(payload, list) else payload.get("rows") or []
        if not rows:
            raise QueryFailed(f"{self.name}: verification produced no comparable result")
        first = rows[0]
        diff = first.get("diff") if isinstance(first, dict) else first[0]
        return int(diff) == 0

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
