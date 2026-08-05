"""Cloud/managed engine adapters: databricks-enzyme, feldera, fabric.

Split from engines.py because these need live credentials to exercise, so they
are validated by a GCI run rather than on a dev box.
"""

from __future__ import annotations

import logging
import os
import re
import time
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
        try:
            return self._src.execute_isolated(sql, timeout_s=max(5.0, timeout_s))
        except Exception as exc:
            message = str(exc)
            if isinstance(exc, TimeoutError) or "timeout" in message.lower():
                raise EngineTimeout(f"{self.name}: {message[:600]}") from exc
            if _is_fatal(message):
                raise EngineCrashed(f"{self.name}: {message[:800]}") from exc
            raise QueryFailed(f"{self.name}: {message[:800]}") from exc

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        self._execute(
            f"CREATE SCHEMA IF NOT EXISTS `{self._src.CATALOG}`.`{self._schema}`",
            timeout_s=120,
        )
        self._execute(f"USE `{self._src.CATALOG}`.`{self._schema}`", timeout_s=60)
        data_dir = _tpcc_data_dir(corpus)
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
    """Feldera (DBSP).

    Classification is a constant: DBSP maintains every view it accepts
    incrementally — there is no full-recompute mode to fall back to. So the
    measurement that carries information here is whether Feldera's SQL compiler
    *accepts* the view at all, which lands in the query-failed / crash buckets.

    Refresh is a no-op for the same reason: the pipeline maintains views
    continuously rather than on demand. Verification is not attempted, so
    correctness is reported as "not determined" rather than assumed.
    """

    name = "feldera"
    supports_verify = False

    _COMPILE_POLL_S = 2.0

    def __init__(self) -> None:
        self._base_url = os.environ.get("FELDERA_URL", "http://pipeline-manager:8080")
        self._pipeline = os.environ.get("COMPILER_BENCH_FELDERA_PIPELINE", "compiler_bench")
        self._corpus: Optional[Corpus] = None
        self._schema_sql: List[str] = []

    def _request(self, method: str, path: str, *, json_body=None, timeout_s: float = 60.0):
        import requests

        url = f"{self._base_url.rstrip('/')}{path}"
        try:
            response = requests.request(
                method, url, json=json_body, timeout=max(5.0, timeout_s)
            )
        except requests.RequestException as exc:
            raise EngineCrashed(f"{self.name}: pipeline-manager unreachable: {exc}") from exc
        if response.status_code >= 500:
            raise EngineCrashed(
                f"{self.name}: pipeline-manager {response.status_code}: {response.text[:400]}"
            )
        return response

    def setup(self, corpus: Corpus) -> None:
        self._corpus = corpus
        # Feldera declares its inputs as part of the program, so the base tables
        # are DDL text prepended to every candidate view rather than state we
        # create up front.
        self._schema_sql = [
            re.sub(r"^CREATE TABLE", "CREATE TABLE", stmt, flags=re.IGNORECASE)
            for stmt in corpus.schema_ddl
        ]

    def run_base_query(self, sql: str, *, timeout_s: float) -> None:
        # No standalone query surface: acceptance is decided by the SQL compiler
        # in create_mv, so this phase is a no-op rather than a fake pass.
        return None

    def create_mv(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        """Compile a program holding just this view and wait for the SQL stage.

        Only the SQL compilation stage is awaited: it is where unsupported SQL is
        rejected, and it completes in seconds, whereas the Rust stage that
        follows takes minutes and would make a corpus run intractable.
        """
        program = ";\n".join(self._schema_sql) + f";\nCREATE MATERIALIZED VIEW {mv_name} AS {sql};"
        self._request(
            "PUT",
            f"/v0/pipelines/{self._pipeline}",
            json_body={"name": self._pipeline, "program_code": program},
            timeout_s=timeout_s,
        )
        deadline = time.monotonic() + timeout_s
        while True:
            if time.monotonic() > deadline:
                raise EngineTimeout(f"{self.name}: SQL compilation exceeded {timeout_s:.0f}s")
            response = self._request(
                "GET", f"/v0/pipelines/{self._pipeline}", timeout_s=30
            )
            body = response.json() if response.content else {}
            status = body.get("program_status")
            status_name = status if isinstance(status, str) else next(iter(status or {}), "")
            if status_name in ("SqlError", "RustError", "SystemError"):
                detail = body.get("program_error") or status
                raise QueryFailed(f"{self.name}: {str(detail)[:800]}")
            if status_name in ("SqlCompiled", "CompilingRust", "Success"):
                return
            time.sleep(self._COMPILE_POLL_S)

    def classify(self, mv_name: str, sql: str, *, timeout_s: float) -> str:
        # Constant by construction: DBSP has no full-recompute mode.
        return INCREMENTAL

    def apply_deltas(self, statements: Sequence[str], *, timeout_s: float) -> None:
        return None

    def refresh(self, mv_name: str, sql: str, *, timeout_s: float) -> None:
        return None

    def drop_mv(self, mv_name: str) -> None:
        try:
            self._request("DELETE", f"/v0/pipelines/{self._pipeline}", timeout_s=60)
        except Exception:
            pass


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
