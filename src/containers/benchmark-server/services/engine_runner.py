"""Per-engine benchmark execution logic."""

import csv
import json
import logging
import os
import shutil
import tempfile
import time
from threading import Barrier, Lock
from typing import Any, Callable, Dict, List, Optional

import requests

from models.config import (
    ENGINE_COMPUTE_SERVICES,
    BenchmarkConfig,
    EngineConfig,
    is_cloud_engine,
)
from models.result import EngineResult
from services.db import DB_LOCK, get_db
from services.docker_manager import DockerManager, compute_cpu_usage_delta
from services.storage_sync import (
    StorageMetricsError,
    require_complete_storage_metrics,
    storage_snapshot_barrier,
)

logger = logging.getLogger(__name__)

HEALTH_TIMEOUT = 300
HEALTH_INTERVAL = 5

_RETRY_DELAYS = [30, 60, 120]
_COMPILER_BENCH_SUMMARY_LOCK = Lock()


def _post_with_retry(
    url: str,
    timeout: int,
    emit: Callable[[str], None],
    label: str,
    **kwargs,
) -> requests.Response:
    """POST with exponential-backoff retry on 5xx / connection errors.

    Retries up to len(_RETRY_DELAYS) times before propagating the last
    exception.  Non-5xx HTTP errors (4xx etc.) are re-raised immediately.
    """
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt, delay in enumerate(
        [0] + _RETRY_DELAYS, start=1
    ):
        if delay:
            emit(f"[{label}] retrying in {delay}s (attempt {attempt}/{len(_RETRY_DELAYS) + 1})")
            time.sleep(delay)
        try:
            resp = requests.post(url, timeout=timeout, **kwargs)
            if resp.status_code >= 500:
                last_exc = requests.HTTPError(
                    f"{resp.status_code} Server Error for url: {url}",
                    response=resp,
                )
                emit(f"[{label}] FAILED: {last_exc} — will retry")
                continue
            resp.raise_for_status()
            return resp
        except requests.ConnectionError as exc:
            last_exc = exc
            emit(f"[{label}] connection error: {exc} — will retry")
            continue
    raise last_exc


# Databricks-enzyme dbt-build retry policy for transient DLT pipeline
# failures (lost executor, INTERNAL_ERROR, transient Connection refused).
# Real model/SQL bugs are NOT retried — see `_is_databricks_transient`.
DATABRICKS_ENZYME_MAX_RETRIES = int(
    os.environ.get("DATABRICKS_ENZYME_MAX_RETRIES", "2")
)
DATABRICKS_ENZYME_RETRY_BACKOFF_S = int(
    os.environ.get("DATABRICKS_ENZYME_RETRY_BACKOFF_S", "60")
)
# Append-time batch-loader heap sizing. The init loader (batch 1) runs BEFORE
# the engine stack is up and can claim a large heap, but the append loader
# (batches 2/3) runs CONCURRENTLY with the live engine server, which by then
# holds the full batch-1 state (heavy for the IVM engines). The loader JVM
# pre-touches its whole heap (-Xms<heap> -XX:+AlwaysPreTouch), so reusing the
# large init heap oversubscribes the host and the append JVM is OOM-killed at
# startup. Appends only load a 1-2% delta, so cap the append heap to the engine
# memory NOT given to the main service (the dbt-server slice) minus this slack.
BATCH_LOADER_APPEND_SLACK_GB = int(
    os.environ.get("BATCH_LOADER_APPEND_SLACK_GB", "8")
)
BATCH_LOADER_APPEND_MIN_HEAP_GB = int(
    os.environ.get("BATCH_LOADER_APPEND_MIN_HEAP_GB", "8")
)
# Substrings in a failed node's dbt `message` that indicate a
# Databricks-side transient (lost executor, DLT pipeline restart, etc.)
# rather than a genuine model error. Match is case-sensitive on the raw
# dbt error text.
DATABRICKS_TRANSIENT_SIGNATURES = (
    "INTERNAL_ERROR: Unexpected failure during pipeline execution",
    "Connection refused",
    "DatabricksServiceException",
    "BAD_REQUEST: finishConnect",
    "TIMEOUT_OCCURRED",
    "PIPELINE_INTERNAL_ERROR",
    "SERVICE_UNAVAILABLE",
    "TEMPORARILY_UNAVAILABLE",
    # DLT pipeline service failed to start on the cluster (startup race /
    # transient cluster-side infra), e.g. "Failed to initialize the pipeline
    # service on cluster <id>. Check the cluster driver logs for more details."
    "Failed to initialize the pipeline service",
)


class OpenIvmValidationError(RuntimeError):
    """Raised when post-batch OpenIVM correctness validation (EXCEPT ALL) fails.

    Distinct from generic ``RuntimeError`` so the OAT loop can recognise it
    and FAIL FAST — a single validation diff means an MV definition or refresh
    path is broken, so running the rest of the sweep would just burn host
    resources reproducing the same bug at larger SFs.
    """


class EngineRunner:
    """
    Runs a full 3-batch benchmark for a single engine.

    Handles engine-specific differences:
    - Spark: batch_loader init/append, full_refresh=true always
    - DuckDB: DuckLake source init/append, full_refresh=true always
    - DuckDB-OpenIVM: no batch_loader, batch_num param, full_refresh only batch 1
    - Feldera: batch_loader init/append, streaming wait for batches 2/3
    """

    def __init__(
        self,
        config: BenchmarkConfig,
        engine_config: EngineConfig,
        emit: Callable[[str], None],
        benchmark_id: Optional[str] = None,
        storage_barrier: Optional[Barrier] = None,
    ) -> None:
        self._config = config
        self._engine = engine_config
        self._emit = emit
        self._result = EngineResult(engine=engine_config.name)
        self._benchmark_id = benchmark_id
        self._storage_barrier = storage_barrier
        docker_host = os.environ.get("DOCKER_HOST_ADDRESS", "localhost")
        self._dbt_url = f"http://{docker_host}:{engine_config.port}"
        self._override_file: Optional[str] = None
        self._batch_override_file: Optional[str] = None
        self._parallel = config.parallel
        self._stats_started = False
        self._stats_error: Optional[str] = None
        self._cpu_start_snapshots: Dict[int, Dict] = {}
        self._compute_services = list(
            ENGINE_COMPUTE_SERVICES.get(engine_config.name, ())
        )

        # Build engine compose args — may include an override file for parallel staging
        engine_compose = os.path.join(config.repo_dir, engine_config.compose_file)
        engine_env = {**config.base_env(), **engine_config.env_dict()}

        if engine_config.staging_dir and engine_config.staging_dir != "staging":
            # Parallel mode: create compose overrides for staging isolation
            self._override_file = self._create_staging_override()
            self._engine_mgr = DockerManager(
                compose_file=engine_compose,
                project_name=engine_config.project_name,
                env=engine_env,
                cwd=config.repo_dir,
                extra_compose_files=[self._override_file],
            )
            # Also create a batch-loader override so appends go to per-engine staging
            if engine_config.name not in ("duckdb-openivm",):
                self._batch_override_file = self._create_batch_staging_override()
        else:
            self._engine_mgr = DockerManager(
                compose_file=engine_compose,
                project_name=engine_config.project_name,
                env=engine_env,
                cwd=config.repo_dir,
            )

        # Calculate JVM heap for batch_loader (60% of available memory, min 4g).
        # This is the INIT heap, used while the engine stack is still down.
        batch_mem_gb = engine_config.main_resources.memory_gb
        batch_heap_gb = max(4, int(batch_mem_gb * 0.6))
        batch_gc_threads = max(4, engine_config.main_resources.cpus // 2)
        batch_conc_gc = max(2, batch_gc_threads // 2)

        # APPEND heap: the loader for batches 2/3 runs alongside the live engine
        # server, so it must fit in the engine memory left free by the main
        # service (the dbt-server slice, == per_engine_mem - main_mem in both
        # serial and parallel modes). Otherwise -Xms pre-touch OOM-kills it.
        dbt_headroom_gb = engine_config.dbt_server_resources.memory_gb
        self._batch_append_heap_gb = max(
            BATCH_LOADER_APPEND_MIN_HEAP_GB,
            min(batch_heap_gb, dbt_headroom_gb - BATCH_LOADER_APPEND_SLACK_GB),
        )

        batch_extra = [self._batch_override_file] if self._batch_override_file else []
        self._batch_mgr = DockerManager(
            compose_file=os.path.join(config.repo_dir, "docker/docker-compose.batch-loader.yml"),
            project_name=f"batch-{engine_config.name}",
            env={
                **config.base_env(),
                "BATCH_LOADER_CPUS": str(engine_config.main_resources.cpus),
                "BATCH_LOADER_MEM": engine_config.main_resources.mem_str,
                "BATCH_LOADER_HEAP": f"{batch_heap_gb}g",
                "BATCH_LOADER_GC_THREADS": str(batch_gc_threads),
                "BATCH_LOADER_CONC_GC_THREADS": str(batch_conc_gc),
            },
            cwd=config.repo_dir,
            extra_compose_files=batch_extra,
        )

    @property
    def result(self) -> EngineResult:
        return self._result

    def _create_staging_override(self) -> str:
        """Create a temporary compose override file that mounts per-engine staging."""
        sf = self._config.scale_factor
        repo = self._config.repo_dir
        staging_host = os.path.join(repo, "mount", "raw", str(sf), "delta", self._engine.staging_dir)

        # Generate an override that adds a volume mount for staging
        # This maps the per-engine staging dir to where dbt expects it
        engine_compose_path = os.path.join(repo, self._engine.compose_file)
        # We know dbt-server always mounts delta — override its staging subdir
        override_content = f"""services:
  dbt-server:
    volumes:
      - {staging_host}:/data/raw/delta/staging
"""
        # Also override main service if it has delta mounts
        if self._engine.name == "spark":
            override_content += f"""  spark:
    volumes:
      - {staging_host}:/data/raw/delta/staging:ro
"""
        elif self._engine.name == "feldera":
            override_content += f"""  pipeline-manager:
    volumes:
      - {staging_host}:/data/raw/delta/staging
"""

        fd, path = tempfile.mkstemp(suffix=".yml", prefix=f"override-{self._engine.name}-")
        with os.fdopen(fd, "w") as f:
            f.write(override_content)
        logger.info("Created staging override: %s", path)
        return path

    def _create_batch_staging_override(self) -> str:
        """Create a compose override for the batch-loader so appends go to per-engine staging."""
        sf = self._config.scale_factor
        repo = self._config.repo_dir
        staging_host = os.path.join(repo, "mount", "raw", str(sf), "delta", self._engine.staging_dir)

        override_content = f"""services:
  spark-batch-loader:
    volumes:
      - type: bind
        source: {staging_host}
        target: /data/delta/staging
"""
        fd, path = tempfile.mkstemp(suffix=".yml", prefix=f"batch-override-{self._engine.name}-")
        with os.fdopen(fd, "w") as f:
            f.write(override_content)
        logger.info("Created batch staging override: %s", path)
        return path

    @property
    def result(self) -> EngineResult:
        """Expose the in-progress EngineResult so callers that catch a
        propagated exception can still recover partial state (batch
        statuses, durations, error text)."""
        return self._result

    def run(self) -> EngineResult:
        """Execute the full 3-batch benchmark."""
        name = self._engine.name
        self._result.status = "running"
        try:
            self._emit(f"[{name}] Starting benchmark")

            # Initialize Delta staging for engines that still consume Delta directly.
            # spark-openivm KEEPS the batch-loader flow: it relies on
            # mount/raw/<SF>/delta/staging being shaped with the CDC columns,
            # then issues `INSERT INTO tpcdi.<t> SELECT * FROM delta.\`...\``
            # via Livy. Under `spark.openivm.changeFeed.mode=cdf` the INSERT
            # writes Delta CDF records that the next REFRESH consumes — no DML
            # interception in this mode.
            # compiler-bench never reads the TPC-DI Delta sources (it loads its
            # own TPC-C data), and the orchestrator skips their generation — so
            # staging them would fail on the missing input rather than merely
            # waste time.
            compiler_bench = self._compiler_bench_enabled()
            if (
                name not in ("duckdb", "duckdb-openivm")
                and not self._parallel
                and not compiler_bench
            ):
                self._batch_loader_init()

            # Start engine stack
            self._emit(f"[{name}] Building and starting compose stack")
            self._engine_mgr.build()
            self._up_with_retry()

            # Wait for dbt-server health
            self._emit(f"[{name}] Waiting for dbt-server health")
            self._wait_for_dbt_health()

            # Start container stats
            self._start_stats()
            if not compiler_bench:
                # Also TPC-DI-only: nothing to measure when those tables are
                # neither generated nor read.
                self._capture_delta_stats(1)

            if compiler_bench:
                # The survey creates and drops thousands of views, so it replaces
                # the timed batches rather than sharing an engine with them.
                self._run_compiler_bench()
                self._stop_stats()
                self._result.status = "completed"
                self._emit(f"[{name}] compiler-bench completed successfully")
                return self._result

            # Run 3 batches
            for batch_num in range(1, 4):
                self._run_batch(batch_num)
                if self._result.batches[batch_num - 1].status == "failed":
                    raise RuntimeError(
                        f"{name} batch {batch_num} failed: "
                        f"{self._result.batches[batch_num - 1].error}"
                    )

            # Post-run: lineage and sql analysis. Stats are stopped in finally
            # so completed batch telemetry survives later failures.
            self._fetch_sql_analysis()
            self._fetch_lineage()

            self._result.status = "completed"
            self._emit(f"[{name}] Benchmark completed successfully")

        except OpenIvmValidationError as e:
            # FATAL: tee onto the result AND re-raise so _run_engines_serial /
            # _run_engine_wave can propagate the typed exception up to the OAT
            # loop, which will abort the sweep instead of running the remaining
            # experiments. finally: below still runs (compose-down + log capture).
            self._result.status = "failed"
            self._result.error = str(e)
            self._emit(f"[{name}] FATAL OpenIVM validation failure: {e}")
            logger.exception("Engine %s OpenIVM validation failure (fatal for OAT)", name)
            if self._storage_barrier is not None:
                self._storage_barrier.abort()
            raise
        except Exception as e:
            self._result.status = "failed"
            self._result.error = str(e)
            self._emit(f"[{name}] FAILED: {e}")
            logger.exception("Engine %s failed", name)
            if self._storage_barrier is not None:
                self._storage_barrier.abort()

        finally:
            # Defensive: any exception in cleanup would otherwise REPLACE the
            # in-flight OpenIvmValidationError (Python finally semantics),
            # which would demote a fatal validation failure into a generic
            # engine error and skip the OAT fail-fast path. Guard every step.
            for cleanup_step, fn in (
                ("stop_stats", self._stop_stats),
                ("collect_feldera_debug", self._collect_feldera_debug),
                ("capture_logs", self._capture_logs),
                ("databricks_enzyme_drop_mvs", self._databricks_enzyme_drop_mvs),
                ("fabric_cleanup", self._fabric_cleanup),
                ("engine_mgr.down", self._engine_mgr.down),
                ("cleanup_staging", self._cleanup_staging),
            ):
                try:
                    fn()
                except Exception as ce:
                    self._emit(f"[{name}] cleanup '{cleanup_step}' WARN: {ce}")
                    logger.warning("Engine %s cleanup %s failed: %s", name, cleanup_step, ce)
            for f in (self._override_file, self._batch_override_file):
                try:
                    if f and os.path.exists(f):
                        os.unlink(f)
                except Exception as ce:
                    logger.warning("Engine %s unlink %s failed: %s", name, f, ce)

        return self._result

    # ------------------------------------------------------------------
    # compiler-bench
    # ------------------------------------------------------------------

    def _compiler_bench_enabled(self) -> bool:
        return os.environ.get("COMPILER_BENCH", "0").strip().lower() in (
            "1", "true", "yes", "on",
        )

    def _run_compiler_bench(self) -> None:
        """Drive the survey on this engine and persist its artifacts.

        Polls rather than blocking on one request: a full corpus is thousands of
        queries. There is no overall deadline beyond the per-query budget the
        dbt-server enforces, so a wedged engine is bounded by
        COMPILER_BENCH_RUN_TIMEOUT_S.
        """
        from models.experiments import CompilerBenchOptions

        name = self._engine.name
        options = CompilerBenchOptions.from_env()
        run_timeout_s = float(os.environ.get("COMPILER_BENCH_RUN_TIMEOUT_S", "86400"))

        self._emit(
            f"[{name}] compiler-bench: sf={options.scale_factor} "
            f"timeout={options.timeout_s:.0f}s limit={options.limit or 'all'} "
            f"storage={'ducklake' if options.ducklake else 'native'}"
        )

        response = _post_with_retry(
            f"{self._dbt_url}/compiler-bench/{name}",
            120,
            self._emit,
            f"{name} compiler-bench start",
            json={
                "limit": options.limit,
                "ducklake": options.ducklake,
                "timeout_s": options.timeout_s,
                "delta_batch_size": options.delta_batch_size,
                "verify": options.verify,
                "classify_only": options.classify_only,
            },
        )
        run_id = response.json()["run_id"]

        deadline = time.time() + run_timeout_s
        last_completed = -1
        while True:
            if time.time() > deadline:
                raise RuntimeError(
                    f"{name} compiler-bench exceeded {run_timeout_s:.0f}s"
                )
            time.sleep(10)
            status_resp = requests.get(
                f"{self._dbt_url}/compiler-bench/runs/{run_id}", timeout=60
            )
            status_resp.raise_for_status()
            payload = status_resp.json()
            state = payload.get("status")
            completed = payload.get("completed", 0)
            if completed != last_completed:
                self._emit(
                    f"[{name}] compiler-bench {completed}/{payload.get('total', '?')}"
                )
                last_completed = completed
            if state == "completed":
                break
            if state == "error":
                raise RuntimeError(f"{name} compiler-bench failed: {payload.get('error')}")

        rows_resp = requests.get(
            f"{self._dbt_url}/compiler-bench/runs/{run_id}?include_rows=1", timeout=600
        )
        rows_resp.raise_for_status()
        final = rows_resp.json()
        self._write_compiler_bench_artifacts(options, final)

        summary = final.get("summary") or {}
        totals = summary.get("totals") or {}
        by_created = summary.get("pct_of_mv_created") or {}
        if summary.get("mode") == "classification_only":
            self._emit(
                f"[{name}] compiler-bench classification: "
                f"{totals.get('incremental', 0)} incremental / "
                f"{totals.get('full_refresh', 0)} non-incremental of "
                f"{totals.get('classified', 0)} classified; "
                f"unknown={totals.get('classification_unknown', 0)} "
                f"crash={totals.get('crashed', 0)} "
                f"timeout={totals.get('timeout', 0)}"
            )
            self._result.extra["compiler_bench"] = summary
            return
        self._emit(
            f"[{name}] compiler-bench: {totals.get('incremental', 0)} incremental / "
            f"{totals.get('full_refresh', 0)} full of {totals.get('mv_created', 0)} created "
            f"({by_created.get('incremental', 0)}% incremental); "
            f"crash={totals.get('crashed', 0)} timeout={totals.get('timeout', 0)} "
            f"translation_failed={totals.get('translation_failed', 0)}"
        )
        self._result.extra["compiler_bench"] = summary

    def _write_compiler_bench_artifacts(self, options, final: dict) -> None:
        # DuckLake and native runs of the same engine must not overwrite each
        # other — they are two different measurements.
        engine_dir = self._engine.name + ("-ducklake" if options.ducklake else "")
        out_dir = os.path.join(
            self._config.repo_dir, "mount", "compiler-bench", "results", engine_dir,
        )
        os.makedirs(out_dir, exist_ok=True)

        columns = final.get("columns") or []
        rows = final.get("rows") or []
        if columns and rows:
            with open(os.path.join(out_dir, "results.csv"), "w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=columns)
                writer.writeheader()
                writer.writerows(rows)
        with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as fh:
            json.dump(final.get("summary") or {}, fh, indent=2)
        summary_columns = final.get("summary_columns") or []
        summary_row = final.get("summary_row") or {}
        if summary_columns and summary_row:
            with open(
                os.path.join(out_dir, "summary.csv"),
                "w",
                newline="",
                encoding="utf-8",
            ) as fh:
                writer = csv.DictWriter(fh, fieldnames=summary_columns)
                writer.writeheader()
                writer.writerow(summary_row)
            self._upsert_compiler_bench_summary(
                os.path.dirname(out_dir),
                engine_dir,
                "ducklake" if options.ducklake else "native",
                summary_columns,
                summary_row,
            )
        self._emit(f"[{self._engine.name}] compiler-bench artifacts -> {out_dir}")

    @staticmethod
    def _upsert_compiler_bench_summary(
        results_dir: str,
        result_set: str,
        storage: str,
        summary_columns: List[str],
        summary_row: dict,
    ) -> None:
        """Atomically maintain one plot-ready row per engine/storage result."""
        columns = ["result_set", "storage", *summary_columns]
        path = os.path.join(results_dir, "summary.csv")
        row = {"result_set": result_set, "storage": storage, **summary_row}
        with _COMPILER_BENCH_SUMMARY_LOCK:
            existing = []
            if os.path.exists(path):
                with open(path, newline="", encoding="utf-8") as fh:
                    existing = [
                        old for old in csv.DictReader(fh)
                        if old.get("result_set") != result_set
                    ]
            existing.append(row)
            existing.sort(key=lambda item: item.get("result_set", ""))
            existing = [
                {column: item.get(column, "") for column in columns}
                for item in existing
            ]
            with tempfile.NamedTemporaryFile(
                "w",
                dir=results_dir,
                prefix=".compiler-bench-summary-",
                suffix=".csv",
                delete=False,
                newline="",
                encoding="utf-8",
            ) as fh:
                writer = csv.DictWriter(fh, fieldnames=columns)
                writer.writeheader()
                writer.writerows(existing)
                temporary = fh.name
            os.chmod(temporary, 0o644)
            os.replace(temporary, path)

    def _databricks_enzyme_drop_mvs(self) -> None:
        """End-of-run: drop the databricks-enzyme MVs so REFRESH SCHEDULE
        does not keep accruing Databricks Serverless SQL bills between
        experiments and after the sweep finishes.

        No-op for any other engine.
        """
        if self._engine.name != "databricks-enzyme":
            return
        try:
            resp = requests.post(
                f"{self._dbt_url}/sources/databricks-enzyme/cleanup-schema",
                timeout=600,
            )
            if resp.status_code == 200:
                self._emit(
                    "[databricks-enzyme] Post-run cleanup-schema OK "
                    "(dropped MVs to halt refresh billing)"
                )
            else:
                self._emit(
                    f"[databricks-enzyme] Post-run cleanup-schema WARN: "
                    f"HTTP {resp.status_code} {resp.text[:200]}"
                )
        except Exception as e:
            self._emit(f"[databricks-enzyme] Post-run cleanup-schema WARN: {e}")

    def _fabric_cleanup(self) -> None:
        """End-of-run: tear down this run's compute lakehouse + environment so no
        experiment leaves stale resources behind (the shared cache lakehouse is
        preserved). Orphan-sweep at the next run's provision is the backstop.
        No-op for any non-fabric engine."""
        if self._engine.name not in ("fabric-openivm-jvm-35", "fabric-jvm-35"):
            return
        try:
            resp = requests.post(f"{self._dbt_url}/environment/fabric/teardown", timeout=1200)
            if resp.status_code == 200:
                data = resp.json()
                self._emit(
                    f"[{self._engine.name}] Post-run teardown OK "
                    f"(deleted={data.get('deleted')})"
                )
            else:
                self._emit(
                    f"[{self._engine.name}] Post-run teardown WARN: "
                    f"HTTP {resp.status_code} {resp.text[:200]}"
                )
        except Exception as e:
            self._emit(f"[{self._engine.name}] Post-run teardown WARN: {e}")

    def _run_fabric(self, batch_num: int) -> str:
        """Fabric engine batch flow (mirrors _run_databricks_enzyme):

        batch 1: (openivm) refresh Environment "35" with the JAR + config →
                 blow up prior Tables/ + state → drop stale-SF cache → azcopy
                 the batch-1 sources into the OneLake Files cache.
        batch 2/3: azcopy the per-batch staging increment into the cache.

        Then a dbt build against Fabric Spark (Livy). The source CREATE/INSERT
        SQL runs inside the dbt session via the load_fabric_sources
        on-run-start macro (batch-num aware). All wall-clock from the cache
        staging through the dbt build is inside the measured batch latency,
        matching the spark-openivm / databricks-enzyme convention.
        """
        name = self._engine.name
        is_openivm = name == "fabric-openivm-jvm-35"
        sf = self._config.scale_factor
        full_refresh = batch_num == 1

        if batch_num == 1:
            self._emit(
                f"[{name}] Provisioning fresh Fabric compute lakehouse + environment"
                f"{' (openivm JAR + Spark config)' if is_openivm else ''}"
            )
            resp = requests.post(
                f"{self._dbt_url}/environment/fabric/provision",
                json={"openivm": is_openivm}, timeout=2400,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"fabric provision failed: HTTP {resp.status_code} {resp.text[:500]}"
                )
            data = resp.json()
            res = data.get("resolved", {})
            self._emit(
                f"[{name}] Provisioned {res.get('lakehouse_name')} "
                f"(lakehouse={res.get('lakehouse_id')} environment={res.get('environment_id')}); "
                f"swept {len(data.get('swept', []))} orphan(s); "
                f"publish={(data.get('publish') or {}).get('publish_state')}"
            )

            last_sf = self._read_last_sf()
            if last_sf is not None and last_sf != sf:
                self._emit(
                    f"[{name}] SF changed {last_sf} -> {sf}; dropping cache sf={last_sf}"
                )
                try:
                    requests.post(
                        f"{self._dbt_url}/sources/fabric/cleanup-cache/{last_sf}",
                        timeout=1200,
                    )
                except Exception as e:
                    self._emit(f"[{name}] cleanup-cache/{last_sf} WARN: {e}")

            self._emit(f"[{name}] Staging sources into OneLake cache (sf={sf})")
            resp = requests.post(
                f"{self._dbt_url}/sources/fabric/init/{sf}", timeout=7200
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"fabric init/{sf} failed: HTTP {resp.status_code} {resp.text[:500]}"
                )
            data = resp.json()
            self._emit(
                f"[{name}] Cache init: files_uploaded={data.get('files_uploaded')} "
                f"already_seeded={data.get('already_seeded')}"
            )
        else:
            self._emit(
                f"[{name}] Staging batch {batch_num} increment into OneLake cache (sf={sf})"
            )
            resp = requests.post(
                f"{self._dbt_url}/sources/fabric/append/{batch_num}/{sf}", timeout=7200
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"fabric append/{batch_num}/{sf} failed: "
                    f"HTTP {resp.status_code} {resp.text[:500]}"
                )
            data = resp.json()
            self._emit(
                f"[{name}] Cache batch {batch_num}: files_uploaded={data.get('files_uploaded')} "
                f"already_seeded={data.get('already_seeded')}"
            )

        run_id = self._run_fabric_dbt(batch_num, full_refresh)
        if batch_num == 1:
            self._write_last_sf(sf)
        return run_id

    def _run_fabric_dbt(self, batch_num: int, full_refresh: bool) -> str:
        """POST /run/<engine> with batch_num (for the source macro), stream
        progress, check + save the result."""
        name = self._engine.name
        resp = requests.post(
            f"{self._dbt_url}/run/{name}",
            json={
                "scale_factor": self._config.scale_factor,
                "full_refresh": full_refresh,
                "batch_num": batch_num,
            },
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]
        self._emit(f"[{name}] dbt run_id={run_id} (batch {batch_num})")

        self._stream_dbt_progress(run_id, batch_num)
        self._check_run_result(run_id, batch_num)
        self._save_run_result(run_id, batch_num)
        return run_id

    def _up_with_retry(self, max_attempts: int = 3, backoff_s: int = 30) -> None:
        """Start the compose stack, retrying on transient mssql startup crashes.

        SQL Server 2025 occasionally hits the sqlpal NtumWaiter ASSERT on
        first boot under heavy host CPU load.  The container's own
        ``restart: on-failure:3`` policy then brings it back up within
        ~10-30s, but docker-compose's ``up -d`` has already exited with
        ``dependency mssql-metastore failed to start ... is unhealthy`` and
        will not retry on its own.  This wrapper re-issues the ``up`` after
        a short backoff so the second invocation can pick up the already-
        restarted mssql container and proceed.

        Any non-transient failure (compose build error, image-not-found,
        port conflict) re-raises immediately on the first attempt because
        the substring matchers below are narrow.
        """
        name = self._engine.name
        last_exc: Optional[Exception] = None
        for attempt in range(1, max_attempts + 1):
            try:
                self._engine_mgr.up(detach=True)
                return
            except RuntimeError as exc:
                msg = str(exc)
                # Only the mssql metastore boot is genuinely transient. Require the
                # message to name mssql-metastore — a bare "is unhealthy" match would
                # misclassify ANY unhealthy dependency (e.g. an engine server that
                # never comes up) as an mssql crash and burn all retries on it,
                # hiding the real error behind a wrong "transient mssql" label. Stacks
                # without mssql (e.g. dbspnet) now surface the real failure at once.
                transient = "mssql-metastore" in msg and (
                    "failed to start" in msg or "is unhealthy" in msg
                )
                if not transient or attempt == max_attempts:
                    raise
                last_exc = exc
                self._emit(
                    f"[{name}] up attempt {attempt}/{max_attempts} hit transient "
                    f"mssql startup crash — sleeping {backoff_s}s then retrying"
                )
                logger.warning(
                    "Transient mssql failure on attempt %d; retrying after %ds",
                    attempt,
                    backoff_s,
                )
                try:
                    self._engine_mgr.down()
                except Exception:
                    logger.exception("Cleanup down() failed between up retries")
                time.sleep(backoff_s)
        if last_exc is not None:
            raise last_exc

    # ----- Batch execution -----

    def _run_batch(self, batch_num: int) -> None:
        """Run a single batch for this engine."""
        name = self._engine.name
        batch = self._result.batches[batch_num - 1]
        batch.status = "running"
        self._emit(f"[{name}] Batch {batch_num} starting")
        self._persist_batch_result(batch_num, batch)

        try:
            # Append data for batches 2/3. DuckDB and DuckDB-OpenIVM manage
            # DuckLake source appends inside their measured batch path; Feldera
            # batches 2/3 append while the pipeline is paused inside _run_feldera_wait.
            # spark-openivm DOES use the batch-loader (so mount/raw/<SF>/delta/batchN
            # gets populated with the correct CDC shape), then _run_spark_openivm
            # additionally fires INSERT INTO via Livy AFTER the timer starts.
            if batch_num > 1 and name not in ("duckdb", "duckdb-openivm") and not (name == "feldera" and batch_num > 1):
                self._batch_loader_append(batch_num)
                self._capture_delta_stats(batch_num)

            if name == "databricks-enzyme":
                self._databricks_enzyme_warmup(batch_num)

            if not is_cloud_engine(name) and name != "feldera":
                self._start_cpu_measurement(batch_num)
            t0 = time.time()
            t0_ms = int(t0 * 1000)

            # Engines with a compile-then-process split (feldera) set this to their
            # own measured processing duration (resume→drain, compile excluded);
            # None means fall back to wall-clock below.
            self._measured_duration_s = None
            run_id = None
            if name == "feldera":
                if batch_num == 1:
                    self._run_feldera_batch1()
                else:
                    self._run_feldera_wait(batch_num)
            elif name == "dbspnet":
                if batch_num == 1:
                    self._run_dbspnet_batch1()
                else:
                    self._run_dbspnet_wait(batch_num)
            elif name == "duckdb":
                self._run_duckdb_ducklake(batch_num)
            elif name == "duckdb-openivm":
                run_id = self._run_duckdb_openivm(batch_num)
            elif name == "spark-openivm":
                run_id = self._run_spark_openivm(batch_num)
            elif name == "databricks-enzyme":
                run_id = self._run_databricks_enzyme(batch_num)
            elif name in ("fabric-openivm-jvm-35", "fabric-jvm-35"):
                run_id = self._run_fabric(batch_num)
            else:
                run_id = self._run_dbt(batch_num)

            wall_s = time.time() - t0
            # Prefer the engine's measured processing duration over wall-clock:
            # wall-clock folds the one-time compile into the batch, and feldera's
            # Rust build is minutes — it would dwarf the actual batch-1 processing
            # (the "not included in duration" compile the log already reports
            # separately). Databricks and Fabric retain end-to-end wall time.
            batch.duration_s = (
                self._measured_duration_s
                if self._measured_duration_s is not None
                else wall_s
            )
            t1_ms = int(time.time() * 1000)
            batch.extra["duration_s_wallclock"] = wall_s
            batch.extra["wall_window_start_ms"] = t0_ms
            batch.extra["wall_window_end_ms"] = t1_ms
            if not is_cloud_engine(name) and name != "feldera":
                self._finish_cpu_measurement(batch_num)

            if (
                name == "duckdb-openivm"
                and run_id
                and batch.status != "failed"
                and os.environ.get("OPENIVM_VALIDATE", "0") != "0"
            ):
                self._validate_duckdb_openivm(run_id, batch_num)

            if (
                name == "spark-openivm"
                and run_id
                and batch.status != "failed"
                and os.environ.get("OPENIVM_VALIDATE", "0") != "0"
            ):
                self._validate_spark_openivm(run_id, batch_num)

            if (
                name == "duckdb-openivm"
                and run_id
                and batch.status != "failed"
                and os.environ.get("OPENIVM_PROFILE_REFRESH", "0") == "1"
            ):
                self._export_duckdb_openivm_profile(run_id, batch_num)

            if (
                name == "spark-openivm"
                and run_id
                and batch.status != "failed"
                and os.environ.get("OPENIVM_PROFILE_REFRESH", "0") == "1"
            ):
                self._export_spark_openivm_profile(run_id, batch_num)

            if (
                name == "spark-openivm"
                and run_id
                and batch.status != "failed"
                and os.environ.get("OPENIVM_QUERY_LOG", "0") == "1"
            ):
                self._export_spark_openivm_query_log(run_id, batch_num)

            if (
                name in ("databricks-enzyme", "spark", "spark-openivm")
                and run_id
                and batch.status != "failed"
            ):
                self._export_query_plans(name, run_id, batch_num)

            if (
                name == "databricks-enzyme"
                and batch.status != "failed"
            ):
                self._export_databricks_enzyme_metrics(batch_num)
                # Pipeline-flow metrics are diagnostic only: autoscaling makes
                # them unsuitable as a CPU proxy, so batch.duration_s remains
                # the measured end-to-end wall clock.
                self._collect_databricks_enzyme_flow_metrics(batch_num, batch)

            # Check status from the stream_progress result
            if batch.status != "failed":
                self._save_openivm_ops_chart(name, batch_num)
                self._capture_storage_metrics(batch_num, batch)
                batch.status = "completed"
                self._emit(
                    f"[{name}] Batch {batch_num} completed in {batch.duration_s:.1f}s"
                )
        except Exception as e:
            batch.status = "failed"
            batch.error = str(e)
            raise
        finally:
            self._ensure_cpu_measurement_status(batch_num)
            # Always persist batch result to benchmark-server DB
            self._persist_batch_result(batch_num, batch)

    def _save_openivm_ops_chart(self, name: str, batch_num: int) -> None:
        if name not in ("spark-openivm", "duckdb-openivm"):
            return
        try:
            from .openivm_ops_chart import save_batch_png

            out = save_batch_png(
                sf=str(self._config.scale_factor),
                engine=name,
                batch=batch_num,
                repo_dir=os.environ.get("REPO_DIR", "/repo"),
            )
            if out:
                self._emit(f"[{name}] op-chart saved: {out}")
            else:
                self._emit(f"[{name}] op-chart render skipped: no telemetry found")
        except Exception as e:
            self._emit(f"[{name}] op-chart render skipped: {e}")

    def _run_dbt(self, batch_num: int) -> str:
        """Trigger a dbt run via the dbt-server REST API. Returns the run_id."""
        name = self._engine.name
        resp = requests.post(
            f"{self._dbt_url}/run/{name}",
            json={"scale_factor": self._config.scale_factor, "full_refresh": True},
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]
        self._emit(f"[{name}] dbt run_id={run_id}")

        self._stream_dbt_progress(run_id, batch_num)
        self._check_run_result(run_id, batch_num)
        self._save_run_result(run_id, batch_num)
        return run_id

    def _run_duckdb_ducklake(self, batch_num: int) -> None:
        """Run DuckDB full refresh against DuckLake-backed source tables."""
        if batch_num == 1:
            self._emit("[duckdb] Initialising DuckLake sources")
            resp = requests.post(f"{self._dbt_url}/sources/duckdb/init", timeout=600)
            resp.raise_for_status()
            src_result = resp.json()
            self._emit(f"[duckdb] Sources initialised: {src_result.get('tables_created', '?')} tables")
        else:
            self._emit(f"[duckdb] Appending batch {batch_num} sources")
            resp = _post_with_retry(
                f"{self._dbt_url}/sources/duckdb/append/{batch_num}",
                timeout=600,
                emit=self._emit,
                label="duckdb",
            )
            src_result = resp.json()
            self._emit(
                f"[duckdb] Batch {batch_num} appended: "
                f"{src_result.get('tables_appended', '?')} tables"
            )

        self._run_dbt(batch_num)

    def _run_feldera_batch1(self) -> None:
        """Feldera batch 1: compile pipeline (paused), resume, then poll for processing.

        Flow:
        1. dbt run compiles all models → deploys pipeline → starts → pauses
           (adapter auto-pauses after start so no data is ingested prematurely)
        2. Resume pipeline via dbt-server /resume/feldera
        3. Poll /wait/feldera for stats-based per-output completion tracking
        """
        resp = requests.post(
            f"{self._dbt_url}/run/feldera",
            json={"scale_factor": self._config.scale_factor, "full_refresh": True},
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]
        self._emit(f"[feldera] dbt run_id={run_id}")

        # Stream compilation progress using cursor polling
        self._poll_feldera_compilation(run_id)
        self._emit("[feldera] Compilation done — pipeline is paused, ready to measure")

        # Fetch compile time recorded by the adapter
        compile_time_s = None
        try:
            ct_resp = requests.get(f"{self._dbt_url}/compile-time/feldera", timeout=10)
            if ct_resp.status_code == 200:
                compile_time_s = ct_resp.json().get("compile_time_s")
        except Exception:
            pass

        # Resume pipeline — this is the start of the benchmark measurement
        self._start_cpu_measurement(1)
        resume_resp = requests.post(f"{self._dbt_url}/resume/feldera", timeout=60)
        resume_resp.raise_for_status()
        start_epoch = resume_resp.json().get("resumed_at_epoch_s", time.time())

        self._emit("[feldera] Pipeline resumed — waiting for processing to complete")

        # Wait for pipeline to process all data, tracking per-output times
        wait_resp = requests.post(
            f"{self._dbt_url}/wait/feldera",
            json={
                "scale_factor": self._config.scale_factor,
                "batch_num": 1,
                "start_epoch_s": start_epoch,
                "compile_time_s": compile_time_s,
            },
            timeout=604800,
        )
        wait_data = wait_resp.json()
        self._finish_cpu_measurement(1)

        if wait_resp.status_code != 200:
            raise RuntimeError(f"Feldera batch 1 wait failed: {wait_data.get('error', 'unknown')}")

        duration = wait_data.get("duration_s", "?")
        if isinstance(duration, (int, float)):
            # Compile-excluded processing time (measured from resume) — record this
            # as the batch duration instead of wall-clock, which includes compile.
            self._measured_duration_s = float(duration)
        compile_info = wait_data.get("compile_time_s")
        self._emit(f"[feldera] Pipeline processing time: {duration}s")
        if compile_info:
            self._emit(f"[feldera] Compile time (not included in duration): {compile_info}s")

        # Store compile time in result for chart annotation
        if compile_info:
            self._result.extra["compile_time_s"] = compile_info

        # Save result
        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        with open(os.path.join(results_dir, "run-feldera-batch1.json"), "w") as f:
            json.dump(wait_data, f, indent=2)

    def _run_dbspnet_batch1(self) -> None:
        """DbspNet batch 1: deploy the program (translate + compile + wire connectors),
        resume (timer start), then wait for the batch to drain.

        Unlike Feldera there is no dbt build / Rust compile: the dbt-server's dbspnet
        handler translates the dbt project into a program and POSTs it to the DbspNet
        control service. Deploy is excluded from the measured duration (like Feldera's
        compile); resume→wait is the measured batch.
        """
        deploy_resp = requests.post(f"{self._dbt_url}/deploy/dbspnet", timeout=1800)
        deploy_resp.raise_for_status()
        compile_time_s = deploy_resp.json().get("compileTimeS")
        self._emit(f"[dbspnet] Deployed — compile {compile_time_s}s (not measured)")

        resume_resp = requests.post(f"{self._dbt_url}/resume/dbspnet", timeout=60)
        resume_resp.raise_for_status()
        start_epoch = resume_resp.json().get("resumed_at_epoch_s", time.time())
        self._emit("[dbspnet] Resumed — waiting for batch 1 to drain")

        wait_resp = requests.post(
            f"{self._dbt_url}/wait/dbspnet",
            json={
                "scale_factor": self._config.scale_factor,
                "batch_num": 1,
                "start_epoch_s": start_epoch,
                "compile_time_s": compile_time_s,
            },
            timeout=604800,
        )
        wait_data = wait_resp.json()
        if wait_resp.status_code != 200:
            raise RuntimeError(f"DbspNet batch 1 wait failed: {wait_data.get('error', 'unknown')}")

        self._emit(f"[dbspnet] Batch 1 processing time: {wait_data.get('duration_s', '?')}s")
        if compile_time_s:
            self._result.extra["compile_time_s"] = compile_time_s

        self._save_dbspnet_result(1, wait_data)

    def _run_dbspnet_wait(self, batch_num: int) -> None:
        """DbspNet batches 2/3: the batch-loader has already appended new source versions
        (generic append in _run_batch, before the timer). Resume drains only those new
        versions; wait blocks until done."""
        resume_resp = requests.post(f"{self._dbt_url}/resume/dbspnet", timeout=60)
        resume_resp.raise_for_status()
        start_epoch = resume_resp.json().get("resumed_at_epoch_s", time.time())
        self._emit(f"[dbspnet] Resumed for batch {batch_num}")

        wait_resp = requests.post(
            f"{self._dbt_url}/wait/dbspnet",
            json={
                "scale_factor": self._config.scale_factor,
                "batch_num": batch_num,
                "start_epoch_s": start_epoch,
            },
            timeout=604800,
        )
        wait_data = wait_resp.json()
        if wait_resp.status_code != 200:
            raise RuntimeError(f"DbspNet batch {batch_num} wait failed: {wait_data.get('error', 'unknown')}")

        self._emit(f"[dbspnet] Batch {batch_num} processing time: {wait_data.get('duration_s', '?')}s")
        self._save_dbspnet_result(batch_num, wait_data)

    def _save_dbspnet_result(self, batch_num: int, wait_data: dict) -> None:
        results_dir = os.path.join(
            self._config.repo_dir, "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        with open(os.path.join(results_dir, f"run-dbspnet-batch{batch_num}.json"), "w") as f:
            json.dump(wait_data, f, indent=2)

    def _poll_feldera_compilation(self, run_id: str) -> None:
        """Poll dbt-server progress endpoint until all Feldera models are compiled.

        The poll loop is resilient to transient HTTP errors. Under heavy host
        load (e.g. Feldera Rust compilation pegging all cores), the cross-
        container HTTP call from benchmark-server → dbt-server via
        host.docker.internal can hit a transient stall and raise
        ``requests.Timeout`` / ``requests.ConnectionError`` / JSON decode
        errors. These are non-fatal: the dbt run and the Feldera compilation
        keep progressing independently, so we just retry the next poll.

        Stale detection uses wall-clock time (``time.monotonic()``) rather
        than iteration counting so that retried timeouts cannot artificially
        blow out the 30-minute compilation guard.
        """
        cursor = 0
        max_stale_s = 1800  # 30 min wall-clock without progress
        now = time.monotonic
        last_progress_at = now()
        last_status_emit_at = now()
        consecutive_errors = 0
        last_warn_at = 0.0

        while True:
            try:
                resp = requests.get(
                    f"{self._dbt_url}/runs/{run_id}/progress",
                    params={"since": cursor},
                    timeout=(5, 30),
                )
                data = resp.json()
                events = data.get("events", [])
                total = data.get("total", 0)
                status = data.get("run_status", "running")

                if consecutive_errors > 0:
                    self._emit(
                        f"[feldera] Polling recovered after "
                        f"{consecutive_errors} transient error(s)"
                    )
                    consecutive_errors = 0

                if events:
                    last_progress_at = now()
                    for i, e in enumerate(events):
                        st = e.get("status", "")
                        name = e.get("name", "")
                        t = e.get("execution_time_s")
                        idx = cursor + i + 1
                        if st in ("success", "pass"):
                            ts = f"{t:.2f}s" if t else "?"
                            self._emit(f"[feldera]   {idx:>3} of {total}  OK    model {name} [{ts}]")
                        elif st == "running":
                            self._emit(f"[feldera]   {idx:>3} of {total}  START model {name}")
                        elif st == "error":
                            self._emit(f"[feldera]   {idx:>3} of {total}  ERROR model {name}")
                    cursor = data.get("next_cursor", cursor + len(events))

                stale_s = now() - last_progress_at
                # Emit periodic status every ~30s while waiting for Feldera
                # pipeline compilation (after all models are compiled by dbt
                # but before the on-run-end hook finishes).
                if (
                    cursor >= total > 0
                    and not events
                    and now() - last_status_emit_at >= 30
                ):
                    self._emit(
                        f"[feldera] Waiting for pipeline compilation... "
                        f"({stale_s:.0f}s)"
                    )
                    last_status_emit_at = now()

                # Wait for the dbt run to fully complete (including on-run-end
                # hook which compiles the Feldera pipeline binary and starts
                # it paused). Just having all model events isn't enough — the
                # on-run-end hook triggers the actual Feldera compilation
                # which can take minutes.
                if status == "completed":
                    return
                if status == "failed":
                    raise RuntimeError("Feldera dbt run failed during compilation")
                if stale_s > max_stale_s:
                    raise TimeoutError(
                        f"Feldera compilation stalled for {stale_s:.0f}s"
                    )

            except (
                requests.ConnectionError,
                requests.Timeout,
                requests.exceptions.JSONDecodeError,
            ) as e:
                consecutive_errors += 1
                # Throttle warnings: first failure, then at most every ~30s.
                t = now()
                if t - last_warn_at >= 30:
                    self._emit(
                        f"[feldera] Transient poll error #{consecutive_errors} "
                        f"({type(e).__name__}: {e}) — retrying"
                    )
                    last_warn_at = t
                # Wall-clock stale check still applies during error storms so
                # a genuinely dead dbt-server eventually trips max_stale_s.
                if now() - last_progress_at > max_stale_s:
                    raise TimeoutError(
                        f"Feldera compilation polling stalled for "
                        f"{max_stale_s}s ({consecutive_errors} consecutive errors)"
                    )

            time.sleep(1)

    def _run_duckdb_openivm(self, batch_num: int) -> str:
        """Run DuckDB-OpenIVM: init/append sources, then standard dbt build."""
        full_refresh = batch_num == 1

        # Source management: init or append before dbt build
        if batch_num == 1:
            self._emit("[duckdb-openivm] Initialising DuckLake sources")
            resp = requests.post(
                f"{self._dbt_url}/sources/duckdb-openivm/init", timeout=600,
            )
            resp.raise_for_status()
            src_result = resp.json()
            self._emit(
                f"[duckdb-openivm] Sources initialised: {src_result.get('tables_created', '?')} tables"
            )
        else:
            self._emit(f"[duckdb-openivm] Appending batch {batch_num} sources")
            resp = _post_with_retry(
                f"{self._dbt_url}/sources/duckdb-openivm/append/{batch_num}",
                timeout=600,
                emit=self._emit,
                label="duckdb-openivm",
            )
            src_result = resp.json()
            self._emit(
                f"[duckdb-openivm] Batch {batch_num} appended: "
                f"{src_result.get('tables_appended', '?')} tables"
            )

        # Standard dbt build
        resp = requests.post(
            f"{self._dbt_url}/run/duckdb-openivm",
            json={
                "scale_factor": self._config.scale_factor,
                "full_refresh": full_refresh,
            },
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]
        self._emit(
            f"[duckdb-openivm] dbt run_id={run_id} "
            f"(batch={batch_num}, full_refresh={full_refresh})"
        )

        self._stream_dbt_progress(run_id, batch_num)
        self._check_run_result(run_id, batch_num)
        self._save_run_result(run_id, batch_num)
        return run_id

    def _run_spark_openivm(self, batch_num: int) -> str:
        """Run spark-openivm: source init/append via DML, then dbt build.

        Flow per batch:
          batch 1 (full refresh):
            1. /sources/spark-openivm/init     — creates db + tracked Delta tables,
                                                  bulk-loads batch1 data via
                                                  INSERT INTO ... SELECT FROM delta.`...`
            2. dbt build --full-refresh        — fabricspark issues DROP MV +
                                                  CREATE MATERIALIZED VIEW per model.
          batch 2/3 (incremental):
            1. /sources/spark-openivm/append/<N> — INSERT INTO tpcdi.staging_<t>
                                                    SELECT * FROM delta.`batchN/...`
                                                    (writes Delta CDF records
                                                    consumed by REFRESH)
            2. dbt build                          — fabricspark issues
                                                    REFRESH MATERIALIZED VIEW per model.

        All wall-clock between the source mutation and the dbt build *is*
        part of the measured batch latency, mirroring the duckdb-openivm
        flow where init/append+dbt are both inside `t0..t1`.
        """
        full_refresh = batch_num == 1

        if batch_num == 1:
            self._emit("[spark-openivm] Initialising sources (DML via Livy)")
            resp = _post_with_retry(
                f"{self._dbt_url}/sources/spark-openivm/init",
                timeout=3600,
                emit=self._emit,
                label="spark-openivm",
            )
            src_result = resp.json()
            if src_result.get("status") != "ok":
                raise RuntimeError(
                    f"spark-openivm source init failed: {src_result.get('error', src_result)}"
                )
            self._emit(
                f"[spark-openivm] Sources initialised: {src_result.get('tables_created', '?')} tables"
            )
        else:
            self._emit(f"[spark-openivm] Appending batch {batch_num} sources (DML via Livy)")
            resp = _post_with_retry(
                f"{self._dbt_url}/sources/spark-openivm/append/{batch_num}",
                timeout=3600,
                emit=self._emit,
                label="spark-openivm",
            )
            src_result = resp.json()
            if src_result.get("status") != "ok":
                raise RuntimeError(
                    f"spark-openivm source append batch {batch_num} failed: "
                    f"{src_result.get('error', src_result)}"
                )
            self._emit(
                f"[spark-openivm] Batch {batch_num} appended: "
                f"{src_result.get('tables_appended', '?')} tables"
            )

        # Standard dbt build through the fabricspark adapter — the custom
        # materialized_view materialization (in dbt-projects/spark-openivm/
        # macros/materializations/materialized_view.sql) dispatches to
        # CREATE/REFRESH MV per the full_refresh flag.
        resp = requests.post(
            f"{self._dbt_url}/run/spark-openivm",
            json={
                "scale_factor": self._config.scale_factor,
                "full_refresh": full_refresh,
            },
            timeout=30,
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]
        self._emit(
            f"[spark-openivm] dbt run_id={run_id} "
            f"(batch={batch_num}, full_refresh={full_refresh})"
        )

        self._stream_dbt_progress(run_id, batch_num)
        self._check_run_result(run_id, batch_num)
        self._save_run_result(run_id, batch_num)
        return run_id

    def _last_sf_marker_path(self) -> str:
        """Host-side marker recording the SF of the last databricks-enzyme run.

        Lives under `mount/benchmark-state/` (the only directory not wiped
        between experiments — see services/orchestrator._clean_mount). When
        the SF changes between two experiments, we drop the previous SF's
        UC Volume subdir to cap storage cost.
        """
        return os.path.join(
            self._config.repo_dir,
            "mount", "benchmark-state", "databricks-enzyme.last-sf",
        )

    def _read_last_sf(self) -> Optional[int]:
        path = self._last_sf_marker_path()
        try:
            with open(path) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def _write_last_sf(self, sf: int) -> None:
        path = self._last_sf_marker_path()
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                f.write(str(sf))
        except OSError as e:
            logger.warning("Failed to write last-sf marker %s: %s", path, e)

    def _databricks_enzyme_warmup(self, batch_num: int) -> None:
        """Run `SELECT 1` against the Databricks SQL warehouse so its
        cold-start latency is NOT charged against the measured batch time.

        Called from `_run_batch` for every batch immediately before
        ``t0 = time.time()``. If the warmup itself fails the batch is
        aborted with a clear error rather than silently penalising the
        engine with a multi-minute warehouse-resume in the timer.
        """
        try:
            resp = requests.post(
                f"{self._dbt_url}/sources/databricks-enzyme/warmup",
                timeout=600,
            )
        except Exception as e:
            raise RuntimeError(
                f"databricks-enzyme warmup request failed before batch {batch_num}: {e}"
            )
        if resp.status_code != 200:
            raise RuntimeError(
                f"databricks-enzyme warmup failed before batch {batch_num}: "
                f"HTTP {resp.status_code} {resp.text[:500]}"
            )
        try:
            data = resp.json()
        except Exception:
            data = {}
        if data.get("status") != "ok":
            raise RuntimeError(
                f"databricks-enzyme warmup returned non-ok before batch {batch_num}: {data}"
            )
        self._emit(
            f"[databricks-enzyme] Warehouse warm (batch {batch_num} pre-timer): "
            f"attempts={data.get('attempts')} elapsed_s={data.get('elapsed_s')}"
        )

    def _databricks_enzyme_validate_incrementalizable(self, sf: int) -> None:
        """Pre-flight diagnostic: EXPLAIN every model under INCREMENTAL STRICT.

        POSTs to dbt-server's
        ``/validate/databricks-enzyme/explain-create-materialized-view/<sf>``
        which runs EXPLAIN CREATE MATERIALIZED VIEW ... REFRESH POLICY
        INCREMENTAL STRICT AS <compiled_sql> for every non-ephemeral
        model in the databricks-enzyme dbt project against the Serverless
        SQL warehouse.

        **Record-only, NOT a gate.** Since the engine now runs under
        REFRESH POLICY AUTO (databricks-enzyme/dbt_project.yml), models
        that the STRICT planner rejects with
        ``MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE: <reason>`` are still
        materializable — Databricks falls back to FULL refresh. We keep
        the STRICT sweep because it's the only way to surface the
        per-model reason strings that feed the
        ``benchmark-heuristics.png`` incrementalization-coverage panel.

        Called after ``init/<sf>`` succeeds (so the planner can resolve
        ``tpcdi_src.*``) and before the dbt build. Cost is in batch 1's
        timer to match the init/<sf>-in-timer convention used by every
        other engine for initial source loading.

        Persists per-model artifacts to
        ``mount/query-plan/<sf>/databricks-enzyme/
        explain-create-materialized-view/`` so it's visually obvious
        which route produced what data — the directory name maps 1-to-1
        with the route segment. The chart code reads
        ``summary.json`` from there to derive the per-model verdicts.

        Raises ``RuntimeError`` only on transport-layer failures
        (non-200/non-422 HTTP, request timeout, malformed JSON). Does
        NOT raise on per-model EXPLAIN failures.
        """
        self._emit(
            f"[databricks-enzyme] Pre-flight diagnostic: EXPLAIN every model "
            f"under REFRESH POLICY INCREMENTAL STRICT (record-only, "
            f"actual run uses AUTO) sf={sf}"
        )
        try:
            resp = requests.post(
                f"{self._dbt_url}/validate/databricks-enzyme/"
                f"explain-create-materialized-view/{sf}",
                timeout=1800,
            )
        except Exception as e:
            raise RuntimeError(
                f"databricks-enzyme explain-create-materialized-view sf={sf} "
                f"request failed: {e}"
            )
        if resp.status_code not in (200, 422):
            raise RuntimeError(
                f"databricks-enzyme explain-create-materialized-view sf={sf} "
                f"failed: HTTP {resp.status_code} {resp.text[:500]}"
            )
        try:
            data = resp.json()
        except Exception as e:
            raise RuntimeError(
                f"databricks-enzyme explain-create-materialized-view sf={sf} "
                f"returned non-JSON: {e}: {resp.text[:500]}"
            )

        self._write_databricks_enzyme_explain_artifacts(sf, data)

        passed = int(data.get("passed") or 0)
        failed = int(data.get("failed") or 0)
        total = int(data.get("total_models") or 0)
        if failed:
            failures = data.get("failures") or []
            sample = ", ".join(f.get("model", "?") for f in failures[:5])
            extra = "" if len(failures) <= 5 else f" (+{len(failures) - 5} more)"
            self._emit(
                f"[databricks-enzyme] Pre-flight: {failed}/{total} models "
                f"NOT incrementalizable under STRICT (will use FULL refresh "
                f"under AUTO): {sample}{extra}. "
                f"See mount/query-plan/{sf}/databricks-enzyme/"
                f"explain-create-materialized-view/summary.json for reasons."
            )
        self._emit(
            f"[databricks-enzyme] Pre-flight done: "
            f"{passed}/{total} incrementalizable, {failed}/{total} fallback-to-FULL; "
            f"{len(data.get('skipped_ephemeral') or [])} ephemeral skipped; "
            f"elapsed_ms={data.get('elapsed_ms')}"
        )

    def _write_databricks_enzyme_explain_artifacts(
        self, sf: int, data: Dict[str, Any],
    ) -> None:
        """Persist the EXPLAIN sweep response to mount under a directory
        whose name matches the route segment that produced it.

        Layout:
          mount/query-plan/<sf>/databricks-enzyme/
            explain-create-materialized-view/
              summary.json           ← response minus the bulky `plans` field
              <sanitized_model>.txt  ← EXPLAIN plan (or error trace)
              <sanitized_model>.sql  ← exact compiled SQL we sent
        """
        base_dir = os.path.join(
            self._config.repo_dir,
            "mount", "query-plan", str(self._config.scale_factor),
            "databricks-enzyme", "explain-create-materialized-view",
        )
        os.makedirs(base_dir, exist_ok=True)

        plans = data.get("plans") or []
        for plan in plans:
            name = plan.get("model") or plan.get("unique_id") or "unknown"
            safe_name = "".join(
                c if c.isalnum() or c in "._-" else "_" for c in name
            )
            txt_path = os.path.join(base_dir, f"{safe_name}.txt")
            sql_path = os.path.join(base_dir, f"{safe_name}.sql")
            plan_body = plan.get("plan") or ""
            if plan.get("error"):
                header = (
                    f"-- STATUS: NOT INCREMENTALIZABLE --\n"
                    f"-- {plan.get('error')}\n"
                )
                body = header + (plan_body or "")
            else:
                body = plan_body
            with open(txt_path, "w") as f:
                f.write(body)
            with open(sql_path, "w") as f:
                f.write(plan.get("compiled_sql") or "")

        summary = {k: v for k, v in data.items() if k != "plans"}
        with open(os.path.join(base_dir, "summary.json"), "w") as f:
            json.dump(summary, f, indent=2, default=str)

        self._emit(
            f"[databricks-enzyme] EXPLAIN artifacts written: "
            f"{base_dir} ({len(plans)} models)"
        )

    def _run_databricks_enzyme(self, batch_num: int) -> str:
        """Run databricks-enzyme: per-batch source sync, then dbt build.

        Batch 1 (full refresh):
          0. (Optional) If a prior experiment ran at a different SF, drop
             its `sf=<prev>` Volume subdir to cap storage cost.
          1. cleanup-schema  — DROP `tpcdi_bench` + `tpcdi_src` CASCADE so
                                last run's MVs stop accruing refresh bills.
          2. init/<sf>       — idempotent upload of local Delta dirs to
                                /Volumes/<catalog>/<schema>/<volume>/sf=<sf>/...
                                plus register source views/tables.
          3. dbt build --full-refresh  — custom MV materialization emits
                                DROP MV + CREATE MV + ALTER MV SET REFRESH
                                POLICY <policy>.

        Batch 2/3 (incremental):
          1. _batch_loader_append already ran (in `_run_batch`) — writes
             new CDC files to /data/raw/delta/{batchN,staging}/<t>/.
          2. append/<batch_num>/<sf> — sync those new files up to the
                                       UC Volume. If strategy is CTAS,
                                       also INSERT INTO the managed
                                       Delta tables.
          3. dbt build (no --full-refresh) — custom MV materialization
                                emits REFRESH MATERIALIZED VIEW per model.

        All wall-clock between the source mutation and the dbt build *is*
        part of the measured batch latency, mirroring the spark-openivm /
        duckdb-openivm flow.
        """
        full_refresh = batch_num == 1
        sf = self._config.scale_factor

        if batch_num == 1:
            # Per-experiment isolation
            self._emit("[databricks-enzyme] Sweeping stale exp_* schemas (> 1 day)")
            try:
                resp = requests.post(
                    f"{self._dbt_url}/sources/databricks-enzyme/sweep-stale",
                    timeout=600,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    self._emit(
                        f"[databricks-enzyme] sweep-stale OK: "
                        f"scanned={data.get('scanned')} "
                        f"dropped={len(data.get('dropped') or [])} "
                        f"kept={len(data.get('kept') or [])}"
                    )
                else:
                    self._emit(
                        f"[databricks-enzyme] sweep-stale WARN: "
                        f"HTTP {resp.status_code} {resp.text[:200]}"
                    )
            except Exception as e:
                self._emit(f"[databricks-enzyme] sweep-stale WARN: {e}")

            last_sf = self._read_last_sf()
            if last_sf is not None and last_sf != sf:
                self._emit(
                    f"[databricks-enzyme] SF changed {last_sf} -> {sf}; "
                    f"dropping cache sf={last_sf} subdir"
                )
                try:
                    resp = requests.post(
                        f"{self._dbt_url}/sources/databricks-enzyme/cleanup-volume/{last_sf}",
                        timeout=600,
                    )
                    if resp.status_code != 200:
                        self._emit(
                            f"[databricks-enzyme] cleanup-volume/{last_sf} WARN: "
                            f"HTTP {resp.status_code} {resp.text[:200]}"
                        )
                except Exception as e:
                    self._emit(
                        f"[databricks-enzyme] cleanup-volume/{last_sf} WARN: {e}"
                    )

            # Per-experiment cleanup-schema
            self._emit(
                "[databricks-enzyme] Dropping prior exp_<ts>_* schemas (idempotent)"
            )
            resp = requests.post(
                f"{self._dbt_url}/sources/databricks-enzyme/cleanup-schema",
                timeout=600,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"databricks-enzyme cleanup-schema failed: "
                    f"HTTP {resp.status_code} {resp.text[:500]}"
                )

            self._emit(f"[databricks-enzyme] Initialising sources for sf={sf}")
            resp = requests.post(
                f"{self._dbt_url}/sources/databricks-enzyme/init/{sf}",
                timeout=7200,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"databricks-enzyme init/{sf} failed: "
                    f"HTTP {resp.status_code} {resp.text[:500]}"
                )
            data = resp.json()
            if data.get("status") != "ok":
                raise RuntimeError(
                    f"databricks-enzyme init/{sf} returned non-ok: {data}"
                )
            self._emit(
                f"[databricks-enzyme] Sources initialised: strategy={data.get('strategy')} "
                f"files_uploaded={data.get('files_uploaded')} "
                f"tables={data.get('tables_created')} "
                f"skipped_upload={data.get('skipped_upload')}"
            )

            # Pre-flight incrementalizability gate. Runs after init/<sf>
            # so `tpcdi_src.*` exists for the planner to resolve, but
            # BEFORE the dbt build so a failure short-circuits without
            # any `CREATE MATERIALIZED VIEW` being emitted. Cost is in
            # batch 1's timer (matches the init/<sf>-in-timer convention
            # used by every other engine for initial source loading).
            self._databricks_enzyme_validate_incrementalizable(sf)
        else:
            self._emit(
                f"[databricks-enzyme] Syncing batch {batch_num} sources to "
                f"UC Volume (sf={sf})"
            )
            resp = requests.post(
                f"{self._dbt_url}/sources/databricks-enzyme/append/{batch_num}/{sf}",
                timeout=7200,
            )
            if resp.status_code != 200:
                raise RuntimeError(
                    f"databricks-enzyme append/{batch_num}/{sf} failed: "
                    f"HTTP {resp.status_code} {resp.text[:500]}"
                )
            data = resp.json()
            if data.get("status") != "ok":
                raise RuntimeError(
                    f"databricks-enzyme append/{batch_num}/{sf} non-ok: {data}"
                )
            self._emit(
                f"[databricks-enzyme] Batch {batch_num} synced: "
                f"strategy={data.get('strategy')} "
                f"uploaded={data.get('files_uploaded')} "
                f"skipped={data.get('files_skipped')} "
                f"appended={data.get('tables_appended')}"
            )

        # Standard dbt build through the dbt-databricks adapter — the
        # custom materialized_view materialization (in dbt-projects/
        # databricks-enzyme/macros/materializations/materialized_view.sql)
        # dispatches to CREATE MV (+ALTER MV SET REFRESH POLICY) on
        # full_refresh and REFRESH MV otherwise. Wrapped in a
        # retry-with-backoff loop because Databricks DLT pipelines
        # transiently fail with INTERNAL_ERROR / lost-executor
        # Connection refused on long-running refreshes — see
        # `_is_databricks_transient`. The wall-clock timer in
        # `_run_batch` covers all retry attempts.
        run_id = self._run_databricks_enzyme_dbt_with_retry(
            batch_num, full_refresh
        )

        if batch_num == 1:
            self._write_last_sf(sf)

        return run_id

    def _run_databricks_enzyme_dbt_with_retry(
        self, batch_num: int, full_refresh: bool
    ) -> str:
        """POST /run/databricks-enzyme, stream progress, check result.

        Retries the dbt build on transient Databricks DLT platform
        failures (lost executor, INTERNAL_ERROR pipeline restart). A
        real model bug — anything whose dbt error message does NOT
        match a transient signature — fails immediately. Returns the
        run_id of the FINAL attempt (whether success or final failure)
        so downstream artifact collection works against the right run.
        """
        sf = self._config.scale_factor
        batch = self._result.batches[batch_num - 1]
        max_attempts = DATABRICKS_ENZYME_MAX_RETRIES + 1
        last_run_id: str = ""

        for attempt in range(1, max_attempts + 1):
            resp = requests.post(
                f"{self._dbt_url}/run/databricks-enzyme",
                json={
                    "scale_factor": sf,
                    "full_refresh": full_refresh,
                },
                timeout=30,
            )
            resp.raise_for_status()
            run_id = resp.json()["run_id"]
            last_run_id = run_id
            attempt_suffix = (
                f" (attempt {attempt}/{max_attempts})"
                if max_attempts > 1
                else ""
            )
            self._emit(
                f"[databricks-enzyme] dbt run_id={run_id} "
                f"(batch={batch_num}, full_refresh={full_refresh})"
                f"{attempt_suffix}"
            )

            self._stream_dbt_progress(run_id, batch_num)
            self._check_run_result(run_id, batch_num)

            if batch.status != "failed":
                # Success — save final result under canonical filename.
                self._save_run_result(run_id, batch_num)
                return run_id

            transient, summary = self._is_databricks_transient(run_id)
            if not transient or attempt == max_attempts:
                # Real bug, or retries exhausted — surface the failure.
                self._save_run_result(run_id, batch_num)
                if attempt == max_attempts and transient:
                    self._emit(
                        f"[databricks-enzyme] batch {batch_num} retries "
                        f"exhausted after {max_attempts} attempts; last "
                        f"failure was transient ({summary})"
                    )
                return run_id

            # Transient: archive this attempt under a numbered filename
            # so forensics keep both the failed and the successful runs,
            # then reset batch state and back off before the next try.
            self._save_run_result(
                run_id, batch_num, suffix=f"-attempt{attempt}"
            )
            backoff = DATABRICKS_ENZYME_RETRY_BACKOFF_S * (2 ** (attempt - 1))
            self._emit(
                f"[databricks-enzyme] batch {batch_num} attempt {attempt} "
                f"hit transient Databricks failure ({summary}); "
                f"sleeping {backoff}s then retrying"
            )
            logger.warning(
                "databricks-enzyme batch %d attempt %d transient failure "
                "(%s); retrying after %ds",
                batch_num, attempt, summary, backoff,
            )
            batch.status = "running"
            batch.error = ""
            time.sleep(backoff)

        return last_run_id

    def _is_databricks_transient(self, run_id: str) -> tuple[bool, str]:
        """Inspect failed nodes from a dbt run and decide if the failure
        was a Databricks-side platform transient (worth retrying) vs a
        real model/SQL bug (must surface immediately).

        Returns (is_transient, short_summary). The decision is
        conservative — if ANY failed node has a non-transient error,
        the whole run is treated as non-transient because re-running
        the build will deterministically hit the real bug again.
        """
        try:
            resp = requests.get(f"{self._dbt_url}/runs/{run_id}", timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            return False, f"could not fetch run JSON: {exc}"

        failed_nodes = [
            n for n in (data.get("nodes") or [])
            if n.get("status") in ("error", "fail")
        ]
        if not failed_nodes:
            # No node-level failures but the run was marked failed —
            # almost always a dbt compile / parse error, not transient.
            return False, "no failing nodes (likely compile/parse error)"

        transient_nodes = []
        non_transient_nodes = []
        for n in failed_nodes:
            msg = n.get("message") or ""
            hit = next(
                (sig for sig in DATABRICKS_TRANSIENT_SIGNATURES if sig in msg),
                None,
            )
            if hit:
                transient_nodes.append((n.get("name", "?"), hit))
            else:
                non_transient_nodes.append((n.get("name", "?"), msg[:120]))

        if non_transient_nodes:
            sample = non_transient_nodes[0]
            return (
                False,
                f"{len(non_transient_nodes)}/{len(failed_nodes)} non-transient "
                f"(e.g. {sample[0]}: {sample[1]!r})",
            )

        sig_summary = ", ".join(
            f"{n}:{s.split(':')[0]}" for n, s in transient_nodes[:3]
        )
        more = (
            f" (+{len(transient_nodes) - 3} more)"
            if len(transient_nodes) > 3 else ""
        )
        return True, f"all {len(transient_nodes)} transient — {sig_summary}{more}"

    def _validate_duckdb_openivm(self, run_id: str, batch_num: int) -> None:
        """Run default OpenIVM correctness validation outside the benchmark timer."""
        self._emit(f"[duckdb-openivm] Validating batch {batch_num} with EXCEPT ALL")
        try:
            resp = requests.post(
                f"{self._dbt_url}/validate/duckdb-openivm/{run_id}",
                timeout=604800,
            )
            data = resp.json()
        except Exception as e:
            # Transport / JSON failure during validation is just as fatal as
            # an explicit diff — we can't conclude correctness either way.
            raise OpenIvmValidationError(
                f"OpenIVM validation request failed for batch {batch_num}: {e}"
            ) from e

        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        with open(
            os.path.join(results_dir, f"validation-duckdb-openivm-batch{batch_num}.json"),
            "w",
        ) as f:
            json.dump(data, f, indent=2)

        if resp.status_code != 200 or data.get("status") != "passed":
            failures = data.get("failures") or []
            detail = ", ".join(
                f"{f.get('name')} diff={f.get('diff_count')}" for f in failures[:5]
            )
            raise OpenIvmValidationError(
                f"OpenIVM validation failed for batch {batch_num}"
                + (f": {detail}" if detail else f": {data.get('error', 'unknown error')}")
            )
        self._emit(
            f"[duckdb-openivm] Validation passed for batch {batch_num}: "
            f"{data.get('models_checked', 0)} models in {data.get('duration_s', '?')}s"
        )

    def _validate_spark_openivm(self, run_id: str, batch_num: int) -> None:
        """Run OpenIVM correctness validation against the spark-openivm
        materialized views over Livy. Mirrors `_validate_duckdb_openivm`
        — keeps the per-batch hook + result-JSON shape engine-agnostic so
        the existing chart/aggregate pipeline can consume both."""
        self._emit(f"[spark-openivm] Validating batch {batch_num} with EXCEPT ALL")
        try:
            resp = requests.post(
                f"{self._dbt_url}/validate/spark-openivm/{run_id}",
                timeout=604800,
            )
            data = resp.json()
        except Exception as e:
            raise OpenIvmValidationError(
                f"OpenIVM validation request failed for batch {batch_num}: {e}"
            ) from e

        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        with open(
            os.path.join(results_dir, f"validation-spark-openivm-batch{batch_num}.json"),
            "w",
        ) as f:
            json.dump(data, f, indent=2)

        if resp.status_code != 200 or data.get("status") != "passed":
            failures = data.get("failures") or []
            detail = ", ".join(
                f"{f.get('schema')}.{f.get('name')} diff={f.get('diff_count')}"
                for f in failures[:5]
            )
            raise OpenIvmValidationError(
                f"OpenIVM validation failed for batch {batch_num}"
                + (f": {detail}" if detail else f": {data.get('error', 'unknown error')}")
            )
        self._emit(
            f"[spark-openivm] Validation passed for batch {batch_num}: "
            f"{data.get('models_checked', 0)} models in {data.get('duration_s', '?')}s"
        )

    def _export_duckdb_openivm_profile(self, run_id: str, batch_num: int) -> None:
        """Export OpenIVM profiling CSVs outside the benchmark timer."""
        self._emit(f"[duckdb-openivm] Exporting OpenIVM profile after batch {batch_num}")
        resp = requests.post(
            f"{self._dbt_url}/profile/duckdb-openivm/{run_id}/{batch_num}",
            timeout=7200,
        )
        data = resp.json()
        if resp.status_code != 200 or data.get("status") != "ok":
            raise RuntimeError(
                f"OpenIVM profile export failed for batch {batch_num}: "
                f"{data.get('error', 'unknown error')}"
            )

        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)

        csv_payloads = data.get("csv") or {}
        file_map = {
            "profile": f"openivm-profile-batch{batch_num}.csv",
            "by_step": f"openivm-profile-by-step-batch{batch_num}.csv",
            "by_view_step": f"openivm-profile-by-view-step-batch{batch_num}.csv",
        }
        for key, filename in file_map.items():
            with open(os.path.join(results_dir, filename), "w", encoding="utf-8") as f:
                f.write(csv_payloads.get(key, ""))

        metadata = {k: v for k, v in data.items() if k != "csv"}
        with open(
            os.path.join(results_dir, f"openivm-profile-export-batch{batch_num}.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(metadata, f, indent=2)

        self._emit(
            f"[duckdb-openivm] OpenIVM profile exported after batch {batch_num}: "
            f"{data.get('row_count', 0)} rows across {data.get('view_count', 0)} views"
        )

    def _export_spark_openivm_profile(self, run_id: str, batch_num: int) -> None:
        """Export spark-openivm refresh-profile CSVs outside the benchmark timer.

        Mirrors `_export_duckdb_openivm_profile`. The dbt-server route issues
        `SHOW OPENIVM REFRESH PROFILE` against the live Livy SQL session.
        """
        self._emit(f"[spark-openivm] Exporting OpenIVM profile after batch {batch_num}")
        resp = requests.post(
            f"{self._dbt_url}/profile/spark-openivm/{run_id}/{batch_num}",
            timeout=7200,
        )
        data = resp.json()
        if resp.status_code != 200 or data.get("status") != "ok":
            raise RuntimeError(
                f"spark-openivm profile export failed for batch {batch_num}: "
                f"{data.get('error', 'unknown error')}"
            )

        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)

        csv_payloads = data.get("csv") or {}
        file_map = {
            "profile": f"spark-openivm-profile-batch{batch_num}.csv",
            "by_step": f"spark-openivm-profile-by-step-batch{batch_num}.csv",
            "by_view_step": f"spark-openivm-profile-by-view-step-batch{batch_num}.csv",
        }
        for key, filename in file_map.items():
            with open(os.path.join(results_dir, filename), "w", encoding="utf-8") as f:
                f.write(csv_payloads.get(key, ""))

        metadata = {k: v for k, v in data.items() if k != "csv"}
        with open(
            os.path.join(results_dir, f"spark-openivm-profile-export-batch{batch_num}.json"),
            "w",
            encoding="utf-8",
        ) as f:
            json.dump(metadata, f, indent=2)

        self._emit(
            f"[spark-openivm] OpenIVM profile exported after batch {batch_num}: "
            f"{data.get('row_count', 0)} rows across {data.get('view_count', 0)} views"
        )

    def _export_spark_openivm_query_log(self, run_id: str, batch_num: int) -> None:
        """Export the per-MV per-refresh SQL trace OpenIVM ran on Spark.

        Runs outside the benchmark timer (called from `run_batch` *after*
        `stream_progress` has stopped the per-batch wall clock — see lines
        267-289 above). For each row of `SHOW OPENIVM QUERY LOG` we:

          1. Group by `(view_name, refresh_id)`.
          2. For each group, write a manifest.json plus one .sql file per
             statement under
             `mount/results/<sf>/spark-openivm/query-log/<view>/<refresh_dir>/`.
          3. Each .sql file is sqlglot-formatted (Spark dialect, pretty=True);
             parse failures fall back to the raw OpenIVM-emitted SQL with a
             `formatted: false` marker in the manifest entry.

        Idempotent: the per-refresh directory is `rmtree`d before being
        re-written so the on-disk state always equals the catalog state.
        """
        self._emit(
            f"[spark-openivm] Exporting OpenIVM query-log after batch {batch_num}"
        )
        resp = requests.post(
            f"{self._dbt_url}/query-log/spark-openivm/{run_id}/{batch_num}",
            timeout=7200,
        )
        data = resp.json()
        if resp.status_code != 200 or data.get("status") != "ok":
            raise RuntimeError(
                f"spark-openivm query-log export failed for batch {batch_num}: "
                f"{data.get('error', 'unknown error')}"
            )

        rows = data.get("rows") or []
        base_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "spark-openivm",
            "query-log",
        )
        os.makedirs(base_dir, exist_ok=True)

        files_written = self._write_query_log_tree(
            base_dir=base_dir,
            rows=rows,
            run_id=run_id,
            batch_num=batch_num,
        )

        self._emit(
            f"[spark-openivm] OpenIVM query-log exported after batch {batch_num}: "
            f"{data.get('row_count', 0)} statements across "
            f"{data.get('refresh_count', 0)} refreshes / "
            f"{data.get('view_count', 0)} MVs "
            f"({files_written} .sql files)"
        )

    def _export_query_plans(self, engine: str, run_id: str, batch_num: int) -> None:
        """Capture EXPLAIN plans for every successful model in this batch.

        Persisted to mount/query-plan/<sf>/<engine>/batch<N>/<model>.txt plus a
        manifest.json. Best-effort: capture failures emit a warning but never
        fail the batch - the timer should already be stopped before this runs.
        """
        self._emit(f"[{engine}] Capturing query plans for batch {batch_num}")
        try:
            resp = requests.post(
                f"{self._dbt_url}/query-plan/{engine}/{run_id}/{batch_num}",
                timeout=1800,
            )
            data = resp.json()
            if resp.status_code != 200 or data.get("status") not in ("ok", "partial"):
                self._emit(
                    f"[{engine}] query-plan capture failed batch {batch_num}: "
                    f"{data.get('error', 'unknown')}"
                )
                return
        except Exception as e:
            self._emit(f"[{engine}] query-plan capture exception batch {batch_num}: {e}")
            return

        base_dir = os.path.join(
            self._config.repo_dir,
            "mount", "query-plan", str(self._config.scale_factor),
            engine, f"batch{batch_num}",
        )
        os.makedirs(base_dir, exist_ok=True)

        plans = data.get("plans") or []
        for plan in plans:
            name = plan.get("name") or plan.get("unique_id") or "unknown"
            safe_name = "".join(c if c.isalnum() or c in "._-" else "_" for c in name)
            with open(os.path.join(base_dir, f"{safe_name}.txt"), "w") as f:
                f.write(plan.get("plan", ""))

        manifest = {
            "engine": engine,
            "run_id": run_id,
            "batch_num": batch_num,
            "summary": data.get("summary", {}),
            "failures": data.get("failures", []),
        }
        with open(os.path.join(base_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2, default=str)

        self._emit(
            f"[{engine}] Query plans captured batch {batch_num}: "
            f"{len(plans)} plans / {len(data.get('failures', []))} failures"
        )

    def _export_databricks_enzyme_metrics(self, batch_num: int) -> None:
        """Capture Delta history / refresh metrics for the databricks-enzyme MVs.

        Persisted to mount/stats/<sf>/databricks-enzyme/refresh-history-batch<N>.json.
        Best-effort: failures here do not fail the batch.
        """
        self._emit(
            f"[databricks-enzyme] Capturing refresh metrics for batch {batch_num}"
        )
        try:
            resp = requests.post(
                f"{self._dbt_url}/metrics/databricks-enzyme/{batch_num}",
                timeout=1800,
            )
            data = resp.json()
            if resp.status_code != 200 or data.get("status") not in ("ok", "partial"):
                self._emit(
                    f"[databricks-enzyme] metrics capture failed batch {batch_num}: "
                    f"{data.get('error', 'unknown')}"
                )
                return
        except Exception as e:
            self._emit(
                f"[databricks-enzyme] metrics capture exception batch {batch_num}: {e}"
            )
            return

        base_dir = os.path.join(
            self._config.repo_dir,
            "mount", "stats", str(self._config.scale_factor),
            "databricks-enzyme",
        )
        os.makedirs(base_dir, exist_ok=True)
        out_path = os.path.join(base_dir, f"refresh-history-batch{batch_num}.json")
        with open(out_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        self._emit(
            f"[databricks-enzyme] Refresh metrics captured batch {batch_num}: "
            f"{len(data.get('relations', []))} relations"
        )

    def _export_databricks_enzyme_pipeline_events(self, batch_num: int) -> None:
        """Capture every Databricks Lakeflow pipeline event for every
        ``MV-<catalog>.*`` pipeline in the workspace. One JSON file per
        update, ALL events for that update embedded (including the
        ``details.planning_information`` blob that tells us whether the
        refresh was incremental or fell back to FULL_RECOMPUTE).

        Persisted to
        ``mount/pipeline-events/<sf>/databricks-enzyme/batch<N>/<schema>.<table>/<update_id>.json``
        plus a per-batch ``manifest.json``. Best-effort: failures here do
        not fail the batch (the timer is already stopped before this runs).
        """
        self._emit(
            f"[databricks-enzyme] Capturing pipeline events for batch {batch_num}"
        )
        try:
            resp = requests.post(
                f"{self._dbt_url}/sources/databricks-enzyme/pipeline-events/{batch_num}",
                timeout=3600,
            )
            data = resp.json()
            if resp.status_code != 200 or data.get("status") not in ("ok", "partial"):
                self._emit(
                    f"[databricks-enzyme] pipeline-events capture failed batch "
                    f"{batch_num}: {data.get('error', 'unknown')}"
                )
                return
        except Exception as e:
            self._emit(
                f"[databricks-enzyme] pipeline-events capture exception batch "
                f"{batch_num}: {e}"
            )
            return

        base_dir = os.path.join(
            self._config.repo_dir,
            "mount", "pipeline-events", str(self._config.scale_factor),
            "databricks-enzyme", f"batch{batch_num}",
        )
        os.makedirs(base_dir, exist_ok=True)

        pipelines = data.get("pipelines") or []
        files_written = 0
        for p in pipelines:
            schema = p.get("schema") or "unknown"
            table = p.get("table") or "unknown"
            mv_key = f"{schema}.{table}"
            safe_mv = "".join(
                c if c.isalnum() or c in "._-" else "_" for c in mv_key
            )
            mv_dir = os.path.join(base_dir, safe_mv)
            os.makedirs(mv_dir, exist_ok=True)
            for upd in p.get("updates", []) or []:
                update_id = upd.get("update_id") or "unknown"
                safe_uid = "".join(
                    c if c.isalnum() or c in "._-" else "_" for c in update_id
                )
                payload = {
                    "pipeline_id": p.get("pipeline_id"),
                    "name": p.get("name"),
                    "schema": schema,
                    "table": table,
                    "update": upd,
                }
                with open(os.path.join(mv_dir, f"{safe_uid}.json"), "w") as f:
                    json.dump(payload, f, indent=2, default=str)
                files_written += 1
            pipeline_level = p.get("pipeline_level_events") or []
            if pipeline_level:
                with open(os.path.join(mv_dir, "__pipeline_events.json"), "w") as f:
                    json.dump({
                        "pipeline_id": p.get("pipeline_id"),
                        "name": p.get("name"),
                        "events": pipeline_level,
                    }, f, indent=2, default=str)
                files_written += 1

        manifest = {
            "batch_num": batch_num,
            "catalog": data.get("catalog"),
            "pipeline_count": data.get("pipeline_count", 0),
            "update_count": data.get("update_count", 0),
            "event_count": data.get("event_count", 0),
            "files_written": files_written,
        }
        with open(os.path.join(base_dir, "manifest.json"), "w") as f:
            json.dump(manifest, f, indent=2, default=str)

        self._emit(
            f"[databricks-enzyme] Pipeline events captured batch {batch_num}: "
            f"{manifest['pipeline_count']} pipelines / "
            f"{manifest['update_count']} updates / "
            f"{manifest['event_count']} events / "
            f"{files_written} files"
        )

    def _collect_databricks_enzyme_flow_metrics(self, batch_num: int, batch) -> None:
        """Persist best-effort pipeline-flow diagnostics without changing timings."""
        from services import databricks_enzyme_compute as dec

        events_dir = os.path.join(
            self._config.repo_dir,
            "mount", "pipeline-events", str(self._config.scale_factor),
            "databricks-enzyme", f"batch{batch_num}",
        )

        t0_ms = batch.extra.get("wall_window_start_ms")
        t1_ms = batch.extra.get("wall_window_end_ms")
        try:
            self._export_databricks_enzyme_pipeline_events(batch_num)
            summary = dec.compute_batch_summary(events_dir, t0_ms, t1_ms)
        except Exception as e:
            batch.extra["databricks_flow_metrics_status"] = "unavailable"
            batch.extra["databricks_flow_metrics_error"] = str(e)
            self._emit(
                f"[databricks-enzyme] flow metrics unavailable for "
                f"batch {batch_num}: {e}"
            )
            return

        bsum = summary["batch"]
        flow_coverage_s = bsum["compute_wall_ms"] / 1000.0
        flow_work_s = bsum["compute_work_ms"] / 1000.0
        batch.extra["databricks_flow_metrics_status"] = "ok"
        batch.extra["flow_coverage_s"] = flow_coverage_s
        batch.extra["flow_work_s"] = flow_work_s
        batch.extra["tables_with_compute"] = bsum["tables_with_compute"]
        batch.extra["updates_in_window"] = bsum["updates_in_window"]
        batch.extra["segments_total"] = bsum["segments_total"]
        batch.extra["segments_fallback"] = bsum["segments_fallback"]

        self._emit(
            f"[databricks-enzyme] Diagnostic flow metrics batch {batch_num}: "
            f"coverage={flow_coverage_s:.1f}s, summed_work={flow_work_s:.1f}s; "
            f"reported duration remains end-to-end wall clock"
        )

        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        sidecar_path = os.path.join(
            results_dir, f"databricks-flow-metrics-batch{batch_num}.json"
        )
        persistence = dec.summarize_for_persistence(summary)
        persistence["batch"]["duration_s_wallclock"] = batch.duration_s
        persistence["batch_num"] = batch_num
        persistence["scale_factor"] = self._config.scale_factor
        with open(sidecar_path, "w") as f:
            json.dump(persistence, f, indent=2)

    def _write_query_log_tree(
        self,
        base_dir: str,
        rows: list,
        run_id: str,
        batch_num: int,
    ) -> int:
        """Render the JSON rows from /query-log/spark-openivm as a directory
        tree of `.sql` files + manifest.json files.

        Returns the number of .sql files written.

        Layout:

            <base_dir>/<view_name>/<refresh_dir>/
                manifest.json
                000__<category>[__attempt<N>].sql
                001__<category>[__attempt<N>].sql
                ...

        `<refresh_dir>` is `create_mv_<nanos>` or `refresh_<nanos>` derived
        from the OpenIVM-minted `refresh_id`.
        """
        # `sqlglot` is added in benchmark-server/requirements.txt. We import
        # it lazily so the rest of engine_runner stays importable even if the
        # image is briefly out of sync.
        try:
            import sqlglot  # type: ignore
            import sqlglot.errors  # type: ignore
        except Exception:  # pragma: no cover — only on missing pip dep
            logger.warning(
                "[spark-openivm] sqlglot not installed — falling back to "
                "raw (unformatted) SQL for the query-log export"
            )
            sqlglot = None  # type: ignore

        def _format_sql(raw: str) -> tuple[str, bool]:
            """Return (formatted_sql, was_formatted_flag).

            Falls back to raw SQL on any parse error so the on-disk artifact
            always contains the actual SQL OpenIVM emitted.
            """
            if not raw or sqlglot is None:
                return (raw or "", False)
            try:
                out = sqlglot.transpile(
                    raw, read="spark", write="spark", pretty=True
                )
                if out:
                    return (out[0], True)
                return (raw, False)
            except Exception:  # noqa: BLE001 — parse errors are expected
                return (raw, False)

        # Group rows by (view_name, refresh_id) preserving stmt_order.
        from collections import OrderedDict

        groups: "OrderedDict[tuple[str, str], list]" = OrderedDict()
        for row in rows:
            view = str(row.get("view_name") or "").strip() or "_unknown_view"
            rid = str(row.get("refresh_id") or "").strip() or "_unknown_refresh"
            groups.setdefault((view, rid), []).append(row)

        # Track which refresh directories we touched so prior runs of the
        # same refresh_id are fully replaced. We rmtree per refresh-id-dir
        # only (not per view dir) so distinct refreshes accumulate across
        # batches as expected.
        files_written = 0
        for (view, rid), stmts in groups.items():
            view_dir = os.path.join(base_dir, view)
            os.makedirs(view_dir, exist_ok=True)

            # Pick the `create_mv_<nanos>` / `refresh_<nanos>` form by
            # stripping the leading `<db>.<view>_` prefix from the refresh_id.
            # Fall back to the raw refresh_id if the prefix doesn't match.
            refresh_dir_name = rid
            if rid.startswith(view + "_"):
                refresh_dir_name = rid[len(view) + 1:]
            refresh_dir = os.path.join(view_dir, refresh_dir_name)
            # Idempotent overwrite: remove + recreate.
            if os.path.isdir(refresh_dir):
                shutil.rmtree(refresh_dir, ignore_errors=True)
            os.makedirs(refresh_dir, exist_ok=True)

            mode = "create"
            total_ms = 0
            manifest_stmts = []
            # Sort by (stmt_order, attempt_idx) for stable filenames.
            stmts_sorted = sorted(
                stmts,
                key=lambda r: (
                    self._safe_int(r.get("stmt_order"), 0),
                    self._safe_int(r.get("attempt_idx"), 0),
                ),
            )
            for row in stmts_sorted:
                stmt_order = self._safe_int(row.get("stmt_order"), 0)
                attempt_idx = self._safe_int(row.get("attempt_idx"), 0)
                duration_ms = self._safe_int(row.get("duration_ms"), 0)
                category = str(row.get("category") or "stmt")
                stmt_kind = str(row.get("stmt_kind") or "other")
                mode = str(row.get("mode") or mode)
                sql_text = str(row.get("sql_text") or "")
                profile_ts = str(row.get("profile_timestamp") or "")

                formatted_sql, was_formatted = _format_sql(sql_text)

                # stmt_order may be -1 for `original_query`; clamp to 0 for
                # the filename prefix but keep the actual value in the
                # manifest so the reader can spot the synthetic event.
                file_order = max(stmt_order, 0)
                attempt_suffix = (
                    f"__attempt{attempt_idx}" if attempt_idx > 0 else ""
                )
                # Defensive: never let a `/` in category create a subdir.
                safe_category = category.replace("/", "_")
                filename = f"{file_order:03d}__{safe_category}{attempt_suffix}.sql"
                with open(
                    os.path.join(refresh_dir, filename), "w", encoding="utf-8"
                ) as f:
                    f.write(formatted_sql)
                    if not formatted_sql.endswith("\n"):
                        f.write("\n")
                files_written += 1

                if duration_ms > 0:
                    total_ms += duration_ms
                manifest_stmts.append({
                    "stmt_order": stmt_order,
                    "attempt_idx": attempt_idx,
                    "category": category,
                    "stmt_kind": stmt_kind,
                    "duration_ms": duration_ms,
                    "sql_file": filename,
                    "profile_timestamp": profile_ts,
                    "formatted": was_formatted,
                })

            manifest = {
                "refresh_id": rid,
                "view_name": view,
                "mode": mode,
                "exported_after_batch": batch_num,
                "exported_after_run_id": run_id,
                "total_duration_ms": total_ms,
                "statements": manifest_stmts,
            }
            with open(
                os.path.join(refresh_dir, "manifest.json"),
                "w",
                encoding="utf-8",
            ) as f:
                json.dump(manifest, f, indent=2)

        return files_written

    @staticmethod
    def _safe_int(value, default: int) -> int:
        try:
            if value is None or value == "":
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _run_feldera_wait(self, batch_num: int) -> None:
        """Feldera batches 2/3: pause → append → resume → poll stats.

        Flow:
        1. Pause pipeline (stops ingestion)
        2. Append new data via batch-loader
        3. Resume pipeline (starts measurement)
        4. Poll /wait/feldera for per-output completion
        """
        # Pause pipeline before appending new data
        self._emit(f"[feldera] Pausing pipeline before batch {batch_num} append")
        pause_resp = requests.post(f"{self._dbt_url}/pause/feldera", timeout=60)
        pause_resp.raise_for_status()

        # Append data while pipeline is paused
        self._batch_loader_append(batch_num)
        self._capture_delta_stats(batch_num)

        # Resume pipeline — this is the start of measurement
        self._emit(f"[feldera] Resuming pipeline for batch {batch_num}")
        self._start_cpu_measurement(batch_num)
        resume_resp = requests.post(f"{self._dbt_url}/resume/feldera", timeout=60)
        resume_resp.raise_for_status()
        start_epoch = resume_resp.json().get("resumed_at_epoch_s", time.time())

        self._emit(f"[feldera] Waiting for pipeline to process batch {batch_num}")
        wait_resp = requests.post(
            f"{self._dbt_url}/wait/feldera",
            json={
                "scale_factor": self._config.scale_factor,
                "batch_num": batch_num,
                "start_epoch_s": start_epoch,
            },
            timeout=604800,
        )
        wait_data = wait_resp.json()
        self._finish_cpu_measurement(batch_num)

        if wait_resp.status_code != 200:
            raise RuntimeError(
                f"Feldera batch {batch_num} wait failed: {wait_data.get('error', 'unknown')}"
            )

        duration = wait_data.get("duration_s", "?")
        if isinstance(duration, (int, float)):
            self._measured_duration_s = float(duration)
        self._emit(f"[feldera] Batch {batch_num} processing time: {duration}s")

        # Save result
        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        with open(os.path.join(results_dir, f"run-feldera-batch{batch_num}.json"), "w") as f:
            json.dump(wait_data, f, indent=2)

    def _stream_dbt_progress(self, run_id: str, batch_num: int) -> None:
        """Stream SSE progress from dbt-server and relay to our emit callback."""
        name = self._engine.name
        try:
            resp = requests.get(
                f"{self._dbt_url}/runs/{run_id}/progress/stream",
                stream=True,
                timeout=604800,
            )
            for line in resp.iter_lines(decode_unicode=True):
                if not line:
                    continue
                if line.startswith("data: "):
                    payload = line[6:]
                    if payload in ("completed", "failed", "not found"):
                        break  # Final status — checked via _check_run_result
                    else:
                        self._emit(f"[{name}] {payload}")
        except Exception as e:
            logger.warning("SSE stream error for %s: %s", name, e)

    def _check_run_result(self, run_id: str, batch_num: int) -> None:
        """Check final run result from dbt-server, polling until terminal status."""
        name = self._engine.name
        batch = self._result.batches[batch_num - 1]
        max_polls = 360
        for _ in range(max_polls):
            try:
                resp = requests.get(f"{self._dbt_url}/runs/{run_id}", timeout=30)
                resp.raise_for_status()
                data = resp.json()
                status = data.get("status", "unknown")
                error = data.get("error", "")

                if status in ("completed", "failed"):
                    if status == "failed":
                        batch.status = "failed"
                        batch.error = error or "dbt run failed"
                    return

                # Still running — wait and poll again
                time.sleep(5)
            except Exception as e:
                logger.warning("Failed to check run result for %s (poll): %s", name, e)
                time.sleep(5)

        # Exhausted polls — mark as failed
        batch.status = "failed"
        batch.error = f"dbt run {run_id} did not reach terminal status after {max_polls * 5}s"

    # ----- Batch loading -----

    def _batch_loader_log_file(self, suffix: str) -> Optional[str]:
        """Path under mount/logs/<sf>/<engine>/ for batch-loader output.

        Written so the real Spark error survives the loader container's `--rm`
        and is archived into the uploaded artifact by _archive_failure_forensics
        (which copies mount/logs/<sf>/<engine>/ on failure).
        """
        try:
            log_dir = os.path.join(
                self._config.repo_dir, "mount", "logs",
                str(self._config.scale_factor), self._engine.name,
            )
            os.makedirs(log_dir, exist_ok=True)
            return os.path.join(log_dir, f"batch-loader-{suffix}.log")
        except OSError:
            return None

    def _run_batch_loader_service(self, cmd_args: List[str], log_suffix: str) -> None:
        """Run the spark-batch-loader one-shot, teeing output to a forensic log."""
        name = self._engine.name
        log_path = self._batch_loader_log_file(log_suffix)
        fh = open(log_path, "w") if log_path else None

        def _sink(line: str) -> None:
            logger.debug("[batch-loader/%s] %s", name, line)
            if fh is not None:
                try:
                    fh.write(line + "\n")
                    fh.flush()
                except OSError:
                    pass

        try:
            self._batch_mgr.run_service(
                "spark-batch-loader",
                cmd_args=cmd_args,
                stream_callback=_sink,
            )
        finally:
            if fh is not None:
                fh.close()

    def _batch_loader_init(self) -> None:
        """Initialize staging from batch1 (serial mode only)."""
        name = self._engine.name
        self._emit(f"[{name}] Batch loader: init")

        self._run_batch_loader_service(["init"], "init")
        self._fix_delta_permissions()
        self._emit(f"[{name}] Batch loader: init complete")

    def _batch_loader_append(self, batch_num: int) -> None:
        """Append a batch to staging.

        Uses the smaller append heap (see __init__): the engine server is
        already running and holds the full batch-1 state, so the loader must
        fit in the memory the main service did not reserve.
        """
        name = self._engine.name
        self._batch_mgr.update_env(
            {"BATCH_LOADER_HEAP": f"{self._batch_append_heap_gb}g"}
        )
        self._emit(
            f"[{name}] Batch loader: append {batch_num} "
            f"(heap {self._batch_append_heap_gb}g)"
        )
        self._run_batch_loader_service(
            ["append", str(batch_num)], f"append{batch_num}"
        )
        self._fix_delta_permissions()
        self._emit(f"[{name}] Batch loader: append {batch_num} complete")

    def _cleanup_staging(self) -> None:
        """Clean up per-engine staging directory after completion."""
        if not self._engine.staging_dir or self._engine.staging_dir == "staging":
            return  # Serial mode or DuckDB-OpenIVM — don't clean shared staging
        sf = self._config.scale_factor
        staging_path = os.path.join(
            self._config.repo_dir, "mount", "raw", str(sf), "delta",
            self._engine.staging_dir,
        )
        if os.path.exists(staging_path):
            shutil.rmtree(staging_path, ignore_errors=True)
            self._emit(f"[{self._engine.name}] Cleaned up staging: {self._engine.staging_dir}")

    def _fix_delta_permissions(self) -> None:
        """Fix Delta directory permissions after Spark writes."""
        sf = self._config.scale_factor
        delta_dir = os.path.join(self._config.repo_dir, "mount", "raw", str(sf), "delta")
        staging = os.path.join(delta_dir, self._engine.staging_dir)
        try:
            os.system(
                f"docker run --rm -v {staging}:/data alpine chmod -R 777 /data 2>/dev/null"
            )
        except Exception:
            pass

    # ----- Telemetry collection -----

    def _start_stats(self) -> None:
        """Start diagnostic samples or mark managed compute as excluded."""
        if is_cloud_engine(self._engine.name):
            reason = (
                "managed autoscaling prevents a fixed, comparable CPU allocation; "
                "local Docker stats only measure the orchestration client"
            )
            for batch in self._result.batches:
                batch.extra["compute_metrics"] = {
                    "status": "excluded",
                    "cpu_time_s": None,
                    "repetition_count": 1,
                    "source": "managed_engine_excluded",
                    "semantics": reason,
                    "reason": reason,
                }
            return
        try:
            response = requests.post(
                f"{self._dbt_url}/stats/containers/start",
                json={
                    "engine": self._engine.name,
                    "scale_factor": self._config.scale_factor,
                },
                timeout=10,
            )
            response.raise_for_status()
            self._stats_started = True
            self._emit(f"[{self._engine.name}] Stats collection started")
        except Exception as e:
            self._stats_error = str(e)
            self._emit(f"[{self._engine.name}] Stats collection WARN: {e}")
            logger.warning("Failed to start stats: %s", e)

    def _start_cpu_measurement(self, batch_num: int) -> None:
        if not self._compute_services:
            return
        try:
            self._cpu_start_snapshots[batch_num] = self._engine_mgr.cpu_usage_snapshot(
                self._compute_services
            )
        except Exception as e:
            batch = self._result.batches[batch_num - 1]
            batch.extra["compute_metrics"] = {
                "status": "error",
                "cpu_time_s": None,
                "source": "docker_cpu_usage_total",
                "semantics": "delta of cumulative Docker CPU counters at batch boundaries",
                "error": f"start CPU snapshot failed: {e}",
            }
            self._emit(
                f"[{self._engine.name}] CPU batch {batch_num} unavailable: {e}"
            )
            logger.warning("Failed to start CPU measurement: %s", e)

    def _finish_cpu_measurement(self, batch_num: int) -> None:
        start = self._cpu_start_snapshots.pop(batch_num, None)
        if start is None:
            return
        batch = self._result.batches[batch_num - 1]
        try:
            end = self._engine_mgr.cpu_usage_snapshot(self._compute_services)
            delta = compute_cpu_usage_delta(start, end)
            batch.extra["compute_metrics"] = {
                "status": "ok",
                "source": "docker_cpu_usage_total",
                "semantics": (
                    "delta of cumulative Docker container CPU counters at "
                    "the measured batch boundaries"
                ),
                "measured_services": list(self._compute_services),
                "cpu_time_stddev_s": 0.0,
                "repetition_count": 1,
                "start_snapshot": start,
                "end_snapshot": end,
                "samples_status": "pending" if self._stats_started else "error",
                **delta,
            }
            if self._stats_started:
                batch.extra["compute_metrics"]["artifact"] = os.path.join(
                    "mount",
                    "stats",
                    str(self._config.scale_factor),
                    self._engine.name,
                    "container_stats.jsonl",
                )
            self._emit(
                f"[{self._engine.name}] CPU batch {batch_num}: "
                f"{delta['cpu_time_s']:.3f} CPU-s from "
                f"{', '.join(self._compute_services)}"
            )
            if self._stats_error:
                batch.extra["compute_metrics"]["samples_error"] = self._stats_error
        except Exception as e:
            batch.extra["compute_metrics"] = {
                "status": "error",
                "cpu_time_s": None,
                "source": "docker_cpu_usage_total",
                "semantics": "delta of cumulative Docker CPU counters at batch boundaries",
                "error": f"end CPU snapshot failed: {e}",
                "start_snapshot": start,
            }
            self._emit(
                f"[{self._engine.name}] CPU batch {batch_num} unavailable: {e}"
            )
            logger.warning("Failed to finish CPU measurement: %s", e)

    def _ensure_cpu_measurement_status(self, batch_num: int) -> None:
        batch = self._result.batches[batch_num - 1]
        if "compute_metrics" in batch.extra:
            return
        if is_cloud_engine(self._engine.name):
            return
        if batch_num in self._cpu_start_snapshots:
            self._cpu_start_snapshots.pop(batch_num, None)
            error = "batch ended before the end CPU snapshot"
        else:
            error = "CPU measurement did not start"
        batch.extra["compute_metrics"] = {
            "status": "error",
            "cpu_time_s": None,
            "source": "docker_cpu_usage_total",
            "semantics": "delta of cumulative Docker CPU counters at batch boundaries",
            "error": error,
        }

    def _stop_stats(self) -> None:
        """Stop the diagnostic sample stream exactly once and persist its status."""
        if is_cloud_engine(self._engine.name):
            return
        if not self._stats_started:
            if self._stats_error:
                for batch in self._result.batches:
                    metrics = batch.extra.get("compute_metrics")
                    if isinstance(metrics, dict):
                        metrics["samples_status"] = "error"
                        metrics["samples_error"] = self._stats_error
                        self._persist_batch_result(batch.batch_num, batch)
            return
        self._stats_started = False
        try:
            resp = requests.post(
                f"{self._dbt_url}/stats/containers/stop",
                timeout=60,
            )
            resp.raise_for_status()
            payload = resp.json()
            count = payload.get("sample_count", 0)
            self._emit(f"[{self._engine.name}] Stats collection stopped ({count} samples)")
            artifact = os.path.join(
                "mount",
                "stats",
                str(self._config.scale_factor),
                self._engine.name,
                "container_stats.jsonl",
            )
            for batch in self._result.batches:
                metrics = batch.extra.get("compute_metrics")
                if not isinstance(metrics, dict):
                    continue
                metrics["samples_status"] = "ok" if count else "error"
                metrics["sample_count"] = count
                if count:
                    metrics["artifact"] = artifact
                else:
                    metrics.pop("artifact", None)
                    metrics["samples_error"] = (
                        "diagnostic collector returned zero samples"
                    )
                self._persist_batch_result(batch.batch_num, batch)
        except Exception as e:
            error = str(e)
            for batch in self._result.batches:
                metrics = batch.extra.get("compute_metrics")
                if not isinstance(metrics, dict):
                    continue
                metrics["samples_status"] = "error"
                metrics["samples_error"] = error
                self._persist_batch_result(batch.batch_num, batch)
            logger.warning("Failed to stop stats: %s", e)

    def _capture_delta_stats(self, batch_num: int) -> None:
        """Capture delta stats for staging tables."""
        name = self._engine.name
        try:
            resp = requests.get(f"{self._dbt_url}/delta-stats", timeout=30)
            results_dir = os.path.join(
                self._config.repo_dir,
                "mount", "results", str(self._config.scale_factor), "dbt-server",
            )
            os.makedirs(results_dir, exist_ok=True)
            with open(os.path.join(results_dir, f"delta-stats-{name}-batch{batch_num}.json"), "w") as f:
                json.dump(resp.json(), f, indent=2)
        except Exception as e:
            logger.warning("Failed to capture delta stats: %s", e)

    def _capture_storage_metrics(self, batch_num: int, batch) -> None:
        """Capture post-batch storage metrics outside the timed window."""
        if os.environ.get("STORAGE_METRICS", "1") == "0":
            return

        name = self._engine.name
        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", str(self._config.scale_factor), "dbt-server",
        )
        os.makedirs(results_dir, exist_ok=True)
        out_path = os.path.join(results_dir, f"storage-{name}-batch{batch_num}.json")
        with storage_snapshot_barrier(self._storage_barrier):
            self._capture_storage_metrics_request(name, batch_num, batch, out_path)

    def _capture_storage_metrics_request(
        self, name: str, batch_num: int, batch, out_path: str
    ) -> None:
        try:
            default_timeout = 1820 if name == "databricks-enzyme" else 110
            configured_timeout = os.environ.get(
                "STORAGE_COLLECTION_TIMEOUT_S", ""
            ).strip()
            request_timeout = (
                self._safe_int(configured_timeout, default_timeout) + 20
                if configured_timeout
                else default_timeout
            )
            resp = requests.get(
                f"{self._dbt_url}/storage/{name}",
                params={"batch_num": batch_num},
                timeout=request_timeout,
            )
            data = resp.json()
            if resp.status_code >= 500:
                data.setdefault("status", "error")
            with open(out_path, "w") as f:
                json.dump(data, f, indent=2)

            totals = data.get("totals") or {}
            batch.extra["storage"] = {
                "status": data.get("status", "unknown"),
                "artifact": os.path.join(
                    "mount", "results", str(self._config.scale_factor), "dbt-server",
                    f"storage-{name}-batch{batch_num}.json",
                ),
                "visible_output_bytes": totals.get("visible_output_bytes", 0),
                "helper_data_bytes": totals.get("helper_data_bytes", 0),
                "metadata_bytes": totals.get("metadata_bytes", 0),
                "source_bytes": totals.get("source_bytes", 0),
                "total_bytes": totals.get("total_bytes", 0),
                "overhead_ratio_helper_to_visible": data.get(
                    "overhead_ratio_helper_to_visible"
                ),
                "base_tables": data.get("base_tables"),
            }
            if data.get("errors"):
                batch.extra["storage"]["errors"] = list(data["errors"])
            elif data.get("error"):
                batch.extra["storage"]["errors"] = [str(data["error"])]
            status = batch.extra["storage"]["status"]
            self._emit(
                f"[{name}] Storage metrics captured for batch {batch_num}: "
                f"visible={totals.get('visible_output_bytes', 0)}B "
                f"helper={totals.get('helper_data_bytes', 0)}B"
            )
            require_complete_storage_metrics(status, name, batch_num)
        except StorageMetricsError:
            raise
        except Exception as e:
            logger.warning("Failed to capture storage metrics for %s batch %d: %s", name, batch_num, e)
            batch.extra["storage"] = {
                "status": "error",
                "artifact": os.path.join(
                    "mount", "results", str(self._config.scale_factor), "dbt-server",
                    f"storage-{name}-batch{batch_num}.json",
                ),
                "error": str(e),
            }
            try:
                with open(out_path, "w") as f:
                    json.dump({
                        "status": "error",
                        "engine": name,
                        "batch_num": batch_num,
                        "error": str(e),
                    }, f, indent=2)
            except Exception as write_error:
                logger.warning(
                    "Failed to write storage error artifact for %s batch %d: %s",
                    name,
                    batch_num,
                    write_error,
                )
            raise StorageMetricsError(
                f"storage metrics failed for {name} batch {batch_num}: {e}"
            ) from e

    def _fetch_lineage(self) -> None:
        """Fetch dbt lineage."""
        name = self._engine.name
        try:
            resp = requests.get(f"{self._dbt_url}/lineage/{name}", timeout=30)
            results_dir = os.path.join(
                self._config.repo_dir,
                "mount", "results", str(self._config.scale_factor), "dbt-server",
            )
            os.makedirs(results_dir, exist_ok=True)
            with open(os.path.join(results_dir, f"lineage-{name}.json"), "w") as f:
                json.dump(resp.json(), f, indent=2)
            self._emit(f"[{name}] Lineage saved")
        except Exception as e:
            logger.warning("Failed to fetch lineage for %s: %s", name, e)

    def _fetch_sql_analysis(self) -> None:
        """Fetch SQL analysis."""
        name = self._engine.name
        try:
            resp = requests.get(f"{self._dbt_url}/sql/{name}", timeout=30)
            results_dir = os.path.join(
                self._config.repo_dir,
                "mount", "results", str(self._config.scale_factor), "dbt-server",
            )
            os.makedirs(results_dir, exist_ok=True)
            with open(os.path.join(results_dir, f"sql-analysis-{name}.json"), "w") as f:
                json.dump(resp.json(), f, indent=2)
            self._emit(f"[{name}] SQL analysis saved")
        except Exception as e:
            logger.warning("Failed to fetch sql analysis for %s: %s", name, e)

    def _collect_feldera_debug(self) -> None:
        """Download Feldera support bundle (circuit profile, heap, metrics, etc.).

        Only runs for the feldera engine. The bundle includes a fresh snapshot
        plus all historically retained snapshots from the pipeline-manager.
        Non-fatal: logs a warning on failure. Writes to a .part file first
        and renames on success to avoid leaving corrupt artifacts.
        """
        if self._engine.name != "feldera":
            return

        self._emit("[feldera] Collecting debug support bundle")
        docker_host = os.environ.get("DOCKER_HOST_ADDRESS", "localhost")
        url = f"http://{docker_host}:8080/v0/pipelines/tpcdi/support_bundle?collect=true"

        debug_dir = os.path.join(
            self._config.repo_dir,
            "mount", "debug", str(self._config.scale_factor), "feldera",
        )
        os.makedirs(debug_dir, exist_ok=True)
        bundle_path = os.path.join(debug_dir, "support-bundle.zip")
        part_path = bundle_path + ".part"

        try:
            resp = requests.get(url, timeout=(10, 300), stream=True)
            resp.raise_for_status()

            with open(part_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            os.replace(part_path, bundle_path)
            size_mb = os.path.getsize(bundle_path) / (1024 * 1024)
            self._emit(f"[feldera] Support bundle saved ({size_mb:.1f} MB): {bundle_path}")
        except Exception as e:
            # Clean up partial download
            if os.path.exists(part_path):
                os.unlink(part_path)
            logger.warning("Failed to collect Feldera debug bundle: %s", e)
            self._emit(f"[feldera] WARNING: Failed to collect debug bundle: {e}")

    def _save_run_result(
        self, run_id: str, batch_num: int, suffix: str = ""
    ) -> None:
        """Save dbt run results JSON.

        ``suffix`` is appended to the filename stem so retry attempts
        can be archived without overwriting the canonical
        ``run-<engine>-batch<N>.json`` (final attempt).
        """
        name = self._engine.name
        try:
            resp = requests.get(f"{self._dbt_url}/runs/{run_id}", timeout=30)
            results_dir = os.path.join(
                self._config.repo_dir,
                "mount", "results", str(self._config.scale_factor), "dbt-server",
            )
            os.makedirs(results_dir, exist_ok=True)
            filename = f"run-{name}-batch{batch_num}{suffix}.json"
            with open(os.path.join(results_dir, filename), "w") as f:
                json.dump(resp.json(), f, indent=2)
        except Exception as e:
            logger.warning("Failed to save run result: %s", e)

    def _persist_batch_result(self, batch_num: int, batch) -> None:
        """Persist batch result to benchmark-server's SQLite."""
        if not self._benchmark_id:
            return
        try:
            payload: Dict[str, Any] = {
                "batch_num": batch_num,
                "duration_s": batch.duration_s,
                "status": batch.status,
                "error": batch.error,
            }
            if getattr(batch, "extra", None):
                payload["extra"] = batch.extra
            result_json = json.dumps(payload, default=str)
            with DB_LOCK:
                conn = get_db()
                conn.execute(
                    """UPDATE engine_batches
                       SET status=?, duration_s=?, result_json=?, error=?
                       WHERE benchmark_id=? AND engine=? AND batch_num=?""",
                    (batch.status, batch.duration_s, result_json, batch.error,
                     self._benchmark_id, self._engine.name, batch_num),
                )
                conn.commit()
                conn.close()
        except Exception as e:
            logger.warning("Failed to persist batch result to DB: %s", e)

    def _capture_logs(self) -> None:
        """Capture all container logs before teardown."""
        logs_dir = os.path.join(
            self._config.repo_dir,
            "mount", "logs", str(self._config.scale_factor), self._engine.name,
        )
        try:
            self._engine_mgr.capture_all_logs(logs_dir)
        except Exception as e:
            logger.warning("Failed to capture logs: %s", e)

    # ----- Health checking -----

    def _wait_for_dbt_health(self) -> None:
        """Wait for the dbt-server to become healthy."""
        for i in range(1, HEALTH_TIMEOUT // HEALTH_INTERVAL + 1):
            try:
                resp = requests.get(f"{self._dbt_url}/health", timeout=5)
                if resp.ok:
                    self._emit(f"[{self._engine.name}] dbt-server is healthy")
                    return
            except (requests.ConnectionError, requests.Timeout):
                # Container still warming up, or a transient cross-container
                # network stall under host load — retry next interval.
                pass
            self._emit(
                f"[{self._engine.name}] Waiting for dbt-server... "
                f"({i}/{HEALTH_TIMEOUT // HEALTH_INTERVAL})"
            )
            time.sleep(HEALTH_INTERVAL)
        raise TimeoutError(
            f"dbt-server for {self._engine.name} not healthy within {HEALTH_TIMEOUT}s"
        )
