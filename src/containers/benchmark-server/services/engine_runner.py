"""Per-engine benchmark execution logic."""

import json
import logging
import os
import shutil
import tempfile
import time
from typing import Callable, Optional

import requests

from models.config import BenchmarkConfig, EngineConfig
from models.result import EngineResult
from services.db import DB_LOCK, get_db
from services.docker_manager import DockerManager

logger = logging.getLogger(__name__)

HEALTH_TIMEOUT = 300
HEALTH_INTERVAL = 5


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
    ) -> None:
        self._config = config
        self._engine = engine_config
        self._emit = emit
        self._result = EngineResult(engine=engine_config.name)
        self._benchmark_id = benchmark_id
        docker_host = os.environ.get("DOCKER_HOST_ADDRESS", "localhost")
        self._dbt_url = f"http://{docker_host}:{engine_config.port}"
        self._override_file: Optional[str] = None
        self._batch_override_file: Optional[str] = None
        self._parallel = config.parallel

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
            if engine_config.name != "duckdb-openivm":
                self._batch_override_file = self._create_batch_staging_override()
        else:
            self._engine_mgr = DockerManager(
                compose_file=engine_compose,
                project_name=engine_config.project_name,
                env=engine_env,
                cwd=config.repo_dir,
            )

        # Calculate JVM heap for batch_loader (60% of available memory, min 4g)
        batch_mem_gb = engine_config.main_resources.memory_gb
        batch_heap_gb = max(4, int(batch_mem_gb * 0.6))
        batch_gc_threads = max(4, engine_config.main_resources.cpus // 2)
        batch_conc_gc = max(2, batch_gc_threads // 2)

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

    def run(self) -> EngineResult:
        """Execute the full 3-batch benchmark."""
        name = self._engine.name
        self._result.status = "running"
        try:
            self._emit(f"[{name}] Starting benchmark")

            # Initialize Delta staging for engines that still consume Delta directly.
            if name not in ("duckdb", "duckdb-openivm") and not self._parallel:
                self._batch_loader_init()

            # Start engine stack
            self._emit(f"[{name}] Building and starting compose stack")
            self._engine_mgr.build()
            self._engine_mgr.up(detach=True)

            # Wait for dbt-server health
            self._emit(f"[{name}] Waiting for dbt-server health")
            self._wait_for_dbt_health()

            # Start container stats
            self._start_stats()
            self._capture_delta_stats(1)

            # Run 3 batches
            for batch_num in range(1, 4):
                self._run_batch(batch_num)
                if self._result.batches[batch_num - 1].status == "failed":
                    raise RuntimeError(
                        f"{name} batch {batch_num} failed: "
                        f"{self._result.batches[batch_num - 1].error}"
                    )

            # Post-run: stats, lineage, sql analysis, logs
            self._stop_stats()
            self._fetch_sql_analysis()
            self._fetch_lineage()

            self._result.status = "completed"
            self._emit(f"[{name}] Benchmark completed successfully")

        except Exception as e:
            self._result.status = "failed"
            self._result.error = str(e)
            self._emit(f"[{name}] FAILED: {e}")
            logger.exception("Engine %s failed", name)

        finally:
            self._collect_feldera_debug()
            self._capture_logs()
            self._engine_mgr.down()
            self._cleanup_staging()
            # Clean up temp override files
            for f in (self._override_file, self._batch_override_file):
                if f and os.path.exists(f):
                    os.unlink(f)

        return self._result

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
            if batch_num > 1 and name not in ("duckdb", "duckdb-openivm") and not (name == "feldera" and batch_num > 1):
                self._batch_loader_append(batch_num)
                self._capture_delta_stats(batch_num)

            t0 = time.time()

            run_id = None
            if name == "feldera":
                if batch_num == 1:
                    self._run_feldera_batch1()
                else:
                    self._run_feldera_wait(batch_num)
            elif name == "duckdb":
                self._run_duckdb_ducklake(batch_num)
            elif name == "duckdb-openivm":
                run_id = self._run_duckdb_openivm(batch_num)
            else:
                self._run_dbt(batch_num)

            batch.duration_s = time.time() - t0

            if (
                name == "duckdb-openivm"
                and run_id
                and batch.status != "failed"
                and os.environ.get("OPENIVM_VALIDATE", "0") != "0"
            ):
                self._validate_duckdb_openivm(run_id, batch_num)

            # Check status from the stream_progress result
            if batch.status != "failed":
                batch.status = "completed"
                self._emit(
                    f"[{name}] Batch {batch_num} completed in {batch.duration_s:.1f}s"
                )
        except Exception as e:
            batch.status = "failed"
            batch.error = str(e)
            raise
        finally:
            # Always persist batch result to benchmark-server DB
            self._persist_batch_result(batch_num, batch)

    def _run_dbt(self, batch_num: int) -> None:
        """Trigger a dbt run via the dbt-server REST API."""
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
            resp = requests.post(f"{self._dbt_url}/sources/duckdb/append/{batch_num}", timeout=600)
            resp.raise_for_status()
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

        if wait_resp.status_code != 200:
            raise RuntimeError(f"Feldera batch 1 wait failed: {wait_data.get('error', 'unknown')}")

        duration = wait_data.get("duration_s", "?")
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

    def _poll_feldera_compilation(self, run_id: str) -> None:
        """Poll dbt-server progress endpoint until all Feldera models are compiled."""
        cursor = 0
        stale_count = 0
        max_stale = 1800  # 30 min — Feldera pipeline compilation can take 5+ min

        while True:
            try:
                resp = requests.get(
                    f"{self._dbt_url}/runs/{run_id}/progress",
                    params={"since": cursor},
                    timeout=30,
                )
                data = resp.json()
                events = data.get("events", [])
                total = data.get("total", 0)
                status = data.get("run_status", "running")

                if events:
                    stale_count = 0
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
                else:
                    stale_count += 1
                    # Emit periodic status when waiting for Feldera pipeline compilation
                    if cursor >= total > 0 and stale_count % 30 == 0:
                        self._emit(f"[feldera] Waiting for pipeline compilation... ({stale_count}s)")

                # Wait for the dbt run to fully complete (including on-run-end
                # hook which compiles the Feldera pipeline binary and starts it paused).
                # Just having all model events isn't enough — the on-run-end hook
                # triggers the actual Feldera compilation which can take minutes.
                if status == "completed":
                    return
                if status == "failed":
                    raise RuntimeError("Feldera dbt run failed during compilation")
                if stale_count > max_stale:
                    raise TimeoutError("Feldera compilation stalled")

            except requests.ConnectionError:
                pass

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
            resp = requests.post(
                f"{self._dbt_url}/sources/duckdb-openivm/append/{batch_num}",
                timeout=600,
            )
            resp.raise_for_status()
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

    def _validate_duckdb_openivm(self, run_id: str, batch_num: int) -> None:
        """Run default OpenIVM correctness validation outside the benchmark timer."""
        self._emit(f"[duckdb-openivm] Validating batch {batch_num} with EXCEPT ALL")
        resp = requests.post(
            f"{self._dbt_url}/validate/duckdb-openivm/{run_id}",
            timeout=604800,
        )
        data = resp.json()

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
            raise RuntimeError(
                f"OpenIVM validation failed for batch {batch_num}"
                + (f": {detail}" if detail else f": {data.get('error', 'unknown error')}")
            )
        self._emit(
            f"[duckdb-openivm] Validation passed for batch {batch_num}: "
            f"{data.get('models_checked', 0)} models in {data.get('duration_s', '?')}s"
        )

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

        if wait_resp.status_code != 200:
            raise RuntimeError(
                f"Feldera batch {batch_num} wait failed: {wait_data.get('error', 'unknown')}"
            )

        duration = wait_data.get("duration_s", "?")
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

    def _batch_loader_init(self) -> None:
        """Initialize staging from batch1 (serial mode only)."""
        name = self._engine.name
        self._emit(f"[{name}] Batch loader: init")

        self._batch_mgr.run_service(
            "spark-batch-loader",
            cmd_args=["init"],
            stream_callback=lambda line: logger.debug("[batch-loader/%s] %s", name, line),
        )
        self._fix_delta_permissions()
        self._emit(f"[{name}] Batch loader: init complete")

    def _batch_loader_append(self, batch_num: int) -> None:
        """Append a batch to staging."""
        name = self._engine.name
        self._emit(f"[{name}] Batch loader: append {batch_num}")
        self._batch_mgr.run_service(
            "spark-batch-loader",
            cmd_args=["append", str(batch_num)],
            stream_callback=lambda line: logger.debug("[batch-loader/%s] %s", name, line),
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
        """Start container stats collection."""
        try:
            requests.post(
                f"{self._dbt_url}/stats/containers/start",
                json={
                    "engine": self._engine.name,
                    "scale_factor": self._config.scale_factor,
                },
                timeout=10,
            )
            self._emit(f"[{self._engine.name}] Stats collection started")
        except Exception as e:
            logger.warning("Failed to start stats: %s", e)

    def _stop_stats(self) -> None:
        """Stop container stats collection."""
        try:
            resp = requests.post(
                f"{self._dbt_url}/stats/containers/stop", timeout=10
            )
            count = resp.json().get("sample_count", 0)
            self._emit(f"[{self._engine.name}] Stats collection stopped ({count} samples)")
        except Exception as e:
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

    def _save_run_result(self, run_id: str, batch_num: int) -> None:
        """Save dbt run results JSON."""
        name = self._engine.name
        try:
            resp = requests.get(f"{self._dbt_url}/runs/{run_id}", timeout=30)
            results_dir = os.path.join(
                self._config.repo_dir,
                "mount", "results", str(self._config.scale_factor), "dbt-server",
            )
            os.makedirs(results_dir, exist_ok=True)
            with open(os.path.join(results_dir, f"run-{name}-batch{batch_num}.json"), "w") as f:
                json.dump(resp.json(), f, indent=2)
        except Exception as e:
            logger.warning("Failed to save run result: %s", e)

    def _persist_batch_result(self, batch_num: int, batch) -> None:
        """Persist batch result to benchmark-server's SQLite."""
        if not self._benchmark_id:
            return
        try:
            result_json = json.dumps({
                "batch_num": batch_num,
                "duration_s": batch.duration_s,
                "status": batch.status,
                "error": batch.error,
            })
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
            except requests.ConnectionError:
                pass
            self._emit(
                f"[{self._engine.name}] Waiting for dbt-server... "
                f"({i}/{HEALTH_TIMEOUT // HEALTH_INTERVAL})"
            )
            time.sleep(HEALTH_INTERVAL)
        raise TimeoutError(
            f"dbt-server for {self._engine.name} not healthy within {HEALTH_TIMEOUT}s"
        )
