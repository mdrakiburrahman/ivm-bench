"""Main benchmark orchestrator — coordinates all phases."""

import copy
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Optional

from models.config import BenchmarkConfig
from models.result import BatchResult, BenchmarkResult, EngineResult
from services.db import DB_LOCK, get_db
from services.docker_manager import DockerManager
from services.engine_runner import EngineRunner
from services.resource_calc import compute_engine_configs

logger = logging.getLogger(__name__)


class Orchestrator:
    """
    Orchestrates the full benchmark pipeline:
      Phase 1: Data generation + DuckDB-OpenIVM build (parallel)
      Phase 2: Engine benchmarks (parallel or serial)
      Phase 3: Chart generation + summary
    """

    def __init__(self, config: BenchmarkConfig) -> None:
        self._config = config
        self._result = BenchmarkResult()
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._running = False
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._benchmark_id: Optional[str] = None

    @property
    def config(self) -> BenchmarkConfig:
        return self._config

    def update_config(self, **overrides) -> None:
        """Update config fields. Only allowed when not running."""
        if self._running:
            raise RuntimeError("Cannot update config while running")
        for key, value in overrides.items():
            if hasattr(self._config, key):
                if key == "benchmark_runs":
                    value = max(1, int(value))
                setattr(self._config, key, value)

    @property
    def result(self) -> BenchmarkResult:
        return self._result

    @property
    def is_running(self) -> bool:
        return self._running

    def get_logs(self, timeout: float = 0.5) -> List[str]:
        """Drain pending log messages."""
        lines: List[str] = []
        try:
            while True:
                lines.append(self._log_queue.get_nowait())
        except queue.Empty:
            pass
        return lines

    def emit(self, msg: str) -> None:
        """Emit a log line to the SSE stream."""
        logger.info(msg)
        self._log_queue.put(msg)

    def start(self) -> None:
        """Start the benchmark in a background thread."""
        with self._lock:
            if self._running:
                raise RuntimeError("Benchmark already running")
            self._running = True
            self._result = BenchmarkResult(
                status="running",
                repetition_count=self._config.benchmark_runs,
            )

            # Create a persistent benchmark run record
            self._benchmark_id = str(uuid.uuid4())
            now_iso = datetime.now(timezone.utc).isoformat()
            config_json = json.dumps({
                "scale_factor": self._config.scale_factor,
                "benchmark_runs": self._config.benchmark_runs,
                "engines": self._config.engines,
                "parallel": self._config.parallel,
                "batch_1_pct": self._config.batch_1_pct,
                "batch_2_pct": self._config.batch_2_pct,
                "batch_3_pct": self._config.batch_3_pct,
            })
            with DB_LOCK:
                conn = get_db()
                conn.execute(
                    "INSERT INTO benchmark_runs (id, status, started_at, config_json) VALUES (?,?,?,?)",
                    (self._benchmark_id, "running", now_iso, config_json),
                )
                # Pre-create engine_batch records
                for engine in self._config.engines:
                    for batch_num in range(1, 4):
                        conn.execute(
                            "INSERT INTO engine_batches (benchmark_id, engine, batch_num, status) VALUES (?,?,?,?)",
                            (self._benchmark_id, engine, batch_num, "pending"),
                        )
                conn.commit()
                conn.close()

            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def _run(self) -> None:
        """Main benchmark execution."""
        t0 = time.time()
        try:
            self._teardown_existing()
            self._clean_mount()
            self._pre_create_dirs()
            self._phase1_prep()
            self._run_benchmark_repetitions()
            self._save_benchmark_results()
            self._phase3_chart()

            self._result.status = "completed"
            self._result.wall_clock_duration_s = time.time() - t0
            if self._config.benchmark_runs == 1:
                self._result.total_duration_s = self._result.wall_clock_duration_s
            self.emit(self._result.summary_table())
            self.emit(f"\nTotal wall-clock time: {self._result.wall_clock_duration_s:.1f}s")

            self._update_benchmark_run("completed", self._result.wall_clock_duration_s)

        except Exception as e:
            self._result.status = "failed"
            self._result.error = str(e)
            self._result.wall_clock_duration_s = time.time() - t0
            self._result.total_duration_s = self._result.wall_clock_duration_s
            self.emit(f"BENCHMARK FAILED: {e}")
            logger.exception("Benchmark failed")
            self._update_benchmark_run("failed", self._result.wall_clock_duration_s, str(e))

        finally:
            self._save_benchmark_results()
            self._dump_server_log()
            self._running = False
            self._log_queue.put("__DONE__")

    def _update_benchmark_run(self, status: str, duration_s: float = None, error: str = None) -> None:
        """Persist benchmark run status to SQLite."""
        if not self._benchmark_id:
            return
        now_iso = datetime.now(timezone.utc).isoformat()
        with DB_LOCK:
            conn = get_db()
            conn.execute(
                "UPDATE benchmark_runs SET status=?, completed_at=?, total_duration_s=?, error=? WHERE id=?",
                (status, now_iso, duration_s, error, self._benchmark_id),
            )
            conn.commit()
            conn.close()

    def _save_benchmark_results(self) -> None:
        """Persist benchmark results JSON to mount/results/<SF>/dbt-server/."""
        try:
            sf = str(self._config.scale_factor)
            results_dir = os.path.join(
                self._config.repo_dir, "mount", "results", sf, "dbt-server"
            )
            os.makedirs(results_dir, exist_ok=True)
            results_path = os.path.join(results_dir, "benchmark-results.json")
            with open(results_path, "w") as f:
                json.dump(self._result.to_dict(), f, indent=2)
            logger.info("Benchmark results written to %s", results_path)
        except Exception as e:
            logger.warning("Failed to save benchmark results: %s", e)

    def _dump_server_log(self) -> None:
        """Copy the benchmark-server log file to mount/logs/<SF>/."""
        try:
            src = "/tmp/benchmark-server.log"
            sf = str(self._config.scale_factor)
            dst_dir = os.path.join(self._config.repo_dir, "mount", "logs", sf)
            os.makedirs(dst_dir, exist_ok=True)
            dst = os.path.join(dst_dir, "benchmark-server.log")
            shutil.copy2(src, dst)
            logger.info("Server log written to %s", dst)
        except Exception as e:
            logger.warning("Failed to dump server log: %s", e)

    # ----- Phase 1: Prep -----

    def _teardown_existing(self) -> None:
        """Tear down any running containers from previous runs."""
        self.emit("=== Tearing down any running containers ===")
        # Teardown by known project names from previous benchmark runs
        compose_configs = [
            ("docker/docker-compose.datagen.yml", "datagen"),
            ("docker/docker-compose.batch-loader.yml", "batch-loader-build"),
            ("docker/docker-compose.duckdb-openivm-build.yml", "duckdb-openivm-build"),
        ]
        # Engine-specific project names
        for engine in ["spark", "duckdb", "duckdb-openivm", "feldera"]:
            from models.config import ENGINE_COMPOSE_FILES
            cf = ENGINE_COMPOSE_FILES.get(engine)
            if cf:
                compose_configs.append((cf, f"bench-{engine}"))
                compose_configs.append((cf, f"build-{engine}"))
            compose_configs.append(("docker/docker-compose.batch-loader.yml", f"batch-{engine}"))
        # Also teardown with default project names
        # (chart-gen no longer needed — chart is generated in-process)

        for cf, pname in compose_configs:
            path = os.path.join(self._config.repo_dir, cf)
            if os.path.exists(path):
                try:
                    mgr = DockerManager(path, project_name=pname, cwd=self._config.repo_dir)
                    mgr.down()
                except Exception:
                    pass

    def _clean_mount(self) -> None:
        """Remove the mount/ directory to start with clean state.

        Preserves mount/benchmark-state/ (benchmark-server's own SQLite).
        When PRESERVE_RAW=1 is set, also preserves mount/raw/ and
        mount/bin/ (idempotent datagen + duckdb-openivm build outputs)
        so multi-hour Phase 1 work can be re-used across triage runs.
        """
        mount_dir = os.path.join(self._config.repo_dir, "mount")
        real_mount = os.path.realpath(mount_dir)
        real_repo = os.path.realpath(self._config.repo_dir)
        if not real_mount.startswith(real_repo + os.sep):
            raise RuntimeError(
                f"Refusing to delete {real_mount} — not under repo {real_repo}"
            )
        if os.path.exists(mount_dir):
            preserve_raw = os.environ.get("PRESERVE_RAW", "0") == "1"
            preserve = {"benchmark-state"}
            if preserve_raw:
                preserve.update({"raw", "bin"})
                self.emit("=== Cleaning mount/ (preserving benchmark-state/, raw/, bin/) ===")
            else:
                self.emit("=== Cleaning mount/ directory (preserving benchmark-state/) ===")
            for entry in os.listdir(mount_dir):
                if entry in preserve:
                    continue
                entry_path = os.path.join(mount_dir, entry)
                if os.path.isdir(entry_path):
                    shutil.rmtree(entry_path)
                else:
                    os.remove(entry_path)
            self.emit("  mount/ cleaned")
        else:
            self.emit("=== mount/ does not exist — nothing to clean ===")

    def _pre_create_dirs(self) -> None:
        """Pre-create mount directories to avoid Docker root ownership."""
        sf = str(self._config.scale_factor)
        repo = self._config.repo_dir
        dirs = []
        for engine in self._config.engines:
            dirs.extend([
                f"mount/results/{sf}/{engine}",
                f"mount/logs/{sf}/{engine}",
                f"mount/stats/{sf}/{engine}",
            ])
            if engine == "feldera":
                dirs.append(f"mount/debug/{sf}/feldera")
        dirs.extend([
            f"mount/results/{sf}/dbt-server",
            f"mount/bin/duckdb-openivm",
        ])
        for d in dirs:
            full = os.path.join(repo, d)
            os.makedirs(full, exist_ok=True)
            os.chmod(full, 0o777)

    def _clean_benchmark_outputs(self) -> None:
        """Clean per-repetition outputs while preserving generated input data."""
        sf = str(self._config.scale_factor)
        repo = self._config.repo_dir
        paths = []
        for engine in self._config.engines:
            paths.extend([
                os.path.join(repo, "mount", "results", sf, engine),
                os.path.join(repo, "mount", "logs", sf, engine),
                os.path.join(repo, "mount", "stats", sf, engine),
            ])
            if engine == "feldera":
                paths.append(os.path.join(repo, "mount", "debug", sf, "feldera"))

        dbt_results = os.path.join(repo, "mount", "results", sf, "dbt-server")
        if os.path.isdir(dbt_results):
            for entry in os.listdir(dbt_results):
                if entry.startswith("repetition-"):
                    continue
                paths.append(os.path.join(dbt_results, entry))

        for path in paths:
            if os.path.isdir(path):
                shutil.rmtree(path)
            elif os.path.exists(path):
                os.remove(path)

    def _phase1_prep(self) -> None:
        """Phase 1: datagen + duckdb-openivm-build + batch-loader build (parallel)."""
        self.emit("")
        self.emit("=== Phase 1: Data generation & build ===")

        tasks = []
        with ThreadPoolExecutor(max_workers=3) as pool:
            tasks.append(pool.submit(self._run_datagen))

            if "duckdb-openivm" in self._config.engines:
                tasks.append(pool.submit(self._run_duckdb_openivm_build))

            tasks.append(pool.submit(self._build_batch_loader))

            for future in as_completed(tasks):
                future.result()  # Raise any exceptions

        self.emit("=== Phase 1: Complete ===")

    def _run_datagen(self) -> None:
        """Run TPC-DI data generation (idempotent)."""
        self.emit("  [datagen] Building images")
        repo = self._config.repo_dir
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.datagen.yml"),
            project_name="datagen",
            env=self._config.base_env(),
            cwd=repo,
        )
        mgr.build(["tpc-di-gen", "spark-digen-delta"])

        self.emit("  [datagen] Running tpc-di-gen → spark-digen-delta")
        mgr.up(
            services=["spark-digen-delta"],
            detach=False,
            stream_callback=lambda line: logger.debug("[datagen] %s", line),
        )

        digen_exit = mgr.get_exit_code("tpc-di-gen")
        delta_exit = mgr.get_exit_code("spark-digen-delta")

        if digen_exit != "0" or delta_exit != "0":
            logs = mgr.logs()
            mgr.down()
            raise RuntimeError(
                f"Datagen failed (tpc-di-gen={digen_exit}, spark-digen-delta={delta_exit})\n{logs[:2000]}"
            )

        mgr.down()
        self.emit("  [datagen] Complete")

        # Fix permissions
        sf = str(self._config.scale_factor)
        delta_dir = os.path.join(repo, "mount", "raw", sf, "delta")
        os.system(f"docker run --rm -v {delta_dir}:/data alpine chmod -R 777 /data")

    def _run_duckdb_openivm_build(self) -> None:
        """Build DuckDB-OpenIVM binary (idempotent)."""
        self.emit("  [duckdb-openivm-build] Building")
        repo = self._config.repo_dir
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.duckdb-openivm-build.yml"),
            project_name="duckdb-openivm-build",
            cwd=repo,
        )
        dockerfile = os.path.join(repo, "src/containers/duckdb-openivm/Dockerfile")
        pinned_commit = None
        with open(dockerfile, "r", encoding="utf-8") as f:
            # The OpenIVM binary is expensive to rebuild. The Dockerfile records
            # the pinned OpenIVM commit as both a build ARG and an image label, so
            # we can reuse the existing builder image when its label still matches
            # the current pin and rebuild only when the pin changes.
            match = re.search(r"^ARG OPENIVM_COMMIT=([0-9a-f]+)$", f.read(), re.MULTILINE)
            if match:
                pinned_commit = match.group(1)
        image_commit = subprocess.run(
            [
                "docker", "image", "inspect",
                "duckdb-openivm-build-duckdb-openivm-builder:latest",
                "--format", "{{ index .Config.Labels \"org.openivm.commit\" }}",
            ],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        if not pinned_commit or image_commit.returncode != 0 or image_commit.stdout.strip() != pinned_commit:
            mgr.build()
        else:
            self.emit(f"  [duckdb-openivm-build] Reusing image for OpenIVM {pinned_commit[:7]}")
        mgr.up(
            services=["duckdb-openivm-builder"],
            detach=False,
            stream_callback=lambda line: logger.debug("[duckdb-openivm-build] %s", line),
        )
        mgr.down()

        binary = os.path.join(repo, "mount", "bin", "duckdb-openivm", "duckdb")
        if not os.path.exists(binary):
            raise RuntimeError("DuckDB-OpenIVM binary not found at mount/bin/duckdb-openivm/duckdb")

        self.emit("  [duckdb-openivm-build] Complete")

    def _build_batch_loader(self) -> None:
        """Build the batch-loader image."""
        self.emit("  [batch-loader] Building image")
        repo = self._config.repo_dir
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.batch-loader.yml"),
            project_name="batch-loader-build",
            env=self._config.base_env(),
            cwd=repo,
        )
        mgr.build()
        self.emit("  [batch-loader] Build complete")

    def _run_benchmark_repetitions(self) -> None:
        """Run the full engine benchmark one or more times and average results."""
        run_count = max(1, self._config.benchmark_runs)
        self._config.benchmark_runs = run_count
        self._result.repetition_count = run_count

        if run_count == 1:
            self._phase2_benchmark()
            return

        repetitions: List[Dict[str, EngineResult]] = []
        for run_idx in range(1, run_count + 1):
            self.emit("")
            self.emit(f"=== Benchmark repetition {run_idx}/{run_count} ===")
            self._teardown_existing()
            self._clean_benchmark_outputs()
            self._pre_create_dirs()

            self._result.engines = {}
            self._phase2_benchmark()
            repetition_engines = copy.deepcopy(self._result.engines)
            repetitions.append(repetition_engines)
            self._save_repetition_result(run_idx, repetition_engines)
            self._archive_repetition_outputs(run_idx)

        self._result.repetitions = repetitions
        self._result.engines = self._average_repetition_results(repetitions)
        self._result.total_duration_s = sum(
            er.total_duration_s for er in self._result.engines.values()
        )
        self._save_benchmark_results()

    def _average_repetition_results(
        self, repetitions: List[Dict[str, EngineResult]],
    ) -> Dict[str, EngineResult]:
        """Average per-engine, per-batch durations across repetitions."""
        averaged: Dict[str, EngineResult] = {}
        for engine in self._config.engines:
            engine_runs = [rep[engine] for rep in repetitions if engine in rep]
            if len(engine_runs) != len(repetitions):
                raise RuntimeError(f"Missing repetition result for engine {engine}")

            batch_nums = [b.batch_num for b in engine_runs[0].batches]
            batches = []
            for idx, batch_num in enumerate(batch_nums):
                durations = [er.batches[idx].duration_s for er in engine_runs]
                statuses = [er.batches[idx].status for er in engine_runs]
                errors = [er.batches[idx].error for er in engine_runs if er.batches[idx].error]
                batches.append(BatchResult(
                    batch_num=batch_num,
                    duration_s=sum(durations) / len(durations),
                    status="completed" if all(s == "completed" for s in statuses) else "failed",
                    error="; ".join(errors) if errors else None,
                ))

            extra = {}
            extra_keys = set().union(*(er.extra.keys() for er in engine_runs))
            for key in extra_keys:
                values = [er.extra.get(key) for er in engine_runs]
                if all(isinstance(v, (int, float)) for v in values):
                    extra[key] = sum(values) / len(values)

            statuses = [er.status for er in engine_runs]
            errors = [er.error for er in engine_runs if er.error]
            averaged[engine] = EngineResult(
                engine=engine,
                batches=batches,
                status="completed" if all(s == "completed" for s in statuses) else "failed",
                error="; ".join(errors) if errors else None,
                extra=extra,
            )

        return averaged

    def _save_repetition_result(
        self, run_idx: int, engines: Dict[str, EngineResult],
    ) -> None:
        """Write a compact JSON result for one benchmark repetition."""
        sf = str(self._config.scale_factor)
        results_dir = os.path.join(
            self._config.repo_dir,
            "mount", "results", sf, "dbt-server", f"repetition-{run_idx}",
        )
        os.makedirs(results_dir, exist_ok=True)
        result = BenchmarkResult(
            status="completed",
            engines=engines,
            repetition_count=1,
            total_duration_s=sum(er.total_duration_s for er in engines.values()),
        )
        with open(os.path.join(results_dir, "benchmark-results.json"), "w") as f:
            json.dump(result.to_dict(), f, indent=2)

    def _archive_repetition_outputs(self, run_idx: int) -> None:
        """Copy dbt-server output files for one repetition before the next clean."""
        sf = str(self._config.scale_factor)
        results_dir = os.path.join(
            self._config.repo_dir, "mount", "results", sf, "dbt-server",
        )
        archive_dir = os.path.join(results_dir, f"repetition-{run_idx}")
        os.makedirs(archive_dir, exist_ok=True)

        for entry in os.listdir(results_dir):
            src = os.path.join(results_dir, entry)
            if entry.startswith("repetition-") or os.path.isdir(src):
                continue
            shutil.copy2(src, os.path.join(archive_dir, entry))

    # ----- Phase 2: Benchmarks -----

    def _phase2_benchmark(self) -> None:
        """Phase 2: Run engine benchmarks (parallel or serial)."""
        self.emit("")
        self.emit("=== Phase 2: Engine benchmarks ===")

        engine_configs = compute_engine_configs(self._config)
        self._engine_configs = engine_configs
        engines = self._config.engines

        if self._config.parallel and len(engines) > 1:
            # Hard barrier: init staging once, copy to per-engine dirs BEFORE any engine starts
            self._init_parallel_staging(engine_configs)
            self.emit(f"  Running {len(engines)} engines in PARALLEL")
            self._run_engines_parallel(engine_configs)
        else:
            self.emit(f"  Running {len(engines)} engines SERIALLY")
            self._run_engines_serial(engine_configs)

        self.emit("=== Phase 2: Complete ===")

    def _build_engine_images(self) -> None:
        """Build Docker images for all selected engines."""
        self.emit("  Building engine images")
        repo = self._config.repo_dir

        from models.config import ENGINE_COMPOSE_FILES
        for engine in self._config.engines:
            cf = ENGINE_COMPOSE_FILES[engine]
            mgr = DockerManager(
                os.path.join(repo, cf),
                project_name=f"build-{engine}",
                env=self._config.base_env(),
                cwd=repo,
            )
            mgr.build()
            self.emit(f"  [{engine}] Images built")

    def _init_parallel_staging(self, engine_configs: Dict) -> None:
        """Run batch_loader init ONCE, then copy staging to per-engine dirs.

        This is a hard barrier — all copies complete before any engine starts.
        """
        repo = self._config.repo_dir
        sf = str(self._config.scale_factor)

        # Determine which engines need Delta staging. DuckDB and DuckDB-OpenIVM
        # both load DuckLake sources directly inside their measured batch path.
        engines_needing_staging = [
            e for e in self._config.engines if e not in ("duckdb", "duckdb-openivm")
        ]
        if not engines_needing_staging:
            return

        self.emit("  [staging] Running batch_loader init (shared)")
        batch_mgr = DockerManager(
            compose_file=os.path.join(repo, "docker/docker-compose.batch-loader.yml"),
            project_name="batch-init-shared",
            env=self._config.base_env(),
            cwd=repo,
        )
        batch_mgr.run_service(
            "spark-batch-loader",
            cmd_args=["init"],
            stream_callback=lambda line: logger.debug("[batch-loader/init] %s", line),
        )

        # Fix permissions on shared staging
        delta_dir = os.path.join(repo, "mount", "raw", sf, "delta")
        staging_src = os.path.join(delta_dir, "staging")
        os.system(f"docker run --rm -v {staging_src}:/data alpine chmod -R 777 /data")

        # Copy staging to per-engine dirs
        for engine_name in engines_needing_staging:
            ec = engine_configs[engine_name]
            dst = os.path.join(delta_dir, ec.staging_dir)
            if os.path.exists(dst):
                shutil.rmtree(dst)
            self.emit(f"  [staging] Copying staging → {ec.staging_dir}")
            shutil.copytree(staging_src, dst)
            os.system(f"docker run --rm -v {dst}:/data alpine chmod -R 777 /data")
            self.emit(f"  [staging] {ec.staging_dir} ready")

        self.emit("  [staging] All per-engine staging dirs initialized")

    def _run_engines_serial(self, engine_configs: Dict) -> None:
        """Run engines one at a time."""
        for name in self._config.engines:
            ec = engine_configs[name]
            runner = EngineRunner(self._config, ec, self.emit, self._benchmark_id)
            result = runner.run()
            self._result.engines[name] = result
            if result.status == "failed":
                raise RuntimeError(f"Engine {name} failed: {result.error}")

    def _run_engines_parallel(self, engine_configs: Dict) -> None:
        """Run all engines concurrently in a single wave."""
        engines = list(self._config.engines)
        self._run_engine_wave(engines, engine_configs)

        # Check if any failed
        failed = [n for n, r in self._result.engines.items() if r.status == "failed"]
        if failed:
            raise RuntimeError(f"Engines failed: {', '.join(failed)}")

    def _run_engine_wave(self, engines: List[str], engine_configs: Dict) -> None:
        """Run a wave of engines in parallel."""
        if not engines:
            return
        futures = {}
        with ThreadPoolExecutor(max_workers=len(engines)) as pool:
            for name in engines:
                ec = engine_configs[name]
                runner = EngineRunner(self._config, ec, self.emit, self._benchmark_id)
                futures[pool.submit(runner.run)] = (name, runner)

            for future in as_completed(futures):
                name, runner = futures[future]
                try:
                    result = future.result()
                    self._result.engines[name] = result
                except Exception as e:
                    er = EngineResult(engine=name, status="failed", error=str(e))
                    self._result.engines[name] = er
                    self.emit(f"[{name}] FAILED: {e}")

    # ----- Phase 3: Chart -----

    def _phase3_chart(self) -> None:
        """Phase 3: Generate results charts directly (no Docker container needed)."""
        self.emit("")
        self.emit("=== Phase 3: Generating results charts ===")

        repo = self._config.repo_dir
        sf = self._config.scale_factor
        b1 = self._config.batch_1_pct
        b2 = self._config.batch_2_pct
        b3 = self._config.batch_3_pct
        results_dir = os.path.join(repo, "mount", "results", str(sf), "dbt-server")
        stats_dir = os.path.join(repo, "mount", "stats", str(sf))

        engine_resources = {}
        if hasattr(self, "_engine_configs") and self._engine_configs:
            for name, ecfg in self._engine_configs.items():
                engine_resources[name] = {
                    "cpus": ecfg.main_resources.cpus,
                    "memory_gb": ecfg.main_resources.memory_gb,
                }

        # --- Scale-factor chart ---
        try:
            from handlers.chart import generate_chart_png

            png_data = generate_chart_png(
                state_dir=results_dir,
                sf=str(sf),
                b1pct=b1,
                b2pct=b2,
                b3pct=b3,
                engine_resources=engine_resources,
                stats_dir=stats_dir,
            )
            if png_data:
                b1_slug = b1.replace(".", "_")
                b2_slug = b2.replace(".", "_")
                b3_slug = b3.replace(".", "_")
                chart_file = f"scale-factor-{sf}-{b1_slug}-{b2_slug}-{b3_slug}.png"
                imgs_dir = os.path.join(repo, "imgs")
                os.makedirs(imgs_dir, exist_ok=True)
                chart_path = os.path.join(imgs_dir, chart_file)
                with open(chart_path, "wb") as f:
                    f.write(png_data)
                self.emit(f"  Scale-factor chart saved to imgs/{chart_file}")
            else:
                self.emit("  WARNING: No data available for scale-factor chart")
        except Exception as e:
            self.emit(f"  WARNING: Scale-factor chart generation failed: {e}")
            logger.warning("Scale-factor chart generation failed: %s", e)

        # --- Heuristics chart ---
        try:
            from handlers.chart import generate_heuristics_png

            heuristics_data = generate_heuristics_png(state_dir=results_dir)
            if heuristics_data:
                imgs_dir = os.path.join(repo, "imgs")
                os.makedirs(imgs_dir, exist_ok=True)
                heuristics_path = os.path.join(imgs_dir, "benchmark-heuristics.png")
                with open(heuristics_path, "wb") as f:
                    f.write(heuristics_data)
                self.emit("  Heuristics chart saved to imgs/benchmark-heuristics.png")
            else:
                self.emit("  WARNING: No data available for heuristics chart")
        except Exception as e:
            self.emit(f"  WARNING: Heuristics chart generation failed: {e}")
            logger.warning("Heuristics chart generation failed: %s", e)


# ---------------------------------------------------------------------------
# Module-level singleton — created lazily on first access
# ---------------------------------------------------------------------------
_instance: Optional[Orchestrator] = None
_instance_lock = threading.Lock()


def get_orchestrator() -> Orchestrator:
    """Return the singleton Orchestrator, creating it on first call."""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                from services.config_loader import load_config
                _instance = Orchestrator(load_config())
    return _instance
