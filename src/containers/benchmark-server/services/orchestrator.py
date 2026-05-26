"""Main benchmark orchestrator — coordinates all phases."""

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
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional

from models.config import BenchmarkConfig
from models.experiments import ExperimentInputs, from_env as experiments_from_env, parse_experiments_json
from models.result import BenchmarkResult, EngineResult
from services import oat_runner
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

        # OAT (one-at-a-time) sweep state. None / empty for single-experiment runs.
        self._experiments_file: Optional[str] = None
        self._experiments: List[ExperimentInputs] = []
        self._oat_run_id: Optional[str] = None
        self._oat_started_at: Optional[str] = None
        self._oat_per_exp_dicts: List[dict] = []

    @property
    def config(self) -> BenchmarkConfig:
        return self._config

    def update_config(self, **overrides) -> None:
        """Update config fields. Only allowed when not running."""
        if self._running:
            raise RuntimeError("Cannot update config while running")
        for key, value in overrides.items():
            if hasattr(self._config, key):
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

    @contextmanager
    def _heartbeat(self, label: str, interval_s: float = 30.0) -> Iterator[None]:
        """Emit a periodic '[label] still running... (Ns elapsed)' message.

        Long-running Docker build / up steps stream their stdout via
        ``subprocess.run(capture_output=True)`` so nothing reaches the SSE feed
        until the step exits. On a cold cache the spark-openivm build alone
        takes ~10 minutes, which is indistinguishable from a hang. The
        heartbeat thread emits a liveness line every ``interval_s`` seconds
        while the wrapped block runs, then stops on exit (success or failure).
        """
        stop = threading.Event()
        t0 = time.time()

        def _beat() -> None:
            while not stop.wait(interval_s):
                elapsed = int(time.time() - t0)
                self.emit(f"  [{label}] still running... ({elapsed}s elapsed)")

        thread = threading.Thread(target=_beat, daemon=True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout=interval_s + 1)

    def start(self, experiments_file: Optional[str] = None) -> None:
        """Start the benchmark in a background thread.

        Modes:
          * single-experiment (back-compat): ``experiments_file`` is None — the
            orchestrator reads classic env vars (SCALE_FACTOR, BATCH_*, ENGINES,
            …), inserts ONE benchmark_runs row up-front, and runs the pipeline
            exactly as it did pre-OAT.
          * OAT sweep: ``experiments_file`` is a path to a JSON file describing
            N experiments. We parse it now to fail fast, create the parent
            ``oat_runs`` record, then iterate inside ``_run_oat`` — one fresh
            ``benchmark_runs`` row per experiment.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("Benchmark already running")
            self._running = True
            self._result = BenchmarkResult(status="running")
            self._experiments_file = experiments_file
            self._experiments = []
            self._oat_run_id = None
            self._oat_per_exp_dicts = []
            self._benchmark_id = None

            if experiments_file:
                if not os.path.exists(experiments_file):
                    self._running = False
                    raise FileNotFoundError(experiments_file)
                with open(experiments_file) as f:
                    self._experiments = parse_experiments_json(f.read())
                if not self._experiments:
                    self._running = False
                    raise ValueError("experiments JSON has no experiments")
                self._oat_run_id = str(uuid.uuid4())
                self._oat_started_at = oat_runner.iso_now()
                with DB_LOCK:
                    conn = get_db()
                    conn.execute(
                        "INSERT INTO oat_runs (id, status, started_at, experiments_file) VALUES (?,?,?,?)",
                        (self._oat_run_id, "running", self._oat_started_at, experiments_file),
                    )
                    conn.commit()
                    conn.close()
            else:
                # Single-experiment mode: insert benchmark_runs eagerly so
                # /benchmark/status returns useful info immediately after start.
                self._init_benchmark_run_record_from_config()

            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def _init_benchmark_run_record_from_config(self) -> None:
        """Create a fresh benchmark_runs row + engine_batches rows from ``self._config``.

        Sets ``self._benchmark_id`` to the new UUID. Called once per experiment
        (once total in single mode, once per OAT iteration in OAT mode).
        """
        self._benchmark_id = str(uuid.uuid4())
        now_iso = datetime.now(timezone.utc).isoformat()
        config_json = json.dumps({
            "scale_factor": self._config.scale_factor,
            "engines": self._config.engines,
            "parallel": self._config.parallel,
            "batch_1_pct": self._config.batch_1_pct,
            "batch_2_pct": self._config.batch_2_pct,
            "batch_3_pct": self._config.batch_3_pct,
            "oat_run_id": self._oat_run_id,
        })
        with DB_LOCK:
            conn = get_db()
            conn.execute(
                "INSERT INTO benchmark_runs (id, status, started_at, config_json) VALUES (?,?,?,?)",
                (self._benchmark_id, "running", now_iso, config_json),
            )
            for engine in self._config.engines:
                for batch_num in range(1, 4):
                    conn.execute(
                        "INSERT INTO engine_batches (benchmark_id, engine, batch_num, status) VALUES (?,?,?,?)",
                        (self._benchmark_id, engine, batch_num, "pending"),
                    )
            conn.commit()
            conn.close()

    def _run(self) -> None:
        """Dispatcher — single experiment vs OAT sweep."""
        try:
            if self._oat_run_id is not None:
                self._run_oat()
            else:
                self._run_single_legacy()
        finally:
            self._running = False
            self._log_queue.put("__DONE__")

    def _run_single_legacy(self) -> None:
        """Single-experiment run (back-compat with pre-OAT benchmark.sh)."""
        t0 = time.time()
        try:
            self._teardown_existing()
            self._clean_mount()
            self._pre_create_dirs()
            self._phase1_prep()
            self._phase2_benchmark()
            self._save_benchmark_results()
            self._phase3_chart()

            self._result.status = "completed"
            self._result.total_duration_s = time.time() - t0
            self.emit(self._result.summary_table())
            self.emit(f"\nTotal wall-clock time: {self._result.total_duration_s:.1f}s")

            self._update_benchmark_run("completed", self._result.total_duration_s)

        except Exception as e:
            self._result.status = "failed"
            self._result.error = str(e)
            self._result.total_duration_s = time.time() - t0
            self.emit(f"BENCHMARK FAILED: {e}")
            logger.exception("Benchmark failed")
            self._update_benchmark_run("failed", self._result.total_duration_s, str(e))

        finally:
            self._save_benchmark_results()
            self._dump_server_log()

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
            ("docker/docker-compose.spark-openivm-build.yml", "spark-openivm-build"),
        ]
        # Engine-specific project names
        for engine in ["spark", "duckdb", "duckdb-openivm", "feldera", "spark-openivm"]:
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

    def _clean_mount(self, force_clean_raw: bool = False) -> None:
        """Remove the mount/ directory to start with clean state.

        Preserves mount/benchmark-state/ (benchmark-server's own SQLite) and
        mount/oat-state/ (OAT artifacts written eagerly before this method
        runs). When PRESERVE_RAW=1 is set AND ``force_clean_raw=False``,
        ALSO preserves mount/raw/ and mount/bin/ (idempotent datagen +
        duckdb-openivm build outputs).

        OAT phase 0 sets ``force_clean_raw=True`` so a stale per-SF Delta
        tree from a previous host run cannot leak into the first OAT
        experiment (datagen short-circuits when raw/<sf>/delta/ already
        exists; with stale data the experiment would silently run with the
        wrong batch percentages).
        """
        mount_dir = os.path.join(self._config.repo_dir, "mount")
        real_mount = os.path.realpath(mount_dir)
        real_repo = os.path.realpath(self._config.repo_dir)
        if not real_mount.startswith(real_repo + os.sep):
            raise RuntimeError(
                f"Refusing to delete {real_mount} — not under repo {real_repo}"
            )
        if os.path.exists(mount_dir):
            preserve_raw = (
                os.environ.get("PRESERVE_RAW", "0") == "1" and not force_clean_raw
            )
            # Always preserve oat-state/ — the OAT runner writes artifacts there
            # BEFORE this method runs (we create the directory + inputs.json
            # eagerly so even a crash leaves a trace).
            preserve = {"benchmark-state", "oat-state"}
            if preserve_raw:
                preserve.update({"raw", "bin"})
                self.emit("=== Cleaning mount/ (preserving benchmark-state/, oat-state/, raw/, bin/) ===")
            elif force_clean_raw:
                self.emit("=== Cleaning mount/ — OAT mode, FORCING raw/ wipe (preserving benchmark-state/, oat-state/) ===")
            else:
                self.emit("=== Cleaning mount/ directory (preserving benchmark-state/, oat-state/) ===")
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
            f"mount/bin/spark-openivm",
        ])
        for d in dirs:
            full = os.path.join(repo, d)
            os.makedirs(full, exist_ok=True)
            os.chmod(full, 0o777)

    def _phase1_prep(self, include_datagen: bool = True) -> None:
        """Phase 1: datagen + duckdb-openivm-build + spark-openivm-build + batch-loader build (parallel).

        ``include_datagen=False`` runs ONLY the build steps. The OAT runner
        uses that during ``_phase0_one_time_prep`` so builds happen once
        per sweep while datagen runs once per experiment (different SF).
        """
        self.emit("")
        self.emit("=== Phase 1: Data generation & build ===")

        # Build the callable list first so the pool's max_workers can match
        # the actual number of submitted tasks. Otherwise adding a 5th build
        # in the future would silently serialize against the fixed 4-slot pool.
        callables = []
        if include_datagen:
            callables.append(self._run_datagen)
        callables.append(self._build_batch_loader)
        if "duckdb-openivm" in self._config.engines:
            callables.append(self._run_duckdb_openivm_build)
        if "spark-openivm" in self._config.engines:
            callables.append(self._run_spark_openivm_build)

        with ThreadPoolExecutor(max_workers=len(callables)) as pool:
            tasks = [pool.submit(fn) for fn in callables]
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
        with self._heartbeat("datagen/build"):
            mgr.build(["tpc-di-gen", "spark-digen-delta"])

        self.emit("  [datagen] Running tpc-di-gen → spark-digen-delta")
        with self._heartbeat("datagen/run"):
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
            self.emit(
                "  [duckdb-openivm-build] Cold cache: compiling DuckDB+OpenIVM "
                "from source (~5-10 min)"
            )
            with self._heartbeat("duckdb-openivm-build/build"):
                mgr.build()
        else:
            self.emit(f"  [duckdb-openivm-build] Reusing image for OpenIVM {pinned_commit[:7]}")
        with self._heartbeat("duckdb-openivm-build/run"):
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

    def _run_spark_openivm_build(self) -> None:
        """Build the spark-openivm assembly jar + duckdb extension + CLI (idempotent).

        Mirrors `_run_duckdb_openivm_build`: regex-parses the pinned
        OPENIVM_SPARK_COMMIT from the Dockerfile, reuses the existing
        builder image when its `org.openivm.spark.commit` label still
        matches, otherwise rebuilds.
        """
        self.emit("  [spark-openivm-build] Building")
        repo = self._config.repo_dir
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.spark-openivm-build.yml"),
            project_name="spark-openivm-build",
            cwd=repo,
        )
        dockerfile = os.path.join(
            repo, "src/containers/spark-openivm-build/Dockerfile"
        )
        pinned_commit = None
        with open(dockerfile, "r", encoding="utf-8") as f:
            content = f.read()
        # The Dockerfile declares the spark-openivm pin in two stages
        # (spark-ext-builder and final). Either match is fine — they're
        # identical by construction.
        match = re.search(
            r"^ARG OPENIVM_SPARK_COMMIT=([0-9a-f]+)$", content, re.MULTILINE
        )
        if match:
            pinned_commit = match.group(1)
        image_commit = subprocess.run(
            [
                "docker", "image", "inspect",
                "spark-openivm-build-spark-openivm-builder:latest",
                "--format", "{{ index .Config.Labels \"org.openivm.spark.commit\" }}",
            ],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        if (
            not pinned_commit
            or image_commit.returncode != 0
            or image_commit.stdout.strip() != pinned_commit
        ):
            self.emit(
                "  [spark-openivm-build] Cold cache: compiling DuckDB+OpenIVM "
                "and sbt-assembling spark extension from source (~10 min)"
            )
            with self._heartbeat("spark-openivm-build/build"):
                mgr.build()
        else:
            self.emit(
                f"  [spark-openivm-build] Reusing image for openivm-spark {pinned_commit[:7]}"
            )
        with self._heartbeat("spark-openivm-build/run"):
            mgr.up(
                services=["spark-openivm-builder"],
                detach=False,
                stream_callback=lambda line: logger.debug(
                    "[spark-openivm-build] %s", line
                ),
            )
        mgr.down()

        bin_dir = os.path.join(repo, "mount", "bin", "spark-openivm")
        for f in ("openivm-extension.jar", "openivm.duckdb_extension", "duckdb"):
            path = os.path.join(bin_dir, f)
            if not os.path.exists(path):
                raise RuntimeError(
                    f"spark-openivm artifact missing at {path}"
                )

        self.emit("  [spark-openivm-build] Complete")

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
        with self._heartbeat("batch-loader/build"):
            mgr.build()
        self.emit("  [batch-loader] Build complete")

    # ----- Phase 2: Benchmarks -----

    def _phase2_benchmark(self) -> None:
        """Phase 2: Run engine benchmarks (parallel or serial)."""
        self.emit("")
        self.emit("=== Phase 2: Engine benchmarks ===")

        engine_configs = compute_engine_configs(self._config)
        self._engine_configs = engine_configs
        engines = self._config.engines

        # When parallel flag is set, resource_calc assigns per-engine staging
        # dirs (e.g. staging-spark-openivm) regardless of engine count.
        # Init staging before any engine starts so the host directories and
        # container mountpoints exist for the compose override mounts.
        if self._config.parallel:
            self._init_parallel_staging(engine_configs)

        if self._config.parallel and len(engines) > 1:
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

    # ----- OAT (one-at-a-time) sweep -----

    def _run_oat(self) -> None:
        """Drive an OAT sweep — run each experiment serially with disk-aware cleanup.

        Lifecycle per OAT run::

            phase 0  one-time prep: clean mount/, run builds for the UNION of
                     engines across all experiments. NOT datagen — that runs
                     per-experiment because SF varies.
            loop     for each experiment:
                       * disk pre-flight (skip if < OAT_MIN_FREE_PCT)
                       * apply knobs (mutate self._config + os.environ)
                       * teardown prev engine stacks
                       * pre-create per-SF dirs
                       * datagen for this SF (idempotent)
                       * phase 2 engine benchmark
                       * write per-experiment outputs.json + master outputs.json
                       * disk-cleanup: wipe raw/<sf>, results/<sf>/<engine>,
                         logs/<sf>/<engine>. Preserve dbt-server/, stats/, bin/.
            phase 3  OAT chart + RESULTS.md generation; copy server log.
        """
        oat_t0 = time.time()
        repo = self._config.repo_dir

        oat_runner.write_inputs(repo, self._oat_run_id, self._experiments,
                                self._experiments_file or "<n/a>")
        oat_runner.maintain_latest_symlink(repo, self._oat_run_id)
        oat_runner.write_master_outputs(
            repo_dir=repo, oat_run_id=self._oat_run_id,
            experiments_file=self._experiments_file or "<n/a>",
            status="running", started_at=self._oat_started_at,
            completed_at=None, total_duration_s=0.0,
            per_experiment_dicts=[],
        )

        self.emit("")
        self.emit(f"=== OAT sweep {self._oat_run_id[:8]} — {len(self._experiments)} experiment(s) ===")
        self.emit(f"  experiments file: {self._experiments_file}")
        min_free_pct = float(os.environ.get("OAT_MIN_FREE_PCT", "10"))
        self.emit(f"  disk-guard threshold: {min_free_pct:.1f}% free")

        # Phase 0 — one-time prep (clean mount once, run builds for the union of engines).
        # Pre-flight disk check: refuse to start if already below threshold.
        ok_pre, pct_pre = oat_runner.disk_check_ok(repo, min_free_pct)
        if not ok_pre:
            err = (
                f"disk_free {pct_pre:.1f}% < {min_free_pct:.1f}% before any work — "
                f"refusing to start OAT sweep"
            )
            self.emit(f"=== OAT pre-flight FAILED: {err} ===")
            self._finalize_oat("failed", oat_t0, error=err)
            return

        try:
            self._phase0_one_time_prep(self._experiments)
        except Exception as e:
            self.emit(f"OAT phase-0 prep FAILED: {e}")
            logger.exception("OAT phase-0 prep failed")
            self._finalize_oat("failed", oat_t0, error=str(e))
            return

        # Post-phase-0 disk check: builds can be large (multi-GB images).
        ok_post0, pct_post0 = oat_runner.disk_check_ok(repo, min_free_pct)
        if not ok_post0:
            err = (
                f"disk_free {pct_post0:.1f}% < {min_free_pct:.1f}% after phase-0 builds — "
                f"refusing to run experiments"
            )
            self.emit(f"=== OAT post-phase-0 FAILED: {err} ===")
            self._finalize_oat("failed", oat_t0, error=err)
            return

        try:
            for idx, inputs in enumerate(self._experiments):
                self._run_one_oat_experiment(idx, inputs, min_free_pct)
                # Persist master after EVERY experiment so a mid-sweep crash
                # still leaves a valid, partial outputs.json + chart input.
                oat_runner.write_master_outputs(
                    repo_dir=repo, oat_run_id=self._oat_run_id,
                    experiments_file=self._experiments_file or "<n/a>",
                    status="running", started_at=self._oat_started_at,
                    completed_at=None,
                    total_duration_s=time.time() - oat_t0,
                    per_experiment_dicts=self._oat_per_exp_dicts,
                )
                # Regenerate the OAT charts + RESULTS.md after every
                # experiment so an external observer (`watch eog ...`, a
                # file-watcher dashboard, `cat .../RESULTS.md`) can see the
                # sweep evolve in real time. Atomic writes inside
                # _phase3_oat_chart guarantee readers never see a half-
                # written PNG/MD. ``silent=True`` keeps the SSE stream
                # quiet — the chart pass logs to /tmp/benchmark-server.log
                # at WARN level on failure regardless.
                self._phase3_oat_chart(silent=True)

            # Propagate any per-experiment failure into the final sweep status.
            # Skipped experiments (disk-guard) don't downgrade — they're a
            # documented graceful outcome — but a real engine failure does.
            failed_count = sum(
                1 for d in self._oat_per_exp_dicts if d.get("status") == "failed"
            )
            final_status = "failed" if failed_count > 0 else "completed"
            final_error = (
                f"{failed_count}/{len(self._experiments)} experiment(s) failed"
                if failed_count else None
            )
            self._finalize_oat(final_status, oat_t0, error=final_error)
        except Exception as e:
            self.emit(f"OAT sweep FAILED: {e}")
            logger.exception("OAT sweep failed")
            self._finalize_oat("failed", oat_t0, error=str(e))

    def _phase0_one_time_prep(self, experiments: List[ExperimentInputs]) -> None:
        """Run teardown, mount-clean, dir pre-create, and BUILDS once per OAT sweep.

        Builds run for the union of engines across all experiments so we
        never have to rebuild mid-sweep.
        """
        # Compute union of engines so phase1 includes all relevant builds.
        union_engines: List[str] = []
        for inp in experiments:
            for e in inp.engines:
                if e not in union_engines:
                    union_engines.append(e)
        # Stash original config + temporarily widen engines for the build phase.
        # We restore the per-experiment config inside the loop via _apply_experiment.
        original_engines = list(self._config.engines)
        self._config.engines = union_engines
        try:
            self._teardown_existing()
            # In OAT mode, FORCE raw/ wipe regardless of PRESERVE_RAW — stale
            # mount/raw/<sf>/ from a previous host run would otherwise leak
            # into the first OAT experiment (datagen short-circuits when the
            # output dir already exists, silently using wrong batch %).
            self._clean_mount(force_clean_raw=True)
            # SF for pre-create dirs at phase 0: use the FIRST experiment's SF.
            # The loop re-creates per-SF dirs inside _run_one_oat_experiment.
            first = experiments[0]
            self._config.scale_factor = first.scale_factor
            self._pre_create_dirs()
            # Builds only — no datagen here. Each experiment runs its own datagen.
            self._phase1_prep(include_datagen=False)
        finally:
            self._config.engines = original_engines

    def _run_one_oat_experiment(
        self, exp_idx: int, inputs: ExperimentInputs, min_free_pct: float
    ) -> None:
        """Execute (or skip) a single experiment inside the OAT loop.

        Cleanup of the SF-specific raw / engine results / logs is run in a
        ``finally`` so even an ENOSPC mid-experiment frees space before the
        next iteration begins. We also re-check disk after datagen — a
        large SF can blow the threshold during data generation alone, in
        which case we abort the experiment cleanly (cleanup still runs).
        """
        repo = self._config.repo_dir
        exp_t0 = time.time()
        started_at = oat_runner.iso_now()

        self.emit("")
        label = inputs.label or f"exp-{exp_idx}"
        self.emit(f"=== OAT experiment [{exp_idx + 1}/{len(self._experiments)}] {label} (SF={inputs.scale_factor}) ===")

        ok, pct_free = oat_runner.disk_check_ok(repo, min_free_pct)
        if not ok:
            self.emit(f"  [oat-disk-guard] only {pct_free:.1f}% free (need ≥{min_free_pct:.1f}%) — SKIPPING")
            exp_dict = oat_runner.build_per_experiment_dict(
                exp_idx=exp_idx, inputs=inputs, result=None,
                status="skipped", started_at=started_at,
                ended_at=oat_runner.iso_now(), wall_clock_s=0.0,
                disk_free_pct=pct_free, error=None,
                skip_reason=f"disk_free {pct_free:.1f}% < {min_free_pct:.1f}%",
                repo_dir=repo, benchmark_id=None,
            )
            self._oat_per_exp_dicts.append(exp_dict)
            oat_runner.write_per_experiment_outputs(repo, self._oat_run_id, exp_idx, exp_dict)
            self._persist_oat_experiment_row(exp_idx, inputs, exp_dict)
            return

        status = "completed"
        error: Optional[str] = None
        ran_cleanup = False
        try:
            self._apply_experiment(inputs)
            self._init_benchmark_run_record_from_config()
            self._result = BenchmarkResult(status="running")

            # Per-experiment teardown + dir pre-create. We do NOT call
            # _clean_mount here — phase 0 already cleaned, and the per-experiment
            # disk_cleanup_after_experiment surgically wipes per-SF state at
            # the END of the iteration.
            self._teardown_existing()
            self._pre_create_dirs()

            # Datagen for THIS SF. Idempotent — but raw/<sf>/ is usually
            # empty because the previous experiment cleanup wiped it.
            self._run_datagen()

            # Re-check disk AFTER datagen. SF=1000 may blow the threshold
            # during datagen alone; bailing here is much cheaper than
            # running the full engine bench and THEN cleaning up.
            ok_post, post_dg_pct = oat_runner.disk_check_ok(repo, min_free_pct)
            if not ok_post:
                raise RuntimeError(
                    f"disk_free {post_dg_pct:.1f}% < {min_free_pct:.1f}% after datagen — "
                    f"aborting experiment to keep host alive"
                )

            self._phase2_benchmark()
            self._save_benchmark_results()

        except Exception as e:
            status = "failed"
            error = str(e)
            self.emit(f"  [oat] experiment FAILED: {e}")
            logger.exception("OAT experiment %d failed", exp_idx)
        finally:
            # ALWAYS reclaim disk space before any artifact write. The
            # cleanup wipes mount/raw/<sf>/ + mount/results/<sf>/<engine>/
            # (preserves dbt-server/, stats/, bin/), so an ENOSPC mid-run
            # cannot prevent the next experiment from starting.
            try:
                oat_runner.disk_cleanup_after_experiment(
                    repo_dir=repo, scale_factor=inputs.scale_factor,
                    engines=inputs.engines, emit=self.emit,
                )
                ran_cleanup = True
            except Exception as ce:
                self.emit(f"  [oat-cleanup] WARN: {ce}")
                logger.exception("OAT cleanup failed for exp %d", exp_idx)

        ended_at = oat_runner.iso_now()
        wall_s = time.time() - exp_t0
        post_pct_free = oat_runner.disk_check_pct_free(repo)

        # Build + persist per-experiment artifacts. AFTER cleanup so tiny
        # outputs.json/db writes don't ENOSPC themselves.
        exp_dict = oat_runner.build_per_experiment_dict(
            exp_idx=exp_idx, inputs=inputs, result=self._result,
            status=status, started_at=started_at, ended_at=ended_at,
            wall_clock_s=wall_s, disk_free_pct=post_pct_free,
            error=error, skip_reason=None,
            repo_dir=repo, benchmark_id=self._benchmark_id,
        )
        self._oat_per_exp_dicts.append(exp_dict)
        try:
            oat_runner.write_per_experiment_outputs(repo, self._oat_run_id, exp_idx, exp_dict)
        except Exception as we:
            self.emit(f"  [oat-output] WARN: per-experiment write failed: {we}")
            logger.exception("OAT per-experiment write failed for exp %d", exp_idx)
        self._persist_oat_experiment_row(exp_idx, inputs, exp_dict)
        if self._benchmark_id:
            self._update_benchmark_run(status, wall_s, error)

        self.emit(
            f"=== OAT experiment [{exp_idx + 1}/{len(self._experiments)}] done — "
            f"status={status} wall={wall_s:.1f}s disk_free={post_pct_free:.1f}% "
            f"cleanup={'ok' if ran_cleanup else 'FAILED'} ==="
        )

    def _apply_experiment(self, inputs: ExperimentInputs) -> None:
        """Mutate ``self._config`` + ``os.environ`` to match this experiment's knobs."""
        self._config.scale_factor = inputs.scale_factor
        self._config.batch_1_pct = inputs.batch_1_pct
        self._config.batch_2_pct = inputs.batch_2_pct
        self._config.batch_3_pct = inputs.batch_3_pct
        self._config.engines = list(inputs.engines)
        self._config.parallel = inputs.parallel
        oat_runner.apply_experiment_env(inputs)
        self.emit(
            f"  [oat-apply] SF={inputs.scale_factor} batches={inputs.batch_1_pct}/"
            f"{inputs.batch_2_pct}/{inputs.batch_3_pct} engines={','.join(inputs.engines)} "
            f"parallel={inputs.parallel}"
        )

    def _persist_oat_experiment_row(
        self, exp_idx: int, inputs: ExperimentInputs, exp_dict: dict,
    ) -> None:
        """UPSERT one row into ``oat_experiments``."""
        with DB_LOCK:
            conn = get_db()
            conn.execute(
                "INSERT OR REPLACE INTO oat_experiments "
                "(oat_run_id, exp_idx, benchmark_id, label, status, "
                " started_at, ended_at, wall_clock_s, disk_free_pct, "
                " inputs_json, outputs_json, error) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    self._oat_run_id,
                    exp_idx,
                    exp_dict.get("benchmark_id"),
                    exp_dict.get("label"),
                    exp_dict.get("status"),
                    exp_dict.get("started_at"),
                    exp_dict.get("ended_at"),
                    exp_dict.get("wall_clock_s"),
                    exp_dict.get("disk_free_pct"),
                    json.dumps(inputs.to_dict()),
                    json.dumps(exp_dict),
                    exp_dict.get("error"),
                ),
            )
            conn.commit()
            conn.close()

    def _finalize_oat(
        self, status: str, oat_t0: float, error: Optional[str] = None,
    ) -> None:
        """Write final outputs.json, run chart pass, copy server log, update DB."""
        repo = self._config.repo_dir
        completed_at = oat_runner.iso_now()
        total = time.time() - oat_t0

        oat_runner.write_master_outputs(
            repo_dir=repo, oat_run_id=self._oat_run_id,
            experiments_file=self._experiments_file or "<n/a>",
            status=status, started_at=self._oat_started_at,
            completed_at=completed_at, total_duration_s=total,
            per_experiment_dicts=self._oat_per_exp_dicts,
            error=error,
        )

        with DB_LOCK:
            conn = get_db()
            conn.execute(
                "UPDATE oat_runs SET status=?, completed_at=?, total_duration_s=?, error=? WHERE id=?",
                (status, completed_at, total, error, self._oat_run_id),
            )
            conn.commit()
            conn.close()

        # Chart + RESULTS.md generation (best-effort — never fail the run).
        self._phase3_oat_chart()
        self._dump_server_log_into_oat_state()

        completed_n = sum(1 for d in self._oat_per_exp_dicts if d.get("status") == "completed")
        skipped_n = sum(1 for d in self._oat_per_exp_dicts if d.get("status") == "skipped")
        failed_n = sum(1 for d in self._oat_per_exp_dicts if d.get("status") == "failed")
        self.emit("")
        self.emit(
            f"=== OAT sweep {self._oat_run_id[:8]} {status} in {total:.1f}s — "
            f"{completed_n} completed, {skipped_n} skipped, {failed_n} failed ==="
        )
        self.emit(f"  artifacts: mount/oat-state/{self._oat_run_id}/")

        # Mirror to BenchmarkResult for /benchmark/status compatibility.
        self._result.status = status
        self._result.total_duration_s = total
        if error:
            self._result.error = error

    def _phase3_oat_chart(self, *, silent: bool = False) -> None:
        """Generate OAT aggregate PNG + per-model PNG + RESULTS.md.

        Writes are atomic (``.tmp`` + ``os.replace``) so a `watch`-style
        external observer always sees a coherent file. Best-effort: failures
        never abort the sweep. Pass ``silent=True`` to suppress emit() noise
        when called per-iteration (the chart is regenerated after every
        experiment, so verbose logs would clutter the SSE stream).
        """
        repo = self._config.repo_dir
        state_dir = oat_runner.state_dir_for(repo, self._oat_run_id)
        try:
            from handlers import oat_chart  # lazy — keeps module import fast
        except Exception as e:
            self.emit(f"  [oat-chart] WARN: oat_chart handler not importable: {e}")
            return

        def _atomic_write(out_path: str, payload, mode: str) -> None:
            tmp = out_path + ".tmp"
            with open(tmp, mode) as f:
                f.write(payload)
            os.replace(tmp, out_path)

        targets = (
            ("aggregate", "chart-oat.png", oat_chart.generate_oat_aggregate_png),
            ("per-model", "chart-per-model.png", oat_chart.generate_oat_per_model_png),
        )
        for kind, fname, fn in targets:
            try:
                data = fn(self._oat_run_id, state_dir=os.path.dirname(state_dir))
                if data:
                    out = os.path.join(state_dir, fname)
                    _atomic_write(out, data, "wb")
                    if not silent:
                        self.emit(f"  [oat-chart] wrote {os.path.relpath(out, repo)}")
            except Exception as e:
                if not silent:
                    self.emit(f"  [oat-chart] WARN: {kind} render failed: {e}")
                logger.warning("OAT chart %s render failed: %s", kind, e)

        try:
            md = oat_chart.generate_oat_results_md(
                self._oat_run_id, state_dir=os.path.dirname(state_dir)
            )
            if md:
                out = os.path.join(state_dir, "RESULTS.md")
                _atomic_write(out, md, "w")
                if not silent:
                    self.emit(f"  [oat-chart] wrote {os.path.relpath(out, repo)}")
        except Exception as e:
            if not silent:
                self.emit(f"  [oat-chart] WARN: RESULTS.md render failed: {e}")
            logger.warning("OAT RESULTS.md render failed: %s", e)

    def _dump_server_log_into_oat_state(self) -> None:
        """Copy /tmp/benchmark-server.log into the OAT state dir."""
        try:
            src = "/tmp/benchmark-server.log"
            dst = os.path.join(
                oat_runner.state_dir_for(self._config.repo_dir, self._oat_run_id),
                "benchmark-server.log",
            )
            shutil.copy2(src, dst)
            logger.info("OAT server log written to %s", dst)
        except Exception as e:
            logger.warning("Failed to copy OAT server log: %s", e)


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
