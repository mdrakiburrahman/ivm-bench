"""Main benchmark orchestrator — coordinates all phases."""

import copy
import hashlib
import json
import logging
import os
import queue
import re
import shutil
import statistics
import subprocess
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import replace
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional

from models.config import BenchmarkConfig, is_cloud_engine
from models.experiments import ExperimentInputs, parse_experiments_json
from models.result import BatchResult, BenchmarkResult, EngineResult
from services import oat_runner
from services.db import DB_LOCK, get_db
from services.docker_manager import DockerManager
from services.engine_runner import EngineRunner, OpenIvmValidationError
from services.resource_calc import compute_engine_configs

logger = logging.getLogger(__name__)


def _average_compute_metrics(batch_extras: List[dict]) -> dict:
    metrics = [extra.get("compute_metrics") for extra in batch_extras]
    if not metrics or not all(isinstance(metric, dict) for metric in metrics):
        return {
            "status": "error",
            "cpu_time_s": None,
            "source": "docker_cpu_usage_total",
            "error": "compute metrics missing from one or more repetitions",
        }
    if all(metric.get("status") == "excluded" for metric in metrics):
        result = copy.deepcopy(metrics[-1])
        result["repetition_count"] = len(metrics)
        return result

    cpu_values = [metric.get("cpu_time_s") for metric in metrics]
    if not all(
        metric.get("status") == "ok" and isinstance(cpu, (int, float))
        for metric, cpu in zip(metrics, cpu_values)
    ):
        return {
            "status": "error",
            "cpu_time_s": None,
            "source": "docker_cpu_usage_total",
            "error": "compute metrics incomplete in one or more repetitions",
            "repetition_statuses": [metric.get("status") for metric in metrics],
        }

    values = [float(value) for value in cpu_values]
    result = copy.deepcopy(metrics[-1])
    result.update({
        "cpu_time_s": statistics.fmean(values),
        "cpu_time_stddev_s": statistics.pstdev(values),
        "repetition_count": len(values),
        "repetition_cpu_time_s": values,
        "repetition_artifacts": [metric.get("artifact") for metric in metrics],
        "repetition_samples_statuses": [
            metric.get("samples_status") for metric in metrics
        ],
    })
    if not all(metric.get("samples_status") == "ok" for metric in metrics):
        result["samples_status"] = "error"
        result["samples_error"] = (
            "diagnostic samples incomplete in one or more repetitions"
        )
    result.pop("start_snapshot", None)
    result.pop("end_snapshot", None)
    return result


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

        # OAT (one-at-a-time) sweep state. Populated on every run because
        # OAT is the only entrypoint.
        self._experiments_file: Optional[str] = None
        self._experiments: List[ExperimentInputs] = []
        self._oat_run_id: Optional[str] = None
        self._oat_started_at: Optional[str] = None
        self._oat_per_exp_dicts: List[dict] = []
        # Set by _run_one_oat_experiment when it catches OpenIvmValidationError.
        # _run_oat checks this after every experiment and aborts the sweep
        # immediately — a single MV-correctness diff is unrecoverable.
        self._oat_fatal_validation_error: Optional[str] = None

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

    def start(self, experiments_file: str) -> None:
        """Start an OAT sweep in a background thread.

        ``experiments_file`` is REQUIRED — must be the absolute in-container
        path of an experiments JSON. A 1-row JSON is a minimal "single
        experiment"; an N-row JSON is a sweep.
        """
        with self._lock:
            if self._running:
                raise RuntimeError("Benchmark already running")
            if not experiments_file:
                raise ValueError("experiments_file is required (OAT-only mode)")
            self._running = True
            self._result = BenchmarkResult(
                status="running",
                repetition_count=self._config.benchmark_runs,
            )
            self._experiments_file = experiments_file
            self._experiments = []
            self._oat_run_id = None
            self._oat_per_exp_dicts = []
            self._oat_fatal_validation_error = None
            self._benchmark_id = None

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

            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()

    def _init_benchmark_run_record_from_config(self) -> None:
        """Create a fresh benchmark_runs row + engine_batches rows from ``self._config``.

        Sets ``self._benchmark_id`` to the new UUID. Called once per OAT
        iteration so each experiment owns a distinct benchmark_runs row.
        """
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
            "batch_2_update_pct": self._config.batch_2_update_pct,
            "batch_2_delete_pct": self._config.batch_2_delete_pct,
            "batch_3_update_pct": self._config.batch_3_update_pct,
            "batch_3_delete_pct": self._config.batch_3_delete_pct,
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
        """Entry point for the OAT sweep thread."""
        try:
            self._run_oat()
        finally:
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
            ("docker/docker-compose.spark-openivm-build.yml", "spark-openivm-build"),
        ]
        # Engine-specific project names
        for engine in ["spark", "duckdb", "duckdb-openivm", "feldera", "spark-openivm", "databricks-enzyme", "fabric-openivm-jvm-35", "fabric-jvm-35"]:
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
            if engine in ("spark", "spark-openivm"):
                dirs.append(f"mount/metrics/{sf}/{engine}/spark-events")
        dirs.extend([
            f"mount/results/{sf}/dbt-server",
            f"mount/bin/duckdb-openivm",
            f"mount/bin/spark-openivm",
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

    def _phase1_prep(
        self, include_datagen: bool = True, compiler_bench: Optional[bool] = None
    ) -> None:
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
        # At phase 0 the per-experiment env is not applied yet, so the caller
        # passes the decision in from the experiments list; None means "read env".
        if compiler_bench is None:
            compiler_bench = self._compiler_bench_enabled()
        # compiler-bench needs the duckdb-openivm image whatever the engine list:
        # it ships the query corpus, and its CLI drives corpus translation.
        if "duckdb-openivm" in self._config.engines or compiler_bench:
            callables.append(self._run_duckdb_openivm_build)
        if "spark-openivm" in self._config.engines or "fabric-openivm-jvm-35" in self._config.engines:
            callables.append(self._run_spark_openivm_build)
        if compiler_bench:
            callables.append(self._run_lpts_build)

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
        # 2h timeout — SF=400 tpc-di-gen alone takes >660s, SF=1000 estimated
        # ~2500s for tpc-di-gen + ~600s for spark-digen-delta. DockerManager
        # default 600s + 60s buffer SIGKILLs the compose subprocess mid-run
        # at SF≥400. Bump to 7200s to safely cover SF up to ~1500.
        with self._heartbeat("datagen/run"):
            mgr.up(
                services=["spark-digen-delta"],
                detach=False,
                timeout=7200,
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
        dockerfile = os.path.join(repo, "src/containers/duckdb-openivm/Dockerfile")
        # The OpenIVM binary is expensive to rebuild. Key the reuse on a hash of
        # the WHOLE Dockerfile (baked into the builder image as a label), not just
        # the OPENIVM_COMMIT — so a base-image / glibc change forces a rebuild too.
        with open(dockerfile, "r", encoding="utf-8") as f:
            dockerfile_hash = hashlib.sha256(f.read().encode("utf-8")).hexdigest()
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.duckdb-openivm-build.yml"),
            project_name="duckdb-openivm-build",
            env={"DUCKDB_OPENIVM_DOCKERFILE_HASH": dockerfile_hash},
            cwd=repo,
        )
        image_hash = subprocess.run(
            [
                "docker", "image", "inspect",
                "duckdb-openivm-build-duckdb-openivm-builder:latest",
                "--format", "{{ index .Config.Labels \"org.openivm.duckdb.dockerfile_hash\" }}",
            ],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        if image_hash.returncode != 0 or image_hash.stdout.strip() != dockerfile_hash:
            self.emit(
                "  [duckdb-openivm-build] Cold cache: compiling DuckDB+OpenIVM "
                "from source (~5-10 min)"
            )
            with self._heartbeat("duckdb-openivm-build/build"):
                mgr.build()
        else:
            self.emit(f"  [duckdb-openivm-build] Reusing image (dockerfile {dockerfile_hash[:7]})")
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

    def _compiler_bench_enabled(self) -> bool:
        return os.environ.get("COMPILER_BENCH", "0").strip().lower() in (
            "1", "true", "yes", "on",
        )

    def _run_lpts_build(self) -> None:
        """Build the standalone LPTS extension used for corpus translation."""
        self.emit("  [lpts-build] Building")
        repo = self._config.repo_dir
        dockerfile = os.path.join(repo, "src/containers/lpts-build/Dockerfile")
        with open(dockerfile, "r", encoding="utf-8") as f:
            dockerfile_hash = hashlib.sha256(f.read().encode("utf-8")).hexdigest()
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.lpts-build.yml"),
            project_name="lpts-build",
            env={"LPTS_BUILD_DOCKERFILE_HASH": dockerfile_hash},
            cwd=repo,
        )
        image_hash = subprocess.run(
            [
                "docker", "image", "inspect", "lpts-build-lpts-builder:latest",
                "--format", "{{ index .Config.Labels \"org.lpts.dockerfile_hash\" }}",
            ],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        if image_hash.returncode != 0 or image_hash.stdout.strip() != dockerfile_hash:
            self.emit("  [lpts-build] Cold cache: compiling DuckDB+LPTS from source (~10 min)")
            with self._heartbeat("lpts-build/build"):
                mgr.build()
        else:
            self.emit(f"  [lpts-build] Reusing image (dockerfile {dockerfile_hash[:7]})")
        with self._heartbeat("lpts-build/run"):
            mgr.up(
                services=["lpts-builder"],
                detach=False,
                stream_callback=lambda line: logger.debug("[lpts-build] %s", line),
            )
        mgr.down()

        extension = os.path.join(repo, "mount", "bin", "lpts", "lpts.duckdb_extension")
        if not os.path.exists(extension):
            raise RuntimeError(f"LPTS extension not found at {extension}")
        self.emit("  [lpts-build] Complete")

    def _run_compiler_bench_prep(self, inputs: "ExperimentInputs") -> None:
        """Translate the corpus (and generate TPC-C data) before engines run."""
        from services import compiler_bench_corpus

        options = inputs.compiler_bench
        self.emit("")
        self.emit("=== compiler-bench prep ===")

        data = compiler_bench_corpus.generate_tpcc_parquet(
            repo_dir=self._config.repo_dir, scale_factor=options.scale_factor
        )
        self.emit(f"  [compiler-bench] TPC-C data: {data['status']} ({data['path']})")

        with self._heartbeat("compiler-bench/translate"):
            meta = compiler_bench_corpus.prepare(
                repo_dir=self._config.repo_dir,
                engines=self._config.engines,
                scale_factor=options.scale_factor,
                limit=options.limit,
            )
        self.emit(
            f"  [compiler-bench] corpus {'(cached)' if meta.get('cached') else 'translated'}: "
            f"{meta['query_count']} queries, {meta['common_count']} common to all dialects"
        )
        for dialect, stats in (meta.get("dialects") or {}).items():
            self.emit(
                f"    {dialect}: {stats['translated']} translated, "
                f"{stats['translation_failed']} untranslatable "
                f"({stats['pct_translated']}%)"
            )
        self.emit("=== compiler-bench prep: Complete ===")

    def _run_spark_openivm_build(self) -> None:
        """Build the spark-openivm assembly jar + duckdb extension + CLI (idempotent).

        Mirrors `_run_duckdb_openivm_build`: regex-parses the pinned
        OPENIVM_SPARK_COMMIT from the Dockerfile, reuses the existing
        builder image when its `org.openivm.spark.commit` label still
        matches, otherwise rebuilds.
        """
        self.emit("  [spark-openivm-build] Building")
        repo = self._config.repo_dir
        dockerfile = os.path.join(
            repo, "src/containers/spark-openivm-build/Dockerfile"
        )
        # Key the reuse on a hash of the WHOLE Dockerfile (baked into the builder
        # image as a label), not just OPENIVM_SPARK_COMMIT — so a base-image /
        # glibc / toolchain change forces a rebuild too. (The pinned commit lives
        # inside the Dockerfile, so the hash also covers commit bumps.)
        with open(dockerfile, "r", encoding="utf-8") as f:
            dockerfile_hash = hashlib.sha256(f.read().encode("utf-8")).hexdigest()
        mgr = DockerManager(
            os.path.join(repo, "docker/docker-compose.spark-openivm-build.yml"),
            project_name="spark-openivm-build",
            env={"SPARK_OPENIVM_DOCKERFILE_HASH": dockerfile_hash},
            cwd=repo,
        )
        image_hash = subprocess.run(
            [
                "docker", "image", "inspect",
                "spark-openivm-build-spark-openivm-builder:latest",
                "--format", "{{ index .Config.Labels \"org.openivm.spark.dockerfile_hash\" }}",
            ],
            capture_output=True,
            text=True,
            cwd=repo,
        )
        if image_hash.returncode != 0 or image_hash.stdout.strip() != dockerfile_hash:
            self.emit(
                "  [spark-openivm-build] Cold cache: compiling DuckDB+OpenIVM "
                "and sbt-assembling spark extension from source (~10 min)"
            )
            with self._heartbeat("spark-openivm-build/build"):
                mgr.build()
        else:
            self.emit(
                f"  [spark-openivm-build] Reusing image (dockerfile {dockerfile_hash[:7]})"
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
            self._archive_repetition_outputs(run_idx, repetition_engines)
            self._save_repetition_result(run_idx, repetition_engines)

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
                batch_extra = copy.deepcopy(engine_runs[-1].batches[idx].extra)
                batch_extra["compute_metrics"] = _average_compute_metrics(
                    [er.batches[idx].extra for er in engine_runs]
                )
                batches.append(BatchResult(
                    batch_num=batch_num,
                    duration_s=sum(durations) / len(durations),
                    status="completed" if all(s == "completed" for s in statuses) else "failed",
                    error="; ".join(errors) if errors else None,
                    extra=batch_extra,
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

    def _archive_repetition_outputs(
        self, run_idx: int, engines: Dict[str, EngineResult]
    ) -> None:
        """Archive one repetition's result files and diagnostic CPU samples."""
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

        for engine, engine_result in engines.items():
            stats_source = os.path.join(
                self._config.repo_dir, "mount", "stats", sf, engine,
                "container_stats.jsonl",
            )
            if not os.path.isfile(stats_source):
                continue
            stats_target = os.path.join(
                archive_dir, f"container-stats-{engine}.jsonl"
            )
            shutil.copy2(stats_source, stats_target)
            relative_target = os.path.relpath(stats_target, self._config.repo_dir)
            for batch in engine_result.batches:
                metrics = (batch.extra or {}).get("compute_metrics")
                if isinstance(metrics, dict) and metrics.get("artifact"):
                    metrics["artifact"] = relative_target

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
        if self._config.schedule == "serial-host-parallel-cloud":
            self._run_serial_host_parallel_cloud()
        else:
            if self._config.parallel:
                self._init_parallel_staging(engine_configs)

            if self._config.parallel and len(engines) > 1:
                self.emit(f"  Running {len(engines)} engines in PARALLEL")
                self._run_engines_parallel(engine_configs)
            else:
                self.emit(f"  Running {len(engines)} engines SERIALLY")
                self._run_engines_serial(engine_configs)

        self.emit("=== Phase 2: Complete ===")

    def _run_serial_host_parallel_cloud(self) -> None:
        """Run host-consuming engines serially (each with full host resources),
        then all cloud-consuming engines in ONE parallel wave.

        Cloud engines offload their compute to a remote service, so their host
        footprint is just the light dbt-server (+ imds-router) orchestration —
        running them concurrently barely touches the host. Host engines keep the
        serial full-host allocation. This reuses the proven serial and
        parallel-wave machinery by swapping ``self._config`` per phase (each
        phase needs its own resource split + port/staging assignment)."""
        host = [e for e in self._config.engines if not is_cloud_engine(e)]
        cloud = [e for e in self._config.engines if is_cloud_engine(e)]
        self.emit(
            f"  serial-host-parallel-cloud: host(serial)=[{', '.join(host) or '-'}] "
            f"cloud(parallel)=[{', '.join(cloud) or '-'}]"
        )
        orig = self._config
        try:
            if host:
                self._config = replace(orig, engines=host, parallel=False, schedule="serial")
                host_configs = compute_engine_configs(self._config)
                self.emit(f"  Running {len(host)} host engine(s) SERIALLY")
                self._run_engines_serial(host_configs)
            if cloud:
                self._config = replace(orig, engines=cloud, parallel=True, schedule="parallel")
                cloud_configs = compute_engine_configs(self._config)
                self._init_parallel_staging(cloud_configs)
                self.emit(f"  Running {len(cloud)} cloud engine(s) in PARALLEL")
                self._run_engine_wave(cloud, cloud_configs)
                failed = [
                    n for n in cloud
                    if n in self._result.engines and self._result.engines[n].status == "failed"
                ]
                if failed:
                    raise RuntimeError(f"Cloud engines failed: {', '.join(failed)}")
        finally:
            self._config = orig

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
            try:
                result = runner.run()
            except OpenIvmValidationError as e:
                # The runner has already cleaned up (compose-down + log capture
                # in its finally). Preserve its partial result (batch
                # statuses, durations, error) — don't build a stripped
                # replacement — then re-raise so the OAT loop fails fast.
                er = runner.result
                er.status = "failed"
                er.error = str(e)
                self._result.engines[name] = er
                raise
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
        """Run a wave of engines in parallel.

        Note: parallel mode is NOT truly fail-fast on OpenIvmValidationError —
        we drain the remaining futures so no compose stacks dangle, then
        propagate. PARALLEL=0 (serial) IS truly fail-fast.
        """
        if not engines:
            return
        futures = {}
        fatal_validation_exc: Optional[OpenIvmValidationError] = None
        storage_barrier = None
        if os.environ.get("STORAGE_METRICS", "1") != "0" and len(engines) > 1:
            storage_barrier = threading.Barrier(len(engines))
        with ThreadPoolExecutor(max_workers=len(engines)) as pool:
            runners: Dict[str, EngineRunner] = {}
            for name in engines:
                ec = engine_configs[name]
                runner = EngineRunner(
                    self._config,
                    ec,
                    self.emit,
                    self._benchmark_id,
                    storage_barrier=storage_barrier,
                )
                runners[name] = runner
                futures[pool.submit(runner.run)] = (name, runner)

            for future in as_completed(futures):
                name, runner = futures[future]
                try:
                    result = future.result()
                    self._result.engines[name] = result
                except OpenIvmValidationError as e:
                    # Preserve the runner's partial result (batches/durations)
                    # for richer artifacts; remember the first fatal exc.
                    er = runner.result
                    er.status = "failed"
                    er.error = str(e)
                    self._result.engines[name] = er
                    self.emit(f"[{name}] FATAL OpenIVM validation failure: {e}")
                    if fatal_validation_exc is None:
                        fatal_validation_exc = e
                except Exception as e:
                    er = runner.result
                    er.status = "failed"
                    er.error = str(e)
                    self._result.engines[name] = er
                    self.emit(f"[{name}] FAILED: {e}")

        if fatal_validation_exc is not None:
            raise fatal_validation_exc

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
            phase 3  OAT chart + results.csv generation; copy server log.
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
                # Regenerate the OAT charts + results.csv after every
                # experiment so an external observer (`watch eog ...`, a
                # file-watcher dashboard, `cat .../results.csv`) can see the
                # sweep evolve in real time. Atomic writes inside
                # _phase3_oat_chart guarantee readers never see a half-
                # written PNG/CSV. ``silent=True`` keeps the SSE stream
                # quiet — the chart pass logs to /tmp/benchmark-server.log
                # at WARN level on failure regardless.
                self._phase3_oat_chart(silent=True)

                # FAIL-FAST: an OpenIVM validation diff is unrecoverable
                # (the MV definition or refresh path is broken). Abort the
                # sweep immediately, marking the remaining experiments as
                # skipped so results.csv / chart-oat.png still render cleanly.
                if self._oat_fatal_validation_error is not None:
                    remaining = len(self._experiments) - (idx + 1)
                    self.emit("")
                    self.emit("=" * 78)
                    self.emit("=== OAT SWEEP ABORTED — OpenIVM validation failure is FATAL ===")
                    self.emit(f"  reason: {self._oat_fatal_validation_error}")
                    self.emit(f"  experiment [{idx + 1}/{len(self._experiments)}] failed validation")
                    self.emit(f"  skipping remaining {remaining} experiment(s)")
                    self.emit("=" * 78)
                    self._record_skipped_remaining(
                        from_idx=idx + 1,
                        reason=f"fail-fast: experiment {idx} had OpenIVM validation failure",
                    )
                    # Refresh master outputs + per-exp chart so the on-disk
                    # state reflects the skips immediately — if the process
                    # dies between here and _finalize_oat, artifacts are
                    # still consistent.
                    oat_runner.write_master_outputs(
                        repo_dir=repo, oat_run_id=self._oat_run_id,
                        experiments_file=self._experiments_file or "<n/a>",
                        status="failed", started_at=self._oat_started_at,
                        completed_at=None,
                        total_duration_s=time.time() - oat_t0,
                        per_experiment_dicts=self._oat_per_exp_dicts,
                        error=f"fatal OpenIVM validation failure: {self._oat_fatal_validation_error}",
                    )
                    self._phase3_oat_chart(silent=True)
                    break

            # Propagate per-experiment failures into the final sweep status.
            # Skipped experiments (disk-guard OR fail-fast) don't downgrade — they
            # are documented graceful outcomes — but a real engine/validation
            # failure does.
            failed_count = sum(
                1 for d in self._oat_per_exp_dicts if d.get("status") == "failed"
            )
            final_status = "failed" if failed_count > 0 else "completed"
            if self._oat_fatal_validation_error is not None:
                final_error = (
                    f"fatal OpenIVM validation failure: {self._oat_fatal_validation_error}"
                )
            elif failed_count:
                final_error = (
                    f"{failed_count}/{len(self._experiments)} experiment(s) failed"
                )
            else:
                final_error = None
            self._finalize_oat(final_status, oat_t0, error=final_error)
        except Exception as e:
            self.emit(f"OAT sweep FAILED: {e}")
            logger.exception("OAT sweep failed")
            self._finalize_oat("failed", oat_t0, error=str(e))

    def _record_skipped_remaining(self, from_idx: int, reason: str) -> None:
        """Write skipped per-experiment dicts for every experiment in [from_idx, N).

        Used by fail-fast: an OpenIVM validation diff aborts the sweep, but
        we still want results.csv and chart-oat.png to render with a complete
        row count so the observer can see exactly which experiments never ran.
        """
        repo = self._config.repo_dir
        now = oat_runner.iso_now()
        disk_pct = oat_runner.disk_check_pct_free(repo)
        for skip_idx in range(from_idx, len(self._experiments)):
            skip_inp = self._experiments[skip_idx]
            skip_dict = oat_runner.build_per_experiment_dict(
                exp_idx=skip_idx, inputs=skip_inp, result=None,
                status="skipped", started_at=now, ended_at=now,
                wall_clock_s=0.0, disk_free_pct=disk_pct,
                error=None, skip_reason=reason,
                repo_dir=repo, benchmark_id=None,
            )
            self._oat_per_exp_dicts.append(skip_dict)
            try:
                oat_runner.write_per_experiment_outputs(
                    repo, self._oat_run_id, skip_idx, skip_dict
                )
            except Exception as we:
                self.emit(f"  [oat-output] WARN: skipped-write failed for exp {skip_idx}: {we}")
                logger.exception("OAT skipped write failed for exp %d", skip_idx)
            self._persist_oat_experiment_row(skip_idx, skip_inp, skip_dict)
            self.emit(
                f"  [oat-skip] experiment [{skip_idx + 1}/{len(self._experiments)}] "
                f"{skip_inp.label or f'exp-{skip_idx}'} → SKIPPED ({reason})"
            )

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
            # The compiler-bench decision comes from the experiments themselves:
            # per-experiment env is applied later, in the loop.
            self._phase1_prep(
                include_datagen=False,
                compiler_bench=any(
                    inp.feature_flags.compiler_bench for inp in experiments
                ),
            )
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
        # Initialise the per-experiment result holder BEFORE any operation
        # that can raise — otherwise a failure in `_apply_experiment` /
        # `_init_benchmark_run_record_from_config` would leave the previous
        # experiment's `self._result` in place and the post-finally save
        # below would write stale data.
        self._result = BenchmarkResult(status="running")
        try:
            self._apply_experiment(inputs)
            self._init_benchmark_run_record_from_config()

            # Per-experiment teardown + dir pre-create. We do NOT call
            # _clean_mount here — phase 0 already cleaned, and the per-experiment
            # disk_cleanup_after_experiment surgically wipes per-SF state at
            # the END of the iteration.
            self._teardown_existing()
            self._pre_create_dirs()

            # Datagen for THIS SF. Idempotent — but raw/<sf>/ is usually
            # empty because the previous experiment cleanup wiped it.
            # compiler-bench runs on its own generated TPC-C data and never reads
            # the TPC-DI Delta sources, so generating them would be pure cost.
            if self._compiler_bench_enabled():
                self.emit("  [datagen] Skipped: compiler-bench uses its own TPC-C data")
            else:
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

            # Translate the corpus before any engine starts, so a translation
            # failure surfaces once here rather than N times mid-run.
            if self._compiler_bench_enabled():
                self._run_compiler_bench_prep(inputs)

            # Repeat the full engine benchmark BENCHMARK_RUNS times and average
            # the per-engine/per-batch timings. With BENCHMARK_RUNS=1 this is a
            # single _phase2_benchmark() call — identical to the original path.
            self._run_benchmark_repetitions()

        except OpenIvmValidationError as e:
            # FAIL-FAST: a correctness diff means the MV definition or refresh
            # path is broken. Remaining experiments would just reproduce the
            # same bug at higher cost, so we abort the OAT sweep immediately
            # after marking this experiment failed. _run_oat reads this state.
            status = "failed"
            error = str(e)
            self._oat_fatal_validation_error = str(e)
            self.emit(f"  [oat] experiment FATALLY FAILED on OpenIVM validation: {e}")
            self.emit(f"  [oat] FAIL-FAST — aborting OAT sweep after this experiment")
            logger.exception("OAT experiment %d had fatal OpenIVM validation failure", exp_idx)
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
            #
            # On FAILURE, archive container logs + dbt-server JSON into
            # mount/oat-state/<oat_run_id>/exp-<idx>/forensics/ BEFORE the
            # wipe — that path survives all cleanup and gives us the Spark
            # driver/executor logs needed to root-cause the failure.
            archive_dir: Optional[str] = None
            if status == "failed" and self._oat_run_id is not None:
                archive_dir = os.path.join(
                    repo, "mount", "oat-state", self._oat_run_id,
                    f"exp-{exp_idx:03d}", "forensics",
                )
            self._emit_spark_metrics(exp_idx, inputs)
            try:
                oat_runner.disk_cleanup_after_experiment(
                    repo_dir=repo, scale_factor=inputs.scale_factor,
                    engines=inputs.engines, emit=self.emit,
                    failure_archive_dir=archive_dir,
                )
                ran_cleanup = True
            except Exception as ce:
                self.emit(f"  [oat-cleanup] WARN: {ce}")
                logger.exception("OAT cleanup failed for exp %d", exp_idx)

        ended_at = oat_runner.iso_now()
        wall_s = time.time() - exp_t0
        post_pct_free = oat_runner.disk_check_pct_free(repo)

        # Reflect per-experiment outcome on self._result BEFORE writing the
        # mount/results/<SF>/dbt-server/benchmark-results.json artifact.
        # Without this, the file would carry the "running"/0.0 placeholder
        # values from the BenchmarkResult init at the top of this method,
        # which breaks GCI PR comments + any other downstream that reads
        # the top-level status/total_duration_s. We save AFTER the cleanup
        # finally so a small-artifact write can't ENOSPC itself, mirroring
        # the per-experiment outputs.json placement just below. Use the same
        # wall_s as the OAT per-experiment artifacts + SQLite for a single
        # consistent duration value across all sinks.
        self._result.status = status
        self._result.total_duration_s = wall_s
        if error:
            self._result.error = error
        if self._oat_run_id is not None:
            try:
                oat_runner.archive_storage_artifacts(
                    repo_dir=repo,
                    oat_run_id=self._oat_run_id,
                    exp_idx=exp_idx,
                    result=self._result,
                )
            except Exception as archive_error:
                self.emit(f"  [oat-storage] WARN: archive failed: {archive_error}")
                logger.exception("OAT storage archive failed for exp %d", exp_idx)
            try:
                oat_runner.archive_compute_artifacts(
                    repo_dir=repo,
                    oat_run_id=self._oat_run_id,
                    exp_idx=exp_idx,
                    result=self._result,
                )
            except Exception as archive_error:
                self.emit(f"  [oat-compute] WARN: archive failed: {archive_error}")
                logger.exception("OAT compute archive failed for exp %d", exp_idx)
        # _save_benchmark_results() catches+logs internally; no extra wrap.
        self._save_benchmark_results()

        # Per-experiment scale-factor + heuristics PNGs under imgs/. Mirrors
        # the pre-OAT single-experiment behavior so each OAT iteration leaves
        # behind a standalone visualization keyed by (SF, b1, b2, b3). Safe
        # to call here: disk_cleanup_after_experiment preserves the
        # mount/results/<sf>/dbt-server/ and mount/stats/<sf>/ inputs the
        # chart code reads. Best-effort — never aborts the sweep.
        self._oat_write_per_experiment_imgs(inputs)

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
        self._config.batch_2_update_pct = inputs.batch_2_update_pct
        self._config.batch_2_delete_pct = inputs.batch_2_delete_pct
        self._config.batch_3_update_pct = inputs.batch_3_update_pct
        self._config.batch_3_delete_pct = inputs.batch_3_delete_pct
        self._config.engines = list(inputs.engines)
        self._config.parallel = inputs.parallel
        self._config.schedule = inputs.schedule
        oat_runner.apply_experiment_env(inputs)

        experiment_id = str(int(time.time() * 1_000_000))
        os.environ["DATABRICKS_EXPERIMENT_ID"] = experiment_id
        self._databricks_experiment_id = experiment_id
        # Fabric per-run id (mirrors the Databricks pattern): the compute
        # lakehouse + environment are created as <base>_<FABRIC_RUN_ID> and torn
        # down / swept by it. The short random suffix guards against multi-runner
        # same-microsecond collisions on the shared workspace.
        os.environ["FABRIC_RUN_ID"] = f"{experiment_id}_{uuid.uuid4().hex[:4]}"

        dml_extras = []
        for ix, (u, d) in enumerate(((inputs.batch_2_update_pct, inputs.batch_2_delete_pct),
                                     (inputs.batch_3_update_pct, inputs.batch_3_delete_pct)), start=2):
            if str(u) != "0" or str(d) != "0":
                dml_extras.append(f"b{ix}u={u}/b{ix}d={d}")
        dml_str = f" dml={','.join(dml_extras)}" if dml_extras else ""
        self.emit(
            f"  [oat-apply] SF={inputs.scale_factor} batches={inputs.batch_1_pct}/"
            f"{inputs.batch_2_pct}/{inputs.batch_3_pct}{dml_str} "
            f"engines={','.join(inputs.engines)} parallel={inputs.parallel} "
            f"databricks_exp_id={experiment_id}"
        )

    def _persist_oat_experiment_row(
        self, exp_idx: int, inputs: ExperimentInputs, exp_dict: dict,
    ) -> None:
        """UPSERT one row into ``oat_experiments`` (best-effort).

        A SQLite failure here (e.g. the bind-mounted state dir vanished
        mid-sweep) must never abort the sweep — the filesystem outputs.json /
        results.csv are the real deliverables. Degrade to a WARN.
        """
        try:
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
        except Exception as e:
            self.emit(
                f"  [oat-db] WARN: failed to persist oat_experiments row "
                f"exp={exp_idx}: {e}"
            )
            logger.warning(
                "Failed to persist oat_experiments row for exp %d: %s", exp_idx, e
            )

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

        # Best-effort: a SQLite failure (e.g. vanished state dir) must not
        # block the chart / results.csv / server-log artifacts written below.
        try:
            with DB_LOCK:
                conn = get_db()
                conn.execute(
                    "UPDATE oat_runs SET status=?, completed_at=?, total_duration_s=?, error=? WHERE id=?",
                    (status, completed_at, total, error, self._oat_run_id),
                )
                conn.commit()
                conn.close()
        except Exception as e:
            self.emit(f"  [oat-db] WARN: failed to update oat_runs row: {e}")
            logger.warning("Failed to update oat_runs row %s: %s", self._oat_run_id, e)

        # Chart + results.csv generation (best-effort — never fail the run).
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
        """Generate OAT aggregate PNG + per-model PNG + results.csv.

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
            with open(os.path.join(state_dir, "outputs.json"), encoding="utf-8") as f:
                state = json.load(f)
            csv_text = oat_runner.generate_results_csv(state)
            if csv_text:
                out = os.path.join(state_dir, "results.csv")
                _atomic_write(out, csv_text, "w")
                if not silent:
                    self.emit(f"  [oat-chart] wrote {os.path.relpath(out, repo)}")
        except Exception as e:
            if not silent:
                self.emit(f"  [oat-chart] WARN: results.csv render failed: {e}")
            logger.warning("OAT results.csv render failed: %s", e)

    def _emit_spark_metrics(self, exp_idx: int, inputs: ExperimentInputs) -> None:
        """Parse Spark event logs → A/B Parquet + PNG + RESULTS.md + zip (issue #36).

        Runs at the end of each experiment, BEFORE disk_cleanup wipes the raw
        spark-events. Persists the bundle into mount/oat-state/<run>/exp-NNN/
        metrics/ so it survives cleanup and is visible under oat-state/latest.
        Best-effort — never aborts the sweep.
        """
        spark_engines = [e for e in inputs.engines if e in ("spark", "spark-openivm")]
        if not spark_engines:
            return
        if not inputs.feature_flags.spark_metrics_capture:
            self.emit("  [spark-metrics] capture disabled for this experiment — skipping")
            return

        repo = self._config.repo_dir
        sf = str(inputs.scale_factor)
        run_id = self._benchmark_id or f"exp-{exp_idx:03d}"
        try:
            from services import spark_metrics

            summary = spark_metrics.process(
                repo_dir=repo, sf=sf, benchmark_id=self._benchmark_id or "",
                run_id=run_id, engines=spark_engines, emit=self.emit,
            )
        except Exception as e:
            self.emit(f"  [spark-metrics] post-processing failed: {e}")
            logger.exception("spark-metrics post-processing failed for exp %d", exp_idx)
            return

        if summary.get("status") != "ok":
            return

        if self._oat_run_id is None:
            return
        try:
            processed_dir = summary.get("processed_dir")
            dest = os.path.join(
                repo, "mount", "oat-state", self._oat_run_id,
                f"exp-{exp_idx:03d}", "metrics",
            )
            if processed_dir and os.path.isdir(processed_dir):
                dest_processed = os.path.join(dest, "processed")
                if os.path.isdir(dest_processed):
                    shutil.rmtree(dest_processed)
                shutil.copytree(processed_dir, dest_processed)
            zip_path = summary.get("zip")
            if zip_path and os.path.exists(zip_path):
                os.makedirs(dest, exist_ok=True)
                shutil.copy2(zip_path, os.path.join(dest, os.path.basename(zip_path)))
            self.emit(
                f"  [spark-metrics] bundle persisted to oat-state/exp-{exp_idx:03d}/metrics/ "
                f"(engines={summary.get('engines')}, both={summary.get('both_engines')})"
            )
        except Exception as e:
            self.emit(f"  [spark-metrics] oat-state copy failed: {e}")
            logger.exception("spark-metrics oat-state copy failed for exp %d", exp_idx)

    def _oat_write_per_experiment_imgs(self, inputs: ExperimentInputs) -> None:
        """Write imgs/scale-factor-<sf>-<b1>-<b2>-<b3>.png + imgs/benchmark-heuristics.png.

        Mirrors the pre-OAT single-experiment chart generation that used to
        live in the orchestrator. Best-effort: any failure here is logged
        and swallowed so it cannot abort the OAT sweep.
        """
        repo = self._config.repo_dir
        sf = str(inputs.scale_factor)
        b1, b2, b3 = inputs.batch_1_pct, inputs.batch_2_pct, inputs.batch_3_pct
        results_dir = os.path.join(repo, "mount", "results", sf, "dbt-server")
        stats_dir = os.path.join(repo, "mount", "stats", sf)
        imgs_dir = os.path.join(repo, "imgs")

        try:
            from handlers.chart import generate_chart_png, generate_heuristics_png
        except Exception as e:
            self.emit(f"  WARNING: chart handler not importable: {e}")
            logger.warning("chart handler import failed: %s", e)
            return

        try:
            png = generate_chart_png(
                state_dir=results_dir, sf=sf,
                b1pct=b1, b2pct=b2, b3pct=b3,
                stats_dir=stats_dir,
            )
            if png:
                os.makedirs(imgs_dir, exist_ok=True)
                slug = f"{sf}-{b1.replace('.', '_')}-{b2.replace('.', '_')}-{b3.replace('.', '_')}"
                out_path = os.path.join(imgs_dir, f"scale-factor-{slug}.png")
                with open(out_path, "wb") as f:
                    f.write(png)
                self.emit(f"  Scale-factor chart saved to imgs/scale-factor-{slug}.png")
            else:
                self.emit(f"  WARNING: no data for scale-factor chart (SF={sf})")
        except Exception as e:
            self.emit(f"  WARNING: scale-factor chart generation failed: {e}")
            logger.warning("Scale-factor chart generation failed: %s", e)

        try:
            png = generate_heuristics_png(state_dir=results_dir)
            if png:
                os.makedirs(imgs_dir, exist_ok=True)
                out_path = os.path.join(imgs_dir, "benchmark-heuristics.png")
                with open(out_path, "wb") as f:
                    f.write(png)
                self.emit("  Heuristics chart saved to imgs/benchmark-heuristics.png")
        except Exception as e:
            self.emit(f"  WARNING: heuristics chart generation failed: {e}")
            logger.warning("Heuristics chart generation failed: %s", e)

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
