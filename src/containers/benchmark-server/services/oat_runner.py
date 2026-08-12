"""OAT-runner helpers — disk guard, per-experiment cleanup, output assembly.

The Orchestrator owns the high-level loop (apply knobs, run engines, save).
This module owns the OAT-specific pure-data + side-effect helpers that don't
need to live on the Orchestrator class:

  * disk_check_pct_free       — current % free on the repo filesystem
  * disk_cleanup_after_experiment — wipe per-SF + per-engine subdirs while
                                    preserving dbt-server JSON, stats, bin/, etc.
  * build_per_experiment_dict — assemble the dict the chart handler consumes
  * write_per_experiment_outputs / write_master_outputs — JSON dumps to
                                    mount/oat-state/<oat_run_id>/
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import shutil
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from models.experiments import ExperimentInputs
from models.result import BenchmarkResult

logger = logging.getLogger(__name__)

_BATCH_NUMBERS = (1, 2, 3)
_BASE_TABLE_PAIRS = {
    "spark-openivm": "spark",
    "duckdb-openivm": "duckdb",
    "fabric-openivm-jvm-35": "fabric-jvm-35",
}
_RAW_REFERENCE_BASELINES = {"databricks-enzyme"}

RESULTS_CSV_FIELDS = (
    "oat_run_id",
    "run_status",
    "run_started_at",
    "run_completed_at",
    "run_total_duration_s",
    "experiments_file",
    "experiment_index",
    "experiment_label",
    "experiment_status",
    "experiment_started_at",
    "experiment_ended_at",
    "experiment_wall_clock_s",
    "experiment_disk_free_pct",
    "benchmark_id",
    "experiment_error",
    "skip_reason",
    "scale_factor",
    "batch_1_pct",
    "batch_2_pct",
    "batch_3_pct",
    "batch_2_update_pct",
    "batch_2_delete_pct",
    "batch_3_update_pct",
    "batch_3_delete_pct",
    "schedule",
    "parallel",
    "openivm_validate",
    "openivm_profile_refresh",
    "openivm_query_log",
    "storage_metrics",
    "preserve_raw",
    "spark_metrics_capture",
    "spark_driver_pct_ram",
    "spark_executor_pct_ram",
    "spark_shuffle_partitions",
    "spark_default_parallelism",
    "spark_dbt_threads",
    "engine",
    "batch_num",
    "batch_status",
    "duration_s",
    "batch_error",
    "openivm_over_spark_duration_ratio",
    "compute_status",
    "compute_cpu_time_s",
    "compute_cpu_stddev_s",
    "compute_repetition_count",
    "compute_source",
    "compute_semantics",
    "compute_error",
    "compute_artifact",
    "storage_status",
    "visible_output_bytes",
    "helper_data_bytes",
    "metadata_bytes",
    "source_bytes",
    "total_bytes",
    "helper_over_visible_ratio",
    "storage_errors",
    "storage_artifact",
    "base_table_source_mode",
    "base_table_bytes",
    "base_table_baseline_kind",
    "base_table_baseline_engine",
    "base_table_baseline_bytes",
    "base_table_storage_overhead_bytes",
    "base_table_storage_overhead_ratio",
)


# ---------------------------------------------------------------------------
# Disk-guard
# ---------------------------------------------------------------------------

def disk_check_pct_free(repo_dir: str) -> float:
    """Return % free on the filesystem holding ``repo_dir`` (0-100)."""
    usage = shutil.disk_usage(repo_dir)
    if usage.total <= 0:
        return 0.0
    return (usage.free / usage.total) * 100.0


def disk_check_ok(repo_dir: str, min_free_pct: float) -> Tuple[bool, float]:
    """(ok, pct_free) — ok if pct_free >= min_free_pct."""
    pct = disk_check_pct_free(repo_dir)
    return (pct >= min_free_pct, pct)


# ---------------------------------------------------------------------------
# Per-experiment cleanup
# ---------------------------------------------------------------------------

def disk_cleanup_after_experiment(
    repo_dir: str,
    scale_factor: int,
    engines: Iterable[str],
    emit: Callable[[str], None] = logger.info,
    failure_archive_dir: Optional[str] = None,
) -> None:
    """Aggressively wipe per-SF / per-engine state, preserving the audit trail.

    WIPED:
      * mount/raw/<sf>/
      * mount/results/<sf>/<engine>/  (Delta output tables, NOT dbt-server JSON dir)
      * mount/logs/<sf>/<engine>/

    PRESERVED:
      * mount/results/<sf>/dbt-server/{*.json,*.csv}     (per-model timings for chart)
      * mount/stats/<sf>/<engine>/container_stats.jsonl  (CPU/MEM history)
      * mount/bin/                                       (idempotent build outputs)
      * mount/benchmark-state/                           (orchestrator SQLite)
      * mount/oat-state/                                 (OAT artifacts)

    FORENSICS (when ``failure_archive_dir`` is set — i.e. the experiment failed):
      Before wiping, copy the following into ``failure_archive_dir`` so they
      survive the rmtree pass and remain available under mount/oat-state/:
        * mount/logs/<sf>/<engine>/         → <archive>/<engine>-logs/
        * mount/results/<sf>/dbt-server/    → <archive>/dbt-server-results/
      Best-effort — archive failures are logged but do not block the wipe.

    Idempotent — missing paths are silently skipped.
    """
    sf = str(scale_factor)
    mount = os.path.join(repo_dir, "mount")
    engines_list = list(engines)

    if failure_archive_dir:
        _archive_failure_forensics(
            repo_dir=repo_dir,
            mount=mount,
            sf=sf,
            engines=engines_list,
            archive_dir=failure_archive_dir,
            emit=emit,
        )

    targets: List[str] = [os.path.join(mount, "raw", sf)]
    keep_events = os.environ.get("SPARK_METRICS_KEEP_EVENTS", "0") == "1"
    for engine in engines_list:
        targets.append(os.path.join(mount, "results", sf, engine))
        targets.append(os.path.join(mount, "logs", sf, engine))
        if engine in ("spark", "spark-openivm") and not keep_events:
            targets.append(os.path.join(mount, "metrics", sf, engine, "spark-events"))

    for target in targets:
        if not os.path.exists(target):
            continue
        real_target = os.path.realpath(target)
        real_mount = os.path.realpath(mount)
        if not real_target.startswith(real_mount + os.sep):
            emit(f"  [oat-cleanup] REFUSED to delete outside-mount path: {real_target}")
            continue
        try:
            shutil.rmtree(target)
            emit(f"  [oat-cleanup] wiped {os.path.relpath(target, repo_dir)}")
        except OSError as e:
            emit(f"  [oat-cleanup] WARN failed to wipe {target}: {e}")


def _archive_failure_forensics(
    *,
    repo_dir: str,
    mount: str,
    sf: str,
    engines: List[str],
    archive_dir: str,
    emit: Callable[[str], None],
) -> None:
    """Copy per-engine logs + dbt-server JSON to ``archive_dir`` before wipe.

    Best-effort: any archive failure is logged + skipped so the wipe still
    runs and reclaims disk.
    """
    try:
        os.makedirs(archive_dir, exist_ok=True)
    except OSError as e:
        emit(f"  [oat-archive] WARN could not create {archive_dir}: {e}")
        return

    # Per-engine container logs (the main forensic payload).
    for engine in engines:
        src = os.path.join(mount, "logs", sf, engine)
        if not os.path.exists(src):
            continue
        dst = os.path.join(archive_dir, f"{engine}-logs")
        try:
            if os.path.exists(dst):
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            emit(f"  [oat-archive] saved {os.path.relpath(dst, repo_dir)}")
        except OSError as e:
            emit(f"  [oat-archive] WARN failed to archive {src} → {dst}: {e}")

    # dbt-server JSON (small but worth bundling for self-contained forensics).
    dbt_src = os.path.join(mount, "results", sf, "dbt-server")
    if os.path.exists(dbt_src):
        dbt_dst = os.path.join(archive_dir, "dbt-server-results")
        try:
            if os.path.exists(dbt_dst):
                shutil.rmtree(dbt_dst)
            shutil.copytree(dbt_src, dbt_dst)
            emit(f"  [oat-archive] saved {os.path.relpath(dbt_dst, repo_dir)}")
        except OSError as e:
            emit(f"  [oat-archive] WARN failed to archive {dbt_src} → {dbt_dst}: {e}")


def archive_storage_artifacts(
    *, repo_dir: str, oat_run_id: str, exp_idx: int, result: BenchmarkResult
) -> None:
    """Copy storage snapshots to immutable, experiment-scoped paths.

    ``mount/results/<SF>/dbt-server`` is reused by later OAT rows.  Update all
    serialized pointers while copying both the latest and repetition-specific
    snapshots into the durable OAT state tree.
    """
    destination = os.path.join(
        repo_dir, "mount", "oat-state", oat_run_id, f"exp-{exp_idx:03d}", "storage"
    )
    os.makedirs(destination, exist_ok=True)

    def archive_engines(engines, repetition: Optional[int] = None) -> None:
        for engine_result in engines.values():
            for batch in engine_result.batches:
                storage = (batch.extra or {}).get("storage")
                if not isinstance(storage, dict) or not storage.get("artifact"):
                    continue
                original = str(storage["artifact"])
                basename = os.path.basename(original)
                if basename in ("", ".", ".."):
                    raise ValueError(f"invalid storage artifact path: {original}")
                if repetition is None:
                    source = os.path.join(repo_dir, original)
                    target_dir = destination
                else:
                    original_dir = os.path.dirname(os.path.join(repo_dir, original))
                    source = os.path.join(original_dir, f"repetition-{repetition}", basename)
                    target_dir = os.path.join(destination, f"repetition-{repetition}")
                repo_real = os.path.realpath(repo_dir)
                source_real = os.path.realpath(source)
                if not source_real.startswith(repo_real + os.sep):
                    raise ValueError(f"storage artifact escapes repository: {original}")
                os.makedirs(target_dir, exist_ok=True)
                target = os.path.join(target_dir, basename)
                if not os.path.isfile(source):
                    storage["status"] = "error"
                    storage["archive_error"] = f"source artifact missing: {original}"
                    with open(target, "w") as artifact:
                        json.dump(
                            {
                                "status": "error",
                                "error": storage["archive_error"],
                            },
                            artifact,
                            indent=2,
                        )
                    storage["artifact"] = os.path.relpath(target, repo_dir)
                    continue
                shutil.copy2(source, target)
                storage["artifact"] = os.path.relpath(target, repo_dir)

    archive_engines(result.engines)
    for repetition, engines in enumerate(result.repetitions, start=1):
        archive_engines(engines, repetition=repetition)


def archive_compute_artifacts(
    *, repo_dir: str, oat_run_id: str, exp_idx: int, result: BenchmarkResult
) -> None:
    """Copy diagnostic CPU samples to immutable experiment-scoped paths."""
    destination = os.path.join(
        repo_dir, "mount", "oat-state", oat_run_id, f"exp-{exp_idx:03d}", "compute"
    )
    os.makedirs(destination, exist_ok=True)

    copied: Dict[Tuple[str, str], Optional[str]] = {}

    def copy_metric(engine: str, metric: dict, target_dir: str) -> Optional[str]:
        original = str(metric["artifact"])
        cache_key = (target_dir, original)
        if cache_key in copied:
            archived = copied[cache_key]
            if archived:
                metric["artifact"] = archived
            else:
                metric.pop("artifact", None)
            return archived
        source = os.path.join(repo_dir, original)
        repo_real = os.path.realpath(repo_dir)
        source_real = os.path.realpath(source)
        if not source_real.startswith(repo_real + os.sep):
            raise ValueError(f"compute artifact escapes repository: {original}")
        if not os.path.isfile(source):
            metric["samples_status"] = "error"
            metric["samples_error"] = f"compute artifact missing: {original}"
            metric.pop("artifact", None)
            copied[cache_key] = None
            return None
        target = os.path.join(target_dir, f"{engine}-{os.path.basename(original)}")
        try:
            os.makedirs(target_dir, exist_ok=True)
            shutil.copy2(source, target)
        except OSError as exc:
            metric["samples_status"] = "error"
            metric["samples_error"] = f"compute artifact archive failed: {exc}"
            metric.pop("artifact", None)
            copied[cache_key] = None
            return None
        archived = os.path.relpath(target, repo_dir)
        copied[cache_key] = archived
        metric["artifact"] = archived
        return archived

    repetition_paths: Dict[Tuple[int, str, str], Optional[str]] = {}
    for repetition, engines in enumerate(result.repetitions, start=1):
        target_dir = os.path.join(destination, f"repetition-{repetition}")
        for engine, engine_result in engines.items():
            for batch in engine_result.batches:
                metric = (batch.extra or {}).get("compute_metrics")
                if not isinstance(metric, dict) or not metric.get("artifact"):
                    continue
                original = str(metric["artifact"])
                repetition_paths[(repetition, engine, original)] = copy_metric(
                    engine, metric, target_dir
                )

    for engine, engine_result in result.engines.items():
        for batch in engine_result.batches:
            metric = (batch.extra or {}).get("compute_metrics")
            if not isinstance(metric, dict):
                continue
            originals = metric.get("repetition_artifacts") or []
            if result.repetitions and originals:
                archived = []
                for index, original in enumerate(originals, start=1):
                    if not original:
                        archived.append(None)
                        continue
                    archived.append(
                        repetition_paths.get((index, engine, str(original)))
                    )
                metric["repetition_artifacts"] = archived
                available = [path for path in archived if path]
                if available:
                    metric["artifact"] = available[-1]
                else:
                    metric.pop("artifact", None)
                    metric["samples_status"] = "error"
                    metric["samples_error"] = (
                        "diagnostic samples unavailable for every repetition"
                    )
            elif metric.get("artifact"):
                copy_metric(engine, metric, destination)


# ---------------------------------------------------------------------------
# Per-experiment output assembly
# ---------------------------------------------------------------------------

def build_per_experiment_dict(
    *,
    exp_idx: int,
    inputs: ExperimentInputs,
    result: Optional[BenchmarkResult],
    status: str,
    started_at: str,
    ended_at: str,
    wall_clock_s: float,
    disk_free_pct: float,
    error: Optional[str],
    skip_reason: Optional[str],
    repo_dir: str,
    benchmark_id: Optional[str],
) -> Dict[str, Any]:
    """Build the per-experiment dict the OAT chart handler consumes.

    Shape (chart handler depends on this exact schema):

    .. code-block:: json

        {
          "exp_idx": 0,
          "label":   "sf=3",
          "scale_factor": 3,
          "status":  "completed",       // or skipped / failed
          "skip_reason": null,
          "wall_clock_s": 1234.5,
          "disk_free_pct": 87.3,
          "started_at": "...",
          "ended_at":   "...",
          "benchmark_id": "<uuid>",
          "error": null,
          "engines": { ... result.engines mirror ... },
          "dbt_run_files": { "spark": [path1, path2, path3], ... },
          "inputs": { ... ExperimentInputs.to_dict() ... }
        }
    """
    engines_out: Dict[str, Any] = {}
    if result is not None:
        for name, er in result.engines.items():
            engines_out[name] = er.to_dict()

    # Per-engine dbt-server JSON file paths (existence pointers for the
    # per-model heatmap). Files may not exist for skipped/failed experiments.
    dbt_run_files: Dict[str, List[str]] = {}
    for engine in inputs.engines:
        files: List[str] = []
        for batch_num in (1, 2, 3):
            files.append(os.path.join(
                "mount", "results", str(inputs.scale_factor), "dbt-server",
                f"run-{engine}-batch{batch_num}.json",
            ))
        dbt_run_files[engine] = files

    return {
        "exp_idx": exp_idx,
        "label": inputs.label or f"exp-{exp_idx}",
        "scale_factor": inputs.scale_factor,
        "status": status,
        "skip_reason": skip_reason,
        "wall_clock_s": wall_clock_s,
        "disk_free_pct": disk_free_pct,
        "started_at": started_at,
        "ended_at": ended_at,
        "benchmark_id": benchmark_id,
        "error": error,
        "engines": engines_out,
        "dbt_run_files": dbt_run_files,
        "inputs": inputs.to_dict(),
    }


# ---------------------------------------------------------------------------
# JSON dump helpers
# ---------------------------------------------------------------------------

def state_dir_for(repo_dir: str, oat_run_id: str) -> str:
    return os.path.join(repo_dir, "mount", "oat-state", oat_run_id)


def write_per_experiment_outputs(
    repo_dir: str, oat_run_id: str, exp_idx: int, exp_dict: Dict[str, Any]
) -> str:
    exp_dir = os.path.join(state_dir_for(repo_dir, oat_run_id), f"exp-{exp_idx:03d}")
    os.makedirs(exp_dir, exist_ok=True)
    out_path = os.path.join(exp_dir, "outputs.json")
    with open(out_path, "w") as f:
        json.dump(exp_dict, f, indent=2)
    return out_path


def write_master_outputs(
    repo_dir: str,
    oat_run_id: str,
    experiments_file: str,
    status: str,
    started_at: str,
    completed_at: Optional[str],
    total_duration_s: float,
    per_experiment_dicts: List[Dict[str, Any]],
    error: Optional[str] = None,
) -> str:
    """Atomic write of the master outputs.json (chart handler reads this)."""
    state_dir = state_dir_for(repo_dir, oat_run_id)
    os.makedirs(state_dir, exist_ok=True)
    master = {
        "oat_run_id": oat_run_id,
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "total_duration_s": total_duration_s,
        "experiments_file": experiments_file,
        "error": error,
        "experiments": per_experiment_dicts,
    }
    out_path = os.path.join(state_dir, "outputs.json")
    tmp = out_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(master, f, indent=2)
    os.replace(tmp, out_path)
    return out_path


def _batch_dict(
    experiment: Dict[str, Any], engine: str, batch_num: int
) -> Dict[str, Any]:
    engine_data = (experiment.get("engines") or {}).get(engine)
    if not isinstance(engine_data, dict):
        return {}
    for batch in engine_data.get("batches") or []:
        if not isinstance(batch, dict):
            continue
        try:
            if int(batch.get("batch_num")) == batch_num:
                return batch
        except (TypeError, ValueError):
            continue
    return {}


def _storage_dict(
    experiment: Dict[str, Any], engine: str, batch_num: int
) -> Dict[str, Any]:
    batch = _batch_dict(experiment, engine, batch_num)
    extra = batch.get("extra") if isinstance(batch, dict) else None
    storage = extra.get("storage") if isinstance(extra, dict) else None
    return storage if isinstance(storage, dict) else {}


def _engine_base_bytes(storage: Dict[str, Any]) -> Optional[int]:
    base_tables = storage.get("base_tables")
    if isinstance(base_tables, dict) and base_tables.get("storage_bytes") is not None:
        try:
            return int(base_tables["storage_bytes"])
        except (TypeError, ValueError, OverflowError):
            return None
    return None


def _duration_ratio(experiment: Dict[str, Any], batch_num: int) -> Optional[float]:
    spark = _batch_dict(experiment, "spark", batch_num).get("duration_s")
    openivm = _batch_dict(experiment, "spark-openivm", batch_num).get("duration_s")
    try:
        return float(openivm) / float(spark) if float(spark) > 0 else None
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _base_table_comparison(
    experiment: Dict[str, Any], engine: str, batch_num: int, storage: Dict[str, Any]
) -> Tuple[str, str, Optional[int], Optional[int], Optional[float]]:
    actual = _engine_base_bytes(storage)
    kind = ""
    baseline: Optional[int] = None
    baseline_engine = _BASE_TABLE_PAIRS.get(engine)
    if baseline_engine:
        baseline = _engine_base_bytes(
            _storage_dict(experiment, baseline_engine, batch_num)
        )
        if baseline is not None:
            kind = "paired_engine"
        else:
            baseline_engine = None
    elif actual is not None and engine not in _RAW_REFERENCE_BASELINES:
        baseline_engine = engine
        baseline = actual
        kind = "self"
    else:
        baseline = None
        kind = ""

    if baseline_engine is None:
        base_tables = storage.get("base_tables")
        reference = (
            base_tables.get("reference_bytes")
            if isinstance(base_tables, dict)
            else None
        )
        try:
            baseline = int(reference) if reference is not None else None
        except (TypeError, ValueError, OverflowError):
            baseline = None
        if baseline is not None:
            baseline_engine = "raw-delta-reference"
            kind = "raw_delta_reference"

    overhead = (
        actual - baseline if actual is not None and baseline is not None else None
    )
    ratio = (
        overhead / baseline
        if overhead is not None and baseline and baseline > 0
        else None
    )
    return kind, baseline_engine or "", baseline, overhead, ratio


def generate_results_csv(state: Dict[str, Any]) -> str:
    """Render one raw, machine-readable row per experiment/engine/batch."""
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=RESULTS_CSV_FIELDS, lineterminator="\n")
    writer.writeheader()
    experiments = state.get("experiments") or []
    for position, experiment in enumerate(experiments):
        if not isinstance(experiment, dict):
            continue
        inputs = experiment.get("inputs") or {}
        if not isinstance(inputs, dict):
            inputs = {}
        flags = inputs.get("feature_flags") or {}
        if not isinstance(flags, dict):
            flags = {}
        tunables = inputs.get("spark_tunables") or {}
        if not isinstance(tunables, dict):
            tunables = {}
        configured_engines = inputs.get("engines") or []
        engines_data = experiment.get("engines") or {}
        if isinstance(configured_engines, str):
            configured_engines = [
                engine.strip()
                for engine in configured_engines.split(",")
                if engine.strip()
            ]
        if not isinstance(engines_data, dict):
            engines_data = {}
        engines = list(dict.fromkeys([*configured_engines, *engines_data.keys()]))
        for engine in engines:
            for batch_num in _BATCH_NUMBERS:
                batch = _batch_dict(experiment, engine, batch_num)
                batch_extra = batch.get("extra") or {}
                if not isinstance(batch_extra, dict):
                    batch_extra = {}
                compute_metrics = batch_extra.get("compute_metrics") or {}
                if not isinstance(compute_metrics, dict):
                    compute_metrics = {}
                storage = _storage_dict(experiment, engine, batch_num)
                base_tables = storage.get("base_tables") or {}
                if not isinstance(base_tables, dict):
                    base_tables = {}
                (
                    baseline_kind,
                    baseline_engine,
                    baseline_bytes,
                    overhead,
                    overhead_ratio,
                ) = _base_table_comparison(experiment, engine, batch_num, storage)
                storage_errors = storage.get("errors")
                if isinstance(storage_errors, list):
                    storage_errors = "; ".join(str(error) for error in storage_errors)
                writer.writerow(
                    {
                        "oat_run_id": state.get("oat_run_id", ""),
                        "run_status": state.get("status", ""),
                        "run_started_at": state.get("started_at", ""),
                        "run_completed_at": state.get("completed_at", ""),
                        "run_total_duration_s": state.get("total_duration_s", ""),
                        "experiments_file": state.get("experiments_file", ""),
                        "experiment_index": experiment.get("exp_idx", position),
                        "experiment_label": experiment.get("label", ""),
                        "experiment_status": experiment.get("status", ""),
                        "experiment_started_at": experiment.get("started_at", ""),
                        "experiment_ended_at": experiment.get("ended_at", ""),
                        "experiment_wall_clock_s": experiment.get("wall_clock_s", ""),
                        "experiment_disk_free_pct": experiment.get("disk_free_pct", ""),
                        "benchmark_id": experiment.get("benchmark_id", ""),
                        "experiment_error": experiment.get("error") or "",
                        "skip_reason": experiment.get("skip_reason") or "",
                        "scale_factor": inputs.get(
                            "scale_factor", experiment.get("scale_factor", "")
                        ),
                        "batch_1_pct": inputs.get("batch_1_pct", ""),
                        "batch_2_pct": inputs.get("batch_2_pct", ""),
                        "batch_3_pct": inputs.get("batch_3_pct", ""),
                        "batch_2_update_pct": inputs.get("batch_2_update_pct", ""),
                        "batch_2_delete_pct": inputs.get("batch_2_delete_pct", ""),
                        "batch_3_update_pct": inputs.get("batch_3_update_pct", ""),
                        "batch_3_delete_pct": inputs.get("batch_3_delete_pct", ""),
                        "schedule": inputs.get("schedule", ""),
                        "parallel": inputs.get("parallel", ""),
                        "openivm_validate": flags.get("openivm_validate", ""),
                        "openivm_profile_refresh": flags.get(
                            "openivm_profile_refresh", ""
                        ),
                        "openivm_query_log": flags.get("openivm_query_log", ""),
                        "storage_metrics": flags.get("storage_metrics", ""),
                        "preserve_raw": flags.get("preserve_raw", ""),
                        "spark_metrics_capture": flags.get("spark_metrics_capture", ""),
                        "spark_driver_pct_ram": tunables.get("driver_pct_ram", ""),
                        "spark_executor_pct_ram": tunables.get("executor_pct_ram", ""),
                        "spark_shuffle_partitions": tunables.get(
                            "shuffle_partitions", ""
                        ),
                        "spark_default_parallelism": tunables.get(
                            "default_parallelism", ""
                        ),
                        "spark_dbt_threads": tunables.get("dbt_threads", ""),
                        "engine": engine,
                        "batch_num": batch_num,
                        "batch_status": batch.get("status", ""),
                        "duration_s": batch.get("duration_s", ""),
                        "batch_error": batch.get("error") or "",
                        "openivm_over_spark_duration_ratio": _duration_ratio(
                            experiment, batch_num
                        ),
                        "compute_status": compute_metrics.get("status", ""),
                        "compute_cpu_time_s": compute_metrics.get("cpu_time_s", ""),
                        "compute_cpu_stddev_s": compute_metrics.get(
                            "cpu_time_stddev_s", ""
                        ),
                        "compute_repetition_count": compute_metrics.get(
                            "repetition_count", ""
                        ),
                        "compute_source": compute_metrics.get("source", ""),
                        "compute_semantics": compute_metrics.get("semantics", ""),
                        "compute_error": compute_metrics.get("error", ""),
                        "compute_artifact": compute_metrics.get("artifact", ""),
                        "storage_status": storage.get("status", ""),
                        "visible_output_bytes": storage.get("visible_output_bytes", ""),
                        "helper_data_bytes": storage.get("helper_data_bytes", ""),
                        "metadata_bytes": storage.get("metadata_bytes", ""),
                        "source_bytes": storage.get("source_bytes", ""),
                        "total_bytes": storage.get("total_bytes", ""),
                        "helper_over_visible_ratio": storage.get(
                            "overhead_ratio_helper_to_visible", ""
                        ),
                        "storage_errors": storage_errors or storage.get("error") or "",
                        "storage_artifact": storage.get("artifact", ""),
                        "base_table_source_mode": base_tables.get("source_mode", ""),
                        "base_table_bytes": _engine_base_bytes(storage),
                        "base_table_baseline_kind": baseline_kind,
                        "base_table_baseline_engine": baseline_engine,
                        "base_table_baseline_bytes": baseline_bytes,
                        "base_table_storage_overhead_bytes": overhead,
                        "base_table_storage_overhead_ratio": overhead_ratio,
                    }
                )
    return output.getvalue()


def write_inputs(
    repo_dir: str,
    oat_run_id: str,
    experiments: List[ExperimentInputs],
    experiments_file: str,
) -> str:
    state_dir = state_dir_for(repo_dir, oat_run_id)
    os.makedirs(state_dir, exist_ok=True)
    inputs_path = os.path.join(state_dir, "inputs.json")
    with open(inputs_path, "w") as f:
        json.dump(
            {
                "oat_run_id": oat_run_id,
                "experiments_file": experiments_file,
                "experiments": [e.to_dict() for e in experiments],
            },
            f,
            indent=2,
        )
    return inputs_path


def maintain_latest_symlink(repo_dir: str, oat_run_id: str) -> None:
    """Replace mount/oat-state/latest → <oat_run_id>. Best-effort."""
    state_root = os.path.join(repo_dir, "mount", "oat-state")
    os.makedirs(state_root, exist_ok=True)
    latest = os.path.join(state_root, "latest")
    try:
        if os.path.islink(latest) or os.path.exists(latest):
            os.remove(latest)
        os.symlink(oat_run_id, latest)
    except OSError as e:
        logger.warning("could not write latest symlink: %s", e)


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Env-var mutation
# ---------------------------------------------------------------------------

def apply_experiment_env(inputs: ExperimentInputs) -> Dict[str, Optional[str]]:
    """Mutate ``os.environ`` to reflect this experiment's knobs.

    Returns a dict of (key -> previous value) so callers can restore env after
    the experiment if they want. Compose / orchestrator code reads from
    ``os.environ`` for all knob-driven plumbing (feature flags, Spark tunables,
    SCALE_FACTOR, batch percentages), so updating env between experiments is
    how knobs flow through.
    """
    new_env = inputs.to_compose_env()
    prev: Dict[str, Optional[str]] = {}

    # 1. Apply the explicit ones from this experiment.
    for k, v in new_env.items():
        prev[k] = os.environ.get(k)
        if v == "":
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    # 2. Spark tunables: if the experiment did NOT pin a value, *unset* any
    # previously-set value so the next experiment cleanly inherits YAML
    # defaults. Without this, a tunable set in experiment N would leak into
    # experiment N+1 even if N+1 didn't override it.
    for k in (
        "SPARK_DRIVER_PCT_RAM",
        "SPARK_EXECUTOR_PCT_RAM",
        "SPARK_SUBMIT_SHUFFLE_PARTITIONS",
        "SPARK_SUBMIT_DEFAULT_PARALLELISM",
        "SPARK_DBT_THREADS",
    ):
        if k not in new_env:
            if k in os.environ:
                prev.setdefault(k, os.environ.get(k))
                os.environ.pop(k, None)
    return prev
