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
          "storage_files": { "spark": [path1, path2, path3], ... },
          "storage": { "spark": {"1": {...}, "2": {...}, "3": {...}}, ... },
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

    storage_files: Dict[str, List[str]] = {}
    storage_out: Dict[str, Dict[str, Any]] = {}
    for engine in inputs.engines:
        files = []
        for batch_num in (1, 2, 3):
            files.append(os.path.join(
                "mount", "results", str(inputs.scale_factor), "dbt-server",
                f"storage-{engine}-batch{batch_num}.json",
            ))
        storage_files[engine] = files

    if result is not None:
        for engine, er in result.engines.items():
            per_batch: Dict[str, Any] = {}
            for batch in er.batches:
                storage = (batch.extra or {}).get("storage")
                if storage:
                    per_batch[str(batch.batch_num)] = storage
            if per_batch:
                storage_out[engine] = per_batch

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
        "storage_files": storage_files,
        "storage": storage_out,
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
