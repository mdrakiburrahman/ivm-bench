"""Feldera client — pause/resume pipeline control and stats-based timing."""

import json
import logging
import os
import time
import urllib.request

logger = logging.getLogger(__name__)

FELDERA_URL = os.environ.get("FELDERA_URL", "http://pipeline-manager:8080")
FELDERA_PIPELINE_NAME = os.environ.get("FELDERA_PIPELINE_NAME", "tpcdi")
FELDERA_GOLD_DIR = os.environ.get("FELDERA_GOLD_DIR", "/data/processed/feldera/gold")
FELDERA_POLL_INTERVAL_S = float(os.environ.get("FELDERA_POLL_INTERVAL_S", "0.1"))
FELDERA_POLL_TIMEOUT_S = int(os.environ.get("FELDERA_POLL_TIMEOUT_S", "6000"))


# ─── Low-level REST helpers ───────────────────────────────────────────────────


def _api_request(method: str, path: str, body: bytes | None = None) -> dict | None:
    """Make a request to the Feldera REST API. Returns parsed JSON or None."""
    url = f"{FELDERA_URL}{path}"
    req = urllib.request.Request(url, method=method, data=body)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            return json.loads(data) if data else {}
    except Exception as exc:
        logger.warning("Feldera API %s %s failed: %s", method, path, exc)
        return None


def get_stats() -> dict | None:
    """Get Feldera pipeline stats. Returns dict or None on failure."""
    return _api_request("GET", f"/v0/pipelines/{FELDERA_PIPELINE_NAME}/stats")


# ─── Pipeline control ─────────────────────────────────────────────────────────


def pause_pipeline() -> bool:
    """Pause the Feldera pipeline. Returns True on success."""
    result = _api_request("POST", f"/v0/pipelines/{FELDERA_PIPELINE_NAME}/pause")
    if result is not None:
        logger.info("Pipeline '%s' pause requested", FELDERA_PIPELINE_NAME)
        # Wait for paused state confirmation
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            stats = get_stats()
            if stats:
                state = stats.get("global_metrics", {}).get("state", "")
                if state == "Paused":
                    logger.info("Pipeline '%s' confirmed paused", FELDERA_PIPELINE_NAME)
                    return True
            time.sleep(0.1)
        logger.error("Timeout waiting for pipeline to reach Paused state")
        return False
    return False


def resume_pipeline() -> bool:
    """Resume the Feldera pipeline. Returns True on success."""
    result = _api_request("POST", f"/v0/pipelines/{FELDERA_PIPELINE_NAME}/resume")
    if result is not None:
        logger.info("Pipeline '%s' resume requested", FELDERA_PIPELINE_NAME)
        # Wait for running state confirmation
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            stats = get_stats()
            if stats:
                state = stats.get("global_metrics", {}).get("state", "")
                if state == "Running":
                    logger.info("Pipeline '%s' confirmed running", FELDERA_PIPELINE_NAME)
                    return True
            time.sleep(0.1)
        logger.error("Timeout waiting for pipeline to reach Running state")
        return False
    return False


# ─── Stats-based completion polling ───────────────────────────────────────────


def poll_output_completion(poll_interval: float | None = None) -> tuple[bool, float, dict]:
    """
    Poll /stats until all output connectors have flushed and pipeline is complete.

    Tracks per-output-table completion by comparing each output connector's
    ``total_processed_steps`` against ``global_metrics.total_initiated_steps``.

    Handles batch-scoped completion: captures a baseline of steps at start
    and requires that ``total_initiated_steps`` advances beyond the baseline
    before declaring any output complete.

    Returns:
        (success, total_duration_s, per_table_times)
        where per_table_times = {view_name: seconds_from_start}
    """
    interval = poll_interval or FELDERA_POLL_INTERVAL_S
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S
    start_wall = time.monotonic()
    per_table_times: dict[str, float] = {}

    # Capture baseline steps to detect when new work begins
    baseline_stats = get_stats()
    baseline_steps = 0
    if baseline_stats:
        baseline_steps = baseline_stats.get("global_metrics", {}).get(
            "total_initiated_steps", 0
        )

    logger.info(
        "Polling Feldera stats for output completion (interval=%.2fs, baseline_steps=%d)",
        interval, baseline_steps,
    )

    while time.monotonic() < deadline:
        stats = get_stats()
        if not stats:
            time.sleep(interval)
            continue

        gm = stats.get("global_metrics", {})
        target_steps = gm.get("total_initiated_steps", 0)
        pipeline_complete = gm.get("pipeline_complete", False)

        # Require that new work has started (steps advanced beyond baseline)
        if target_steps <= baseline_steps:
            time.sleep(interval)
            continue

        # Track per-output connector completion
        for output in stats.get("outputs", []):
            config = output.get("config", {})
            view_name = config.get("stream", "")
            if not view_name or view_name in per_table_times:
                continue
            metrics = output.get("metrics", {})
            processed_steps = metrics.get("total_processed_steps", 0)
            if processed_steps >= target_steps:
                elapsed = time.monotonic() - start_wall
                per_table_times[view_name] = round(elapsed, 3)
                logger.debug(
                    "Output '%s' completed at %.3fs (steps=%d/%d)",
                    view_name, elapsed, processed_steps, target_steps,
                )

        if pipeline_complete or (
            target_steps > baseline_steps
            and gm.get("total_completed_steps", 0) >= target_steps
            and gm.get("buffered_input_records", 0) == 0
        ):
            total_duration = time.monotonic() - start_wall
            # Mark any remaining outputs as completed now
            for output in stats.get("outputs", []):
                config = output.get("config", {})
                view_name = config.get("stream", "")
                if view_name and view_name not in per_table_times:
                    per_table_times[view_name] = round(total_duration, 3)
            logger.info(
                "Pipeline complete in %.2fs (%d output tables tracked, pipeline_complete=%s)",
                total_duration, len(per_table_times), pipeline_complete,
            )
            return True, round(total_duration, 3), per_table_times

        time.sleep(interval)

    total_duration = time.monotonic() - start_wall
    logger.error("Timeout waiting for pipeline completion after %.1fs", total_duration)
    return False, round(total_duration, 3), per_table_times


def wait_for_pipeline_complete() -> tuple[bool, dict]:
    """
    Simple wait for pipeline_complete flag. Returns (success, final_stats).
    Uses the same polling interval as poll_output_completion.
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S

    while time.monotonic() < deadline:
        stats = get_stats()
        if stats:
            gm = stats.get("global_metrics", {})
            if gm.get("pipeline_complete", False):
                return True, stats
        time.sleep(FELDERA_POLL_INTERVAL_S)

    return False, get_stats() or {}


# ─── Legacy helpers (kept for compatibility) ──────────────────────────────────


def get_gold_table_names() -> set[str]:
    """Return set of gold table directory names."""
    if not os.path.isdir(FELDERA_GOLD_DIR):
        return set()
    return {
        name for name in os.listdir(FELDERA_GOLD_DIR)
        if os.path.isdir(os.path.join(FELDERA_GOLD_DIR, name))
    }

