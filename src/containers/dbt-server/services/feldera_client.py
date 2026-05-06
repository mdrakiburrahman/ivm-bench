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
# How long the pipeline must remain unchanged (no new initiated steps,
# no new transmitted records on any output, no new processed input
# records) before we declare it quiesced. Used as a fall-back completion
# signal when ``pipeline_complete`` never trips because
# ``mode: snapshot_and_follow`` inputs cannot reach EOI.
FELDERA_QUIESCENCE_S = float(os.environ.get("FELDERA_QUIESCENCE_S", "120"))


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

    Recognises three "done" signals (in order of preference):

    1. ``pipeline_complete = true`` from Feldera (strict — requires all
       inputs at EOI, which never happens for ``mode: snapshot_and_follow``
       inputs).
    2. ``total_completed_steps >= total_initiated_steps`` and no buffered
       input records (the original heuristic).
    3. **Quiescence:** all input that has been received has also been
       processed AND nothing has changed in
       ``total_initiated_steps`` / ``total_processed_records`` /
       per-output ``transmitted_records`` for ``FELDERA_QUIESCENCE_S``
       consecutive seconds. This catches the case where DBSP has settled
       on the result but Feldera never increments
       ``total_completed_steps`` past the snapshot transaction (common
       at large SF when ``transaction_mode: always`` plus a single
       snapshot transaction means steps stay "open" forever).

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
        "Polling Feldera stats for output completion "
        "(interval=%.2fs, baseline_steps=%d, quiescence_s=%.1f)",
        interval, baseline_steps, FELDERA_QUIESCENCE_S,
    )

    # Quiescence tracking: snapshot of (initiated_steps, processed_records,
    # tuple of per-output transmitted_records). When this snapshot stays
    # unchanged for ``FELDERA_QUIESCENCE_S`` seconds AND total_processed
    # >= total_input, we declare the pipeline done.
    last_signature: tuple | None = None
    last_change_t = time.monotonic()

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

        outputs_list = stats.get("outputs", [])

        # Track per-output connector completion
        for output in outputs_list:
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

        completion_reason: str | None = None

        if pipeline_complete:
            completion_reason = "pipeline_complete=true"
        elif (
            target_steps > baseline_steps
            and gm.get("total_completed_steps", 0) >= target_steps
            and gm.get("buffered_input_records", 0) == 0
        ):
            completion_reason = "total_completed_steps>=initiated"
        else:
            # Quiescence check. Compose a signature that captures real
            # data progress (records flowing through the circuit and out
            # to sinks). We deliberately omit ``total_initiated_steps``
            # because ``mode: snapshot_and_follow`` inputs keep polling
            # for new Delta commits even after the snapshot is exhausted,
            # incrementing the step counter forever without any new data.
            # If the signature stays unchanged for FELDERA_QUIESCENCE_S
            # seconds AND the circuit has fully drunk all input, treat
            # as done.
            total_input = gm.get("total_input_records", 0)
            total_processed = gm.get("total_processed_records", 0)
            buffered_in = gm.get("buffered_input_records", 0)
            transmitted = tuple(
                (
                    o.get("config", {}).get("stream", ""),
                    o.get("metrics", {}).get("transmitted_records", 0),
                    o.get("metrics", {}).get("queued_records", 0),
                    o.get("metrics", {}).get("queued_batches", 0),
                    o.get("metrics", {}).get("buffered_records", 0),
                    o.get("metrics", {}).get("batch_records_written", 0),
                )
                for o in outputs_list
            )
            signature = (
                total_input,
                total_processed,
                buffered_in,
                transmitted,
            )
            now = time.monotonic()
            if signature != last_signature:
                last_signature = signature
                last_change_t = now
            elif (
                total_input > 0
                and total_processed >= total_input
                and buffered_in == 0
                and (now - last_change_t) >= FELDERA_QUIESCENCE_S
            ):
                completion_reason = (
                    f"quiescence({FELDERA_QUIESCENCE_S:.0f}s,"
                    f"processed={total_processed}/{total_input})"
                )

        if completion_reason is not None:
            total_duration = time.monotonic() - start_wall
            # Mark any remaining outputs as completed now
            for output in outputs_list:
                config = output.get("config", {})
                view_name = config.get("stream", "")
                if view_name and view_name not in per_table_times:
                    per_table_times[view_name] = round(total_duration, 3)
            logger.info(
                "Pipeline complete in %.2fs (%d output tables tracked, reason=%s)",
                total_duration, len(per_table_times), completion_reason,
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

