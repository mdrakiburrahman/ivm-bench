"""Feldera client — pipeline polling and Delta Lake timestamp extraction."""

import json
import logging
import os
import time
import urllib.request

logger = logging.getLogger(__name__)

FELDERA_URL = os.environ.get("FELDERA_URL", "http://pipeline-manager:8080")
FELDERA_PIPELINE_NAME = os.environ.get("FELDERA_PIPELINE_NAME", "tpcdi")
FELDERA_GOLD_DIR = os.environ.get("FELDERA_GOLD_DIR", "/data/processed/feldera/gold")
FELDERA_POLL_INTERVAL_S = int(os.environ.get("FELDERA_POLL_INTERVAL_S", "5"))
FELDERA_POLL_TIMEOUT_S = int(os.environ.get("FELDERA_POLL_TIMEOUT_S", "6000"))


def get_stats() -> dict | None:
    """Get Feldera pipeline stats. Returns dict or None on failure."""
    url = f"{FELDERA_URL}/v0/pipelines/{FELDERA_PIPELINE_NAME}/stats"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def poll_until_idle(baseline_input: int = 0) -> tuple[bool, dict]:
    """
    Poll Feldera pipeline until it finishes processing.
    Returns (success, final_stats).
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S

    while time.monotonic() < deadline:
        stats = get_stats()
        if not stats:
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        gm = stats.get("global_metrics", {})
        total_in = gm.get("total_input_records", 0)
        total_proc = gm.get("total_processed_records", 0)
        pipeline_complete = gm.get("pipeline_complete", False)

        if baseline_input > 0 and total_in <= baseline_input:
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        if pipeline_complete or (total_in > 0 and total_proc >= total_in):
            return True, stats

        time.sleep(FELDERA_POLL_INTERVAL_S)

    return False, get_stats() or {}


def wait_for_commit_done() -> bool:
    """Poll until Feldera internal commit is complete."""
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S
    logger.info("Waiting for Feldera commit to complete (transaction_status -> NoTransaction)")

    while time.monotonic() < deadline:
        stats = get_stats()
        if not stats:
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        gm = stats.get("global_metrics", {})
        tx_status = gm.get("transaction_status", "NoTransaction")

        if tx_status == "NoTransaction":
            logger.info("Feldera commit complete (transaction_status=NoTransaction)")
            return True

        logger.info("Feldera commit in progress (transaction_status=%s)", tx_status)
        time.sleep(FELDERA_POLL_INTERVAL_S)

    logger.error("Timeout waiting for Feldera commit to complete")
    return False


def get_latest_delta_commit_ts(gold_dir: str | None = None) -> dict:
    """
    Scan gold Delta table _delta_log dirs for the latest commitInfo.timestamp.
    Returns {table_name: latest_commit_timestamp_ms, '__max__': overall_max}.
    """
    gold_dir = gold_dir or FELDERA_GOLD_DIR
    results = {}
    max_ts = 0

    if not os.path.isdir(gold_dir):
        return results

    for table_name in os.listdir(gold_dir):
        log_dir = os.path.join(gold_dir, table_name, "_delta_log")
        if not os.path.isdir(log_dir):
            continue

        table_max_ts = 0
        for log_file in sorted(os.listdir(log_dir)):
            if not log_file.endswith(".json"):
                continue
            log_path = os.path.join(log_dir, log_file)
            try:
                commit_ts = 0
                has_data = False
                with open(log_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        entry = json.loads(line)
                        commit_info = entry.get("commitInfo")
                        if commit_info and "timestamp" in commit_info:
                            commit_ts = commit_info["timestamp"]
                        if "add" in entry:
                            has_data = True
                if has_data and commit_ts > table_max_ts:
                    table_max_ts = commit_ts
            except (json.JSONDecodeError, OSError):
                continue

        if table_max_ts > 0:
            results[table_name] = table_max_ts
            if table_max_ts > max_ts:
                max_ts = table_max_ts

    if max_ts > 0:
        results["__max__"] = max_ts

    return results


def get_gold_table_names() -> set[str]:
    """Return set of gold table directory names."""
    if not os.path.isdir(FELDERA_GOLD_DIR):
        return set()
    return {
        name for name in os.listdir(FELDERA_GOLD_DIR)
        if os.path.isdir(os.path.join(FELDERA_GOLD_DIR, name))
    }


def wait_for_all_delta_commits(start_time_epoch_s: float) -> tuple[bool, dict]:
    """
    Poll until ALL gold tables have at least one Delta commit after start_time_epoch_s.
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S
    expected_tables = get_gold_table_names()
    start_ms = start_time_epoch_s * 1000.0

    if not expected_tables:
        logger.warning("No gold tables found in %s", FELDERA_GOLD_DIR)
        return False, {}

    logger.info(
        "Waiting for Delta commits on %d gold tables (after epoch_ms=%.0f)",
        len(expected_tables), start_ms,
    )

    while time.monotonic() < deadline:
        commit_times = get_latest_delta_commit_ts()
        committed_tables = set()
        for table_name in expected_tables:
            ts = commit_times.get(table_name, 0)
            if ts > start_ms:
                committed_tables.add(table_name)

        if committed_tables >= expected_tables:
            logger.info("All %d gold tables have Delta commits after start_time", len(expected_tables))
            return True, commit_times

        missing = expected_tables - committed_tables
        logger.debug(
            "Waiting for Delta commits: %d/%d done, missing: %s",
            len(committed_tables), len(expected_tables), sorted(missing)[:5],
        )
        time.sleep(FELDERA_POLL_INTERVAL_S)

    logger.error("Timeout waiting for all gold table Delta commits")
    return False, get_latest_delta_commit_ts()


def wait_for_delta_settle(start_time_epoch_s: float, settle_seconds: int = 3) -> tuple[bool, dict]:
    """
    After pipeline commit, wait for Delta output flush to settle.
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S
    start_ms = start_time_epoch_s * 1000.0
    last_max_ts = 0
    stable_since = None

    logger.info(
        "Waiting for Delta commits to settle (settle=%ds, after epoch_ms=%.0f)",
        settle_seconds, start_ms,
    )

    while time.monotonic() < deadline:
        commit_times = get_latest_delta_commit_ts()
        current_max_ts = 0
        for table_name, ts in commit_times.items():
            if table_name == "__max__":
                continue
            if ts > start_ms and ts > current_max_ts:
                current_max_ts = ts

        if current_max_ts == 0:
            stable_since = None
            time.sleep(1)
            continue

        if current_max_ts > last_max_ts:
            last_max_ts = current_max_ts
            stable_since = time.monotonic()
            time.sleep(1)
            continue

        if stable_since and (time.monotonic() - stable_since) >= settle_seconds:
            num_updated = sum(1 for t, ts in commit_times.items() if t != "__max__" and ts > start_ms)
            logger.info(
                "Delta commits settled: %d tables updated after start_time",
                num_updated,
            )
            return True, commit_times

        time.sleep(1)

    logger.error("Timeout waiting for Delta commits to settle")
    return False, get_latest_delta_commit_ts()


def adjust_duration(start_time_epoch_s: float) -> tuple[float | None, dict]:
    """
    Compute actual execution time from Delta commit timestamps.
    Returns (adjusted_duration_s, per_table_times).
    """
    commit_times = get_latest_delta_commit_ts()
    start_ms = start_time_epoch_s * 1000.0
    max_ts = 0
    per_table_times = {}

    for table_name, ts in commit_times.items():
        if table_name == "__max__":
            continue
        if ts > start_ms:
            duration = round((ts / 1000.0) - start_time_epoch_s, 2)
            per_table_times[table_name] = duration
            if ts > max_ts:
                max_ts = ts

    if max_ts == 0:
        return None, {}

    adjusted_duration_s = round((max_ts / 1000.0) - start_time_epoch_s, 2)
    return adjusted_duration_s, per_table_times
