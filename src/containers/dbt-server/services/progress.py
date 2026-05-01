"""Live progress tracking for dbt runs."""

import json
import re
import threading
from collections import OrderedDict


LIVE_PROGRESS: dict = {}
PROGRESS_LOCK = threading.Lock()


def init_progress(run_id: str):
    with PROGRESS_LOCK:
        LIVE_PROGRESS[run_id] = {"total": 0, "events": OrderedDict()}


def parse_log_line(run_id: str, line: str):
    """Parse a single dbt JSON log line and update live progress."""
    line = line.strip()
    if not line or not line.startswith("{"):
        return

    try:
        entry = json.loads(line)
    except json.JSONDecodeError:
        return

    info = entry.get("info", {})
    data = entry.get("data", {})
    code = info.get("code", "")

    with PROGRESS_LOCK:
        prog = LIVE_PROGRESS.get(run_id)
        if not prog:
            return

        if code == "Q027":
            prog["total"] = data.get("node_count", prog["total"])

        elif code == "Q011":
            node_info = data.get("node_info", {})
            uid = node_info.get("unique_id", "")
            if uid:
                prog["events"][uid] = {
                    "unique_id": uid,
                    "name": node_info.get("node_name", uid.split(".")[-1]),
                    "resource_type": node_info.get("resource_type", ""),
                    "status": "running",
                    "execution_time_s": None,
                    "rows_affected": None,
                    "started_at": node_info.get("node_started_at", ""),
                    "message": info.get("msg", ""),
                }

        elif code == "Q012":
            node_info = data.get("node_info", {})
            uid = node_info.get("unique_id", "")
            if uid:
                exec_time = node_info.get("execution_time", 0)
                if not exec_time:
                    started = node_info.get("node_started_at", "")
                    finished = node_info.get("node_finished_at", "")
                    if started and finished:
                        from datetime import datetime as _dt
                        try:
                            t0 = _dt.fromisoformat(started)
                            t1 = _dt.fromisoformat(finished)
                            exec_time = (t1 - t0).total_seconds()
                        except Exception:
                            pass
                status = node_info.get("node_status", "success")
                msg = info.get("msg", "")
                rows = None
                m = re.search(r"\[(\d+)\s+rows?\s", msg)
                if m:
                    rows = int(m.group(1))
                existing = prog["events"].get(uid, {})
                prog["events"][uid] = {
                    **existing,
                    "unique_id": uid,
                    "name": node_info.get("node_name", uid.split(".")[-1]),
                    "resource_type": node_info.get("resource_type", ""),
                    "status": status,
                    "execution_time_s": round(exec_time, 2) if exec_time else None,
                    "rows_affected": rows,
                    "finished_at": node_info.get("node_finished_at", ""),
                    "message": msg,
                }


def cleanup_progress(run_id: str):
    with PROGRESS_LOCK:
        LIVE_PROGRESS.pop(run_id, None)


def get_progress(run_id: str) -> dict | None:
    """Get a snapshot of live progress for a run_id."""
    with PROGRESS_LOCK:
        return LIVE_PROGRESS.get(run_id)
