"""dbt runner — executes dbt build and parses results."""

import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone

from services.db import DB_LOCK, get_db
from services.feldera_client import (
    adjust_duration,
    poll_until_idle,
    wait_for_all_delta_commits,
    wait_for_commit_done,
)
from services.progress import cleanup_progress, init_progress, parse_log_line

PROJECTS_DIR = "/app/dbt-projects"


def run_dbt(run_id: str, engine: str, scale_factor: int, full_refresh: bool):
    """Execute a dbt build for the given engine and track results."""
    project_dir = os.path.join(PROJECTS_DIR, engine)
    if not os.path.isdir(project_dir):
        _fail_run(run_id, f"No dbt project for engine '{engine}'")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    with DB_LOCK:
        conn = get_db()
        conn.execute(
            "UPDATE runs SET status='running', started_at=? WHERE run_id=?",
            (now_iso, run_id),
        )
        conn.commit()
        conn.close()

    init_progress(run_id)

    log_path = f"/data/logs/{engine}"
    os.makedirs(log_path, exist_ok=True)

    cmd = [
        "dbt", "build",
        "--profiles-dir", project_dir,
        "--project-dir", project_dir,
        "--target", engine,
        "--log-format", "json",
        "--log-level", "info",
        "--log-path", log_path,
    ]
    if full_refresh:
        cmd.append("--full-refresh")

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = project_dir
    env["SCALE_FACTOR"] = str(scale_factor)

    start_ts = time.monotonic()
    stderr_buf = []
    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env, bufsize=1,
        )

        def _drain_stderr():
            for line in proc.stderr:
                stderr_buf.append(line)
        t_err = threading.Thread(target=_drain_stderr, daemon=True)
        t_err.start()

        for line in proc.stdout:
            parse_log_line(run_id, line)

        proc.wait(timeout=7200)
        t_err.join(timeout=5)
        elapsed = time.monotonic() - start_ts

        if proc.returncode != 0:
            stderr_tail = "".join(stderr_buf[-50:])
            _fail_run(
                run_id,
                f"dbt exited {proc.returncode}:\n{stderr_tail[-2000:]}",
                elapsed,
            )
            _parse_results(run_id, project_dir)
            cleanup_progress(run_id)
            return

        _parse_results(run_id, project_dir)

        if engine == "feldera":
            conn = get_db()
            row = conn.execute("SELECT started_at FROM runs WHERE run_id=?", (run_id,)).fetchone()
            conn.close()
            run_started_at = datetime.fromisoformat(row["started_at"])
            start_epoch_s = run_started_at.timestamp()

            success, _ = poll_until_idle(baseline_input=0)
            if not success:
                _fail_run(run_id, "Feldera pipeline did not reach idle state within timeout", time.monotonic() - start_ts)
                cleanup_progress(run_id)
                return

            if not wait_for_commit_done():
                _fail_run(run_id, "Feldera commit did not complete within timeout", time.monotonic() - start_ts)
                cleanup_progress(run_id)
                return

            success, _ = wait_for_all_delta_commits(start_epoch_s)
            if not success:
                _fail_run(run_id, "Feldera Delta output did not flush within timeout", time.monotonic() - start_ts)
                cleanup_progress(run_id)
                return

            adjusted_duration, per_table_times = adjust_duration(start_epoch_s)
            final_duration = adjusted_duration if adjusted_duration and adjusted_duration > 0 else round(elapsed, 2)

            if per_table_times:
                with DB_LOCK:
                    conn = get_db()
                    for table_name, table_duration in per_table_times.items():
                        conn.execute(
                            "UPDATE run_nodes SET execution_time_s=? WHERE run_id=? AND name=?",
                            (table_duration, run_id, table_name),
                        )
                    conn.commit()
                    conn.close()
        else:
            final_duration = round(elapsed, 2)

        completed_at = datetime.now(timezone.utc).isoformat()
        with DB_LOCK:
            conn = get_db()
            conn.execute(
                "UPDATE runs SET status='completed', completed_at=?, duration_s=? WHERE run_id=?",
                (completed_at, final_duration, run_id),
            )
            conn.commit()
            conn.close()

        cleanup_progress(run_id)

    except subprocess.TimeoutExpired:
        proc.kill()
        _fail_run(run_id, "dbt timed out after 7200s", time.monotonic() - start_ts)
        cleanup_progress(run_id)
    except Exception as e:
        _fail_run(run_id, str(e), time.monotonic() - start_ts)
        cleanup_progress(run_id)


def _fail_run(run_id: str, error: str, duration_s: float = None):
    completed_at = datetime.now(timezone.utc).isoformat()
    with DB_LOCK:
        conn = get_db()
        conn.execute(
            "UPDATE runs SET status='failed', completed_at=?, duration_s=?, error=? WHERE run_id=?",
            (completed_at, round(duration_s, 2) if duration_s else None, error, run_id),
        )
        conn.commit()
        conn.close()


def _parse_results(run_id: str, project_dir: str):
    """Parse manifest.json + run_results.json for per-node timing & compiled SQL."""
    target_dir = os.path.join(project_dir, "target")
    manifest_path = os.path.join(target_dir, "manifest.json")
    results_path = os.path.join(target_dir, "run_results.json")

    manifest_nodes = {}
    if os.path.exists(manifest_path):
        with open(manifest_path) as f:
            manifest = json.load(f)
        for uid, node in manifest.get("nodes", {}).items():
            manifest_nodes[uid] = {
                "compiled_sql": node.get("compiled_code") or node.get("compiled_sql", ""),
                "depends_on": json.dumps(node.get("depends_on", {}).get("nodes", [])),
                "resource_type": node.get("resource_type", ""),
            }

    if not os.path.exists(results_path):
        return

    with open(results_path) as f:
        run_results = json.load(f)

    rows = []
    for r in run_results.get("results", []):
        uid = r.get("unique_id", "")
        mdata = manifest_nodes.get(uid, {})
        rows.append((
            run_id,
            uid,
            uid.split(".")[-1] if "." in uid else uid,
            mdata.get("resource_type", ""),
            r.get("execution_time"),
            r.get("status", ""),
            mdata.get("compiled_sql", ""),
            mdata.get("depends_on", "[]"),
            r.get("adapter_response", {}).get("rows_affected"),
        ))

    if rows:
        with DB_LOCK:
            conn = get_db()
            conn.executemany(
                """INSERT OR REPLACE INTO run_nodes
                   (run_id, unique_id, name, resource_type, execution_time_s, status, compiled_sql, depends_on, rows_affected)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            conn.commit()
            conn.close()
