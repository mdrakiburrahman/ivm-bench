"""dbt runner — executes dbt build and parses results."""

import json
import os
import subprocess
import threading
import time
from datetime import datetime, timezone

from services.db import DB_LOCK, get_db
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
        cmd.extend([
            "--full-refresh",
            "--threads",
            "1",
        ])

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = project_dir
    env["SCALE_FACTOR"] = str(scale_factor)
    env["PYTHONPATH"] = (
        f"/app:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else "/app"
    )

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

        if proc.returncode not in (0, 2):
            stderr_tail = "".join(stderr_buf[-50:])
            _fail_run(
                run_id,
                f"dbt exited {proc.returncode}:\n{stderr_tail[-2000:]}",
                elapsed,
            )
            _parse_results(run_id, project_dir)
            cleanup_progress(run_id)
            return

        parsed = _parse_results(run_id, project_dir)
        if proc.returncode == 2 and (
            parsed["nodes"] == 0 or parsed["errors"] > 0
        ):
            stderr_tail = "".join(stderr_buf[-50:])
            _fail_run(
                run_id,
                f"dbt exited 2 with {parsed['errors']} node errors "
                f"and {parsed['nodes']} parsed nodes:\n{stderr_tail[-2000:]}",
                elapsed,
            )
            cleanup_progress(run_id)
            return

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


def _parse_results(run_id: str, project_dir: str) -> dict[str, int]:
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
        return {"nodes": 0, "errors": 0}

    with open(results_path) as f:
        run_results = json.load(f)

    rows = []
    error_count = 0
    for r in run_results.get("results", []):
        uid = r.get("unique_id", "")
        mdata = manifest_nodes.get(uid, {})
        status = r.get("status", "")
        if status in ("error", "fail"):
            error_count += 1
        rows.append((
            run_id,
            uid,
            uid.split(".")[-1] if "." in uid else uid,
            mdata.get("resource_type", ""),
            r.get("execution_time"),
            status,
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

    return {"nodes": len(rows), "errors": error_count}
