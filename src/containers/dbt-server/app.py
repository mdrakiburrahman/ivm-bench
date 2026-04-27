"""
dbt-server: Flask REST API that wraps dbt for benchmark runs.

Streams real-time node progress via /runs/<id>/progress (Retry-After driven).
Stores final run metadata + per-node DAG timing in SQLite.
"""

import json
import os
import re
import sqlite3
import subprocess
import threading
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

STATE_DIR = os.environ.get("STATE_DIR", "/data/state")
PROJECTS_DIR = "/app/dbt-projects"

DB_PATH = os.path.join(STATE_DIR, "state.db")
DB_LOCK = threading.Lock()

# In-memory live progress keyed by run_id
# Each entry: {"total": int, "events": OrderedDict[unique_id -> {...}]}
LIVE_PROGRESS = {}
PROGRESS_LOCK = threading.Lock()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    os.makedirs(STATE_DIR, exist_ok=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            engine TEXT NOT NULL,
            scale_factor INTEGER NOT NULL,
            full_refresh INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'queued',
            created_at TEXT NOT NULL,
            started_at TEXT,
            completed_at TEXT,
            duration_s REAL,
            error TEXT
        );
        CREATE TABLE IF NOT EXISTS run_nodes (
            run_id TEXT NOT NULL,
            unique_id TEXT NOT NULL,
            name TEXT NOT NULL,
            resource_type TEXT,
            execution_time_s REAL,
            status TEXT,
            compiled_sql TEXT,
            depends_on TEXT,
            rows_affected INTEGER,
            PRIMARY KEY (run_id, unique_id),
            FOREIGN KEY (run_id) REFERENCES runs(run_id)
        );
    """)
    conn.close()


# ---------------------------------------------------------------------------
# Live progress helpers — parse dbt JSON log lines in real-time
# ---------------------------------------------------------------------------
# dbt JSON log event codes we care about:
#   Q027  ConcurrencyLine  — "Concurrency: N threads (target=...)"
#   Q011  NodeStart        — node execution started
#   Q012  NodeFinished     — node execution finished (has execution_time)
#   Z022  NodeCompiling    — node compiling (we treat as "queued")

def _init_progress(run_id: str):
    with PROGRESS_LOCK:
        LIVE_PROGRESS[run_id] = {"total": 0, "events": OrderedDict()}


def _parse_log_line(run_id: str, line: str):
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
            # ConcurrencyLine: extract total node count
            prog["total"] = data.get("node_count", prog["total"])

        elif code == "Q011":
            # NodeStart
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
            # NodeFinished
            node_info = data.get("node_info", {})
            uid = node_info.get("unique_id", "")
            if uid:
                # Compute execution time from timestamps
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
                # Parse rows from message like "OK created ... [10 rows in 3.2s]"
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


def _cleanup_progress(run_id: str):
    with PROGRESS_LOCK:
        LIVE_PROGRESS.pop(run_id, None)


# ---------------------------------------------------------------------------
# dbt runner — streams stdout for live progress
# ---------------------------------------------------------------------------

def run_dbt(run_id: str, engine: str, scale_factor: int, full_refresh: bool):
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

    _init_progress(run_id)

    cmd = [
        "dbt", "build",
        "--profiles-dir", project_dir,
        "--project-dir", project_dir,
        "--target", engine,
        "--log-format", "json",
        "--log-level", "info",
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

        # Read stderr in a background thread
        def _drain_stderr():
            for line in proc.stderr:
                stderr_buf.append(line)
        t_err = threading.Thread(target=_drain_stderr, daemon=True)
        t_err.start()

        # Parse stdout JSON lines in real-time
        for line in proc.stdout:
            _parse_log_line(run_id, line)

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
            _cleanup_progress(run_id)
            return

        _parse_results(run_id, project_dir)

        completed_at = datetime.now(timezone.utc).isoformat()
        with DB_LOCK:
            conn = get_db()
            conn.execute(
                "UPDATE runs SET status='completed', completed_at=?, duration_s=? WHERE run_id=?",
                (completed_at, round(elapsed, 2), run_id),
            )
            conn.commit()
            conn.close()

        _cleanup_progress(run_id)

    except subprocess.TimeoutExpired:
        proc.kill()
        _fail_run(run_id, "dbt timed out after 7200s", time.monotonic() - start_ts)
        _cleanup_progress(run_id)
    except Exception as e:
        _fail_run(run_id, str(e), time.monotonic() - start_ts)
        _cleanup_progress(run_id)


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


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/run/<engine>", methods=["POST"])
def trigger_run(engine):
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    full_refresh = body.get("full_refresh", False)

    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    with DB_LOCK:
        conn = get_db()
        conn.execute(
            "INSERT INTO runs (run_id, engine, scale_factor, full_refresh, status, created_at) VALUES (?,?,?,?,?,?)",
            (run_id, engine, scale_factor, int(full_refresh), "queued", created_at),
        )
        conn.commit()
        conn.close()

    t = threading.Thread(target=run_dbt, args=(run_id, engine, scale_factor, full_refresh), daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "status": "queued"}), 202


@app.route("/runs")
def list_runs():
    conn = get_db()
    rows = conn.execute(
        "SELECT run_id, engine, scale_factor, status, created_at, duration_s FROM runs ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/runs/<run_id>")
def get_run(run_id):
    conn = get_db()
    run = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if not run:
        conn.close()
        return jsonify({"error": "not found"}), 404

    run_dict = dict(run)
    run_dict["full_refresh"] = bool(run_dict.get("full_refresh"))

    nodes = conn.execute(
        "SELECT * FROM run_nodes WHERE run_id=? ORDER BY execution_time_s DESC", (run_id,)
    ).fetchall()
    conn.close()

    node_list = []
    for n in nodes:
        nd = dict(n)
        nd["depends_on"] = json.loads(nd.get("depends_on") or "[]")
        node_list.append(nd)

    run_dict["nodes"] = node_list

    # Build edges for DAG visualization
    edges = []
    for n in node_list:
        for dep in n.get("depends_on", []):
            edges.append({"from": dep, "to": n["unique_id"]})
    run_dict["edges"] = edges

    return jsonify(run_dict)


@app.route("/runs/<run_id>/progress")
def get_progress(run_id):
    """
    Live progress endpoint.  Returns completed/running/pending node counts
    and per-node status, plus a Retry-After header while the run is active.

    Query params:
      since=<index>  — only return events with index >= since (cursor).
                        Allows the client to print only *new* completions.
    """
    since = request.args.get("since", 0, type=int)

    # Check run status in DB
    conn = get_db()
    run = conn.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
    conn.close()

    if not run:
        return jsonify({"error": "not found"}), 404

    run_status = run["status"]

    # If run is finished (completed/failed) and no live progress, return final from DB
    with PROGRESS_LOCK:
        live = LIVE_PROGRESS.get(run_id)

    if live is None and run_status in ("completed", "failed"):
        conn = get_db()
        nodes = conn.execute(
            "SELECT unique_id, name, resource_type, execution_time_s, status, rows_affected "
            "FROM run_nodes WHERE run_id=? ORDER BY rowid", (run_id,)
        ).fetchall()
        conn.close()
        events = [dict(n) for n in nodes]
        resp = jsonify({
            "run_status": run_status,
            "total": len(events),
            "completed": len([e for e in events if e["status"] not in ("running",)]),
            "running": 0,
            "pending": 0,
            "events": events if since == 0 else [],
            "next_cursor": len(events),
        })
        return resp

    # Live progress available — build response from in-memory state
    with PROGRESS_LOCK:
        if live is None:
            # Run is queued/running but progress hasn't started yet
            resp = jsonify({
                "run_status": run_status,
                "total": 0,
                "completed": 0,
                "running": 0,
                "pending": 0,
                "events": [],
                "next_cursor": 0,
            })
            resp.headers["Retry-After"] = "3"
            return resp

        all_events = list(live["events"].values())
        # Q027 counts models only; hooks also emit Q011/Q012, so take the max
        total = max(live["total"], len(all_events))

    completed = [e for e in all_events if e["status"] not in ("running",)]
    running = [e for e in all_events if e["status"] == "running"]
    pending_count = max(0, total - len(all_events))

    # Only return events the client hasn't seen (cursor-based)
    new_events = all_events[since:]

    resp_data = {
        "run_status": run_status,
        "total": total,
        "completed": len(completed),
        "running": len(running),
        "pending": pending_count,
        "running_nodes": [e["name"] for e in running],
        "events": new_events,
        "next_cursor": len(all_events),
    }

    resp = jsonify(resp_data)
    if run_status in ("running", "queued"):
        resp.headers["Retry-After"] = "2"
    return resp

@app.route("/runs/<run_id>/progress/stream")
def stream_progress(run_id):
    """
    SSE-style streaming progress endpoint.  Returns pre-formatted text lines
    that the client can echo directly — no JSON parsing needed.

    Protocol (text/event-stream):
      event: progress
      data: <formatted line>

      event: done
      data: <status>   (completed | failed)

    The client can simply: curl -N .../stream | grep '^data: ' | sed 's/^data: //'
    """
    import time as _time

    def _fmt_event(e, idx, total):
        """Format a single node event as a dbt-CLI-style line."""
        st = e.get("status", "")
        name = e.get("name", "")
        rtype = e.get("resource_type", "model")
        t = e.get("execution_time_s")
        rows = e.get("rows_affected")

        if st == "running":
            return f"  {idx:>3} of {total}  START {rtype} {name}"
        elif st in ("success", "pass"):
            ts = f"{t:.2f}s" if t else "?"
            row_str = f" [{rows} rows]" if rows else ""
            return f"  {idx:>3} of {total}  OK    {rtype} {name}{row_str} [{ts}]"
        elif st == "error":
            ts = f"{t:.2f}s" if t else "?"
            return f"  {idx:>3} of {total}  ERROR {rtype} {name} [{ts}]"
        else:
            return f"  {idx:>3} of {total}  {st:5s} {rtype} {name}"

    def generate():
        cursor = 0

        while True:
            # Check run status
            conn = get_db()
            run = conn.execute(
                "SELECT status FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            conn.close()

            if not run:
                yield "event: error\ndata: not found\n\n"
                return

            run_status = run["status"]

            with PROGRESS_LOCK:
                live = LIVE_PROGRESS.get(run_id)

            if live is None and run_status in ("completed", "failed"):
                # Emit final events from DB if cursor == 0
                if cursor == 0:
                    conn = get_db()
                    nodes = conn.execute(
                        "SELECT unique_id, name, resource_type, "
                        "execution_time_s, status, rows_affected "
                        "FROM run_nodes WHERE run_id=? ORDER BY rowid",
                        (run_id,),
                    ).fetchall()
                    conn.close()
                    total = len(nodes)
                    for i, n in enumerate(nodes):
                        line = _fmt_event(dict(n), i + 1, total)
                        yield f"event: progress\ndata: {line}\n\n"
                yield f"event: done\ndata: {run_status}\n\n"
                return

            if live is None:
                # Queued but not started yet
                _time.sleep(2)
                continue

            with PROGRESS_LOCK:
                all_events = list(live["events"].values())
                total = max(live["total"], len(all_events))

            new_events = all_events[cursor:]
            if new_events:
                for i, e in enumerate(new_events):
                    idx = cursor + i + 1
                    line = _fmt_event(e, idx, total)
                    yield f"event: progress\ndata: {line}\n\n"
                cursor = len(all_events)

                # Summary line for running nodes
                running = [e for e in all_events if e["status"] == "running"]
                completed = [e for e in all_events if e["status"] != "running"]
                if running and run_status == "running":
                    rn_str = ", ".join(e["name"] for e in running[:4])
                    if len(running) > 4:
                        rn_str += f" (+{len(running) - 4} more)"
                    summary = (
                        f"  ... {len(completed)}/{total} done, "
                        f"{len(running)} running: {rn_str}"
                    )
                    yield f"event: progress\ndata: {summary}\n\n"

            if run_status in ("completed", "failed"):
                yield f"event: done\ndata: {run_status}\n\n"
                return

            _time.sleep(2)

    return Response(generate(), mimetype="text/event-stream")



init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
