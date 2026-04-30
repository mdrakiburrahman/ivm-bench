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

import urllib.request

from flask import Flask, Response, jsonify, request

app = Flask(__name__)

STATE_DIR = os.environ.get("STATE_DIR", "/data/state")
PROJECTS_DIR = "/app/dbt-projects"

DB_PATH = os.path.join(STATE_DIR, "state.db")
DB_LOCK = threading.Lock()

# Feldera configuration
FELDERA_URL = os.environ.get("FELDERA_URL", "http://pipeline-manager:8080")
FELDERA_PIPELINE_NAME = os.environ.get("FELDERA_PIPELINE_NAME", "tpcdi")
FELDERA_GOLD_DIR = os.environ.get("FELDERA_GOLD_DIR", "/data/processed/feldera/gold")
FELDERA_POLL_INTERVAL_S = int(os.environ.get("FELDERA_POLL_INTERVAL_S", "5"))
FELDERA_POLL_TIMEOUT_S = int(os.environ.get("FELDERA_POLL_TIMEOUT_S", "6000"))

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
# Feldera helpers — pipeline polling + Delta Lake timestamp extraction
# ---------------------------------------------------------------------------

def _feldera_get_stats():
    """Get Feldera pipeline stats. Returns dict or None on failure."""
    url = f"{FELDERA_URL}/v0/pipelines/{FELDERA_PIPELINE_NAME}/stats"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read())
    except Exception:
        return None


def _feldera_poll_until_idle(baseline_input=0):
    """
    Poll Feldera pipeline until it finishes processing.
    If baseline_input > 0, first wait for new input to arrive, then wait for
    processing to catch up.
    Returns (success: bool, final_stats: dict).
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S

    while time.monotonic() < deadline:
        stats = _feldera_get_stats()
        if not stats:
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        gm = stats.get("global_metrics", {})
        total_in = gm.get("total_input_records", 0)
        total_proc = gm.get("total_processed_records", 0)
        pipeline_complete = gm.get("pipeline_complete", False)

        # If we have a baseline, wait for new input to arrive first
        if baseline_input > 0 and total_in <= baseline_input:
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        # Wait for processing to catch up to input
        if pipeline_complete or (total_in > 0 and total_proc >= total_in):
            return True, stats

        time.sleep(FELDERA_POLL_INTERVAL_S)

    return False, _feldera_get_stats() or {}


def _get_latest_delta_commit_ts(gold_dir=None):
    """
    Scan all gold Delta table _delta_log dirs for the latest commitInfo.timestamp
    that contains actual data (add actions).
    Returns dict: {table_name: latest_commit_timestamp_ms} for tables with data commits,
    plus a special key '__max__' with the overall latest timestamp.
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
                # Only count commits that have actual data files
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


def _feldera_wait_for_all_delta_commits(start_time_epoch_s):
    """
    Poll until ALL gold tables have at least one Delta commit with data AFTER start_time_epoch_s.
    Used for batch 1 where every table gets initial data.
    Returns (success: bool, commit_times: dict).
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S
    expected_tables = _get_gold_table_names()
    start_ms = start_time_epoch_s * 1000.0

    if not expected_tables:
        app.logger.warning("No gold tables found in %s", FELDERA_GOLD_DIR)
        return False, {}

    app.logger.info(
        "Waiting for Delta commits on %d gold tables (after epoch_ms=%.0f)",
        len(expected_tables), start_ms,
    )

    while time.monotonic() < deadline:
        commit_times = _get_latest_delta_commit_ts()
        committed_tables = set()
        for table_name in expected_tables:
            ts = commit_times.get(table_name, 0)
            if ts > start_ms:
                committed_tables.add(table_name)

        if committed_tables >= expected_tables:
            app.logger.info("All %d gold tables have Delta commits after start_time", len(expected_tables))
            return True, commit_times

        missing = expected_tables - committed_tables
        app.logger.debug(
            "Waiting for Delta commits: %d/%d done, missing: %s",
            len(committed_tables), len(expected_tables), sorted(missing)[:5],
        )
        time.sleep(FELDERA_POLL_INTERVAL_S)

    app.logger.error("Timeout waiting for all gold table Delta commits")
    return False, _get_latest_delta_commit_ts()


def _feldera_wait_for_delta_settle(start_time_epoch_s, settle_seconds=30):
    """
    After pipeline is idle, wait for Delta commits to settle (no new commits
    appearing for settle_seconds). Used for batch 2/3 where not all tables
    may be updated.
    Returns (success: bool, commit_times: dict).
    """
    deadline = time.monotonic() + FELDERA_POLL_TIMEOUT_S
    start_ms = start_time_epoch_s * 1000.0
    last_max_ts = 0
    stable_since = None

    app.logger.info(
        "Waiting for Delta commits to settle (settle=%ds, after epoch_ms=%.0f)",
        settle_seconds, start_ms,
    )

    while time.monotonic() < deadline:
        commit_times = _get_latest_delta_commit_ts()
        # Find max commit timestamp after start_time
        current_max_ts = 0
        for table_name, ts in commit_times.items():
            if table_name == "__max__":
                continue
            if ts > start_ms and ts > current_max_ts:
                current_max_ts = ts

        if current_max_ts == 0:
            # No commits after start_time yet — keep waiting
            stable_since = None
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        if current_max_ts > last_max_ts:
            # New commit appeared — reset settle timer
            last_max_ts = current_max_ts
            stable_since = time.monotonic()
            time.sleep(FELDERA_POLL_INTERVAL_S)
            continue

        # No new commits since last check
        if stable_since and (time.monotonic() - stable_since) >= settle_seconds:
            num_updated = sum(1 for t, ts in commit_times.items() if t != "__max__" and ts > start_ms)
            app.logger.info(
                "Delta commits settled: %d tables updated after start_time",
                num_updated,
            )
            return True, commit_times

        time.sleep(FELDERA_POLL_INTERVAL_S)

    app.logger.error("Timeout waiting for Delta commits to settle")
    return False, _get_latest_delta_commit_ts()


def _feldera_adjust_duration(start_time_epoch_s):
    """
    After pipeline is idle, compute actual execution time from Delta commit timestamps.
    Only considers commits that occurred AFTER start_time_epoch_s (scoped to current batch).
    start_time_epoch_s: epoch seconds when the run started (wall clock).
    Returns (adjusted_duration_s, per_table_times: dict).
    """
    commit_times = _get_latest_delta_commit_ts()
    start_ms = start_time_epoch_s * 1000.0
    max_ts = 0
    per_table_times = {}

    for table_name, ts in commit_times.items():
        if table_name == "__max__":
            continue
        # Only count commits that happened after our start time
        if ts > start_ms:
            duration = round((ts / 1000.0) - start_time_epoch_s, 2)
            per_table_times[table_name] = duration
            if ts > max_ts:
                max_ts = ts

    if max_ts == 0:
        return None, {}

    adjusted_duration_s = round((max_ts / 1000.0) - start_time_epoch_s, 2)
    return adjusted_duration_s, per_table_times


def _get_gold_table_names():
    """Return set of gold table directory names."""
    if not os.path.isdir(FELDERA_GOLD_DIR):
        return set()
    return {
        name for name in os.listdir(FELDERA_GOLD_DIR)
        if os.path.isdir(os.path.join(FELDERA_GOLD_DIR, name))
    }


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

        # For Feldera: wait for pipeline to finish and use Delta commit timestamps
        if engine == "feldera":
            # Record wall clock start time (when dbt started)
            # We need to compute from the DB started_at
            conn = get_db()
            row = conn.execute("SELECT started_at FROM runs WHERE run_id=?", (run_id,)).fetchone()
            conn.close()
            run_started_at = datetime.fromisoformat(row["started_at"])
            start_epoch_s = run_started_at.timestamp()

            # Poll until pipeline is idle (processing complete)
            success, _ = _feldera_poll_until_idle(baseline_input=0)
            if not success:
                _fail_run(run_id, "Feldera pipeline did not reach idle state within timeout", time.monotonic() - start_ts)
                _cleanup_progress(run_id)
                return

            # Wait until ALL gold tables have Delta commits (output fully flushed)
            success, _ = _feldera_wait_for_all_delta_commits(start_epoch_s)
            if not success:
                _fail_run(run_id, "Feldera Delta output did not flush within timeout", time.monotonic() - start_ts)
                _cleanup_progress(run_id)
                return

            # Get actual execution time from Delta commit timestamps
            adjusted_duration, per_table_times = _feldera_adjust_duration(start_epoch_s)
            final_duration = adjusted_duration if adjusted_duration and adjusted_duration > 0 else round(elapsed, 2)

            # Update per-node execution times for gold models based on Delta timestamps
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


@app.route("/chart")
def chart():
    """
    Generate a PNG chart of model execution times across engines and batches.
    Discovers run-<engine>-batch<N>.json files in STATE_DIR.
    Returns image/png.
    """
    import glob as _glob
    from io import BytesIO

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    # Discover and parse result files
    records = []
    for filepath in sorted(_glob.glob(os.path.join(STATE_DIR, "run-*.json"))):
        filename = os.path.basename(filepath)
        m = re.match(r"run-(\w+)-batch(\d+)\.json", filename)
        if not m:
            continue
        engine = m.group(1)
        batch_num = int(m.group(2))

        with open(filepath) as f:
            data = json.load(f)

        for node in data.get("nodes", []):
            if node.get("resource_type") != "model":
                continue
            records.append({
                "query_label": node["name"],
                "engine": engine,
                "batch_num": batch_num,
                "duration_s": node["execution_time_s"],
            })

    if not records:
        return jsonify({"error": "No result files found in STATE_DIR"}), 404

    # Organize data
    batches = sorted(set(r["batch_num"] for r in records))
    engines = sorted(set(r["engine"] for r in records))
    queries = sorted(set(r["query_label"] for r in records))

    # Build lookup: (batch, engine, query) -> duration
    lookup = {}
    for r in records:
        lookup[(r["batch_num"], r["engine"], r["query_label"])] = r["duration_s"]

    # Create side-by-side subplots (one per batch)
    n_batches = len(batches)
    n_engines = len(engines)
    n_queries = len(queries)
    bar_thickness = 0.25
    group_spacing = n_engines * bar_thickness + 0.4  # gap between groups
    colors = plt.cm.tab10.colors

    fig, axes = plt.subplots(
        1, n_batches,
        figsize=(7 * n_batches, max(10, n_queries * 0.9)),
    )
    if n_batches == 1:
        axes = [axes]

    # Find the max duration per batch to determine "proportionally long" threshold
    batch_maxes = {}
    for r in records:
        b = r["batch_num"]
        batch_maxes[b] = max(batch_maxes.get(b, 0), r["duration_s"])

    for ax_idx, batch in enumerate(batches):
        ax = axes[ax_idx]
        x_max = batch_maxes.get(batch, 1)
        label_threshold = x_max * 0.15  # bars longer than 15% of max get a label

        tick_centers = []
        for q_idx, query in enumerate(queries):
            group_base = q_idx * group_spacing
            for i, engine in enumerate(engines):
                y_pos = group_base + i * bar_thickness
                val = lookup.get((batch, engine, query), 0)
                ax.barh(y_pos, val, bar_thickness * 0.9, align='center',
                        label=engine if q_idx == 0 else None,
                        color=colors[i % len(colors)])
                # Label on the bar for proportionally long durations
                if val >= label_threshold:
                    ax.text(val * 0.5, y_pos, f"{query} ({val:.1f}s)",
                            ha='center', va='center', fontsize=6,
                            color='white', fontweight='bold')
            tick_centers.append(group_base + (n_engines - 1) * bar_thickness / 2)

        ax.set_yticks(tick_centers)
        ax.set_yticklabels(queries, fontsize=9)
        ax.set_ylim(-0.4, (n_queries - 1) * group_spacing + n_engines * bar_thickness)
        ax.invert_yaxis()
        ax.set_xlabel("Duration (seconds)", fontsize=11)
        ax.set_title(f"Batch {batch}", fontsize=13)
        ax.legend(loc="lower right", fontsize=10)
        ax.grid(axis="x", alpha=0.3)

        # Light horizontal separators between query groups
        for q_idx in range(1, n_queries):
            sep_y = q_idx * group_spacing - 0.2
            ax.axhline(y=sep_y, color='gray', linewidth=0.3, alpha=0.5)

    fig.suptitle("dbt Model Execution Time by Engine (SF=5)", fontsize=14, y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    # Return PNG
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="image/png")


@app.route("/stats/feldera")
def stats_feldera():
    """
    Proxy to Feldera pipeline stats API.
    Returns total_input_records and total_processed_records for baseline capture.
    """
    stats = _feldera_get_stats()
    if not stats:
        return jsonify({"error": "Could not reach Feldera pipeline"}), 502
    gm = stats.get("global_metrics", {})
    return jsonify({
        "total_input_records": gm.get("total_input_records", 0),
        "total_processed_records": gm.get("total_processed_records", 0),
        "pipeline_complete": gm.get("pipeline_complete", False),
        "state": gm.get("state", "unknown"),
    })


@app.route("/wait/feldera", methods=["POST"])
def wait_feldera():
    """
    Wait for Feldera pipeline to finish processing incremental data (batches 2/3).

    Body: {
        "scale_factor": N,
        "batch_num": 2,
        "baseline_input": 12345,  // total_input_records BEFORE append (from /stats/feldera)
        "start_epoch_s": 1234567890.12  // wall clock when timing started (before append)
    }

    Polls the Feldera pipeline stats API until processing catches up to input,
    then reads Delta Lake commit timestamps to compute per-model execution times.
    Returns a response compatible with the standard run result format (nodes[], duration_s, edges).
    Also saves the result as run-feldera-batch<N>.json in STATE_DIR.
    """
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    batch_num = body.get("batch_num", 2)
    baseline_input = body.get("baseline_input", 0)
    start_epoch_s = body.get("start_epoch_s", time.time())

    # Poll until pipeline finishes processing
    success, final_stats = _feldera_poll_until_idle(baseline_input=baseline_input)

    if not success:
        return jsonify({
            "error": "Feldera pipeline did not reach idle state within timeout",
            "pipeline_stats": final_stats,
        }), 504

    # Wait for Delta commits to settle (not all tables may be updated in batch 2/3)
    success, _ = _feldera_wait_for_delta_settle(start_epoch_s)
    if not success:
        return jsonify({
            "error": "Feldera Delta output did not flush within timeout",
            "pipeline_stats": final_stats,
        }), 504

    # Get per-table execution times from Delta commit timestamps (scoped to this batch)
    adjusted_duration, per_table_times = _feldera_adjust_duration(start_epoch_s)
    wall_duration = round(time.time() - start_epoch_s, 2)
    final_duration = adjusted_duration if adjusted_duration and adjusted_duration > 0 else wall_duration

    # Build nodes list compatible with chart expectations
    # Include all gold tables; tables not updated in this batch get 0.0
    all_gold_tables = _get_gold_table_names()
    nodes = []
    for table_name in sorted(all_gold_tables):
        table_duration = per_table_times.get(table_name, 0.0)
        # Only include tables that were actually updated (positive duration)
        if table_duration > 0:
            nodes.append({
                "run_id": None,
                "unique_id": f"model.tpcdi.{table_name}",
                "name": table_name,
                "resource_type": "model",
                "execution_time_s": table_duration,
                "status": "success",
                "compiled_sql": "",
                "depends_on": [],
                "rows_affected": None,
            })

    # Build result matching the standard GET /runs/<id> format
    result = {
        "engine": "feldera",
        "scale_factor": scale_factor,
        "full_refresh": False,
        "status": "completed",
        "created_at": datetime.fromtimestamp(start_epoch_s, tz=timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": final_duration,
        "nodes": nodes,
        "edges": [],
        "pipeline_stats": final_stats.get("global_metrics", {}) if final_stats else {},
    }

    # Save to STATE_DIR as run-feldera-batch<N>.json
    result_path = os.path.join(STATE_DIR, f"run-feldera-batch{batch_num}.json")
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)

    return jsonify(result)


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
