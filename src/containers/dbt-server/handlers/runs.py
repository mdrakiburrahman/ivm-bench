"""Run management handler — trigger, list, get, progress."""

import json
import threading
import time
import uuid
from datetime import datetime, timezone

from flask import Blueprint, Flask, Response, jsonify, request

from handlers.base import BaseHandler
from services.db import DB_LOCK, get_db
from services.dbt_runner import run_dbt
from services.progress import LIVE_PROGRESS, PROGRESS_LOCK

bp = Blueprint("runs", __name__)


@bp.route("/run/<engine>", methods=["POST"])
def trigger_run(engine):
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    full_refresh = body.get("full_refresh", False)
    batch_num = body.get("batch_num", 1)

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

    t = threading.Thread(target=run_dbt, args=(run_id, engine, scale_factor, full_refresh, batch_num), daemon=True)
    t.start()

    return jsonify({"run_id": run_id, "status": "queued"}), 202


@bp.route("/runs")
def list_runs():
    conn = get_db()
    rows = conn.execute(
        "SELECT run_id, engine, scale_factor, status, created_at, duration_s FROM runs ORDER BY created_at DESC"
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@bp.route("/runs/<run_id>")
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

    edges = []
    for n in node_list:
        for dep in n.get("depends_on", []):
            edges.append({"from": dep, "to": n["unique_id"]})
    run_dict["edges"] = edges

    return jsonify(run_dict)


@bp.route("/runs/<run_id>/progress")
def get_progress(run_id):
    """Live progress with cursor-based pagination."""
    since = request.args.get("since", 0, type=int)

    conn = get_db()
    run = conn.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
    conn.close()

    if not run:
        return jsonify({"error": "not found"}), 404

    run_status = run["status"]

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
        return jsonify({
            "run_status": run_status,
            "total": len(events),
            "completed": len([e for e in events if e["status"] not in ("running",)]),
            "running": 0,
            "pending": 0,
            "events": events if since == 0 else [],
            "next_cursor": len(events),
        })

    with PROGRESS_LOCK:
        if live is None:
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
        total = max(live["total"], len(all_events))

    completed = [e for e in all_events if e["status"] not in ("running",)]
    running = [e for e in all_events if e["status"] == "running"]
    pending_count = max(0, total - len(all_events))

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


@bp.route("/runs/<run_id>/progress/stream")
def stream_progress(run_id):
    """SSE-style streaming progress endpoint."""

    def _fmt_event(e, idx, total):
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
                time.sleep(2)
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

                running = [e for e in all_events if e["status"] == "running"]
                completed_evts = [e for e in all_events if e["status"] != "running"]
                if running and run_status == "running":
                    rn_str = ", ".join(e["name"] for e in running[:4])
                    if len(running) > 4:
                        rn_str += f" (+{len(running) - 4} more)"
                    summary = (
                        f"  ... {len(completed_evts)}/{total} done, "
                        f"{len(running)} running: {rn_str}"
                    )
                    yield f"event: progress\ndata: {summary}\n\n"

            if run_status in ("completed", "failed"):
                yield f"event: done\ndata: {run_status}\n\n"
                return

            time.sleep(2)

    return Response(generate(), mimetype="text/event-stream")


class RunsHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
