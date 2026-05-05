"""Feldera-specific handler — pause/resume control and stats-based timing."""

import json
import os
import time
from datetime import datetime, timezone

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services.db import STATE_DIR
from services.feldera_client import (
    get_stats,
    pause_pipeline,
    poll_output_completion,
    resume_pipeline,
)

bp = Blueprint("feldera", __name__)


@bp.route("/stats/feldera")
def stats_feldera():
    """Proxy to Feldera pipeline stats API."""
    stats = get_stats()
    if not stats:
        return jsonify({"error": "Could not reach Feldera pipeline"}), 502
    gm = stats.get("global_metrics", {})
    return jsonify({
        "total_input_records": gm.get("total_input_records", 0),
        "total_processed_records": gm.get("total_processed_records", 0),
        "pipeline_complete": gm.get("pipeline_complete", False),
        "state": gm.get("state", "unknown"),
    })


@bp.route("/pause/feldera", methods=["POST"])
def pause_feldera():
    """Pause the Feldera pipeline so it stops ingesting data."""
    success = pause_pipeline()
    if not success:
        return jsonify({"error": "Failed to pause Feldera pipeline"}), 504
    return jsonify({"status": "paused"})


@bp.route("/resume/feldera", methods=["POST"])
def resume_feldera():
    """Resume the Feldera pipeline so it begins ingesting data."""
    success = resume_pipeline()
    if not success:
        return jsonify({"error": "Failed to resume Feldera pipeline"}), 504
    return jsonify({"status": "resumed", "resumed_at_epoch_s": time.time()})


@bp.route("/compile-time/feldera")
def compile_time_feldera():
    """Return the Feldera pipeline compile time written by the dbt adapter."""
    compile_path = os.environ.get(
        "FELDERA_COMPILE_TIME_PATH", "/tmp/feldera_compile_time.json"
    )
    if not os.path.exists(compile_path):
        return jsonify({"compile_time_s": None})
    try:
        with open(compile_path) as f:
            data = json.load(f)
        # Return compile time for the default pipeline (tpcdi)
        pipeline_name = os.environ.get("FELDERA_PIPELINE_NAME", "tpcdi")
        compile_time = data.get(pipeline_name)
        return jsonify({"compile_time_s": compile_time})
    except Exception:
        return jsonify({"compile_time_s": None})


@bp.route("/wait/feldera", methods=["POST"])
def wait_feldera():
    """
    Wait for Feldera pipeline to finish processing all data.

    Polls the /stats endpoint to track per-output-table completion using
    total_processed_steps vs total_initiated_steps. Returns accurate
    per-table timing and overall batch duration.
    """
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    batch_num = body.get("batch_num", 1)
    start_epoch_s = body.get("start_epoch_s", time.time())
    compile_time_s = body.get("compile_time_s")

    success, total_duration, per_table_times = poll_output_completion()

    if not success:
        return jsonify({
            "error": "Feldera pipeline did not complete within timeout",
            "duration_s": total_duration,
            "per_table_times": per_table_times,
        }), 504

    # Build per-table nodes for chart compatibility (only include tracked outputs)
    nodes = []
    for view_name, elapsed in sorted(per_table_times.items()):
        nodes.append({
            "run_id": None,
            "unique_id": f"model.tpcdi.{view_name}",
            "name": view_name,
            "resource_type": "model",
            "execution_time_s": elapsed,
            "status": "success",
            "compiled_sql": "",
            "depends_on": [],
            "rows_affected": None,
        })

    result = {
        "engine": "feldera",
        "scale_factor": scale_factor,
        "full_refresh": batch_num == 1,
        "status": "completed",
        "created_at": datetime.fromtimestamp(start_epoch_s, tz=timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": total_duration,
        "nodes": nodes,
        "edges": [],
        "pipeline_stats": (get_stats() or {}).get("global_metrics", {}),
    }

    if compile_time_s is not None:
        result["compile_time_s"] = compile_time_s

    result_path = os.path.join(STATE_DIR, f"run-feldera-batch{batch_num}.json")
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)

    return jsonify(result)


class FelderaHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
