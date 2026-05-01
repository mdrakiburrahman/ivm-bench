"""Feldera-specific handler — stats and wait endpoints."""

import json
import os
import time
from datetime import datetime, timezone

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services.db import STATE_DIR
from services.feldera_client import (
    adjust_duration,
    get_gold_table_names,
    get_stats,
    poll_until_idle,
    wait_for_commit_done,
    wait_for_delta_settle,
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


@bp.route("/wait/feldera", methods=["POST"])
def wait_feldera():
    """
    Wait for Feldera pipeline to finish processing incremental data (batches 2/3).
    """
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    batch_num = body.get("batch_num", 2)
    baseline_input = body.get("baseline_input", 0)
    start_epoch_s = body.get("start_epoch_s", time.time())

    success, final_stats = poll_until_idle(baseline_input=baseline_input)

    if not success:
        return jsonify({
            "error": "Feldera pipeline did not reach idle state within timeout",
            "pipeline_stats": final_stats,
        }), 504

    if not wait_for_commit_done():
        return jsonify({
            "error": "Feldera commit did not complete within timeout",
            "pipeline_stats": final_stats,
        }), 504

    success, _ = wait_for_delta_settle(start_epoch_s)
    if not success:
        return jsonify({
            "error": "Feldera Delta output did not flush within timeout",
            "pipeline_stats": final_stats,
        }), 504

    adjusted_duration, per_table_times = adjust_duration(start_epoch_s)
    wall_duration = round(time.time() - start_epoch_s, 2)
    final_duration = adjusted_duration if adjusted_duration and adjusted_duration > 0 else wall_duration

    all_gold_tables = get_gold_table_names()
    nodes = []
    for table_name in sorted(all_gold_tables):
        table_duration = per_table_times.get(table_name, 0.0)
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

    result_path = os.path.join(STATE_DIR, f"run-feldera-batch{batch_num}.json")
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(result_path, "w") as f:
        json.dump(result, f, indent=2)

    return jsonify(result)


class FelderaHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
