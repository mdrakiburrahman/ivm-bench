"""DbspNet-specific handler — deploy/resume/wait against the DbspNet control service.

Mirrors the Feldera handler, but the deploy is the compile-only bypass (translate the dbt
project → program → POST to the DbspNet service) instead of a dbt adapter, and completion
is a blocking /wait (DbspNet drains a batch to completion — no stats quiescence polling).
"""

import json
import os
import time
from datetime import datetime, timezone

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services.db import STATE_DIR
from services.dbspnet_client import deploy, get_stats, resume, wait

bp = Blueprint("dbspnet", __name__)


@bp.route("/stats/dbspnet")
def stats_dbspnet():
    stats = get_stats()
    if stats is None:
        return jsonify({"error": "Could not reach DbspNet service"}), 502
    return jsonify(stats)


@bp.route("/deploy/dbspnet", methods=["POST"])
def deploy_dbspnet():
    """Translate the dbt project and deploy it (compile + wire Delta connectors)."""
    result = deploy()
    return jsonify(result)


@bp.route("/pause/dbspnet", methods=["POST"])
def pause_dbspnet():
    # A DbspNet batch drains to completion, so there is no ingestion to pause.
    return jsonify({"status": "ok"})


@bp.route("/resume/dbspnet", methods=["POST"])
def resume_dbspnet():
    """Start ingesting the current batch (timer start)."""
    result = resume()
    return jsonify({"status": "resumed", "resumed_at_epoch_s": result.get("resumedAtEpochS", time.time())})


@bp.route("/wait/dbspnet", methods=["POST"])
def wait_dbspnet():
    """Block until the batch has drained and outputs are written; write the result JSON."""
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    batch_num = body.get("batch_num", 1)
    start_epoch_s = body.get("start_epoch_s", time.time())
    compile_time_s = body.get("compile_time_s")

    result = wait()
    duration = result.get("durationS", 0.0)

    # Per-output nodes for chart compatibility (chart requires every node "success").
    nodes = []
    for o in result.get("outputs", []):
        nodes.append({
            "run_id": None,
            "unique_id": f"model.tpcdi.{o.get('view')}",
            "name": o.get("view"),
            "resource_type": "model",
            "execution_time_s": duration,
            "status": o.get("status", "success"),
            "compiled_sql": "",
            "depends_on": [],
            "rows_affected": o.get("rows"),
        })

    out = {
        "engine": "dbspnet",
        "scale_factor": scale_factor,
        "full_refresh": batch_num == 1,
        "status": "completed",
        "created_at": datetime.fromtimestamp(start_epoch_s, tz=timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": duration,
        "nodes": nodes,
        "edges": [],
        "pipeline_stats": get_stats() or {},
    }
    if compile_time_s is not None:
        out["compile_time_s"] = compile_time_s

    os.makedirs(STATE_DIR, exist_ok=True)
    with open(os.path.join(STATE_DIR, f"run-dbspnet-batch{batch_num}.json"), "w") as f:
        json.dump(out, f, indent=2)

    return jsonify(out)


class DbspNetHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
