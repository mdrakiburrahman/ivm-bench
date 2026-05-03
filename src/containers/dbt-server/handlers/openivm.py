"""OpenIVM handler — trigger and manage OpenIVM benchmark runs."""

import threading
import uuid
from datetime import datetime, timezone

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services.db import DB_LOCK, get_db
from services.openivm_runner import run_openivm

bp = Blueprint("openivm", __name__)


@bp.route("/run/openivm", methods=["POST"])
def trigger_openivm_run():
    """Trigger an OpenIVM benchmark run.

    Body JSON:
        scale_factor (int): TPC-DI scale factor (default 3).
        full_refresh (bool): True for batch 1, False for batch 2/3.
        batch_num (int): Batch number (default 1).
    """
    body = request.get_json(force=True, silent=True) or {}
    scale_factor = body.get("scale_factor", 3)
    full_refresh = body.get("full_refresh", True)
    batch_num = body.get("batch_num", 1)

    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc).isoformat()

    with DB_LOCK:
        conn = get_db()
        conn.execute(
            "INSERT INTO runs (run_id, engine, scale_factor, full_refresh, status, created_at) VALUES (?,?,?,?,?,?)",
            (run_id, "openivm", scale_factor, int(full_refresh), "queued", created_at),
        )
        conn.commit()
        conn.close()

    t = threading.Thread(
        target=run_openivm,
        args=(run_id, scale_factor, full_refresh, batch_num),
        daemon=True,
    )
    t.start()

    return jsonify({"run_id": run_id, "status": "queued"}), 202


class OpenIVMHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
