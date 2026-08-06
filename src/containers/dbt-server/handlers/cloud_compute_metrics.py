"""Cloud task/CPU telemetry endpoint."""

import logging

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services import cloud_compute_metrics

logger = logging.getLogger(__name__)
bp = Blueprint("cloud_compute_metrics", __name__)


@bp.route("/cloud-compute/<engine>/<int:batch_num>", methods=["POST"])
def collect(engine: str, batch_num: int):
    body = request.get_json(silent=True) or {}
    try:
        start_ms = int(body["start_ms"])
        end_ms = int(body["end_ms"])
        result = cloud_compute_metrics.collect(engine, start_ms, end_ms)
        result["engine"] = engine
        result["batch_num"] = batch_num
        return jsonify(result), 200
    except (KeyError, TypeError, ValueError) as exc:
        return jsonify({"status": "error", "error": str(exc)}), 400
    except Exception as exc:
        logger.exception(
            "[cloud-compute] collection failed engine=%s batch=%d", engine, batch_num
        )
        return jsonify({"status": "error", "error": str(exc)}), 500


class CloudComputeMetricsHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
