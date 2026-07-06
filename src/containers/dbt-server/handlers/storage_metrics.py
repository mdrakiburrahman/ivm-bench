"""Storage metrics handler."""

import logging

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services import storage_metrics

logger = logging.getLogger(__name__)
bp = Blueprint("storage_metrics", __name__)


@bp.route("/storage/<engine>", methods=["GET"])
def get_storage_metrics(engine):
    batch_num = request.args.get("batch_num", type=int)
    try:
        result = storage_metrics.collect_storage_metrics(engine, batch_num)
        status_code = 200 if result.get("status") != "unsupported" else 404
        return jsonify(result), status_code
    except Exception as e:
        logger.exception("[storage] collection failed for engine=%s", engine)
        return jsonify({
            "status": "error",
            "engine": engine,
            "batch_num": batch_num,
            "error": str(e),
        }), 500


class StorageMetricsHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
