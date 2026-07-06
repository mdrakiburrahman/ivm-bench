"""Engine-agnostic /query-plan/<engine>/<run_id>/<batch_num> endpoint."""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services import query_plans

logger = logging.getLogger(__name__)

bp = Blueprint("query_plans", __name__)


@bp.route(
    "/query-plan/<engine>/<run_id>/<int:batch_num>",
    methods=["POST"],
)
def explain(engine: str, run_id: str, batch_num: int):
    try:
        return jsonify(query_plans.collect_query_plans(engine, run_id, batch_num)), 200
    except ValueError as e:
        return jsonify({"status": "error", "error": str(e)}), 400
    except Exception as e:
        logger.exception(
            "[query-plans] capture failed engine=%s run_id=%s batch=%d",
            engine,
            run_id,
            batch_num,
        )
        return jsonify({"status": "error", "error": str(e)}), 500


class QueryPlansHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
