"""spark-openivm source-table management endpoints.

The benchmark-server calls these before each dbt build:
  - POST /sources/spark-openivm/init           (before batch 1 dbt build)
  - POST /sources/spark-openivm/append/<N>     (before batch 2/3 dbt builds)

The standard /run/<engine> route handles dbt builds.
"""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services import spark_openivm_sources

logger = logging.getLogger(__name__)

bp = Blueprint("spark_openivm", __name__)


@bp.route("/sources/spark-openivm/init", methods=["POST"])
def spark_openivm_sources_init():
    """Create database + tracked Delta tables and load batch1 data via DML."""
    try:
        result = spark_openivm_sources.init_sources()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[spark-openivm] Source init failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/spark-openivm/append/<int:batch_num>", methods=["POST"])
def spark_openivm_sources_append(batch_num):
    """INSERT new batch{N} rows into each tracked staging table."""
    try:
        result = spark_openivm_sources.append_sources(batch_num)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[spark-openivm] Source append batch %d failed", batch_num)
        return jsonify({"status": "error", "error": str(e)}), 500


class SparkOpenIVMHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
