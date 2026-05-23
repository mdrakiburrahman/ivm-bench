"""spark-openivm source-table management endpoints.

The benchmark-server calls these before each dbt build:
  - POST /sources/spark-openivm/init           (before batch 1 dbt build)
  - POST /sources/spark-openivm/append/<N>     (before batch 2/3 dbt builds)

Plus a post-batch correctness validation endpoint, mirroring the
duckdb-openivm path; called when OPENIVM_VALIDATE=1:
  - POST /validate/spark-openivm/<run_id>

The standard /run/<engine> route handles dbt builds.
"""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services import spark_openivm_sources, spark_openivm_validation

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


@bp.route("/validate/spark-openivm/<run_id>", methods=["POST"])
def spark_openivm_validate_run(run_id):
    """Validate OpenIVM materialized views against compiled dbt SQL.

    Mirrors /validate/duckdb-openivm/<run_id>. EXCEPT-ALL comparison runs
    through a Livy SQL session against the spark-openivm Spark cluster.
    """
    try:
        result = spark_openivm_validation.validate_run(run_id)
        status_code = 200 if result["status"] == "passed" else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.exception("[spark-openivm] Validation failed")
        return jsonify({"status": "error", "error": str(e)}), 500


class SparkOpenIVMHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
