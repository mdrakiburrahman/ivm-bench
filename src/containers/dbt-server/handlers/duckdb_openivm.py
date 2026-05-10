"""DuckDB and DuckDB-OpenIVM DuckLake source management endpoints.

The standard /run/<engine> route handles dbt builds.
This handler provides source table management that the benchmark-server
calls before triggering dbt builds.
"""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services import duckdb_openivm_sources, duckdb_sources, openivm_validation

logger = logging.getLogger(__name__)

bp = Blueprint("duckdb_openivm", __name__)


@bp.route("/sources/duckdb/init", methods=["POST"])
def duckdb_sources_init():
    """Create DuckLake schemas and batch-1 source tables for DuckDB."""
    try:
        result = duckdb_sources.init_sources()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[duckdb] Source init failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/duckdb/append/<int:batch_num>", methods=["POST"])
def duckdb_sources_append(batch_num):
    """INSERT new batch data into DuckDB's DuckLake staging tables."""
    try:
        result = duckdb_sources.append_sources(batch_num)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[duckdb] Source append batch %d failed", batch_num)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/duckdb-openivm/init", methods=["POST"])
def openivm_sources_init():
    """Create DuckLake schemas and batch-1 source tables.

    Called by the benchmark-server before the first dbt build.
    """
    try:
        result = duckdb_openivm_sources.init_sources()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[duckdb-openivm] Source init failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/duckdb-openivm/append/<int:batch_num>", methods=["POST"])
def openivm_sources_append(batch_num):
    """INSERT new batch data into staging tables.

    Called by the benchmark-server before dbt builds for batch 2/3.
    """
    try:
        result = duckdb_openivm_sources.append_sources(batch_num)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[duckdb-openivm] Source append batch %d failed", batch_num)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/validate/duckdb-openivm/<run_id>", methods=["POST"])
def openivm_validate_run(run_id):
    """Validate OpenIVM materialized views against compiled dbt SQL."""
    try:
        result = openivm_validation.validate_run(run_id)
        status_code = 200 if result["status"] == "passed" else 500
        return jsonify(result), status_code
    except Exception as e:
        logger.exception("[duckdb-openivm] Validation failed")
        return jsonify({"status": "error", "error": str(e)}), 500


class DuckDBOpenIVMHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
