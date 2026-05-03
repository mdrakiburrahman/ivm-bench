"""DuckDB-OpenIVM handler — source management endpoints.

The standard /run/<engine> route handles dbt builds.
This handler provides source table management (init/append) that the
benchmark-server calls before triggering dbt builds.
"""

import logging

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services.duckdb_openivm_sources import append_sources, init_sources

logger = logging.getLogger(__name__)

bp = Blueprint("duckdb_openivm", __name__)


@bp.route("/sources/duckdb-openivm/init", methods=["POST"])
def sources_init():
    """Create DuckLake schemas and batch-1 source tables.

    Called by the benchmark-server before the first dbt build.
    """
    try:
        result = init_sources()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[duckdb-openivm] Source init failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/duckdb-openivm/append/<int:batch_num>", methods=["POST"])
def sources_append(batch_num):
    """INSERT new batch data into staging tables.

    Called by the benchmark-server before dbt builds for batch 2/3.
    """
    try:
        result = append_sources(batch_num)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[duckdb-openivm] Source append batch %d failed", batch_num)
        return jsonify({"status": "error", "error": str(e)}), 500


class DuckDBOpenIVMHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
