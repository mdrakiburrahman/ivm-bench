"""RisingWave source management endpoints.

The standard /run/<engine> route handles dbt builds. This handler provides the
source table management the benchmark-server calls before triggering them.

RisingWave cannot read the raw Delta payload itself, so unlike the DuckDB
engines these endpoints stream the data in over pgwire — see
services/risingwave_sources.py.
"""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler

from services import risingwave_sources

logger = logging.getLogger(__name__)

bp = Blueprint("risingwave", __name__)


@bp.route("/sources/risingwave/init", methods=["POST"])
def risingwave_sources_init():
    """Create the tpcdi source tables and load batch 1."""
    try:
        result = risingwave_sources.init_sources()
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[risingwave] Source init failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/risingwave/append/<int:batch_num>", methods=["POST"])
def risingwave_sources_append(batch_num):
    """Mutate existing staging rows, then append this batch's rows.

    There is no refresh step to follow: every model in the RisingWave project is
    a MATERIALIZED VIEW, so the append propagates through the DAG on its own.
    """
    try:
        # Do not flush here: engine_runner times the append and the
        # propagation separately, and a flush inside the append would fold
        # the MV-graph work into the load time.
        result = risingwave_sources.append_sources(batch_num, flush=False)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("[risingwave] Source append batch %d failed", batch_num)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/risingwave/flush", methods=["POST"])
def risingwave_flush():
    """Block until every pending barrier has been applied to the MV graph.

    This is RisingWave's equivalent of the other engines' refresh wait: FLUSH
    returns once the appended rows are visible in the downstream views, which is
    what makes the batch timing comparable.
    """
    try:
        conn = risingwave_sources._connect()
        try:
            conn.cursor().execute("FLUSH")
        finally:
            conn.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception("[risingwave] Flush failed")
        return jsonify({"status": "error", "error": str(e)}), 500


class RisingWaveHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
