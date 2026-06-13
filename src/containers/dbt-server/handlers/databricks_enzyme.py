"""databricks-enzyme source / cleanup / pre-flight endpoints.

The benchmark-server calls these around each dbt build:

  - POST /sources/databricks-enzyme/init/<sf>
        Idempotent per-SF Delta upload + source-table registration.
  - POST /sources/databricks-enzyme/append/<batch_num>/<sf>
        Sync new staging Delta files and (if CTAS strategy) INSERT INTO.
  - POST /sources/databricks-enzyme/cleanup-schema
        DROP all 5 per-experiment schemas (data + bronze/silver/gold/work)
        CASCADE — kills all MVs so no background refresh keeps billing.
  - POST /sources/databricks-enzyme/cleanup-volume/<sf>
        Remove the cache `sf=<sf>/` subdirectory recursively (used when
        we want to force a per-SF cache rebuild; not normally needed).
  - POST /sources/databricks-enzyme/cleanup-all
        End-of-sweep teardown: every `exp_*` schema + the entire cache
        volume.
  - POST /sources/databricks-enzyme/sweep-stale
        Drop `exp_<ts>_*` schemas older than 1 day. Called at the start
        of every experiment to recover from crashed prior experiments.
  - POST /sources/databricks-enzyme/warmup
        SELECT 1 against the warehouse so the timer doesn't pay cold-start.
  - POST /metrics/databricks-enzyme/<batch_num>
        Capture Delta history / Enzyme refresh stats for the batch's MVs.

  - POST /validate/databricks-enzyme/explain-create-materialized-view/<sf>
        Pre-flight incrementalizability gate. Runs EXPLAIN CREATE
        MATERIALIZED VIEW ... REFRESH POLICY INCREMENTAL STRICT AS
        <compiled_sql> for every model in the databricks-enzyme dbt
        project. Returns 200 if every model is incrementalizable, 422
        if any model fails. Called by the benchmark-server BEFORE the
        batch-1 timer starts so failures fail the engine without
        polluting timing data.

The standard /run/<engine> route handles the dbt build itself.
"""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services import databricks_enzyme_sources

logger = logging.getLogger(__name__)

bp = Blueprint("databricks_enzyme", __name__)


@bp.route("/sources/databricks-enzyme/init/<int:sf>", methods=["POST"])
def init(sf: int):
    try:
        return jsonify(databricks_enzyme_sources.init_sources(sf)), 200
    except Exception as e:
        logger.exception("[databricks-enzyme] init sf=%d failed", sf)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route(
    "/sources/databricks-enzyme/append/<int:batch_num>/<int:sf>",
    methods=["POST"],
)
def append(batch_num: int, sf: int):
    try:
        result = databricks_enzyme_sources.append_sources(batch_num, sf)
        return jsonify(result), 200
    except Exception as e:
        logger.exception(
            "[databricks-enzyme] append batch=%d sf=%d failed", batch_num, sf,
        )
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/databricks-enzyme/cleanup-schema", methods=["POST"])
def cleanup_schema():
    try:
        return jsonify(databricks_enzyme_sources.cleanup_schemas()), 200
    except Exception as e:
        logger.exception("[databricks-enzyme] cleanup-schema failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route(
    "/sources/databricks-enzyme/cleanup-volume/<int:sf>", methods=["POST"],
)
def cleanup_volume(sf: int):
    try:
        return jsonify(databricks_enzyme_sources.cleanup_volume_for_sf(sf)), 200
    except Exception as e:
        logger.exception("[databricks-enzyme] cleanup-volume sf=%d failed", sf)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/databricks-enzyme/cleanup-all", methods=["POST"])
def cleanup_all():
    try:
        return jsonify(databricks_enzyme_sources.cleanup_all()), 200
    except Exception as e:
        logger.exception("[databricks-enzyme] cleanup-all failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/databricks-enzyme/sweep-stale", methods=["POST"])
def sweep_stale():
    """Drop per-experiment schemas from prior crashed/abandoned experiments.

    Called by the benchmark-server at experiment START (before
    init/<sf>). Lists every ``exp_<microsec_ts>_*`` schema in the
    configured catalog, parses the embedded microsecond timestamp,
    and drops CASCADE anything older than
    ``DATABRICKS_STALE_MAX_AGE_SECONDS`` (default 86400 = 1 day).
    Active experiments are safe by construction (freshly-minted IDs
    are always age 0).

    Idempotent. Safe to call concurrently from multiple experiments —
    ``DROP SCHEMA IF EXISTS … CASCADE`` is the only mutation.
    """
    try:
        return jsonify(databricks_enzyme_sources.sweep_stale_schemas()), 200
    except Exception as e:
        logger.exception("[databricks-enzyme] sweep-stale failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/databricks-enzyme/warmup", methods=["POST"])
def warmup():
    try:
        return jsonify(databricks_enzyme_sources.warmup()), 200
    except Exception as e:
        logger.exception("[databricks-enzyme] warmup failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route(
    "/metrics/databricks-enzyme/<int:batch_num>", methods=["POST"],
)
def metrics(batch_num: int):
    from services import databricks_enzyme_metrics

    try:
        return jsonify(
            databricks_enzyme_metrics.collect_refresh_metrics(batch_num)
        ), 200
    except Exception as e:
        logger.exception(
            "[databricks-enzyme] metrics capture failed for batch=%d",
            batch_num,
        )
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route(
    "/validate/databricks-enzyme/explain-create-materialized-view/<int:sf>",
    methods=["POST"],
)
def explain_create_materialized_view(sf: int):
    """Pre-flight incrementalizability gate.

    Runs `EXPLAIN CREATE MATERIALIZED VIEW ... REFRESH POLICY INCREMENTAL
    STRICT AS <compiled_sql>` for every non-ephemeral model in the
    databricks-enzyme dbt project against the Databricks Serverless SQL
    warehouse and returns one of:
      - 200 {status: "ok", ...}     every model is incrementalizable
      - 422 {status: "error", ...}  at least one model is NOT
      - 500 {status: "error", ...}  infrastructure/compile failure

    The benchmark-server calls this AFTER `init/<sf>` and BEFORE the
    batch-1 timer starts.
    """
    from services import databricks_enzyme_explain

    try:
        result = databricks_enzyme_explain.explain_all_models(sf)
    except Exception as e:
        logger.exception(
            "[databricks-enzyme] explain-create-materialized-view sf=%d failed",
            sf,
        )
        return jsonify({"status": "error", "error": str(e)}), 500

    http_code = 200 if result.get("status") == "ok" else 422
    return jsonify(result), http_code


@bp.route(
    "/sources/databricks-enzyme/pipeline-events/<int:batch_num>",
    methods=["POST"],
)
def pipeline_events(batch_num: int):
    from services import databricks_enzyme_pipeline_events

    try:
        return jsonify(
            databricks_enzyme_pipeline_events.collect_pipeline_events(batch_num)
        ), 200
    except Exception as e:
        logger.exception(
            "[databricks-enzyme] pipeline-events capture failed for batch=%d",
            batch_num,
        )
        return jsonify({"status": "error", "error": str(e)}), 500


class DatabricksEnzymeHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
