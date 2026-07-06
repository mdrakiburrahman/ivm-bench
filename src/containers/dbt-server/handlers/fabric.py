"""Microsoft Fabric source-cache + Environment endpoints for the
``fabric-openivm-jvm-35`` / ``fabric-jvm-35`` engines.

The benchmark-server calls these around each dbt build:

  - POST /environment/fabric/refresh
        Upload the openivm JAR + push a fresh Spark config into Environment
        "35", then publish. openivm engine only, before batch 1.
  - POST /sources/fabric/init/<sf>
        azcopy-stage the batch-1 + initial-staging + audit Delta dirs into the
        lakehouse ``Files/_shared_cache/tpcdi_raw_cache/sf=<N>/`` cache.
  - POST /sources/fabric/append/<batch_num>/<sf>
        azcopy-stage the per-batch staging increment into the cache.
  - POST /sources/fabric/cleanup
        Blow-up: drop everything under ``Tables/`` + the openivm state under
        ``Files/_openivm`` so the next experiment starts clean.
  - POST /sources/fabric/cleanup-cache/<sf>
        Drop the ``sf=<sf>`` cache subdir (used when the SF changes).

The source-table CREATE/INSERT SQL itself runs inside the dbt Livy session via
the ``load_fabric_sources`` on-run-start macro. The standard /run/<engine>
route handles the dbt build.
"""

import logging

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services import fabric

logger = logging.getLogger(__name__)

bp = Blueprint("fabric", __name__)


@bp.route("/environment/fabric/refresh", methods=["POST"])
def refresh_environment():
    try:
        return jsonify(fabric.refresh_environment()), 200
    except Exception as e:
        logger.exception("[fabric] environment refresh failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/fabric/init/<int:sf>", methods=["POST"])
def init(sf: int):
    try:
        return jsonify(fabric.seed_cache_init(sf)), 200
    except Exception as e:
        logger.exception("[fabric] cache init sf=%d failed", sf)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/fabric/append/<int:batch_num>/<int:sf>", methods=["POST"])
def append(batch_num: int, sf: int):
    try:
        return jsonify(fabric.seed_cache_batch(sf, batch_num)), 200
    except Exception as e:
        logger.exception("[fabric] cache append batch=%d sf=%d failed", batch_num, sf)
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/fabric/cleanup", methods=["POST"])
def cleanup():
    try:
        return jsonify(fabric.cleanup_tables_and_state()), 200
    except Exception as e:
        logger.exception("[fabric] cleanup failed")
        return jsonify({"status": "error", "error": str(e)}), 500


@bp.route("/sources/fabric/cleanup-cache/<int:sf>", methods=["POST"])
def cleanup_cache(sf: int):
    try:
        return jsonify(fabric.cleanup_cache_for_sf(sf)), 200
    except Exception as e:
        logger.exception("[fabric] cleanup-cache sf=%d failed", sf)
        return jsonify({"status": "error", "error": str(e)}), 500


class FabricHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
