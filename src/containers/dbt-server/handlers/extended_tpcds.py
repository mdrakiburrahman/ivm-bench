"""Batch execution for the Extended TPC-DS OAT workload."""
import logging
from flask import Blueprint, Flask, jsonify
from handlers.base import BaseHandler

bp = Blueprint("extended_tpcds", __name__)

@bp.route("/workloads/extended-tpcds/<engine>/batch/<int:batch_num>", methods=["POST"])
def run(engine, batch_num):
    try:
        from services.extended_tpcds import _LOCK, run_batch
        with _LOCK:
            result = run_batch(engine, batch_num, validate_results=False)
        return jsonify(result), 200 if result["status"] == "completed" else 422
    except Exception as error:
        logging.exception("Extended TPC-DS batch failed")
        return jsonify({"status":"failed", "error":str(error)}), 500

@bp.route("/workloads/extended-tpcds/<engine>/validate/<int:batch_num>", methods=["POST"])
def validate(engine, batch_num):
    try:
        from services.extended_tpcds import _LOCK, validate_batch
        with _LOCK:
            result = validate_batch(engine, batch_num)
        return jsonify(result), 200 if result["status"] == "passed" else 422
    except Exception as error:
        logging.exception("Extended TPC-DS validation failed")
        return jsonify({"status":"failed", "error":str(error)}), 500

class ExtendedTPCDSHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
