"""Benchmark handler — start, stream, and status endpoints (OAT-only)."""

import logging
import os
import time

from flask import Blueprint, Flask, Response, jsonify, request, stream_with_context

from handlers.base import BaseHandler
from services.orchestrator import get_orchestrator

logger = logging.getLogger(__name__)
bp = Blueprint("benchmark", __name__)


def _resolve_experiments_file(body: dict) -> str:
    """Return an experiments-file path. Empty string means 'no file provided'.

    Priority:
      1. POST body field ``experiments_file``
      2. ``BENCHMARK_EXPERIMENTS_FILE`` env var (set by benchmark.sh /
         docker-compose.benchmark-server.yml)
    """
    if body and body.get("experiments_file"):
        return body["experiments_file"]
    return (os.environ.get("BENCHMARK_EXPERIMENTS_FILE", "") or "").strip()


@bp.route("/benchmark", methods=["POST"])
def start_benchmark():
    """Start an OAT sweep.

    Body fields (all optional):
      * ``experiments_file``  — absolute path inside the benchmark-server
                                container of an experiments JSON. Falls
                                back to ``BENCHMARK_EXPERIMENTS_FILE``.
    """
    orch = get_orchestrator()
    if orch.is_running:
        return jsonify({"error": "Benchmark already running"}), 409

    body = request.get_json(silent=True) or {}
    if body:
        logger.info("POST /benchmark body=%s", body)

    experiments_file = _resolve_experiments_file(body)
    if not experiments_file:
        return jsonify({
            "error": "experiments_file is required — pass it in the POST body "
                     "or set the BENCHMARK_EXPERIMENTS_FILE env var",
        }), 400
    if not os.path.exists(experiments_file):
        return jsonify({
            "error": f"experiments_file not found at {experiments_file} "
                     f"(in benchmark-server container)",
        }), 404

    try:
        orch.start(experiments_file=experiments_file)
    except FileNotFoundError as e:
        return jsonify({"error": f"experiments_file not found: {e}"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({
        "status": "started",
        "experiments_file": experiments_file,
        "oat_run_id": orch._oat_run_id,
    }), 202


@bp.route("/benchmark/stream", methods=["GET"])
def stream_benchmark():
    """SSE endpoint that streams OAT progress."""
    orch = get_orchestrator()

    # Auto-start if not running yet. Picks up BENCHMARK_EXPERIMENTS_FILE.
    if not orch.is_running and orch.result.status == "pending":
        experiments_file = _resolve_experiments_file({})
        if not experiments_file:
            return jsonify({
                "error": "experiments_file is required — set BENCHMARK_EXPERIMENTS_FILE "
                         "before hitting /benchmark/stream",
            }), 400
        logger.info("GET /benchmark/stream — auto-starting OAT (file=%r)", experiments_file)
        try:
            orch.start(experiments_file=experiments_file)
        except (FileNotFoundError, ValueError, RuntimeError) as e:
            return jsonify({"error": str(e)}), 400

    def generate():
        while True:
            logs = orch.get_logs()
            for line in logs:
                if line == "__DONE__":
                    yield f"event: done\ndata: {orch.result.status}\n\n"
                    return
                # SSE data: lines must not contain raw newlines; split.
                for sub in line.splitlines() or [line]:
                    yield f"event: progress\ndata: {sub}\n\n"
            if not orch.is_running and not logs:
                yield f"event: done\ndata: {orch.result.status}\n\n"
                return
            time.sleep(0.5)

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@bp.route("/benchmark/status", methods=["GET"])
def benchmark_status():
    """Get current OAT status and results."""
    orch = get_orchestrator()
    result = orch.result.to_dict()
    if orch._oat_run_id:
        result["oat_run_id"] = orch._oat_run_id
        result["experiments_file"] = orch._experiments_file
    logger.info("GET /benchmark/status → status=%s", result.get("status"))
    return jsonify(result)


class BenchmarkHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)

