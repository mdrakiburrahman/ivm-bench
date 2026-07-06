"""Benchmark handler — start, stream, and status endpoints."""

import logging
import time

from flask import Blueprint, Flask, Response, jsonify, request, stream_with_context

from handlers.base import BaseHandler
from services.orchestrator import get_orchestrator

logger = logging.getLogger(__name__)
bp = Blueprint("benchmark", __name__)


@bp.route("/benchmark", methods=["POST"])
def start_benchmark():
    """Start a benchmark run. Optionally override config via JSON body."""
    orch = get_orchestrator()
    if orch.is_running:
        return jsonify({"error": "Benchmark already running"}), 409

    body = request.get_json(silent=True) or {}
    if body:
        logger.info("POST /benchmark body=%s", body)
        orch.update_config(**{
            k: v for k, v in body.items()
            if k in ("parallel", "engines", "benchmark_runs")
        })

    orch.start()
    return jsonify({"status": "started", "config": _config_summary(orch)}), 202


@bp.route("/benchmark/stream", methods=["GET"])
def stream_benchmark():
    """SSE endpoint that streams benchmark progress."""
    orch = get_orchestrator()

    # Auto-start if not running yet
    if not orch.is_running and orch.result.status == "pending":
        logger.info("GET /benchmark/stream — auto-starting benchmark")
        orch.start()

    def generate():
        while True:
            logs = orch.get_logs()
            for line in logs:
                if line == "__DONE__":
                    yield f"event: done\ndata: {orch.result.status}\n\n"
                    return
                yield f"event: progress\ndata: {line}\n\n"
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
    """Get current benchmark status and results."""
    orch = get_orchestrator()
    result = orch.result.to_dict()
    logger.info("GET /benchmark/status → status=%s", result.get("status"))
    return jsonify(result)


def _config_summary(orch) -> dict:
    """Return a safe summary of the current config for API responses."""
    cfg = orch.config
    return {
        "scale_factor": cfg.scale_factor,
        "benchmark_runs": cfg.benchmark_runs,
        "engines": cfg.engines,
        "parallel": cfg.parallel,
    }


class BenchmarkHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
