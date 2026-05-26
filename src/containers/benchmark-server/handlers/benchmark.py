"""Benchmark handler — start, stream, and status endpoints."""

import logging
import os
import time

from flask import Blueprint, Flask, Response, jsonify, request, stream_with_context

from handlers.base import BaseHandler
from services.orchestrator import get_orchestrator

logger = logging.getLogger(__name__)
bp = Blueprint("benchmark", __name__)


def _resolve_experiments_file(body: dict) -> str:
    """Return an experiments-file path or '' for the back-compat (env-only) path.

    Priority:
      1. POST body field ``experiments_file``
      2. ``BENCHMARK_EXPERIMENTS_FILE`` env var (set by benchmark.sh /
         docker-compose.benchmark-server.yml when running an OAT sweep)
      3. None — use the classic env-var path (SCALE_FACTOR + BATCH_*)
    """
    if body and body.get("experiments_file"):
        return body["experiments_file"]
    return (os.environ.get("BENCHMARK_EXPERIMENTS_FILE", "") or "").strip()


@bp.route("/benchmark", methods=["POST"])
def start_benchmark():
    """Start a benchmark run.

    Body fields (all optional):
      * ``experiments_file``  — absolute path inside the benchmark-server
                                container of an OAT experiments JSON.
      * ``parallel`` / ``engines`` — legacy single-experiment overrides
                                (ignored when ``experiments_file`` is set).
    """
    orch = get_orchestrator()
    if orch.is_running:
        return jsonify({"error": "Benchmark already running"}), 409

    body = request.get_json(silent=True) or {}
    if body:
        logger.info("POST /benchmark body=%s", body)

    experiments_file = _resolve_experiments_file(body)
    if experiments_file and not os.path.exists(experiments_file):
        return jsonify({
            "error": f"experiments_file not found at {experiments_file} "
                     f"(in benchmark-server container)",
        }), 404

    # Legacy overrides only meaningful for single-experiment mode.
    if not experiments_file and body:
        orch.update_config(**{
            k: v for k, v in body.items() if k in ("parallel", "engines")
        })

    try:
        orch.start(experiments_file=experiments_file or None)
    except FileNotFoundError as e:
        return jsonify({"error": f"experiments_file not found: {e}"}), 404
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    return jsonify({
        "status": "started",
        "mode": "oat" if experiments_file else "single",
        "experiments_file": experiments_file or None,
        "oat_run_id": orch._oat_run_id,
        "config": _config_summary(orch),
    }), 202


@bp.route("/benchmark/stream", methods=["GET"])
def stream_benchmark():
    """SSE endpoint that streams benchmark progress."""
    orch = get_orchestrator()

    # Auto-start if not running yet. Picks up BENCHMARK_EXPERIMENTS_FILE if set.
    if not orch.is_running and orch.result.status == "pending":
        experiments_file = _resolve_experiments_file({})
        logger.info(
            "GET /benchmark/stream — auto-starting (mode=%s, file=%r)",
            "oat" if experiments_file else "single",
            experiments_file or None,
        )
        try:
            orch.start(experiments_file=experiments_file or None)
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
    """Get current benchmark status and results."""
    orch = get_orchestrator()
    result = orch.result.to_dict()
    # In OAT mode add a top-level pointer so callers can find the artifact dir.
    if orch._oat_run_id:
        result["oat_run_id"] = orch._oat_run_id
        result["experiments_file"] = orch._experiments_file
    logger.info("GET /benchmark/status → status=%s", result.get("status"))
    return jsonify(result)


def _config_summary(orch) -> dict:
    """Return a safe summary of the current config for API responses."""
    cfg = orch.config
    return {
        "scale_factor": cfg.scale_factor,
        "engines": cfg.engines,
        "parallel": cfg.parallel,
    }


class BenchmarkHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)

