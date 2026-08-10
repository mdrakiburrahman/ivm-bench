"""Container stats handler — REST API for starting/stopping stats collection."""

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services import container_stats
from services.container_stats import collector

bp = Blueprint("container_stats", __name__)


@bp.route("/stats/containers/start", methods=["POST"])
def start_stats():
    """Start collecting container stats.

    Body: {"engine": "spark", "scale_factor": 3, "poll_interval_s": 15}
    """
    body = request.get_json(force=True, silent=True) or {}
    engine = body.get("engine")
    scale_factor = body.get("scale_factor", 3)
    poll_interval = body.get("poll_interval_s", 15)

    if not engine:
        return jsonify({"error": "engine is required"}), 400

    started = collector.start(engine, scale_factor, poll_interval)
    if not started:
        return jsonify({"error": "stats collection already running", "status": collector.get_status()}), 409

    return jsonify({"status": "started", "engine": engine, "poll_interval_s": poll_interval}), 200


@bp.route("/stats/containers/stop", methods=["POST"])
def stop_stats():
    """Stop collecting container stats and persist results."""
    body = request.get_json(silent=True) or {}
    samples = collector.stop()
    summaries = []
    services = body.get("included_services") or []
    for window in body.get("batch_windows") or []:
        summary = container_stats.summarize_cpu_seconds(
            samples,
            int(window["start_ms"]),
            int(window["end_ms"]),
            [str(service) for service in services],
        )
        summary["batch_num"] = int(window["batch_num"])
        summaries.append(summary)
    return jsonify({
        "status": "stopped",
        "sample_count": len(samples),
        "batch_summaries": summaries,
    }), 200


@bp.route("/stats/containers")
def get_stats_status():
    """Get current stats collection status."""
    return jsonify(collector.get_status())


class ContainerStatsHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
