"""Health check handler."""

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler

bp = Blueprint("health", __name__)


@bp.route("/health")
def health():
    return jsonify({"status": "ok"})


class HealthHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
