"""
benchmark-server: Flask REST API that orchestrates the full IVM benchmark.

Replaces the 640+ line benchmark.sh with a Dockerized server that
coordinates datagen, engine builds, batch loading, and dbt runs.
"""

import logging
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, g, request

from handlers.health import HealthHandler
from handlers.benchmark import BenchmarkHandler
from handlers.chart import ChartHandler
from services.db import init_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("benchmark-server")

app = Flask(__name__)


@app.before_request
def log_request():
    g.start_time = time.time()
    body = ""
    if request.content_length and request.content_length < 4096:
        body = f" body={request.get_data(as_text=True)}"
    logger.info(">>> %s %s%s", request.method, request.full_path.rstrip("?"), body)


@app.after_request
def log_response(response):
    duration_ms = (time.time() - g.get("start_time", time.time())) * 1000
    body = ""
    if response.content_length and response.content_length < 4096 and response.content_type and "json" in response.content_type:
        body = f" body={response.get_data(as_text=True)}"
    logger.info(
        "<<< %s %s → %d (%.1fms)%s",
        request.method, request.full_path.rstrip("?"),
        response.status_code, duration_ms, body,
    )
    return response


def register_handlers(flask_app: Flask) -> None:
    """Register all route handlers."""
    handlers = [
        HealthHandler(),
        BenchmarkHandler(),
        ChartHandler(),
    ]
    for handler in handlers:
        handler.register(flask_app)


register_handlers(app)
init_db()
