"""
dbt-server: Flask REST API that wraps dbt for benchmark runs.

Refactored into modular handlers, services, and models.
This file is the entry point — registers Blueprints and starts the app.
"""

import logging
import sys
import os
import time

# Ensure the app directory is on the path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask, g, request

from handlers.chart import ChartHandler
from handlers.container_stats import ContainerStatsHandler
from handlers.databricks_enzyme import DatabricksEnzymeHandler
from handlers.dbspnet import DbspNetHandler
from handlers.delta_stats import DeltaStatsHandler
from handlers.fabric import FabricHandler
from handlers.feldera import FelderaHandler
from handlers.health import HealthHandler
from handlers.lineage import LineageHandler
from handlers.duckdb_openivm import DuckDBOpenIVMHandler
from handlers.query_plans import QueryPlansHandler
from handlers.spark_openivm import SparkOpenIVMHandler
from handlers.runs import RunsHandler
from handlers.sql_analysis import SQLAnalysisHandler
from services.db import init_db

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("dbt-server")

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
    # Log response body for non-streaming, reasonably sized responses
    body = ""
    if response.content_length and response.content_length < 4096 and response.content_type and "json" in response.content_type:
        body = f" body={response.get_data(as_text=True)}"
    logger.info("<<< %s %s → %d (%.1fms)%s",
                request.method, request.full_path.rstrip("?"),
                response.status_code, duration_ms, body)
    return response


def register_handlers(flask_app: Flask) -> None:
    """Register all route handlers."""
    handlers = [
        HealthHandler(),
        DuckDBOpenIVMHandler(),
        SparkOpenIVMHandler(),
        DatabricksEnzymeHandler(),
        FabricHandler(),
        RunsHandler(),
        FelderaHandler(),
        DbspNetHandler(),
        ChartHandler(),
        LineageHandler(),
        SQLAnalysisHandler(),
        ContainerStatsHandler(),
        DeltaStatsHandler(),
        QueryPlansHandler(),
    ]
    for handler in handlers:
        handler.register(flask_app)


register_handlers(app)
init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
