"""
dbt-server: Flask REST API that wraps dbt for benchmark runs.

Refactored into modular handlers, services, and models.
This file is the entry point — registers Blueprints and starts the app.
"""

import sys
import os

# Ensure the app directory is on the path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import Flask

from handlers.chart import ChartHandler
from handlers.feldera import FelderaHandler
from handlers.health import HealthHandler
from handlers.lineage import LineageHandler
from handlers.runs import RunsHandler
from handlers.sql_analysis import SQLAnalysisHandler
from services.db import init_db

app = Flask(__name__)


def register_handlers(flask_app: Flask) -> None:
    """Register all route handlers."""
    handlers = [
        HealthHandler(),
        RunsHandler(),
        FelderaHandler(),
        ChartHandler(),
        LineageHandler(),
        SQLAnalysisHandler(),
    ]
    for handler in handlers:
        handler.register(flask_app)


register_handlers(app)
init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
