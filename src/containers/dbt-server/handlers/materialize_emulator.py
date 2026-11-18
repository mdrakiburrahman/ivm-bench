"""Native source lifecycle and completion endpoints for Materialize Emulator."""

import logging
import os
from pathlib import Path

import psycopg2
from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services.materialize_fence import fence_outputs
from services.materialize_sources import append_source_tables, init_source_tables
from services.materialize_validation import validate_outputs
from sql_catalog import load_sql

logger = logging.getLogger(__name__)
bp = Blueprint("materialize_emulator", __name__)


def _connection():
    connection = psycopg2.connect(
        host=os.environ.get("MATERIALIZE_HOST", "materialize-emulator"),
        port=int(os.environ.get("MATERIALIZE_PORT", "6875")),
        user=os.environ.get("MATERIALIZE_USER", "materialize"),
        dbname=os.environ.get("MATERIALIZE_DATABASE", "materialize"),
        connect_timeout=10,
    )
    connection.autocommit = True
    return connection


def _execute(action, batch_num=None):
    connection = None
    try:
        connection = _connection()
        raw = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
        if action == "init":
            result = init_source_tables(connection, raw)
        elif action == "append":
            result = append_source_tables(connection, raw, batch_num)
        elif action == "fence":
            result = fence_outputs(connection)
        elif action == "validate":
            result = validate_outputs(connection)
        else:
            with connection.cursor() as cursor:
                cursor.execute(load_sql("materialize-emulator", "inline", "dbt_server/services_materialize_fence/replica_size"))
                rows = cursor.fetchall()
            if len(rows) != 1:
                raise RuntimeError(f"Expected one Materialize cluster replica, got {len(rows)}")
            cluster, size, processes, workers, cpu, memory = rows[0]
            processes, workers, cpu, memory = map(int, (processes, workers, cpu, memory))
            requested = os.environ["MATERIALIZE_EMULATOR_CLUSTER_SIZE"]
            cpu_budget = int(os.environ["MATERIALIZE_EMULATOR_CPUS"])
            memory_budget = int(os.environ["MATERIALIZE_EMULATOR_MEM"].removesuffix("g")) * 1024**3
            if size != requested or cpu > cpu_budget * 10**9 or memory > memory_budget:
                raise RuntimeError(
                    f"Materialize replica {size} ({cpu} nanocores, {memory} bytes) "
                    f"does not match requested {requested} within {cpu_budget} CPUs/{memory_budget} bytes"
                )
            result = {
                "cluster": cluster, "size": size, "processes": processes,
                "workers": workers, "cpu_nano_cores": cpu, "memory_bytes": memory,
            }
        return jsonify(result)
    except Exception as exc:
        logger.exception("Materialize Emulator %s failed", action)
        return jsonify({"status": "error", "error": str(exc)}), 500
    finally:
        if connection is not None:
            connection.close()


@bp.route("/sources/materialize-emulator/init", methods=["POST"])
def init():
    return _execute("init")


@bp.route("/sources/materialize-emulator/append/<int:batch_num>", methods=["POST"])
def append(batch_num):
    return _execute("append", batch_num)


@bp.route("/sources/materialize-emulator/fence", methods=["POST"])
def fence():
    return _execute("fence")


@bp.route("/validate/materialize-emulator", methods=["POST"])
def validate():
    return _execute("validate")


@bp.route("/status/materialize-emulator", methods=["POST"])
def status():
    return _execute("status")


class MaterializeEmulatorHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
