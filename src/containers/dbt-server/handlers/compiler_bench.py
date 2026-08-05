"""compiler-bench routes.

POST starts a run in a background thread and returns a run id; GET polls it.
A corpus run is long (thousands of queries), so it cannot live inside one
request. Results are returned in the response body rather than written to disk:
the benchmark-server owns the artifact layout, as it does for the profile and
query-log exports.
"""

import logging
import os
import threading
import uuid
from typing import Dict

from flask import Blueprint, Flask, jsonify, request

from handlers.base import BaseHandler
from services.compiler_bench import (
    CSV_COLUMNS,
    CompilerBenchRunner,
    get_adapter,
    load,
    result_to_row,
)

logger = logging.getLogger(__name__)

bp = Blueprint("compiler_bench", __name__)

_RUNS: Dict[str, dict] = {}
_RUNS_LOCK = threading.Lock()


def _set(run_id: str, **fields) -> None:
    with _RUNS_LOCK:
        _RUNS.setdefault(run_id, {}).update(fields)


def _execute(run_id: str, engine: str, options: dict) -> None:
    try:
        # The adapters read the storage mode from the environment, matching how
        # every other engine knob reaches them.
        if options["ducklake"]:
            os.environ["COMPILER_BENCH_DUCKLAKE"] = "1"
        corpus = load(engine, limit=options["limit"])
        _set(run_id, status="running", total=len(corpus.queries), completed=0)

        runner = CompilerBenchRunner(
            get_adapter(engine),
            corpus,
            timeout_s=options["timeout_s"],
            delta_batch_size=options["delta_batch_size"],
            verify=options["verify"],
            progress=lambda p: _set(run_id, completed=p["completed"]),
        )
        summary = runner.run()
        _set(
            run_id,
            status="completed",
            summary=summary,
            columns=CSV_COLUMNS,
            rows=[result_to_row(r) for r in runner.results],
        )
        logger.info(
            "[compiler-bench] %s completed: %s", engine, summary.get("totals")
        )
    except Exception as exc:
        logger.exception("[compiler-bench] %s failed", engine)
        _set(run_id, status="error", error=str(exc))


@bp.route("/compiler-bench/<engine>", methods=["POST"])
def start(engine: str):
    body = request.get_json(force=True, silent=True) or {}
    options = {
        "limit": int(body.get("limit", 0) or 0),
        "timeout_s": float(body.get("timeout_s", 60.0)),
        "delta_batch_size": int(body.get("delta_batch_size", 10)),
        "verify": bool(body.get("verify", True)),
        "ducklake": bool(body.get("ducklake", False)),
    }
    run_id = str(uuid.uuid4())
    _set(run_id, status="queued", engine=engine, options=options)
    threading.Thread(
        target=_execute, args=(run_id, engine, options), daemon=True
    ).start()
    return jsonify({"run_id": run_id, "status": "queued", "engine": engine}), 202


@bp.route("/compiler-bench/runs/<run_id>", methods=["GET"])
def get_run(run_id: str):
    with _RUNS_LOCK:
        run = _RUNS.get(run_id)
        payload = dict(run) if run else None
    if payload is None:
        return jsonify({"error": "not found"}), 404
    # Rows are only useful once, and they are large — the caller asks for them
    # explicitly so status polling stays cheap.
    if request.args.get("include_rows", "0") not in ("1", "true", "yes"):
        payload.pop("rows", None)
    return jsonify(payload), 200


class CompilerBenchHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
