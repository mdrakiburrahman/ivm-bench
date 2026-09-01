"""DbspNet client — deploy/resume/wait against the DbspNet control service.

The DbspNet integration's compile-only bypass: instead of a dbt adapter deploying via
`dbt build`, we translate the dbt project into a program (dbt_to_program) and POST it to
the DbspNet control service (the .NET pipeline-manager analogue), then drive batches with
resume/wait. Simpler than feldera_client: DbspNet drains a batch to completion, so /wait
just blocks — no stats quiescence heuristics.
"""

import json
import logging
import os
import urllib.request

from services.dbt_to_program import build_program

logger = logging.getLogger(__name__)

DBSPNET_URL = os.environ.get("DBSPNET_URL", "http://dbspnet-server:8080")
DBSPNET_PROJECT_DIR = os.environ.get("DBSPNET_PROJECT_DIR", "/app/dbt-projects/dbspnet")


def _api_request(method: str, path: str, body: dict | None = None, timeout: int = 604800) -> dict:
    """Call the DbspNet control service. Raises on HTTP error."""
    url = f"{DBSPNET_URL}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, method=method, data=data)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else {}


def deploy(project_dir: str | None = None) -> dict:
    """Translate the dbt project → program, and deploy it (compile + wire connectors).

    Returns the DbspNet /deploy result (includes compile time). Timing-neutral: this is
    the one-time deploy, excluded from the measured batch (like Feldera's compile).
    """
    project_dir = project_dir or DBSPNET_PROJECT_DIR
    prog = build_program(project_dir)

    # Views dbt declares but does not bind to a sink: `+stored: true` with the output
    # connector removed (fact_market_history, daily_market_pulse — at SF=100 their
    # truncate-mode writers never drain). Feldera computes them and the benchmark measures
    # that compute; only the Delta write is skipped. DbspNet prunes any view that is not an
    # output, together with its upstream chain, so these have to be named explicitly or the
    # two engines are not running the same workload.
    bound = {b["view"] for b in prog["output_bindings"]}
    stored_views = [v for v in prog["outputs"] if v not in bound]
    spec = {
        "program": prog["program"],
        "inputs": prog["inputs"],
        "outputs": prog["output_bindings"],
        "storedViews": stored_views,
    }
    logger.info(
        "Deploying DbspNet program: %d statements, %d inputs, %d outputs, %d stored-only (%s)",
        len(spec["program"]), len(spec["inputs"]), len(spec["outputs"]),
        len(stored_views), ", ".join(stored_views) or "none",
    )
    return _api_request("POST", "/deploy", spec, timeout=1800)


def compile_check(project_dir: str | None = None) -> dict:
    """Dry-run compile (no connectors) — validate the DAG compiles."""
    prog = build_program(project_dir or DBSPNET_PROJECT_DIR)
    return _api_request("POST", "/compile", {"program": prog["program"], "outputs": prog["outputs"]}, timeout=1800)


def resume() -> dict:
    """Start ingesting the current batch (timer start). Returns {resumed_at_epoch_s}."""
    return _api_request("POST", "/resume", timeout=60)


def wait() -> dict:
    """Block until the batch has drained and outputs are written (timer stop).

    Returns {duration_s, ticks, outputs:[{view, rows, status}]}.
    """
    return _api_request("POST", "/wait")


def get_stats() -> dict | None:
    try:
        return _api_request("GET", "/stats", timeout=30)
    except Exception as exc:
        logger.warning("DbspNet /stats failed: %s", exc)
        return None
