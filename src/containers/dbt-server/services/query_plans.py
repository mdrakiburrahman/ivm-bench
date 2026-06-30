"""Engine-agnostic EXPLAIN runner for capturing query plans.

Runs `EXPLAIN FORMATTED <compiled_sql>` for every model in a given dbt run
against the engine's native SQL endpoint and returns the plain-text plans
plus per-model metadata. Called from the benchmark-server *after* the
batch timer has stopped so the plan-capture cost is never charged to the
engine's measured latency.

Supported engines (matched against the standard engine names used in the
benchmark harness):

  - `databricks-enzyme` — uses databricks-sql-connector against the
                          warehouse defined in databricks_enzyme_sources
  - `spark`             — opens a fresh Livy `kind: sql` session at
                          http://spark:8998 (independent of the live
                          dbt-fabricspark session so the engine's own
                          driver is left untouched)
  - `spark-openivm`     - attaches to the long-lived Livy session from
                          spark_openivm_sources.LivyClient so the EXPLAIN
                          executes inside the same Spark driver that
                          owns the OpenIVM RocksDB catalog (and therefore
                          sees the rewritten MV query)
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import requests

from services.db import DB_LOCK, get_db

logger = logging.getLogger(__name__)

# Resource types we care about — same set the dbt-server already persists
# into `run_nodes`. We deliberately skip seeds / tests / snapshots because
# EXPLAIN on them is not interesting for IVM analysis.
_MODEL_RESOURCE_TYPES = ("model",)

_SPARK_LIVY_URL = os.environ.get("SPARK_LIVY_URL", "http://spark:8998")
_LIVY_OPEN_TIMEOUT_S = 600.0
_LIVY_STMT_TIMEOUT_S = 600.0


# ---------------------------------------------------------------------------
# Compiled SQL retrieval
# ---------------------------------------------------------------------------


def _fetch_compiled_sql(run_id: str) -> List[Dict[str, Any]]:
    """Return all successful model nodes from `run_nodes` for this run."""
    placeholders = ",".join(["?"] * len(_MODEL_RESOURCE_TYPES))
    with DB_LOCK:
        conn = get_db()
        rows = conn.execute(
            f"""SELECT unique_id, name, resource_type, status, compiled_sql
                FROM run_nodes
                WHERE run_id = ?
                  AND resource_type IN ({placeholders})
                  AND compiled_sql IS NOT NULL
                  AND compiled_sql <> ''
                ORDER BY name""",
            (run_id, *_MODEL_RESOURCE_TYPES),
        ).fetchall()
        conn.close()
    out = []
    for r in rows:
        out.append({
            "unique_id": r[0],
            "name": r[1],
            "resource_type": r[2],
            "status": r[3],
            "compiled_sql": r[4],
        })
    return out


# ---------------------------------------------------------------------------
# Plan-text normalisation
# ---------------------------------------------------------------------------


def _rows_to_plan_text(rows: List[List[Any]]) -> str:
    """EXPLAIN returns a single-column result; flatten to one big text block."""
    parts: List[str] = []
    for row in rows:
        if not row:
            continue
        cell = row[0]
        if cell is None:
            continue
        parts.append(str(cell))
    return "\n".join(parts).rstrip()


# ---------------------------------------------------------------------------
# Databricks plan capture
# ---------------------------------------------------------------------------


def _databricks_explain(sql: str) -> str:
    """Run EXPLAIN FORMATTED against the Databricks Serverless SQL warehouse.

    Uses the persistent connection that databricks_enzyme_sources already
    manages so we don't double-pay warehouse startup cost.
    """
    from services import databricks_enzyme_sources as src

    sql_stripped = sql.strip().rstrip(";")
    explain_sql = f"EXPLAIN FORMATTED {sql_stripped}"
    df = src._execute(explain_sql)
    if df is None or df.empty:
        return ""
    # databricks-sql-connector returns the plan in column 0 — one row per
    # plan line. Flatten to a single text block.
    return "\n".join(str(v) for v in df.iloc[:, 0].tolist()).rstrip()


def _collect_databricks_plans(
    nodes: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for node in nodes:
        if node.get("status") != "success":
            continue
        try:
            plan_text = _databricks_explain(node["compiled_sql"])
            successes.append({**node, "plan": plan_text})
        except Exception as exc:
            logger.warning(
                "[query-plans/databricks-enzyme] EXPLAIN failed for %s: %s",
                node.get("name"),
                exc,
            )
            failures.append({**node, "error": str(exc)})
    return successes, failures


# ---------------------------------------------------------------------------
# Livy-backed plan capture (Spark + Spark-OpenIVM)
# ---------------------------------------------------------------------------


def _livy_execute(base_url: str, session_id: str, sql: str) -> Dict[str, Any]:
    resp = requests.post(
        f"{base_url}/sessions/{session_id}/statements",
        json={"code": sql},
        timeout=_LIVY_OPEN_TIMEOUT_S,
    )
    resp.raise_for_status()
    stmt_id = resp.json()["id"]

    import time as _time

    started = _time.monotonic()
    while True:
        if _time.monotonic() - started > _LIVY_STMT_TIMEOUT_S:
            raise RuntimeError(
                f"Livy EXPLAIN statement {stmt_id} timed out after "
                f"{_LIVY_STMT_TIMEOUT_S}s"
            )
        sresp = requests.get(
            f"{base_url}/sessions/{session_id}/statements/{stmt_id}",
            timeout=_LIVY_OPEN_TIMEOUT_S,
        )
        sresp.raise_for_status()
        body = sresp.json()
        state = body.get("state")
        if state == "available":
            return body.get("output") or {}
        if state in ("error", "cancelled"):
            raise RuntimeError(f"Livy EXPLAIN failed: state={state} body={body}")
        _time.sleep(0.5)


def _livy_output_to_plan(output: Dict[str, Any]) -> str:
    """Spark Livy EXPLAIN returns the plan via `application/json` rows
    (Dataset[Row]) when run via `kind: sql`, OR as `text/plain` when the
    statement runs via spark.sql(...).show(). Handle both."""
    data = (output or {}).get("data") or {}
    payload = data.get("application/json")
    if isinstance(payload, dict):
        rows = payload.get("data") or []
        if rows:
            return _rows_to_plan_text(rows)
    text = data.get("text/plain") or ""
    return str(text).rstrip()


def _spark_open_explain_session() -> str:
    """Open a fresh Livy `kind: sql` session and return its id. Caller owns
    teardown via `_spark_close_explain_session`."""
    sess_resp = requests.post(
        f"{_SPARK_LIVY_URL}/sessions",
        json={"kind": "sql", "name": "explain-spark"},
        timeout=_LIVY_OPEN_TIMEOUT_S,
    )
    sess_resp.raise_for_status()
    session_id = str(sess_resp.json()["id"])

    import time as _time

    started = _time.monotonic()
    while True:
        if _time.monotonic() - started > _LIVY_OPEN_TIMEOUT_S:
            raise RuntimeError(
                "Livy explain session did not become idle in "
                f"{_LIVY_OPEN_TIMEOUT_S}s"
            )
        s = requests.get(
            f"{_SPARK_LIVY_URL}/sessions/{session_id}",
            timeout=_LIVY_OPEN_TIMEOUT_S,
        )
        s.raise_for_status()
        if s.json().get("state") == "idle":
            return session_id
        _time.sleep(1.0)


def _spark_close_explain_session(session_id: str) -> None:
    try:
        requests.delete(
            f"{_SPARK_LIVY_URL}/sessions/{session_id}",
            timeout=_LIVY_OPEN_TIMEOUT_S,
        )
    except Exception:
        logger.warning(
            "[query-plans/spark] failed to delete explain session %s",
            session_id,
        )


def _spark_explain_on_session(session_id: str, sql: str) -> str:
    sql_stripped = sql.strip().rstrip(";")
    output = _livy_execute(
        _SPARK_LIVY_URL,
        session_id,
        f"EXPLAIN FORMATTED {sql_stripped}",
    )
    return _livy_output_to_plan(output)


def _collect_spark_plans(
    nodes: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    runnable = [n for n in nodes if n.get("status") == "success"]
    if not runnable:
        return successes, failures
    try:
        session_id = _spark_open_explain_session()
    except Exception as exc:
        logger.warning(
            "[query-plans/spark] failed to open explain session: %s", exc
        )
        for node in runnable:
            failures.append({**node, "error": f"session open failed: {exc}"})
        return successes, failures
    try:
        for node in runnable:
            try:
                plan_text = _spark_explain_on_session(session_id, node["compiled_sql"])
                successes.append({**node, "plan": plan_text})
            except Exception as exc:
                logger.warning(
                    "[query-plans/spark] EXPLAIN failed for %s: %s",
                    node.get("name"),
                    exc,
                )
                failures.append({**node, "error": str(exc)})
    finally:
        _spark_close_explain_session(session_id)
    return successes, failures


def _spark_openivm_explain(sql: str) -> str:
    """Spark-OpenIVM plan capture — attaches to the shared LivyClient that
    spark_openivm_sources manages so the EXPLAIN runs against the SAME Spark
    driver that owns the OpenIVM RocksDB catalog. The plan therefore reflects
    any OpenIVM-rewritten relations / catalog state."""
    from services.spark_openivm_sources import LivyClient

    sql_stripped = sql.strip().rstrip(";")
    with LivyClient() as livy:
        result = livy.execute(f"EXPLAIN FORMATTED {sql_stripped}")
    return _livy_output_to_plan(result.get("output") or {})


def _collect_spark_openivm_plans(
    nodes: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    successes: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []
    for node in nodes:
        if node.get("status") != "success":
            continue
        try:
            plan_text = _spark_openivm_explain(node["compiled_sql"])
            successes.append({**node, "plan": plan_text})
        except Exception as exc:
            logger.warning(
                "[query-plans/spark-openivm] EXPLAIN failed for %s: %s",
                node.get("name"),
                exc,
            )
            failures.append({**node, "error": str(exc)})
    return successes, failures


# ---------------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------------


_ENGINE_DISPATCH = {
    "databricks-enzyme": _collect_databricks_plans,
    "spark": _collect_spark_plans,
    "spark-openivm": _collect_spark_openivm_plans,
}


def collect_query_plans(engine: str, run_id: str, batch_num: int) -> dict:
    """Collect EXPLAIN FORMATTED for every successful model in this run.

    Returns:
      {
        "status": "ok",
        "engine": ...,
        "run_id": ...,
        "batch_num": ...,
        "plans": [
          {"unique_id": ..., "name": ..., "plan": "<plain text>"},
          ...
        ],
        "failures": [
          {"unique_id": ..., "name": ..., "error": "..."},
        ],
        "summary": {
          "total_nodes": int,
          "successes": int,
          "failures": int,
        }
      }
    """
    handler = _ENGINE_DISPATCH.get(engine)
    if handler is None:
        raise ValueError(f"Unsupported engine for query-plan capture: {engine}")
    nodes = _fetch_compiled_sql(run_id)
    successes, failures = handler(nodes)
    return {
        "status": "ok",
        "engine": engine,
        "run_id": run_id,
        "batch_num": batch_num,
        "plans": successes,
        "failures": failures,
        "summary": {
            "total_nodes": len(nodes),
            "successes": len(successes),
            "failures": len(failures),
        },
    }
