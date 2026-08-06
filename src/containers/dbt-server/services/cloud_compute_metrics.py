"""Best-effort cloud compute telemetry for Databricks and Microsoft Fabric.

The benchmark timer measures elapsed refresh latency.  This module separately
collects the amount of distributed task work performed during that wall-clock
window.  Databricks exposes task duration through ``system.query.history``;
Fabric exposes Spark task metrics through its monitoring REST API.

These values are deliberately labelled task/CPU time rather than cost.  Task
time is the sum across concurrently executing tasks and can therefore exceed
wall time.  Only Fabric currently exposes executor CPU time through this path.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def summarize_databricks_rows(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    rows = list(rows)
    task_ms = sum(
        _number((row.get("metrics") or {}).get("task_total_time_ms")) for row in rows
    )
    result = {
        "status": "ok" if rows else "unavailable",
        "source": "databricks_query_history_api",
        "semantics": "sum(metrics.task_total_time_ms) across queries started in the batch window",
        "task_time_s": task_ms / 1000.0,
        "cpu_time_s": None,
        "query_count": len(rows),
        "queries": rows,
    }
    if not rows:
        result["error"] = "system.query.history returned no rows for the batch window"
    return result


def collect_databricks(start_ms: int, end_ms: int) -> Dict[str, Any]:
    """Collect SQL task duration for queries started in ``[start_ms,end_ms]``.

    The Query History REST API returns queries visible to the calling principal
    without requiring grants on the ``system.query`` schema.  We intentionally
    do not filter by warehouse: Dynamic Tables may execute on managed pipeline
    compute rather than the SQL warehouse that submits DDL.
    """
    from services import databricks_enzyme_sources as src

    api = src._workspace_client().api_client
    query: Dict[str, Any] = {
        "filter_by": {
            "query_start_time_range": {
                "start_time_ms": int(start_ms),
                "end_time_ms": int(end_ms),
            }
        },
        "include_metrics": True,
        "max_results": 1000,
    }
    rows: List[Dict[str, Any]] = []
    while True:
        response = api.do(
            "GET",
            "/api/2.0/sql/history/queries",
            query=query,
            headers={"Accept": "application/json"},
        )
        rows.extend(response.get("res") or [])
        page_token = response.get("next_page_token")
        if not page_token or not response.get("has_next_page"):
            break
        query = {"include_metrics": True, "max_results": 1000, "page_token": page_token}
    result = summarize_databricks_rows(rows)
    result.update({"window_start_ms": start_ms, "window_end_ms": end_ms})
    return result


def _parse_time_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).replace("GMT", "+00:00").replace("Z", "+00:00")
    try:
        return int(datetime.fromisoformat(text).timestamp() * 1000)
    except ValueError:
        return None


def _overlaps_window(stage: Dict[str, Any], start_ms: int, end_ms: int) -> bool:
    submitted = _parse_time_ms(stage.get("submissionTime"))
    completed = _parse_time_ms(stage.get("completionTime"))
    if submitted is None:
        return False
    return submitted <= end_ms and (completed is None or completed >= start_ms)


def summarize_fabric_stages(stages: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    stages = list(stages)
    run_ms = sum(_number(stage.get("executorRunTime")) for stage in stages)
    cpu_ns = sum(_number(stage.get("executorCpuTime")) for stage in stages)
    task_count = sum(_number(stage.get("numCompleteTasks")) for stage in stages)
    result = {
        "status": "ok" if stages else "unavailable",
        "source": "fabric_spark_monitoring_api",
        "semantics": "sum of Spark StageData executor metrics for completed stages overlapping the batch window",
        "task_time_s": run_ms / 1000.0,
        "cpu_time_s": cpu_ns / 1_000_000_000.0,
        "stage_count": len(stages),
        "task_count": int(task_count),
    }
    if not stages:
        result["error"] = "Fabric monitoring returned no completed stages for the batch window"
    return result


def _fabric_values(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    for key in ("value", "data", "sessions"):
        value = payload.get(key) if isinstance(payload, dict) else None
        if isinstance(value, list):
            return value
    return []


def _fabric_payload(url: str, *, params: Optional[Dict[str, Any]] = None) -> Any:
    from services import fabric

    response = fabric._fabric_req(
        "GET", url, headers=fabric._fabric_headers(), params=params or {}
    )
    response.raise_for_status()
    return response.json()


def _fabric_get(url: str, *, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    return _fabric_values(_fabric_payload(url, params=params))


def _session_id(engine: str) -> str:
    path = f"/tmp/{engine}-livy.session-id"
    with open(path) as handle:
        session_id = handle.read().strip()
    if not session_id:
        raise RuntimeError(f"empty Fabric Livy session id in {path}")
    return session_id


def collect_fabric(engine: str, start_ms: int, end_ms: int) -> Dict[str, Any]:
    """Collect Spark task and executor CPU time from Fabric monitoring APIs."""
    from services import fabric

    lakehouse_id = fabric._compute_lakehouse_id()
    livy_id = _session_id(engine)
    base = (
        f"{fabric.FABRIC_API_BASE}/v1/workspaces/{fabric.WORKSPACE_ID}"
        f"/lakehouses/{lakehouse_id}/livySessions/{livy_id}"
    )
    session = _fabric_payload(base)
    app_id = str((session or {}).get("sparkApplicationId", ""))
    if not app_id:
        raise RuntimeError(f"Fabric session {livy_id} has no sparkApplicationId")

    app_base = f"{base}/applications/{app_id}"
    stages = _fabric_get(f"{app_base}/stages")
    selected = [
        stage
        for stage in stages
        if str(stage.get("status", "")).upper() == "COMPLETE"
        and _overlaps_window(stage, start_ms, end_ms)
    ]
    result = summarize_fabric_stages(selected)
    result.update(
        {
            "window_start_ms": start_ms,
            "window_end_ms": end_ms,
            "livy_id": livy_id,
            "spark_application_id": app_id,
        }
    )
    return result


def collect(engine: str, start_ms: int, end_ms: int) -> Dict[str, Any]:
    if engine == "databricks-enzyme":
        return collect_databricks(start_ms, end_ms)
    if engine in ("fabric-jvm-35", "fabric-openivm-jvm-35"):
        return collect_fabric(engine, start_ms, end_ms)
    raise ValueError(f"cloud compute metrics unsupported for engine {engine!r}")
