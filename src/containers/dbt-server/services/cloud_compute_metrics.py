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
    task_ms = sum(_number(row.get("total_task_duration_ms")) for row in rows)
    result = {
        "status": "ok" if rows else "unavailable",
        "source": "databricks_system_query_history",
        "semantics": "sum(total_task_duration_ms) across queries overlapping the batch window",
        "task_time_s": task_ms / 1000.0,
        "cpu_time_s": None,
        "query_count": len(rows),
        "queries": rows,
    }
    if not rows:
        result["error"] = "system.query.history returned no rows for the batch window"
    return result


def collect_databricks(start_ms: int, end_ms: int) -> Dict[str, Any]:
    """Collect SQL task duration for queries overlapping ``[start_ms,end_ms]``.

    The collection query starts after ``end_ms`` and is consequently excluded
    by the upper bound.  The GCI workspace is dedicated to the benchmark, so we
    intentionally do not filter by warehouse: Dynamic Tables may execute on
    managed pipeline compute rather than the SQL warehouse that submits DDL.
    """
    from services import databricks_enzyme_sources as src

    sql = f"""
        SELECT statement_id, statement_type, execution_status,
               start_time, end_time, total_duration_ms,
               total_task_duration_ms, read_bytes, written_bytes
        FROM system.query.history
        WHERE start_time <= timestamp_millis({int(end_ms)})
          AND COALESCE(end_time, CURRENT_TIMESTAMP()) >= timestamp_millis({int(start_ms)})
          AND total_task_duration_ms IS NOT NULL
        ORDER BY start_time
    """
    conn = src._get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        names = [str(column[0]).lower() for column in cursor.description]
        rows = []
        for values in cursor.fetchall():
            row = dict(zip(names, values))
            for key, value in list(row.items()):
                if isinstance(value, datetime):
                    row[key] = value.isoformat()
            rows.append(row)
    finally:
        cursor.close()
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


def summarize_fabric_tasks(
    tasks: Iterable[Dict[str, Any]], *, stage_count: int
) -> Dict[str, Any]:
    tasks = list(tasks)
    run_ms = 0.0
    cpu_ns = 0.0
    for task in tasks:
        metrics = task.get("taskMetrics") or {}
        run_ms += _number(metrics.get("executorRunTime"))
        cpu_ns += _number(metrics.get("executorCpuTime"))
    result = {
        "status": "ok" if tasks else "unavailable",
        "source": "fabric_spark_monitoring_api",
        "semantics": "sum of successful Spark task metrics for stages overlapping the batch window",
        "task_time_s": run_ms / 1000.0,
        "cpu_time_s": cpu_ns / 1_000_000_000.0,
        "stage_count": stage_count,
        "task_count": len(tasks),
    }
    if not tasks:
        result["error"] = "Fabric monitoring returned no completed tasks for the batch window"
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
    tasks: List[Dict[str, Any]] = []
    for stage in selected:
        stage_id = stage.get("stageId")
        attempt_id = stage.get("attemptId", 0)
        offset = 0
        while True:
            page = _fabric_get(
                f"{app_base}/stages/{stage_id}/{attempt_id}/taskList",
                params={"offset": offset, "length": 10000, "status": "success"},
            )
            successful = [
                task for task in page if str(task.get("status", "")).upper() == "SUCCESS"
            ]
            tasks.extend(successful)
            if len(page) < 10000:
                break
            offset += len(page)

    result = summarize_fabric_tasks(tasks, stage_count=len(selected))
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
