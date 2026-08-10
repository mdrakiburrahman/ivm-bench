"""Best-effort remote compute telemetry for Databricks and Microsoft Fabric.

The benchmark timer measures elapsed refresh latency.  This module separately
collects the amount of distributed work performed during that wall-clock
window. Databricks exposes executor CPU time through each materialized
view's Lakeflow event log and bills serverless MV pipelines in DBUs through
``system.billing.usage``; Fabric exposes Spark task metrics through its
monitoring REST API.

Task and CPU time are sums across concurrently executing tasks and can
therefore exceed wall time.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional


_UUID = re.compile(r"^[0-9a-fA-F-]{36}$")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _valid_update_ids(values: Iterable[Any]) -> List[str]:
    return sorted({str(value) for value in values if _UUID.fullmatch(str(value))})


def _valid_updates(values: Iterable[Any]) -> List[Dict[str, str]]:
    updates = set()
    for value in values:
        if not isinstance(value, dict):
            continue
        schema = str(value.get("schema", ""))
        table = str(value.get("table", ""))
        update_id = str(value.get("update_id", ""))
        if (
            _IDENTIFIER.fullmatch(schema)
            and _IDENTIFIER.fullmatch(table)
            and _UUID.fullmatch(update_id)
        ):
            updates.add((schema, table, update_id))
    return [
        {"schema": schema, "table": table, "update_id": update_id}
        for schema, table, update_id in sorted(updates)
    ]


def _number(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def summarize_databricks_billing(
    rows: Iterable[Dict[str, Any]],
    pipeline_work_s: Optional[float],
    event_rows: Iterable[Dict[str, Any]] = (),
) -> Dict[str, Any]:
    rows = list(rows)
    event_rows = list(event_rows)
    dbus = sum(_number(row.get("usage_quantity")) for row in rows)
    cpu_values = [
        _number(row["executor_cpu_time_ms"])
        for row in event_rows
        if row.get("executor_cpu_time_ms") is not None
    ]
    result = {
        "status": "ok" if pipeline_work_s is not None else "unavailable",
        "source": "databricks_mv_event_log+system.billing.usage",
        "semantics": (
            "task_time_s is summed MV flow duration from pipeline events; "
            "cpu_time_s is summed flow_progress executor_cpu_time_ms from "
            "the exact MV update event logs; "
            "billing_quantity is billed serverless usage attributed by dlt_update_id"
        ),
        "task_time_s": pipeline_work_s,
        "cpu_time_s": sum(cpu_values) / 1000.0 if cpu_values else None,
        "event_metric_row_count": len(event_rows),
        "event_metrics": event_rows,
        "billing_quantity": dbus if rows else None,
        "billing_unit": "DBU",
        "billing_status": "ok" if rows else "pending",
        "billing_row_count": len(rows),
        "billing_rows": rows,
    }
    if pipeline_work_s is None:
        result["error"] = "Databricks pipeline flow-work telemetry is unavailable"
    elif not rows:
        result["error"] = (
            "system.billing.usage has not published rows for these pipeline updates yet"
        )
    return result


def collect_databricks(
    start_ms: int,
    end_ms: int,
    update_ids: Iterable[str],
    updates: Iterable[Dict[str, Any]],
    pipeline_work_s: Optional[float],
) -> Dict[str, Any]:
    """Collect CPU time and billed DBUs for exact Lakeflow MV updates."""
    from services import databricks_enzyme_sources as src

    ids = _valid_update_ids(update_ids)
    valid_updates = _valid_updates(updates)
    rows: List[Dict[str, Any]] = []
    event_rows: List[Dict[str, Any]] = []
    query_error = None
    event_errors: List[str] = []
    conn = src._get_connection()

    for update in valid_updates:
        fq = f"`{src.CATALOG}`.`{update['schema']}`.`{update['table']}`"
        sql = f"""
            SELECT
              SUM(TRY_CAST(get_json_object(
                    details,
                    '$.flow_progress.metrics.executor_cpu_time_ms'
                  ) AS DOUBLE)) AS executor_cpu_time_ms,
              SUM(TRY_CAST(get_json_object(
                    details,
                    '$.flow_progress.metrics.executor_time_ms'
                  ) AS DOUBLE)) AS executor_time_ms,
              SUM(TRY_CAST(get_json_object(
                    details,
                    '$.flow_progress.metrics.num_output_bytes'
                  ) AS DOUBLE)) AS output_bytes
            FROM event_log(TABLE({fq}))
            WHERE origin.update_id = ?
              AND event_type = 'flow_progress'
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql, [update["update_id"]])
                values = cursor.fetchone()
                if values and any(value is not None for value in values):
                    event_rows.append(
                        {
                            **update,
                            "executor_cpu_time_ms": (
                                _number(values[0]) if values[0] is not None else None
                            ),
                            "executor_time_ms": (
                                _number(values[1]) if values[1] is not None else None
                            ),
                            "output_bytes": (
                                _number(values[2]) if values[2] is not None else None
                            ),
                        }
                    )
        except Exception as exc:
            event_errors.append(
                f"{update['schema']}.{update['table']}: {exc}"
            )

    if ids:
        literals = ", ".join(f"'{value}'" for value in ids)
        sql = f"""
            SELECT usage_metadata.dlt_update_id AS update_id,
                   sku_name,
                   SUM(usage_quantity) AS usage_quantity
              FROM system.billing.usage
             WHERE billing_origin_product = 'SQL'
               AND usage_metadata.dlt_update_id IN ({literals})
             GROUP BY usage_metadata.dlt_update_id, sku_name
        """
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                columns = [str(column[0]) for column in (cursor.description or [])]
                rows = [
                    dict(zip((column.lower() for column in columns), values))
                    for values in cursor.fetchall()
                ]
                for row in rows:
                    row["usage_quantity"] = _number(row.get("usage_quantity"))
        except Exception as exc:
            query_error = str(exc)

    result = summarize_databricks_billing(rows, pipeline_work_s, event_rows)
    if event_errors:
        result["cpu_error"] = "; ".join(event_errors)
    elif not valid_updates:
        result["cpu_error"] = "no valid Databricks MV updates were supplied"
    elif not any(
        row.get("executor_cpu_time_ms") is not None for row in event_rows
    ):
        result["cpu_error"] = (
            "materialized-view event logs returned no executor CPU metrics"
        )
    if query_error:
        result["billing_error"] = (
            f"system.billing.usage query failed: {query_error}"
        )
    elif not ids:
        result["billing_error"] = (
            "no Databricks pipeline update IDs were supplied"
        )
    result.update(
        {
            "window_start_ms": start_ms,
            "window_end_ms": end_ms,
            "update_ids": ids,
            "updates": valid_updates,
        }
    )
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
        "billing_status": "not_applicable",
        "billing_quantity": None,
        "billing_unit": None,
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


def collect(
    engine: str,
    start_ms: int,
    end_ms: int,
    *,
    update_ids: Iterable[str] = (),
    updates: Iterable[Dict[str, Any]] = (),
    pipeline_work_s: Optional[float] = None,
) -> Dict[str, Any]:
    if engine == "databricks-enzyme":
        return collect_databricks(
            start_ms,
            end_ms,
            update_ids,
            updates,
            pipeline_work_s,
        )
    if engine in ("fabric-jvm-35", "fabric-openivm-jvm-35"):
        return collect_fabric(engine, start_ms, end_ms)
    raise ValueError(f"cloud compute metrics unsupported for engine {engine!r}")
