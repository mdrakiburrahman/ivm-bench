"""Databricks MV pipeline-events collector.

Each Materialized View created with `REFRESH POLICY INCREMENTAL STRICT`
is backed by a Lakeflow Declarative Pipeline named
``MV-<catalog>.<schema>.<table>``. Every REFRESH triggers an "update"
that emits a stream of events. The most valuable one is
``event_type: "planning_information"`` whose
``details.planning_information.technique_information[*].maintenance_type``
tells us whether the refresh was incremental
(``MAINTENANCE_TYPE_NO_OP`` / ``..._APPEND`` / ``..._INCREMENTAL``) or
fell back to ``MAINTENANCE_TYPE_FULL_RECOMPUTE``, plus
``incrementalization_issues[*]`` (e.g. ``DATA_HAS_CHANGED`` with
``prevent_incrementalization: true``).

The SDK's ``PipelineEvent.as_dict()`` strips the ``details`` field, so
we call the raw REST API (``GET /api/2.0/pipelines/<id>/events``) and
return the unmodified JSON each event ships with.

The benchmark-server writes this JSON under
``mount/pipeline-events/<sf>/databricks-enzyme/batch<N>/<schema>.<table>/<update_id>.json``
(one file per update, all events embedded).
"""

import logging
import re
from typing import Any, Dict, List, Optional

from databricks.sdk import WorkspaceClient

from services import databricks_enzyme_sources as src

logger = logging.getLogger(__name__)

_PIPELINE_ID_PATTERN = re.compile(r"pipelines\.pipelineId=([0-9a-fA-F\-]{8,})")

_MAX_UPDATES_PER_PIPELINE = 500
_MAX_EVENTS_PER_PIPELINE = 5000
_EVENTS_PAGE_SIZE = 250


def _candidate_mv_schemas() -> List[str]:
    out: List[str] = []
    seen = set()
    for s in [src.DBT_SCHEMA, *src.LAYER_SCHEMAS]:
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _list_mvs_in_schema(conn, schema: str) -> List[Dict[str, str]]:
    """Return every MATERIALIZED_VIEW currently in <catalog>.<schema>.
    Prefers `information_schema.tables`; falls back to SHOW VIEWS."""
    catalog = src.CATALOG
    rows: List[Dict[str, str]] = []
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT table_name FROM {catalog}.information_schema.tables "
                f"WHERE table_schema = ? AND table_type = 'MATERIALIZED_VIEW'",
                [schema],
            )
            for r in cur.fetchall():
                rows.append({"schema": schema, "table": r[0]})
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/pipeline-events] information_schema lookup failed for %s.%s: %s",
            catalog, schema, exc,
        )
    return rows


def _extract_pipeline_id(conn, schema: str, table: str) -> Optional[str]:
    """Read `Table Properties` from DESCRIBE EXTENDED and extract
    ``pipelines.pipelineId=<uuid>``. Returns None on any failure."""
    catalog = src.CATALOG
    fq = f"`{catalog}`.`{schema}`.`{table}`"
    try:
        with conn.cursor() as cur:
            cur.execute(f"DESCRIBE EXTENDED {fq}")
            for row in cur.fetchall():
                col = (row[0] or "").strip() if len(row) > 0 else ""
                val = (row[1] or "") if len(row) > 1 else ""
                if col == "Table Properties":
                    m = _PIPELINE_ID_PATTERN.search(val)
                    if m:
                        return m.group(1)
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/pipeline-events] DESCRIBE EXTENDED %s failed: %s",
            fq, exc,
        )
    return None


def _discover_mv_pipelines(ws: WorkspaceClient) -> List[Dict[str, Any]]:
    """For each MV currently in our catalog's tracked schemas, resolve its
    backing pipeline ID via DESCRIBE EXTENDED Table Properties. (We avoid
    ``list_pipelines`` because MV-backing pipelines are not enumerable by
    the SP — they must be addressed by ID.)"""
    catalog = src.CATALOG
    conn = src._get_connection()
    out: List[Dict[str, Any]] = []
    for schema in _candidate_mv_schemas():
        mvs = _list_mvs_in_schema(conn, schema)
        for mv in mvs:
            pid = _extract_pipeline_id(conn, mv["schema"], mv["table"])
            if not pid:
                continue
            out.append({
                "pipeline_id": pid,
                "name": f"MV-{catalog}.{mv['schema']}.{mv['table']}",
                "schema": mv["schema"],
                "table": mv["table"],
                "state": "",
            })
    return out


def _list_all_updates(
    ws: WorkspaceClient, pipeline_id: str, max_updates: int = _MAX_UPDATES_PER_PIPELINE,
) -> List[Dict[str, Any]]:
    """Paginate `list_updates` until exhausted or cap hit. Returns per-update
    metadata (cause, state, full_refresh, refresh_selection, creation_time)."""
    updates: List[Dict[str, Any]] = []
    page_token: Optional[str] = None
    try:
        while True:
            resp = ws.pipelines.list_updates(
                pipeline_id,
                max_results=100,
                page_token=page_token,
            )
            batch = list(getattr(resp, "updates", None) or [])
            for u in batch:
                updates.append({
                    "update_id": getattr(u, "update_id", None),
                    "state": str(getattr(u, "state", "")) if getattr(u, "state", None) else "",
                    "cause": str(getattr(u, "cause", "")) if getattr(u, "cause", None) else "",
                    "creation_time": getattr(u, "creation_time", None),
                    "full_refresh": getattr(u, "full_refresh", None),
                    "refresh_selection": list(getattr(u, "refresh_selection", None) or []),
                    "full_refresh_selection": list(
                        getattr(u, "full_refresh_selection", None) or []
                    ),
                    "cluster_id": getattr(u, "cluster_id", None),
                })
                if len(updates) >= max_updates:
                    return updates
            page_token = getattr(resp, "next_page_token", None)
            if not page_token or not batch:
                break
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/pipeline-events] list_updates(%s) failed: %s",
            pipeline_id,
            exc,
        )
    return updates


def _list_all_events_raw(
    ws: WorkspaceClient,
    pipeline_id: str,
    max_events: int = _MAX_EVENTS_PER_PIPELINE,
) -> List[Dict[str, Any]]:
    """Page through ``GET /api/2.0/pipelines/<id>/events`` and return raw
    JSON dicts (NOT SDK dataclasses) so the ``details`` field — which the
    SDK ``PipelineEvent.as_dict()`` strips — is preserved."""
    api = ws.api_client
    events: List[Dict[str, Any]] = []
    query: Dict[str, Any] = {"max_results": _EVENTS_PAGE_SIZE}
    truncated = False
    try:
        while True:
            resp = api.do(
                "GET",
                f"/api/2.0/pipelines/{pipeline_id}/events",
                query=query,
                headers={"Accept": "application/json"},
            )
            batch = resp.get("events") or []
            for ev in batch:
                events.append(ev)
                if len(events) >= max_events:
                    truncated = True
                    return events
            next_token = resp.get("next_page_token")
            if not next_token or not batch:
                break
            query = {"page_token": next_token}
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/pipeline-events] raw events fetch failed for %s: %s",
            pipeline_id,
            exc,
        )
    if truncated:
        events.append({
            "_truncated": True,
            "_max_events": max_events,
        })
    return events


def _group_events_by_update(events: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Bucket events by ``origin.update_id``. Events without an update_id
    (e.g. pipeline-level lifecycle events) are filed under ``__no_update__``."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for ev in events:
        if ev.get("_truncated"):
            continue
        origin = ev.get("origin") or {}
        update_id = origin.get("update_id") or "__no_update__"
        out.setdefault(update_id, []).append(ev)
    return out


def collect_pipeline_events(batch_num: int) -> dict:
    """For every ``MV-<catalog>.*`` pipeline in the workspace, list every
    update and group every event by update_id. Returns a JSON-ready payload
    the benchmark-server writes to disk one-file-per-update."""
    ws = src._workspace_client()
    catalog = src.CATALOG
    pipelines = _discover_mv_pipelines(ws)

    out_pipelines: List[Dict[str, Any]] = []
    total_update_count = 0
    total_event_count = 0
    for p in pipelines:
        pid = p.get("pipeline_id")
        if not pid:
            continue
        updates_meta = _list_all_updates(ws, pid)
        meta_by_id: Dict[str, Dict[str, Any]] = {
            u["update_id"]: u for u in updates_meta if u.get("update_id")
        }
        events = _list_all_events_raw(ws, pid)
        grouped = _group_events_by_update(events)

        update_ids = set(meta_by_id.keys()) | set(grouped.keys())
        if "__no_update__" in update_ids:
            update_ids.discard("__no_update__")

        per_update: List[Dict[str, Any]] = []
        for uid in sorted(update_ids):
            meta = meta_by_id.get(uid, {"update_id": uid})
            evs = grouped.get(uid, [])
            per_update.append({
                **meta,
                "event_count": len(evs),
                "events": evs,
            })
            total_event_count += len(evs)

        pipeline_level_events = grouped.get("__no_update__", [])
        total_update_count += len(per_update)
        total_event_count += len(pipeline_level_events)
        out_pipelines.append({
            "pipeline_id": pid,
            "name": p["name"],
            "schema": p["schema"],
            "table": p["table"],
            "state": p["state"],
            "updates": per_update,
            "pipeline_level_events": pipeline_level_events,
        })

    return {
        "status": "ok",
        "batch_num": batch_num,
        "catalog": catalog,
        "pipeline_count": len(out_pipelines),
        "update_count": total_update_count,
        "event_count": total_event_count,
        "pipelines": out_pipelines,
    }
