"""spark-openivm SQL trace (query-log) export helpers.

Sibling to `services/spark_openivm_profile.py`. Runs
`SHOW OPENIVM QUERY LOG` against the live Livy SQL session and returns the
rows as a structured JSON payload that the benchmark-server then formats and
writes to disk as a directory tree of `.sql` files per MV per refresh.

NO formatting is done here on purpose — dbt-server stays agnostic of the
output layout. The benchmark-server is the only place that knows where the
files land and is the right place to invoke `sqlglot.transpile(...)`.
"""

import logging
from typing import Any, Dict, List, Tuple

from services.spark_openivm_sources import LivyClient

logger = logging.getLogger(__name__)

# Columns returned by `SHOW OPENIVM QUERY LOG`. Matches the output schema of
# `ShowQueryLogCommand` (see openivm-spark/.../ShowQueryLogCommand.scala).
_QUERY_LOG_COLS: Tuple[str, ...] = (
    "refresh_id",
    "view_name",
    "profile_timestamp",
    "stmt_order",
    "attempt_idx",
    "mode",
    "category",
    "stmt_kind",
    "duration_ms",
    "sql_text",
)


def _extract_rows(output: Dict[str, Any], label: str) -> List[List[Any]]:
    """Pull the `application/json` table payload out of a Livy SQL output.

    Spark's `kind: sql` session returns
        {"schema": {"fields": [{"name": ..., "type": ...}, ...]},
         "data":   [[c1, c2, ...], ...]}
    """
    data = (output or {}).get("data") or {}
    payload = data.get("application/json")
    if isinstance(payload, dict):
        rows = payload.get("data") or []
        return [list(row) for row in rows]
    text = data.get("text/plain") or ""
    if text:
        logger.warning(
            "[spark-openivm] %s: Livy returned text/plain instead of "
            "application/json — refusing to parse: %s",
            label,
            text[:400],
        )
    return []


def _column_names(output: Dict[str, Any]) -> List[str]:
    data = (output or {}).get("data") or {}
    payload = data.get("application/json")
    if isinstance(payload, dict):
        schema = payload.get("schema") or {}
        return [str(f.get("name")) for f in (schema.get("fields") or [])]
    return []


def _sort_key(row: Dict[str, Any]) -> Tuple[str, str, int, int]:
    ts = row.get("profile_timestamp") or ""
    rid = row.get("refresh_id") or ""
    try:
        order = int(row.get("stmt_order") or 0)
    except (TypeError, ValueError):
        order = 0
    try:
        attempt = int(row.get("attempt_idx") or 0)
    except (TypeError, ValueError):
        attempt = 0
    return (str(ts), str(rid), order, attempt)


def _fetch_query_log_rows(livy: LivyClient) -> List[Dict[str, Any]]:
    """Run `SHOW OPENIVM QUERY LOG` and return dict rows.

    Defensive against column-order drift: maps by schema field name.
    """
    result = livy.execute("SHOW OPENIVM QUERY LOG")
    output = result.get("output") or {}
    cols = _column_names(output) or list(_QUERY_LOG_COLS)
    raw = _extract_rows(output, "SHOW OPENIVM QUERY LOG")
    rows: List[Dict[str, Any]] = []
    for row in raw:
        if len(row) != len(cols):
            logger.warning(
                "[spark-openivm] query-log row column-count mismatch: "
                "expected %d, got %d — skipping",
                len(cols),
                len(row),
            )
            continue
        rows.append({col: val for col, val in zip(cols, row)})
    rows.sort(key=_sort_key)
    return rows


def export_query_log(run_id: str, batch_num: int) -> dict:
    """Export spark-openivm refresh SQL log rows as structured JSON.

    Returns:
        {
          "status": "ok",
          "run_id": <run_id>,
          "batch_num": <batch_num>,
          "row_count": <int>,
          "view_count": <int>,
          "refresh_count": <int>,
          "rows": [
            {
              "refresh_id": "default.mv_foo_1735301845123456789",
              "view_name": "default.mv_foo",
              "profile_timestamp": "...",
              "stmt_order": 0,
              "attempt_idx": 0,
              "mode": "create" | "refresh",
              "category": "original_query" | ...,
              "stmt_kind": "ctas" | "merge" | ...,
              "duration_ms": 12,
              "sql_text": "<full SQL>"
            },
            ...
          ]
        }
    """
    with LivyClient() as livy:
        rows = _fetch_query_log_rows(livy)

    view_count = len({str(r.get("view_name")) for r in rows if r.get("view_name")})
    refresh_count = len({str(r.get("refresh_id")) for r in rows if r.get("refresh_id")})

    return {
        "status": "ok",
        "run_id": run_id,
        "batch_num": batch_num,
        "row_count": len(rows),
        "view_count": view_count,
        "refresh_count": refresh_count,
        "rows": rows,
    }
