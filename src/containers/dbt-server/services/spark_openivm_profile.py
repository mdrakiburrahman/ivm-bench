"""spark-openivm profiling export helpers.

Mirror of `services/openivm_profile.py` for the spark-openivm engine.

Calls `SHOW OPENIVM REFRESH PROFILE` against the live Livy SQL session
(opened by `spark_openivm_sources.LivyClient`), collects every row of the
RocksDB-backed refresh profile catalog, and writes three CSV payloads that
match the duckdb-openivm files byte-for-byte in shape:

  - `profile`       – one row per per-refresh step, ordered by
                       `(profile_timestamp, refresh_id, step_order)`.
  - `by_step`       – aggregate by `step_name` (row_count / sum / avg / max).
  - `by_view_step`  – aggregate by `(view_name, step_name)`.

Each row is tagged with `exported_after_batch` and `exported_after_run_id`
columns at SELECT time so the cumulative catalog stays traceable to the
batch that exported it.
"""

import csv
import io
import logging
from typing import Any, Dict, List, Optional, Tuple

from services.spark_openivm_sources import LivyClient

logger = logging.getLogger(__name__)

# Columns returned by `SHOW OPENIVM REFRESH PROFILE` — matches the schema of
# `ShowRefreshProfileCommand` (see openivm-spark/.../ShowRefreshProfileCommand.scala).
_PROFILE_COLS: Tuple[str, ...] = (
    "refresh_id",
    "view_name",
    "profile_timestamp",
    "step_order",
    "step_name",
    "duration_ms",
    "detail",
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
    # Fall back to text/plain — only happens if the cluster mis-routes a sql
    # statement to spark-shell kind; treat as empty result with a warning.
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


def _format_value(value: Any) -> str:
    """Coerce one Livy cell into the canonical CSV string form."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _write_csv(headers: List[str], rows: List[List[Any]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(headers)
    for row in rows:
        writer.writerow([_format_value(v) for v in row])
    return buf.getvalue()


def _safe_float(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _sort_key(row: Dict[str, Any]) -> Tuple[str, str, int]:
    ts = row.get("profile_timestamp") or ""
    rid = row.get("refresh_id") or ""
    try:
        order = int(row.get("step_order") or 0)
    except (TypeError, ValueError):
        order = 0
    return (str(ts), str(rid), order)


def _aggregate_by_step(
    rows: List[Dict[str, Any]]
) -> List[Tuple[str, int, float, float, float]]:
    by_step: Dict[str, Dict[str, float]] = {}
    for row in rows:
        step = str(row.get("step_name") or "")
        agg = by_step.setdefault(
            step, {"row_count": 0.0, "total_ms": 0.0, "max_ms": 0.0}
        )
        agg["row_count"] += 1
        dur = _safe_float(row.get("duration_ms"))
        agg["total_ms"] += dur
        if dur > agg["max_ms"]:
            agg["max_ms"] = dur
    out: List[Tuple[str, int, float, float, float]] = []
    for step, agg in by_step.items():
        row_count = int(agg["row_count"])
        total_ms = agg["total_ms"]
        max_ms = agg["max_ms"]
        avg_ms = total_ms / row_count if row_count else 0.0
        out.append((step, row_count, total_ms, avg_ms, max_ms))
    out.sort(key=lambda r: r[2], reverse=True)
    return out


def _aggregate_by_view_step(
    rows: List[Dict[str, Any]]
) -> List[Tuple[str, str, int, float, float, float]]:
    by_pair: Dict[Tuple[str, str], Dict[str, float]] = {}
    for row in rows:
        view = str(row.get("view_name") or "")
        step = str(row.get("step_name") or "")
        agg = by_pair.setdefault(
            (view, step), {"row_count": 0.0, "total_ms": 0.0, "max_ms": 0.0}
        )
        agg["row_count"] += 1
        dur = _safe_float(row.get("duration_ms"))
        agg["total_ms"] += dur
        if dur > agg["max_ms"]:
            agg["max_ms"] = dur
    out: List[Tuple[str, str, int, float, float, float]] = []
    for (view, step), agg in by_pair.items():
        row_count = int(agg["row_count"])
        total_ms = agg["total_ms"]
        max_ms = agg["max_ms"]
        avg_ms = total_ms / row_count if row_count else 0.0
        out.append((view, step, row_count, total_ms, avg_ms, max_ms))
    out.sort(key=lambda r: r[3], reverse=True)
    return out


def _fetch_profile_rows(livy: LivyClient) -> List[Dict[str, Any]]:
    """Run `SHOW OPENIVM REFRESH PROFILE` and return dict rows.

    Defensive against Livy returning rows in a slightly different column order
    than the catalog declares: maps by `application/json.schema.fields[*].name`.
    """
    result = livy.execute("SHOW OPENIVM REFRESH PROFILE")
    output = result.get("output") or {}
    cols = _column_names(output) or list(_PROFILE_COLS)
    raw = _extract_rows(output, "SHOW OPENIVM REFRESH PROFILE")
    rows: List[Dict[str, Any]] = []
    for row in raw:
        if len(row) != len(cols):
            logger.warning(
                "[spark-openivm] profile row column-count mismatch: "
                "expected %d, got %d — skipping",
                len(cols),
                len(row),
            )
            continue
        rows.append({col: val for col, val in zip(cols, row)})
    rows.sort(key=_sort_key)
    return rows


def export_profile(run_id: str, batch_num: int) -> dict:
    """Export spark-openivm refresh-profile rows + summaries as CSV strings.

    Cumulative: the catalog accumulates rows across the entire benchmark run.
    Each exported row is annotated with `exported_after_batch` and
    `exported_after_run_id` so a post-mortem reader can attribute every entry
    to the batch that exported it without dropping any history.
    """
    with LivyClient() as livy:
        rows = _fetch_profile_rows(livy)

    # ---------------- Main profile CSV ----------------
    profile_headers = [
        "exported_after_batch",
        "exported_after_run_id",
        *_PROFILE_COLS,
    ]
    profile_rows: List[List[Any]] = [
        [batch_num, run_id] + [row.get(col) for col in _PROFILE_COLS]
        for row in rows
    ]
    profile_csv = _write_csv(profile_headers, profile_rows)

    # ---------------- by_step CSV ----------------
    by_step_headers = [
        "exported_after_batch",
        "exported_after_run_id",
        "step_name",
        "row_count",
        "total_ms",
        "avg_ms",
        "max_ms",
    ]
    by_step_rows: List[List[Any]] = [
        [batch_num, run_id, step, row_count, total_ms, avg_ms, max_ms]
        for (step, row_count, total_ms, avg_ms, max_ms) in _aggregate_by_step(rows)
    ]
    by_step_csv = _write_csv(by_step_headers, by_step_rows)

    # ---------------- by_view_step CSV ----------------
    by_view_step_headers = [
        "exported_after_batch",
        "exported_after_run_id",
        "view_name",
        "step_name",
        "row_count",
        "total_ms",
        "avg_ms",
        "max_ms",
    ]
    by_view_step_rows: List[List[Any]] = [
        [batch_num, run_id, view, step, row_count, total_ms, avg_ms, max_ms]
        for (view, step, row_count, total_ms, avg_ms, max_ms) in _aggregate_by_view_step(
            rows
        )
    ]
    by_view_step_csv = _write_csv(by_view_step_headers, by_view_step_rows)

    view_count = len({str(r.get("view_name")) for r in rows if r.get("view_name")})

    return {
        "status": "ok",
        "run_id": run_id,
        "batch_num": batch_num,
        "row_count": len(rows),
        "view_count": view_count,
        "csv": {
            "profile": profile_csv,
            "by_step": by_step_csv,
            "by_view_step": by_view_step_csv,
        },
    }
