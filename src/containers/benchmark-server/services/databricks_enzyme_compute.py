"""Databricks Lakeflow pipeline-events → pure compute time extraction.

Each ``REFRESH POLICY INCREMENTAL STRICT`` materialized view is backed by
a Lakeflow Declarative Pipeline. The wall-clock time we measure around
``dbt build`` for the ``databricks-enzyme`` engine includes a lot of
overhead the engine itself does NOT pay for in production:

* per-MV pipeline cluster provisioning + warm-up,
* queueing across the workspace,
* per-update planning + queue → STARTING latency,
* harness setup (sweep-stale, init/<sf>, EXPLAIN pre-flight).

For an honest comparison against engines that don't have a per-query
remote pipeline (Spark/DuckDB/Feldera), we report **pure compute time**
for ``databricks-enzyme``. This module derives those numbers from the
pipeline-events JSON the dbt-server already captured to disk.

Per-MV pure compute (preferred path):
    last ``flow_progress`` event with
    ``details.flow_progress.status == COMPLETED``
        → ``details.flow_progress.metrics.execution_duration_ms``
    Fallback: ``COMPLETED.timestamp − first RUNNING.timestamp`` for that
    ``(update_id, flow_id)``.

Per-batch pure compute:
    coverage-time = union (interval-merge) of every
    ``[RUNNING_start, COMPLETED_end]`` window across every flow that
    ran inside this batch's wall-clock window.
    sum-of-compute = Σ per-MV compute (forensics secondary metric).

Failure mode:
    If the pipeline-events directory is missing or no COMPLETED events
    are found for the batch, this module raises. The caller (engine
    runner) MUST surface that as a batch failure — silently falling
    back to wall-clock would let inflated numbers (with pipeline
    overhead) ship to the reviewer, which the user has forbidden.
"""

from __future__ import annotations

import glob as _glob
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


# --------------------------------------------------------------------- #
# Pure helpers (testable; no IO)                                        #
# --------------------------------------------------------------------- #


_ISO_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})")


def _parse_iso_ms(ts: Any) -> Optional[int]:
    """Parse Databricks ISO-8601 timestamp to epoch-ms.

    Databricks emits ``2025-11-13T20:24:31.456Z`` style strings. We
    tolerate variable fractional precision and an optional ``Z`` suffix.
    Numeric inputs (epoch-ms) are returned as-is. Returns ``None`` on
    any parse failure rather than raising — caller decides whether to
    fail or skip.
    """
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    if not isinstance(ts, str):
        return None
    s = ts.strip()
    if not s:
        return None
    # Normalize trailing Z to +00:00 so fromisoformat accepts it.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    # Pad fractional seconds to 6 digits (datetime.fromisoformat is strict
    # about microsecond precision in 3.10; 3.11+ is more lenient).
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        # Best effort: strip fractional, retry.
        m = _ISO_TS.match(s)
        if not m:
            return None
        try:
            dt = datetime.fromisoformat(m.group(1) + "+00:00")
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


@dataclass
class FlowSegment:
    """One contiguous RUNNING → COMPLETED window for a single flow."""

    flow_id: str
    flow_name: str
    started_ms: int
    completed_ms: int
    execution_duration_ms: Optional[int]
    source: str  # "metric" | "timestamp_delta" | "running_completed_delta"

    @property
    def duration_ms(self) -> int:
        return max(0, self.completed_ms - self.started_ms)


def _flow_progress_details(ev: dict) -> Optional[dict]:
    details = ev.get("details")
    if not isinstance(details, dict):
        return None
    fp = details.get("flow_progress")
    if not isinstance(fp, dict):
        return None
    return fp


def _flow_id_of(ev: dict) -> Optional[str]:
    """Stable per-flow identifier across all flow_progress states.

    Databricks emits ``flow_progress`` events with ``origin.flow_id``
    populated only AFTER the flow has reached STARTING — QUEUED and
    PLANNING events ship with an EMPTY ``flow_id`` string. To group
    all 5 events for the same flow into a single segment (so the
    UI-matching ``COMPLETED − QUEUED`` duration can be computed) we
    use ``origin.flow_name`` as the primary key — it is always
    populated and uniquely identifies the flow within an update.

    Fallback order:
        1. ``origin.flow_name`` (always present, fully qualified)
        2. ``origin.flow_id`` (only post-STARTING)
        3. ``origin.dataset_name`` (always present)
        4. ``details.flow_progress.flow_id`` (legacy)
    """
    origin = ev.get("origin") or {}
    fname = origin.get("flow_name")
    if fname:
        return str(fname)
    fid = origin.get("flow_id")
    if fid:
        return str(fid)
    dname = origin.get("dataset_name")
    if dname:
        return str(dname)
    fp = _flow_progress_details(ev) or {}
    return fp.get("flow_id")


def _flow_name_of(ev: dict) -> str:
    origin = ev.get("origin") or {}
    return str(origin.get("flow_name") or origin.get("dataset_name") or "")


def extract_flow_segments(events: List[dict]) -> List[FlowSegment]:
    """Walk a single update's events and emit one segment per
    ``(flow_id, QUEUED → COMPLETED)`` pair.

    Per-flow duration matches the Databricks pipeline UI's "Duration"
    column: ``COMPLETED.timestamp − QUEUED.timestamp``. This is the
    full lifecycle of the flow on the cluster — the time Databricks
    bills compute for — EXCLUDING the update-level cluster-spinup
    phases (``WAITING_FOR_RESOURCES``, ``INITIALIZING``,
    ``SETTING_UP_TABLES``) which dominate wall-clock for single-MV
    pipelines.

    The start anchor walks down in preference order
    ``QUEUED → PLANNING → STARTING → RUNNING`` so flows missing earlier
    states (rare; mostly NO_OP early-exits) still produce a valid
    segment. If Databricks ever ships a
    ``details.flow_progress.metrics.execution_duration_ms`` on the
    COMPLETED event, we override the timestamp delta with that value
    (``source = "metric"``) — but as of the event schema captured here
    that field is not populated, so we report ``source =
    "timestamp_delta"`` which is the well-defined UI-matching number.
    """
    by_flow: Dict[str, List[Tuple[str, int, dict]]] = {}
    for ev in events:
        if not isinstance(ev, dict):
            continue
        if ev.get("event_type") != "flow_progress":
            continue
        fp = _flow_progress_details(ev)
        if not fp:
            continue
        status = (fp.get("status") or "").upper()
        if status not in (
            "QUEUED", "PLANNING", "STARTING", "RUNNING",
            "COMPLETED", "FAILED", "SKIPPED", "STOPPED",
        ):
            continue
        fid = _flow_id_of(ev)
        if not fid:
            continue
        ts_ms = _parse_iso_ms(ev.get("timestamp"))
        if ts_ms is None:
            continue
        by_flow.setdefault(fid, []).append((status, ts_ms, ev))

    segments: List[FlowSegment] = []
    for fid, entries in by_flow.items():
        entries.sort(key=lambda x: x[1])
        start_anchor: Optional[int] = None
        flow_name = ""
        for status, ts_ms, ev in entries:
            if not flow_name:
                flow_name = _flow_name_of(ev)
            # Use the first event we see (in chronological order) as
            # the start anchor — matches UI Duration semantics:
            # COMPLETED - first(QUEUED|PLANNING|STARTING|RUNNING).
            if start_anchor is None and status in (
                "QUEUED", "PLANNING", "STARTING", "RUNNING",
            ):
                start_anchor = ts_ms
            elif status in ("COMPLETED", "FAILED", "STOPPED", "SKIPPED"):
                start = start_anchor if start_anchor is not None else ts_ms
                fp = _flow_progress_details(ev) or {}
                metrics = fp.get("metrics") or {}
                metric_ms_raw = metrics.get("execution_duration_ms")
                metric_ms: Optional[int]
                try:
                    metric_ms = int(metric_ms_raw) if metric_ms_raw is not None else None
                except (TypeError, ValueError):
                    metric_ms = None
                if status == "COMPLETED" and metric_ms is not None and metric_ms >= 0:
                    source = "metric"
                    duration_ms = metric_ms
                else:
                    source = "timestamp_delta"
                    duration_ms = max(0, ts_ms - start)
                segments.append(FlowSegment(
                    flow_id=fid,
                    flow_name=flow_name,
                    started_ms=start,
                    completed_ms=ts_ms,
                    execution_duration_ms=duration_ms,
                    source=source,
                ))
                start_anchor = None
    return segments


def coverage_time_ms(intervals: Iterable[Tuple[int, int]]) -> int:
    """Compute the total ms covered by the *union* of half-open intervals.

    Coverage-time is the wall-clock window during which AT LEAST one flow
    was actively RUNNING. Equivalent to the engine's elapsed time if it
    had zero per-query overhead. The standard interval-merge algorithm.
    """
    sorted_ivs = sorted(
        ((a, b) for a, b in intervals if b >= a),
        key=lambda x: x[0],
    )
    if not sorted_ivs:
        return 0
    total = 0
    cur_start, cur_end = sorted_ivs[0]
    for s, e in sorted_ivs[1:]:
        if s <= cur_end:
            if e > cur_end:
                cur_end = e
        else:
            total += cur_end - cur_start
            cur_start, cur_end = s, e
    total += cur_end - cur_start
    return total


# --------------------------------------------------------------------- #
# Disk → batch-level summary                                            #
# --------------------------------------------------------------------- #


@dataclass
class PerTableCompute:
    schema: str
    table: str
    update_id: str
    compute_ms: int
    coverage_ms: int
    source: str  # "metric" | "timestamp_delta" | mix
    segment_count: int
    fallback_count: int
    creation_time_ms: Optional[int]


def _normalize_table_name(name: str) -> str:
    return (name or "").strip().lower()


def load_pipeline_events_for_batch(
    base_dir: str,
) -> List[Tuple[str, dict]]:
    """Read every ``<schema>.<table>/<update_id>.json`` under
    ``base_dir`` (which should be
    ``mount/pipeline-events/<sf>/databricks-enzyme/batch<N>/``).
    Returns list of (relative_path, payload).
    """
    out: List[Tuple[str, dict]] = []
    if not os.path.isdir(base_dir):
        return out
    pattern = os.path.join(base_dir, "*", "*.json")
    for fp in sorted(_glob.glob(pattern)):
        fname = os.path.basename(fp)
        if fname == "manifest.json" or fname.startswith("__"):
            continue
        try:
            with open(fp) as f:
                payload = json.load(f)
        except (OSError, ValueError):
            continue
        rel = os.path.relpath(fp, base_dir)
        out.append((rel, payload))
    return out


def compute_batch_summary(
    base_dir: str,
    window_start_ms: Optional[int],
    window_end_ms: Optional[int],
) -> Dict[str, Any]:
    """Aggregate per-MV pure compute and per-batch coverage-time.

    ``window_start_ms`` and ``window_end_ms`` (epoch-ms) bracket the
    batch's measured wall-clock. Updates whose ``creation_time`` falls
    INSIDE the window are attributed to this batch. If the window is
    not supplied, every update on disk is included (used by
    backfill / debug paths).

    Returns:
        ``{
            "tables": {"<schema>.<table>": [PerTableCompute, ...]},
            "batch": {
                "compute_wall_ms": int,   # coverage-time, primary
                "compute_work_ms": int,   # sum of per-MV compute, secondary
                "tables_with_compute": int,
                "updates_considered": int,
                "updates_in_window": int,
                "segments_total": int,
                "segments_fallback": int,
            },
        }``

    Raises ``RuntimeError`` if no per-update payloads were found on disk
    (caller should surface this as a hard batch failure).
    """
    payloads = load_pipeline_events_for_batch(base_dir)
    if not payloads:
        raise RuntimeError(
            f"databricks-enzyme: no pipeline-events JSON under {base_dir}"
        )

    tables: Dict[str, List[PerTableCompute]] = {}
    all_intervals: List[Tuple[int, int]] = []
    updates_considered = 0
    updates_in_window = 0
    segments_total = 0
    segments_fallback = 0

    for _rel, payload in payloads:
        schema = payload.get("schema") or "unknown"
        table = payload.get("table") or "unknown"
        upd = payload.get("update") or {}
        updates_considered += 1
        update_id = upd.get("update_id") or "unknown"
        creation_time_raw = upd.get("creation_time")
        creation_time_ms: Optional[int]
        if isinstance(creation_time_raw, (int, float)):
            creation_time_ms = int(creation_time_raw)
        else:
            creation_time_ms = _parse_iso_ms(creation_time_raw)

        if window_start_ms is not None and window_end_ms is not None:
            if creation_time_ms is None:
                continue
            # 30s slack on each end to absorb timer / API clock jitter.
            slack = 30_000
            if creation_time_ms < window_start_ms - slack:
                continue
            if creation_time_ms > window_end_ms + slack:
                continue

        updates_in_window += 1
        events = upd.get("events") or []
        segs = extract_flow_segments(events)
        if not segs:
            continue

        update_compute_ms = sum(s.execution_duration_ms or 0 for s in segs)
        update_coverage = coverage_time_ms(
            (s.started_ms, s.completed_ms) for s in segs
        )
        update_fallback = sum(1 for s in segs if s.source != "metric")
        update_source = (
            "metric"
            if all(s.source == "metric" for s in segs)
            else ("timestamp_delta" if all(s.source != "metric" for s in segs) else "mixed")
        )
        ptc = PerTableCompute(
            schema=schema,
            table=table,
            update_id=update_id,
            compute_ms=update_compute_ms,
            coverage_ms=update_coverage,
            source=update_source,
            segment_count=len(segs),
            fallback_count=update_fallback,
            creation_time_ms=creation_time_ms,
        )
        key = f"{schema}.{table}"
        tables.setdefault(key, []).append(ptc)
        all_intervals.extend(
            (s.started_ms, s.completed_ms) for s in segs
        )
        segments_total += len(segs)
        segments_fallback += update_fallback

    compute_wall_ms = coverage_time_ms(all_intervals)
    compute_work_ms = sum(
        ptc.compute_ms for plist in tables.values() for ptc in plist
    )

    return {
        "tables": tables,
        "batch": {
            "compute_wall_ms": int(compute_wall_ms),
            "compute_work_ms": int(compute_work_ms),
            "tables_with_compute": len(tables),
            "updates_considered": updates_considered,
            "updates_in_window": updates_in_window,
            "segments_total": segments_total,
            "segments_fallback": segments_fallback,
        },
    }


def best_per_table_compute_ms(per_table: List[PerTableCompute]) -> Optional[int]:
    """For a given MV, if multiple updates fell inside the window (e.g.
    a transient REFRESH retry), prefer the LATEST one — that's the
    update whose result is what the next batch sees. Returns the
    chosen update's compute_ms.
    """
    if not per_table:
        return None
    sorted_updates = sorted(
        per_table,
        key=lambda p: (p.creation_time_ms or 0, p.update_id),
    )
    return sorted_updates[-1].compute_ms


def summarize_for_persistence(summary: Dict[str, Any]) -> Dict[str, Any]:
    """Convert the structured PerTableCompute payloads into a
    JSON-friendly forensics blob for ``databricks-compute-batch<N>.json``.
    """
    out_tables: Dict[str, List[Dict[str, Any]]] = {}
    for key, updates in summary["tables"].items():
        out_tables[key] = [
            {
                "schema": u.schema,
                "table": u.table,
                "update_id": u.update_id,
                "compute_ms": u.compute_ms,
                "coverage_ms": u.coverage_ms,
                "source": u.source,
                "segment_count": u.segment_count,
                "fallback_count": u.fallback_count,
                "creation_time_ms": u.creation_time_ms,
            }
            for u in sorted(updates, key=lambda p: (p.creation_time_ms or 0, p.update_id))
        ]
    return {
        "batch": summary["batch"],
        "tables": out_tables,
    }


__all__ = [
    "FlowSegment",
    "PerTableCompute",
    "extract_flow_segments",
    "coverage_time_ms",
    "compute_batch_summary",
    "load_pipeline_events_for_batch",
    "best_per_table_compute_ms",
    "summarize_for_persistence",
]
