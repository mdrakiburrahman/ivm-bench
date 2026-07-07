"""Homogeneous Spark-native metrics A/B.

Single consolidated module that:

  1. Parses Spark's native **event log** (rolling JSON) captured for the
     ``spark`` and ``spark-openivm`` engines under
     ``mount/metrics/<sf>/<engine>/spark-events/`` — no engine-specific code.
  2. Maps each Spark SQL execution → dbt model via the ``ivm_node=<unique_id>``
     query-comment dbt stamps on every statement, and → batch via the
     per-batch wall-clock windows the engine runner records in SQLite.
  3. Emits a per-engine ``executions.jsonl`` sidecar and three query-aligned
     Parquet tables (``metrics_long`` / ``metrics_by_model`` / ``timeseries``)
     under ``mount/metrics/<sf>/processed/`` (DuckDB does the aggregation +
     Parquet write, so no pyarrow dependency).
  4. Renders an A/B diff PNG + RESULTS.md and bundles everything into
     ``mount/metrics/<sf>/spark-metrics-<run_id>.zip`` (+ a ``latest`` copy).

The Parquet schema + paths are a STABLE CONTRACT for openivm-spark (issue §6).

Runs offline: ``process()`` reads everything from disk + SQLite, so the parser
can be re-run against captured logs without re-running the benchmark.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sqlite3
import zipfile
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

ENGINES: Tuple[str, str] = ("spark", "spark-openivm")

# Engine→colour for the A/B chart (kept close to the rest of the harness).
_ENGINE_COLOURS = {"spark": "#4C78A8", "spark-openivm": "#F58518"}

# Statement grouping + write-target extraction from the Spark event log.
# Livy stamps every SQL execution's description with "Job group for statement N"
# (statement ids reset per Spark application). A dbt model build's Delta write
# command names the target ``schema.model`` in its Arguments line.
_STMT_RE = re.compile(r"statement (\d+)")
_WRITE_CMDS = (
    "AtomicReplaceTableAsSelect", "ReplaceTableAsSelect", "AtomicCreateTableAsSelect",
    "CreateTableAsSelect", "AppendData", "WriteIntoDelta", "OverwriteByExpression",
    "MergeIntoCommand", "ReplaceData", "WriteDelta",
)
# The Delta V2 write command's Arguments name the target as
# ``DeltaCatalog@<hash>, <schema>.<model>`` (source Delta scans instead show as
# FileScan/PreparedDeltaFileIndex), so this is the write-target signature.
_WRITE_TARGET_RE = re.compile(r"DeltaCatalog[^,\n]*,\s*([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)")
# spark-openivm's INCREMENTAL refresh writes via MergeIntoCommand into the MV's
# backing store at ``_ivm/views/<schema>/<model>`` (named first in the merge
# Arguments; source MVs appear lower in the plan), so the first match = target.
_IVM_VIEW_RE = re.compile(r"_ivm/views/([A-Za-z0-9_]+)/([A-Za-z0-9_]+)")
# dbt `location_root: none` makes the plain-spark target/scan path ``none/<model>``.
_NONE_RE = re.compile(r"none/([A-Za-z0-9_]+)")

# Headline KPI columns (issue §5) — the stable metrics_by_model contract.
HEADLINE_KPIS: Tuple[str, ...] = (
    "wall_clock_ms",
    "peak_jvm_heap_bytes",
    "peak_execution_memory_bytes",
    "input_bytes",
    "files_scanned",
    "shuffle_read_bytes",
    "shuffle_write_bytes",
    "spill_memory_bytes",
    "spill_disk_bytes",
    "records_read",
    "records_written",
    "gc_time_ms",
)


# ---------------------------------------------------------------------------
# Event-log discovery + parsing
# ---------------------------------------------------------------------------

def _iter_app_groups(events_root: str) -> List[Tuple[str, List[str]]]:
    """Group event-log part files by Spark application.

    Rolling logs live in ``<root>/eventlog_v2_<appId>/events_<n>_<appId>``; each
    appId is a distinct Spark application (one per Livy session / dbt batch).
    Returns ``[(app_id, [files-sorted-by-part])]``. Statement/execution ids
    RESET per application, so callers MUST scope grouping per app group.
    """
    if not os.path.isdir(events_root):
        return []
    groups: Dict[str, List[Tuple[int, str]]] = {}
    for dirpath, _dirnames, filenames in os.walk(events_root):
        for fn in filenames:
            if fn.startswith("appstatus_") or fn.endswith(".crc"):
                continue
            full = os.path.join(dirpath, fn)
            base = fn[:-len(".inprogress")] if fn.endswith(".inprogress") else fn
            m = re.match(r"events_(\d+)_(.+)$", base)
            if m:
                idx, app_id = int(m.group(1)), m.group(2)
            else:
                idx, app_id = 0, os.path.basename(dirpath) or base
            groups.setdefault(app_id, []).append((idx, full))
    out: List[Tuple[str, List[str]]] = []
    for app_id, parts in sorted(groups.items()):
        parts.sort(key=lambda p: p[0])
        out.append((app_id, [p[1] for p in parts]))
    return out


def _iter_event_files(events_root: str) -> List[str]:
    """Flat list of all event-log part files (used only for presence checks)."""
    return [f for _app, files in _iter_app_groups(events_root) for f in files]


def _safe_num(value: Any) -> float:
    try:
        if value is None or value == "":
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _model_from_write_plan(plan: str) -> Optional[str]:
    """Extract the write-target model name from a Spark physical plan.

    dbt model builds land as a Delta write whose command Arguments name the
    target ``schema.model`` — e.g. ``AtomicReplaceTableAsSelect`` (full refresh)
    or ``WriteIntoDelta`` / ``AppendData`` (incremental). For the plain-`spark`
    engine the table is also distinguishable by its ``none/<model>`` location
    (dbt `location_root: none`). spark-openivm's MV refresh writes the MV's
    backing Delta table, matched the same way. Returns the bare model name.
    """
    if not plan:
        return None
    if not any(w in plan for w in _WRITE_CMDS):
        return None
    m = _WRITE_TARGET_RE.search(plan)
    if m:
        return m.group(2)
    m = _IVM_VIEW_RE.search(plan)
    if m:
        return m.group(2)
    nm = _NONE_RE.findall(plan)
    if nm:
        return nm[-1]
    return None


def _task_metric_rows(tm: Dict[str, Any]) -> Dict[str, float]:
    """Flatten a Spark ``Task Metrics`` blob into our canonical metric names."""
    inp = tm.get("Input Metrics", {}) or {}
    out = tm.get("Output Metrics", {}) or {}
    sr = tm.get("Shuffle Read Metrics", {}) or {}
    sw = tm.get("Shuffle Write Metrics", {}) or {}
    return {
        "executor_run_time_ms": _safe_num(tm.get("Executor Run Time")),
        "executor_cpu_time_ns": _safe_num(tm.get("Executor CPU Time")),
        "gc_time_ms": _safe_num(tm.get("JVM GC Time")),
        "result_size_bytes": _safe_num(tm.get("Result Size")),
        "peak_execution_memory_bytes": _safe_num(tm.get("Peak Execution Memory")),
        "spill_memory_bytes": _safe_num(tm.get("Memory Bytes Spilled")),
        "spill_disk_bytes": _safe_num(tm.get("Disk Bytes Spilled")),
        "input_bytes": _safe_num(inp.get("Bytes Read")),
        "records_read": _safe_num(inp.get("Records Read")),
        "output_bytes": _safe_num(out.get("Bytes Written")),
        "records_written": _safe_num(out.get("Records Written")),
        "shuffle_read_bytes": _safe_num(sr.get("Remote Bytes Read"))
        + _safe_num(sr.get("Local Bytes Read")),
        "shuffle_read_records": _safe_num(sr.get("Total Records Read")),
        "shuffle_write_bytes": _safe_num(sw.get("Shuffle Bytes Written")),
        "shuffle_write_records": _safe_num(sw.get("Shuffle Records Written")),
    }


# SQL-plan task accumulables surfaced by name (best-effort; absent ⇒ 0).
# Note: "number of files read" is a driver-side SQL metric, handled separately
# via SparkListenerDriverAccumUpdates (see parse_app_events).
_ACCUM_NAMES: Dict[str, str] = {}

# Map an SQL-metric accumulatorId → our canonical name, scraped from the plan
# metric defs (``{"name":"number of files read","accumulatorId":N,...}``).
_FILES_ACCUM_RE = re.compile(r'"name":"number of files read","accumulatorId":(\d+)')


def _accum_rows(task_info: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for acc in task_info.get("Accumulables", []) or []:
        name = str(acc.get("Name", "")).strip().lower()
        canon = _ACCUM_NAMES.get(name)
        if canon:
            out[canon] = out.get(canon, 0.0) + _safe_num(acc.get("Update"))
    return out


def parse_app_events(files: List[str]) -> Dict[str, Any]:
    """Parse ONE Spark application's event log into maps + a task list.

    Statement/execution ids are scoped to this application. Resolves each
    execution → dbt model by grouping executions by Livy ``statement N`` and
    taking the model from whichever execution in that statement carries the
    Delta write-target (``parse → resolve`` two-pass).
    """
    executions: Dict[int, Dict[str, Any]] = {}
    stage_to_exec: Dict[int, int] = {}
    stage_to_job: Dict[int, int] = {}
    tasks: List[Dict[str, Any]] = []
    stage_exec_metrics: List[Dict[str, Any]] = []
    files_accum_ids: set = set()  # accumulatorIds that count "files read"

    for path in files:
        try:
            f = open(path, "r", encoding="utf-8", errors="replace")
        except OSError:
            continue
        with f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                etype = ev.get("Event", "")

                if etype.endswith("SparkListenerSQLExecutionStart"):
                    eid = ev.get("executionId")
                    if eid is None:
                        continue
                    desc = ev.get("description") or ""
                    plan = ev.get("physicalPlanDescription") or ""
                    sm = _STMT_RE.search(desc)
                    executions[eid] = {
                        "start_ms": _safe_num(ev.get("time")),
                        "end_ms": None,
                        "stmt": int(sm.group(1)) if sm else None,
                        "model_self": _model_from_write_plan(plan),
                        "model": None,
                        "files_scanned": 0.0,
                        "description": desc[:200],
                    }
                    files_accum_ids.update(int(x) for x in _FILES_ACCUM_RE.findall(line))
                elif etype.endswith("SparkListenerSQLAdaptiveExecutionUpdate"):
                    # AQE re-plans expose fresh metric defs (new accumulatorIds).
                    files_accum_ids.update(int(x) for x in _FILES_ACCUM_RE.findall(line))
                elif etype.endswith("SparkListenerDriverAccumUpdates"):
                    eid = ev.get("executionId")
                    meta = executions.get(eid)
                    if meta is not None:
                        for pair in ev.get("accumUpdates", []) or []:
                            if len(pair) == 2 and int(pair[0]) in files_accum_ids:
                                meta["files_scanned"] += _safe_num(pair[1])
                elif etype.endswith("SparkListenerSQLExecutionEnd"):
                    eid = ev.get("executionId")
                    if eid in executions:
                        executions[eid]["end_ms"] = _safe_num(ev.get("time"))
                elif etype == "SparkListenerJobStart":
                    job_id = ev.get("Job ID")
                    props = ev.get("Properties", {}) or {}
                    exec_id_raw = props.get("spark.sql.execution.id")
                    exec_id = int(exec_id_raw) if exec_id_raw not in (None, "") else None
                    for sid in ev.get("Stage IDs", []) or []:
                        stage_to_job[sid] = job_id
                        if exec_id is not None:
                            stage_to_exec[sid] = exec_id
                elif etype == "SparkListenerTaskEnd":
                    info = ev.get("Task Info", {}) or {}
                    tm = ev.get("Task Metrics") or {}
                    tasks.append({
                        "stage_id": ev.get("Stage ID"),
                        "task_id": info.get("Task ID"),
                        "finish_ms": _safe_num(info.get("Finish Time")),
                        "metrics": _task_metric_rows(tm),
                        "accums": _accum_rows(info),
                    })
                elif etype == "SparkListenerStageExecutorMetrics":
                    em = ev.get("Executor Metrics", {}) or {}
                    stage_exec_metrics.append({
                        "stage_id": ev.get("Stage ID"),
                        "jvm_heap": _safe_num(em.get("JVMHeapMemory")),
                        "on_heap_exec": _safe_num(em.get("OnHeapExecutionMemory")),
                        "on_heap_storage": _safe_num(em.get("OnHeapStorageMemory")),
                        "off_heap_exec": _safe_num(em.get("OffHeapExecutionMemory")),
                        "off_heap_storage": _safe_num(em.get("OffHeapStorageMemory")),
                    })

    # Resolve statement → model from any execution that carried a write-target,
    # then stamp every execution in that statement with the model.
    stmt_model: Dict[int, str] = {}
    for meta in executions.values():
        stmt, model = meta.get("stmt"), meta.get("model_self")
        if stmt is not None and model:
            stmt_model.setdefault(stmt, model)
    for meta in executions.values():
        meta["model"] = meta.get("model_self") or stmt_model.get(meta.get("stmt"))

    return {
        "executions": executions,
        "stage_to_exec": stage_to_exec,
        "stage_to_job": stage_to_job,
        "tasks": tasks,
        "stage_exec_metrics": stage_exec_metrics,
    }


# ---------------------------------------------------------------------------
# Batch attribution
# ---------------------------------------------------------------------------

def read_batch_windows(
    db_path: str, benchmark_id: str, engine: str
) -> Dict[int, Tuple[float, float]]:
    """Read per-batch wall-clock windows from benchmark-server's SQLite.

    The engine runner stamps ``wall_window_{start,end}_ms`` into the
    ``engine_batches.result_json`` ``extra`` blob (engine_runner.py ~L471).
    """
    windows: Dict[int, Tuple[float, float]] = {}
    if not benchmark_id or not os.path.exists(db_path):
        return windows
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT batch_num, result_json FROM engine_batches "
            "WHERE benchmark_id=? AND engine=?",
            (benchmark_id, engine),
        ).fetchall()
        conn.close()
    except sqlite3.Error as e:
        logger.warning("[spark-metrics] batch-window read failed: %s", e)
        return windows
    for r in rows:
        try:
            extra = (json.loads(r["result_json"]) or {}).get("extra", {})
        except (TypeError, json.JSONDecodeError):
            extra = {}
        start = extra.get("wall_window_start_ms")
        end = extra.get("wall_window_end_ms")
        if start is not None and end is not None:
            windows[int(r["batch_num"])] = (float(start), float(end))
    return windows


def _batch_for_ts(ts_ms: float, windows: Dict[int, Tuple[float, float]]) -> Optional[int]:
    for batch, (start, end) in windows.items():
        if start <= ts_ms <= end:
            return batch
    return None


# ---------------------------------------------------------------------------
# Long-form row construction
# ---------------------------------------------------------------------------

def build_engine_rows(
    engine: str,
    sf: str,
    run_id: str,
    events_root: str,
    windows: Dict[int, Tuple[float, float]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int, int]:
    """Return (long_rows, ts_rows, exec_rows, n_apps, n_execs) for one engine.

    Iterates per Spark application (statement/exec ids reset per app, so each
    app group is parsed + resolved independently) and concatenates the rows.
    """
    long_rows: List[Dict[str, Any]] = []
    ts_rows: List[Dict[str, Any]] = []
    exec_rows: List[Dict[str, Any]] = []
    n_apps = 0
    n_execs = 0

    _TS_METRICS = (
        "input_bytes", "shuffle_read_bytes", "shuffle_write_bytes",
        "spill_memory_bytes", "spill_disk_bytes", "peak_execution_memory_bytes",
    )

    for app_id, files in _iter_app_groups(events_root):
        parsed = parse_app_events(files)
        executions = parsed["executions"]
        stage_to_exec = parsed["stage_to_exec"]
        stage_to_job = parsed["stage_to_job"]
        n_apps += 1
        n_execs += len(executions)

        def exec_meta(stage_id: Any) -> Tuple[Optional[int], Optional[str], Optional[int]]:
            eid = stage_to_exec.get(stage_id)
            meta = executions.get(eid) if eid is not None else None
            model = meta["model"] if meta else None
            start = meta["start_ms"] if meta else None
            batch = _batch_for_ts(start, windows) if start is not None else None
            return eid, model, batch

        for t in parsed["tasks"]:
            sid = t["stage_id"]
            eid, model, batch = exec_meta(sid)
            job_id = stage_to_job.get(sid)
            finish_ms = t["finish_ms"]
            merged = dict(t["metrics"])
            merged.update(t["accums"])
            for mname, mval in merged.items():
                long_rows.append({
                    "engine": engine, "sf": sf, "run_id": run_id, "batch": batch,
                    "dbt_model": model, "execution_id": eid, "job_id": job_id,
                    "stage_id": sid, "task_id": t["task_id"],
                    "metric_name": mname, "metric_value": float(mval),
                    "event_ts": finish_ms,
                })
            for mname in _TS_METRICS:
                if mname in merged:
                    ts_rows.append({
                        "engine": engine, "batch": batch, "dbt_model": model,
                        "ts": finish_ms, "metric_name": mname,
                        "metric_value": float(merged[mname]),
                    })

        # Per-stage peak memory → fan out onto the owning execution/model/batch.
        for sm in parsed["stage_exec_metrics"]:
            sid = sm["stage_id"]
            eid, model, batch = exec_meta(sid)
            for mname, mval in (
                ("peak_jvm_heap_bytes", sm["jvm_heap"]),
                ("peak_on_heap_execution_bytes", sm["on_heap_exec"]),
                ("peak_on_heap_storage_bytes", sm["on_heap_storage"]),
            ):
                long_rows.append({
                    "engine": engine, "sf": sf, "run_id": run_id, "batch": batch,
                    "dbt_model": model, "execution_id": eid, "job_id": None,
                    "stage_id": sid, "task_id": None,
                    "metric_name": mname, "metric_value": float(mval),
                    "event_ts": None,
                })

        # executions.jsonl — one object per SQL execution (the join sidecar).
        # Also emit per-execution SQL metrics (files scanned) as long rows.
        for eid, meta in sorted(executions.items()):
            start = meta.get("start_ms")
            batch = _batch_for_ts(start, windows) if start is not None else None
            model = meta.get("model")
            files_scanned = float(meta.get("files_scanned") or 0.0)
            exec_rows.append({
                "engine": engine, "sf": sf, "run_id": run_id, "app_id": app_id,
                "execution_id": eid, "dbt_model": model,
                "batch": batch,
                "start_ms": start, "end_ms": meta.get("end_ms"),
                "duration_ms": (meta["end_ms"] - start)
                if (meta.get("end_ms") is not None and start is not None) else None,
                "files_scanned": files_scanned,
                "description": meta.get("description"),
            })
            if files_scanned:
                long_rows.append({
                    "engine": engine, "sf": sf, "run_id": run_id, "batch": batch,
                    "dbt_model": model, "execution_id": eid, "job_id": None,
                    "stage_id": None, "task_id": None,
                    "metric_name": "files_scanned", "metric_value": files_scanned,
                    "event_ts": start,
                })

    return long_rows, ts_rows, exec_rows, n_apps, n_execs


# ---------------------------------------------------------------------------
# DuckDB aggregation + Parquet write
# ---------------------------------------------------------------------------

def _write_parquet_and_rollup(
    long_rows: List[Dict[str, Any]],
    ts_rows: List[Dict[str, Any]],
    exec_rows: List[Dict[str, Any]],
    processed_dir: str,
) -> List[Dict[str, Any]]:
    """Write the three Parquet tables via DuckDB; return metrics_by_model rows."""
    import duckdb
    import pandas as pd

    os.makedirs(processed_dir, exist_ok=True)
    long_df = pd.DataFrame(long_rows)
    ts_df = pd.DataFrame(ts_rows)

    con = duckdb.connect()
    con.register("long_df", long_df)
    con.register("ts_df", ts_df)

    con.execute(
        f"COPY long_df TO '{os.path.join(processed_dir, 'metrics_long.parquet')}' "
        "(FORMAT PARQUET)"
    )
    con.execute(
        f"COPY ts_df TO '{os.path.join(processed_dir, 'timeseries.parquet')}' "
        "(FORMAT PARQUET)"
    )

    # metrics_by_model — pivot the headline KPIs. SUM for additive metrics,
    # MAX for the peak/high-water metrics; wall_clock is summed from execution
    # durations (handled via the execution table below, joined back in).
    max_metrics = {"peak_jvm_heap_bytes", "peak_execution_memory_bytes"}
    agg_exprs = []
    for kpi in HEADLINE_KPIS:
        if kpi == "wall_clock_ms":
            continue
        fn = "max" if kpi in max_metrics else "sum"
        agg_exprs.append(
            f"COALESCE({fn}(CASE WHEN metric_name='{kpi}' THEN metric_value END), 0) AS {kpi}"
        )
    by_model_sql = (
        "SELECT engine, sf, run_id, batch, dbt_model, "
        + ", ".join(agg_exprs)
        + " FROM long_df GROUP BY engine, sf, run_id, batch, dbt_model"
    )
    by_model = con.execute(by_model_sql).fetchdf()

    # wall_clock_ms per (engine,batch,model) from execution durations.
    exec_df = pd.DataFrame(exec_rows)
    if not exec_df.empty:
        con.register("exec_df", exec_df)
        wall = con.execute(
            "SELECT engine, batch, dbt_model, "
            "COALESCE(SUM(duration_ms), 0) AS wall_clock_ms "
            "FROM exec_df GROUP BY engine, batch, dbt_model"
        ).fetchdf()
        by_model = by_model.merge(
            wall, on=["engine", "batch", "dbt_model"], how="left"
        )
    else:
        by_model["wall_clock_ms"] = 0.0
    by_model["wall_clock_ms"] = by_model["wall_clock_ms"].fillna(0.0)

    ordered = ["engine", "sf", "run_id", "batch", "dbt_model", *HEADLINE_KPIS]
    by_model = by_model[[c for c in ordered if c in by_model.columns]]
    con.register("by_model_df", by_model)
    con.execute(
        f"COPY by_model_df TO '{os.path.join(processed_dir, 'metrics_by_model.parquet')}' "
        "(FORMAT PARQUET)"
    )
    con.close()
    return by_model.to_dict("records")


# ---------------------------------------------------------------------------
# A/B diff chart + RESULTS.md
# ---------------------------------------------------------------------------

def _fmt_bytes(n: float) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"


def _fmt_count(n: float) -> str:
    n = float(n or 0)
    for unit in ("", "K", "M", "B", "T"):
        if abs(n) < 1000.0:
            return f"{n:.0f}{unit}" if unit == "" else f"{n:.1f}{unit}"
        n /= 1000.0
    return f"{n:.1f}P"


# Row order for the metric x query lifecycle grid; any extra time-bearing
# metric present in the data is appended after these.
_METRIC_ROW_ORDER = (
    "input_bytes", "records_read", "files_scanned",
    "output_bytes", "records_written",
    "shuffle_read_bytes", "shuffle_read_records",
    "shuffle_write_bytes", "shuffle_write_records",
    "spill_memory_bytes", "spill_disk_bytes",
    "peak_execution_memory_bytes",
    "executor_run_time_ms", "executor_cpu_time_ns",
    "gc_time_ms", "result_size_bytes",
)
# High-water metrics → running max along the lifecycle; everything else is
# additive → cumulative sum.
_PEAK_METRICS = {
    "peak_execution_memory_bytes", "peak_jvm_heap_bytes",
    "peak_on_heap_execution_bytes", "peak_on_heap_storage_bytes",
}


def _render_diff_png(processed_dir: str, out_path: str, max_models: int = 24) -> bool:
    """Metric x query lifecycle grid: one ROW per captured metric, one COLUMN
    per dbt query (model), each cell a cumulative time-series over the query's
    runtime with ``spark`` vs ``spark-openivm`` overlaid.

    Reads ``metrics_long.parquet`` via DuckDB (memory-safe at scale), auto-picks
    the batch with the most models shared by both engines (prefers an
    incremental batch, where the IVM A/B is starkest).
    """
    import duckdb
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    long_p = os.path.join(processed_dir, "metrics_long.parquet")
    if not os.path.exists(long_p):
        return False
    con = duckdb.connect()
    rp = f"read_parquet('{long_p}')"

    batch_row = con.execute(
        f"WITH t AS (SELECT batch, dbt_model, count(DISTINCT engine) ne FROM {rp} "
        f"WHERE event_ts IS NOT NULL AND dbt_model IS NOT NULL AND batch IS NOT NULL "
        f"GROUP BY batch, dbt_model) "
        f"SELECT batch FROM t GROUP BY batch HAVING count(*) FILTER (WHERE ne>=2)>=2 "
        f"ORDER BY (batch>1) DESC, count(*) FILTER (WHERE ne>=2) DESC, batch DESC LIMIT 1"
    ).fetchone()
    if not batch_row:
        con.close()
        return False
    batch = batch_row[0]

    models = [r[0] for r in con.execute(
        f"SELECT dbt_model FROM {rp} WHERE batch={batch} AND metric_name='input_bytes' "
        f"AND dbt_model IN (SELECT dbt_model FROM {rp} WHERE batch={batch} "
        f"  AND dbt_model IS NOT NULL AND event_ts IS NOT NULL "
        f"  GROUP BY dbt_model HAVING count(DISTINCT engine)>=2) "
        f"GROUP BY dbt_model ORDER BY sum(metric_value) DESC LIMIT {max_models}"
    ).fetchall()]
    if not models:
        con.close()
        return False

    present = {r[0] for r in con.execute(
        f"SELECT DISTINCT metric_name FROM {rp} WHERE batch={batch} AND event_ts IS NOT NULL"
    ).fetchall()}
    metrics = [m for m in _METRIC_ROW_ORDER if m in present] + \
              sorted(m for m in present if m not in _METRIC_ROW_ORDER)

    model_list = ",".join("'" + m.replace("'", "''") + "'" for m in models)
    df = con.execute(
        f"SELECT engine, dbt_model, metric_name, event_ts, metric_value FROM {rp} "
        f"WHERE batch={batch} AND event_ts IS NOT NULL AND dbt_model IN ({model_list})"
    ).df()
    sf = con.execute(f"SELECT sf FROM {rp} LIMIT 1").fetchone()[0]
    con.close()
    if df.empty:
        return False

    engines = [e for e in ENGINES if e in set(df["engine"])]
    nrows, ncols = len(metrics), len(models)
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(2.4 * ncols, 1.55 * nrows), squeeze=False,
    )
    # Pre-group for speed: (model, engine, metric) -> sorted frame.
    grouped = {k: g.sort_values("event_ts")
               for k, g in df.groupby(["dbt_model", "engine", "metric_name"], sort=False)}

    for ri, metric in enumerate(metrics):
        is_bytes = "byte" in metric
        fmt = _fmt_bytes if is_bytes else _fmt_count
        agg_max = metric in _PEAK_METRICS
        for ci, model in enumerate(models):
            ax = axes[ri][ci]
            for eng in engines:
                g = grouped.get((model, eng, metric))
                if g is None or g.empty:
                    continue
                t0 = g["event_ts"].min()
                x = (g["event_ts"] - t0) / 1000.0
                y = g["metric_value"].cummax() if agg_max else g["metric_value"].cumsum()
                ax.plot(x, y, lw=1.1, color=_ENGINE_COLOURS.get(eng, "#888"))
            ax.tick_params(labelsize=6, length=2)
            ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _p, f=fmt: f(v)))
            ax.margins(x=0.03, y=0.12)
            ax.grid(True, alpha=0.2)
            if ri == 0:
                ax.set_title(model, fontsize=8)
            if ci == 0:
                ax.set_ylabel(metric, fontsize=8, rotation=0, ha="right", va="center")
                ax.yaxis.set_label_coords(-0.55, 0.5)

    handles = [plt.Line2D([0], [0], color=_ENGINE_COLOURS.get(e, "#888"), lw=2, label=e)
               for e in engines]
    fig.legend(handles=handles, loc="upper right", fontsize=11, ncol=len(engines))
    fig.suptitle(
        f"Per-query lifecycle — ALL metrics (rows) × queries (cols), cumulative over "
        f"query runtime (SF={sf}, batch {batch}; x=seconds since query start)",
        fontsize=13, y=0.997,
    )
    fig.supxlabel("seconds since query start", fontsize=10)
    fig.tight_layout(rect=[0.02, 0.01, 1, 0.985])
    tmp = out_path + ".tmp.png"
    fig.savefig(tmp, dpi=85)
    plt.close(fig)
    os.replace(tmp, out_path)
    return True


def _render_results_md(by_model_rows: List[Dict[str, Any]], out_path: str, run_id: str) -> None:
    import pandas as pd

    df = pd.DataFrame(by_model_rows)
    lines: List[str] = [
        "# Spark metrics A/B — `spark` vs `spark-openivm`",
        "",
        f"- run_id: `{run_id}`",
    ]
    if df.empty:
        lines.append("\n_No metrics captured._\n")
        _atomic_write(out_path, "\n".join(lines))
        return

    sf = df["sf"].iloc[0] if "sf" in df.columns else "?"
    lines.append(f"- scale factor: `{sf}`")
    engines = [e for e in ENGINES if e in set(df["engine"])]
    lines.append(f"- engines present: {', '.join(f'`{e}`' for e in engines)}")
    lines.append("")

    # Totals table (batch-summed; peaks max).
    lines += ["## Totals (all batches)", "",
              "| KPI | " + " | ".join(engines) + " | ratio (openivm/spark) |",
              "|---|" + "---|" * (len(engines) + 1)]
    max_metrics = {"peak_jvm_heap_bytes", "peak_execution_memory_bytes"}
    for kpi in HEADLINE_KPIS:
        cells = []
        vals: Dict[str, float] = {}
        for eng in engines:
            sub = df[df["engine"] == eng]
            v = float(sub[kpi].max() if kpi in max_metrics else sub[kpi].sum()) if kpi in sub.columns else 0.0
            vals[eng] = v
            cells.append(_fmt_bytes(v) if kpi.endswith("_bytes") else f"{v:,.0f}")
        ratio = ""
        if "spark" in vals and "spark-openivm" in vals and vals["spark"]:
            ratio = f"{vals['spark-openivm'] / vals['spark']:.2f}×"
        lines.append(f"| {kpi} | " + " | ".join(cells) + f" | {ratio} |")
    lines.append("")
    _atomic_write(out_path, "\n".join(lines))


def _atomic_write(path: str, text: str) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Top-level entry point
# ---------------------------------------------------------------------------

def process(
    repo_dir: str,
    sf: str,
    benchmark_id: str,
    run_id: str,
    engines: Optional[Iterable[str]] = None,
    emit: Callable[[str], None] = logger.info,
) -> Dict[str, Any]:
    """Parse event logs for both engines and emit all artifacts.

    Returns a summary dict (also handy for the REST layer / tests).
    """
    engines = [e for e in (engines or ENGINES) if e in ENGINES]
    metrics_root = os.path.join(repo_dir, "mount", "metrics", str(sf))
    processed_dir = os.path.join(metrics_root, "processed")
    db_path = os.path.join(repo_dir, "mount", "benchmark-state", "benchmark.db")

    all_long: List[Dict[str, Any]] = []
    all_ts: List[Dict[str, Any]] = []
    per_engine_exec: Dict[str, List[Dict[str, Any]]] = {}
    engines_with_data: List[str] = []

    for engine in engines:
        events_root = os.path.join(metrics_root, engine, "spark-events")
        files = _iter_event_files(events_root)
        if not files:
            emit(f"[spark-metrics] {engine}: no event-log files under {events_root}")
            continue
        windows = read_batch_windows(db_path, benchmark_id, engine)
        long_rows, ts_rows, exec_rows, n_apps, n_execs = build_engine_rows(
            engine, str(sf), run_id, events_root, windows
        )
        all_long.extend(long_rows)
        all_ts.extend(ts_rows)
        per_engine_exec[engine] = exec_rows
        if long_rows:
            engines_with_data.append(engine)

        # Write the per-engine executions.jsonl sidecar.
        exec_path = os.path.join(metrics_root, engine, "executions.jsonl")
        os.makedirs(os.path.dirname(exec_path), exist_ok=True)
        with open(exec_path, "w", encoding="utf-8") as f:
            for row in exec_rows:
                f.write(json.dumps(row) + "\n")
        mapped = sum(1 for r in exec_rows if r.get("dbt_model"))
        emit(
            f"[spark-metrics] {engine}: {n_apps} app(s), {n_execs} executions "
            f"({mapped} model-mapped), {len(long_rows)} metric rows "
            f"({len(files)} event files)"
        )

    if not all_long:
        emit("[spark-metrics] no event-log data for any engine — nothing to emit")
        return {"status": "empty", "engines": [], "run_id": run_id, "sf": str(sf)}

    by_model_rows = _write_parquet_and_rollup(all_long, all_ts, [
        r for rows in per_engine_exec.values() for r in rows
    ], processed_dir)

    png_path = os.path.join(processed_dir, "spark-ab-diff.png")
    md_path = os.path.join(processed_dir, "RESULTS.md")
    try:
        _render_diff_png(processed_dir, png_path)
    except Exception as e:  # chart is best-effort
        emit(f"[spark-metrics] diff PNG render failed: {e}")
        logger.exception("diff PNG render failed")
    _render_results_md(by_model_rows, md_path, run_id)

    zip_path = os.path.join(metrics_root, f"spark-metrics-{run_id}.zip")
    _zip_processed(processed_dir, zip_path)
    latest_zip = os.path.join(metrics_root, "spark-metrics-latest.zip")
    try:
        shutil.copy2(zip_path, latest_zip)
    except OSError as e:
        emit(f"[spark-metrics] latest-zip copy failed: {e}")

    summary = {
        "status": "ok",
        "run_id": run_id,
        "sf": str(sf),
        "engines": engines_with_data,
        "both_engines": all(e in engines_with_data for e in ENGINES),
        "model_rows": len(by_model_rows),
        "long_rows": len(all_long),
        "processed_dir": processed_dir,
        "zip": zip_path,
    }
    emit(
        f"[spark-metrics] processed: engines={engines_with_data} "
        f"models={len(by_model_rows)} → {processed_dir}"
    )
    return summary


def _zip_processed(processed_dir: str, zip_path: str) -> None:
    tmp = zip_path + ".tmp"
    with zipfile.ZipFile(tmp, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, _dn, filenames in os.walk(processed_dir):
            for fn in filenames:
                full = os.path.join(dirpath, fn)
                zf.write(full, os.path.relpath(full, processed_dir))
    os.replace(tmp, zip_path)
