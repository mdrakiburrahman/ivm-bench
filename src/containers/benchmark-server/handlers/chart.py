"""Chart handler — generates PNG visualization of benchmark results.

Generates these chart types:
1. Scale-factor chart (scale-factor-*.png): batch durations, source data,
   per-query execution times, and container resource time series.
2. Heuristics chart (benchmark-heuristics.png): operator heatmaps,
   lineage DAGs, and operator chains per query.
"""

import glob as _glob
import json
import logging
import os
import re
from collections import defaultdict
from io import BytesIO
from typing import Any, Dict, Optional, Tuple

from flask import Blueprint, Flask, Response, jsonify, request

from handlers.base import BaseHandler

logger = logging.getLogger(__name__)

bp = Blueprint("chart", __name__)

# ---------- Constants ----------

ENGINE_COLORS = {
    "duckdb": "#f7e900",
    "spark": "#da571b",
    "feldera": "#ca46bf",
    "duckdb-openivm": "#4a90d9",
    "spark-openivm": "#27ae60",
    "databricks-enzyme": "#c0392b",
}

ENGINE_ORDER = ["duckdb", "spark", "duckdb-openivm", "spark-openivm", "feldera", "databricks-enzyme"]

BATCH_COLORS = {1: "#3498db", 2: "#e67e22", 3: "#2ecc71"}

OPERATOR_ORDER = [
    "cte", "join_cross", "join_inner", "join_left", "join_right",
    "join_full", "join_left_anti", "join_left_semi", "join_right_anti",
    "join_right_semi", "aggregates", "window_functions", "sort",
    "distinct", "subqueries", "delete_update",
]

OPERATOR_SHORT = {
    "cte": "CTE", "join_cross": "J-CROSS", "join_inner": "J-INNER",
    "join_left": "J-LEFT", "join_right": "J-RIGHT", "join_full": "J-FULL",
    "join_left_anti": "J-L-ANTI", "join_left_semi": "J-L-SEMI",
    "join_right_anti": "J-R-ANTI", "join_right_semi": "J-R-SEMI",
    "aggregates": "AGG", "window_functions": "WIN", "sort": "SORT",
    "distinct": "DIST", "subqueries": "SUBQ", "delete_update": "DML",
}

OPERATOR_COLORS = {
    "cte": "#9b59b6", "join_cross": "#1a5276", "join_inner": "#2980b9",
    "join_left": "#3498db", "join_right": "#5dade2", "join_full": "#1abc9c",
    "join_left_anti": "#48c9b0", "join_left_semi": "#76d7c4",
    "join_right_anti": "#117a65", "join_right_semi": "#148f77",
    "aggregates": "#27ae60", "window_functions": "#e67e22",
    "sort": "#f39c12", "distinct": "#e74c3c",
    "subqueries": "#8d6e63", "delete_update": "#c0392b",
}

ROLE_COLORS = {
    "source": "#27ae60",
    "intermediate": "#3498db",
    "target": "#e67e22",
    "standalone": "#95a5a6",
}


# ---------- Helpers ----------

def _get_engine_color(engine: str) -> str:
    return ENGINE_COLORS.get(engine.lower(), "#888888")


def _sort_engines(engines: list) -> list:
    order = {e: i for i, e in enumerate(ENGINE_ORDER)}
    return sorted(engines, key=lambda e: order.get(e, 999))


def _format_duration(seconds: float) -> str:
    s = int(seconds)
    h, remainder = divmod(s, 3600)
    m, sec = divmod(remainder, 60)
    return f"{h:02d}:{m:02d}:{sec:02d}"


# ==========================================================================
# Scale-Factor Chart
# ==========================================================================

def generate_chart_png(
    state_dir: str,
    sf: str = "?",
    b1pct: str = "",
    b2pct: str = "",
    b3pct: str = "",
    engine_resources: Optional[dict] = None,
    stats_dir: Optional[str] = None,
) -> Optional[bytes]:
    """Generate the scale-factor PNG chart.

    Sections (top to bottom):
    1. Batch duration bar chart (from benchmark-results.json)
    2. Source data stats (from delta-stats-*.json)
    3. Per-query execution times (from run-*.json)
    4. Container resource time series (from container_stats.jsonl)
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    batch_pcts = {1: b1pct, 2: b2pct, 3: b3pct}

    # --- Load execution time records ---
    records = []
    for filepath in sorted(_glob.glob(os.path.join(state_dir, "run-*.json"))):
        filename = os.path.basename(filepath)
        m = re.match(r"run-(.+)-batch(\d+)\.json", filename)
        if not m:
            continue
        engine = m.group(1)
        batch_num = int(m.group(2))
        with open(filepath) as f:
            data = json.load(f)
        for node in data.get("nodes", []):
            if node.get("resource_type") != "model":
                continue
            records.append({
                "query_label": node["name"],
                "engine": engine,
                "batch_num": batch_num,
                "duration_s": node["execution_time_s"],
            })

    # --- Load benchmark results for bar chart ---
    benchmark_data = None
    br_path = os.path.join(state_dir, "benchmark-results.json")
    if os.path.exists(br_path):
        with open(br_path) as f:
            benchmark_data = json.load(f)

    # --- Load delta-stats ---
    delta_stats = {}
    for batch in [1, 2, 3]:
        for pattern in [f"delta-stats-batch{batch}.json",
                        f"delta-stats-*-batch{batch}.json"]:
            for stats_file in _glob.glob(os.path.join(state_dir, pattern)):
                with open(stats_file) as f:
                    sdata = json.load(f)
                if batch not in delta_stats:
                    delta_stats[batch] = sdata.get("tables", [])

    # --- Load container stats for time series ---
    container_stats = {}
    if stats_dir and os.path.isdir(stats_dir):
        for engine_name in os.listdir(stats_dir):
            engine_dir = os.path.join(stats_dir, engine_name)
            stats_file = os.path.join(engine_dir, "container_stats.jsonl")
            if os.path.isfile(stats_file):
                entries = []
                with open(stats_file) as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entries.append(json.loads(line))
                if entries:
                    container_stats[engine_name] = entries

    # --- Determine what sections to show ---
    has_bar_chart = (
        benchmark_data is not None
        and isinstance(benchmark_data.get("engines"), dict)
        and len(benchmark_data["engines"]) > 0
    )
    has_delta_stats = len(delta_stats) > 0
    has_exec_times = len(records) > 0
    has_time_series = len(container_stats) > 0

    if not has_bar_chart and not has_exec_times:
        return None

    batches = sorted(set(r["batch_num"] for r in records)) if records else [1, 2, 3]
    n_batches = len(batches)

    # Prepare exec-time lookup
    all_exec_engines = _sort_engines(list(set(r["engine"] for r in records)))
    n_engines = len(all_exec_engines)
    lookup = {}
    for r in records:
        lookup[(r["batch_num"], r["engine"], r["query_label"])] = r["duration_s"]

    # Keep models appearing in at least 1 engine
    all_queries = sorted(set(r["query_label"] for r in records))
    queries = []
    for q in all_queries:
        engine_set = set()
        for b in batches:
            for e in all_exec_engines:
                if lookup.get((b, e, q), 0) > 0:
                    engine_set.add(e)
        if len(engine_set) >= 1:
            queries.append(q)
    n_queries = len(queries)

    # --- Section heights ---
    n_bar_engines = len(benchmark_data["engines"]) if has_bar_chart else 0
    bar_h = 2.0 + 0.5 * max(1, n_bar_engines) if has_bar_chart else 0
    delta_h = 4.0 if has_delta_stats else 0
    exec_h = max(8, n_queries * 0.7) if has_exec_times and n_queries > 0 else 0

    ts_engines = _sort_engines(list(container_stats.keys()))
    n_ts_engines = len(ts_engines)
    ts_h = 12.0 if has_time_series else 0

    total_h = bar_h + delta_h + exec_h + ts_h
    if total_h == 0:
        return None

    fig_width = max(7 * n_batches, 7 * max(n_ts_engines, 1))

    # --- Build GridSpec ---
    sections = []
    ratios = []
    if has_bar_chart:
        sections.append("bar")
        ratios.append(bar_h)
    if has_delta_stats:
        sections.append("delta")
        ratios.append(delta_h)
    if has_exec_times and n_queries > 0:
        sections.append("exec")
        ratios.append(exec_h)
    if has_time_series:
        sections.append("ts")
        ratios.append(ts_h)

    fig = plt.figure(figsize=(fig_width, total_h + 2))
    gs = fig.add_gridspec(len(sections), 1, height_ratios=ratios, hspace=0.35)
    sec_idx = 0

    # ===== Section: Batch Duration Bar Chart =====
    if "bar" in sections:
        ax_bar = fig.add_subplot(gs[sec_idx])
        sec_idx += 1

        engine_durations = []
        for ename, edata in benchmark_data["engines"].items():
            batch_map = {}
            for b in edata.get("batches", []):
                batch_map[b["batch_num"]] = b["duration_s"]
            total = sum(batch_map.values())
            compile_time = edata.get("extra", {}).get("compile_time_s")
            engine_durations.append((ename, batch_map, total, compile_time))
        engine_durations.sort(key=lambda x: x[2])

        y_pos = list(range(len(engine_durations)))
        for yi, (ename, batch_map, total, compile_time) in zip(y_pos, engine_durations):
            left = 0
            for bn in sorted(batch_map.keys()):
                dur = batch_map[bn]
                color = BATCH_COLORS.get(bn, "#888888")
                ax_bar.barh(
                    yi, dur, left=left, height=0.6,
                    color=color, edgecolor="white", linewidth=0.5,
                    label=f"Batch {bn}" if yi == 0 else None,
                )
                if dur > total * 0.05:
                    ax_bar.text(
                        left + dur / 2, yi, _format_duration(dur),
                        ha="center", va="center", fontsize=8,
                        color="white", fontweight="bold",
                    )
                left += dur

            # Total duration label
            total_label = f"Total: {_format_duration(total)}"
            if compile_time:
                total_label += f"  \u2699 Compile: {_format_duration(compile_time)}"
            ax_bar.text(
                left + total * 0.02, yi,
                total_label,
                ha="left", va="center", fontsize=9, fontweight="bold",
                color="black",
            )

        ax_bar.set_yticks(y_pos)
        ax_bar.set_yticklabels(
            [e[0] for e in engine_durations], fontsize=10, fontweight="bold",
        )
        ax_bar.set_xlabel("Duration (seconds)", fontsize=10)
        ax_bar.set_title(
            "Batch Duration by Engine (fastest \u2192 slowest)", fontsize=12,
        )
        ax_bar.invert_yaxis()
        handles, labels = ax_bar.get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        ax_bar.legend(by_label.values(), by_label.keys(),
                      loc="upper right", fontsize=9)
        ax_bar.grid(axis="x", alpha=0.3)

    # ===== Section: Delta-Stats (Source Data) =====
    if "delta" in sections:
        gs_delta = gs[sec_idx].subgridspec(1, n_batches, wspace=0.3)
        sec_idx += 1
        for ax_idx, batch in enumerate(batches):
            ax = fig.add_subplot(gs_delta[0, ax_idx])
            tables_data = delta_stats.get(batch, [])
            if not tables_data:
                ax.set_visible(False)
                continue

            table_names = [t["name"] for t in tables_data]
            rows_vals = [t.get("rows", 0) or 0 for t in tables_data]
            size_vals = [t.get("size_gb", 0) or 0 for t in tables_data]

            n_tables = len(table_names)
            bh = 0.15
            group_gap = bh * 2 + 0.5
            y_positions = np.arange(n_tables) * group_gap

            ax.barh(y_positions - bh / 2, rows_vals, bh * 0.9,
                    label="Rows", color="#4a90d9", alpha=0.8)
            ax.set_xlabel("Rows", color="#4a90d9", fontsize=9)
            ax.tick_params(axis="x", labelcolor="#4a90d9", labelsize=8)
            ax.set_yticks(y_positions)
            ax.set_yticklabels(table_names, fontsize=8)
            ax.invert_yaxis()
            pct = batch_pcts.get(batch, "?")
            ax.set_title(
                f"Batch {batch} \u2014 {sum(size_vals):.2f} GB \u2014 {pct}%",
                fontsize=11,
            )
            ax.xaxis.set_major_formatter(plt.FuncFormatter(
                lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6
                else f"{v/1e3:.0f}K" if v >= 1e3
                else f"{v:.0f}"
            ))

            ax2 = ax.twiny()
            ax2.barh(y_positions + bh / 2, size_vals, bh * 0.9,
                     label="Size (GB)", color="#e07b39", alpha=0.8)
            ax2.set_xlabel("Size (GB)", color="#e07b39", fontsize=9)
            ax2.tick_params(axis="x", labelcolor="#e07b39", labelsize=8)

            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2,
                      loc="lower right", fontsize=8)
            ax.grid(axis="x", alpha=0.3)

    # ===== Section: Per-Query Execution Times =====
    if "exec" in sections:
        gs_exec = gs[sec_idx].subgridspec(1, n_batches, wspace=0.3)
        sec_idx += 1

        bar_thickness = 0.25
        group_spacing = n_engines * bar_thickness + 0.4

        batch_maxes = {}
        for r in records:
            b = r["batch_num"]
            batch_maxes[b] = max(batch_maxes.get(b, 0), r["duration_s"])

        for ax_idx, batch in enumerate(batches):
            ax = fig.add_subplot(gs_exec[0, ax_idx])
            x_max = batch_maxes.get(batch, 1)
            label_threshold = x_max * 0.15

            tick_centers = []
            for q_idx, query in enumerate(queries):
                group_base = q_idx * group_spacing
                for i, engine in enumerate(all_exec_engines):
                    y_bar = group_base + i * bar_thickness
                    val = lookup.get((batch, engine, query), 0)
                    ax.barh(
                        y_bar, val, bar_thickness * 0.9, align="center",
                        label=engine if q_idx == 0 else None,
                        color=_get_engine_color(engine),
                    )
                    if val >= label_threshold:
                        ax.text(
                            val * 0.5, y_bar,
                            f"{query} ({val:.1f}s)",
                            ha="center", va="center", fontsize=6,
                            color="white", fontweight="bold",
                        )
                tick_centers.append(
                    group_base + (n_engines - 1) * bar_thickness / 2,
                )

            ax.set_yticks(tick_centers)
            ax.set_yticklabels(queries, fontsize=9)
            ax.set_ylim(
                -0.4,
                (n_queries - 1) * group_spacing + n_engines * bar_thickness,
            )
            ax.invert_yaxis()
            ax.set_xlabel("Duration (seconds)", fontsize=11)
            ax.set_title(f"Batch {batch}", fontsize=13)
            ax.legend(loc="lower right", fontsize=10)
            ax.grid(axis="x", alpha=0.3)

            for q_idx in range(1, n_queries):
                sep_y = q_idx * group_spacing - 0.2
                ax.axhline(y=sep_y, color="gray", linewidth=0.3, alpha=0.5)

    # ===== Section: Container Time Series =====
    if "ts" in sections:
        metrics = [
            ("cpu_pct", "CPU %"),
            ("mem_mb", "Memory (MB)"),
            ("net_in_mb", "Net In (MB)"),
            ("net_out_mb", "Net Out (MB)"),
        ]
        gs_ts = gs[sec_idx].subgridspec(
            len(metrics), n_ts_engines, hspace=0.5, wspace=0.3,
        )
        sec_idx += 1

        container_cmap = {}
        ccycle = [
            "#e74c3c", "#3498db", "#2ecc71", "#f39c12",
            "#9b59b6", "#1abc9c", "#e67e22", "#34495e",
        ]

        for eng_idx, engine in enumerate(ts_engines):
            entries = container_stats[engine]

            by_ctr = defaultdict(lambda: {
                "t": [], "cpu_pct": [], "mem_mb": [],
                "net_in_mb": [], "net_out_mb": [],
            })
            for entry in entries:
                c = entry["container"]
                by_ctr[c]["t"].append(entry["timestamp_s"])
                for mk, _ in metrics:
                    by_ctr[c][mk].append(entry.get(mk, 0))

            all_t = [t for cd in by_ctr.values() for t in cd["t"]]
            t0 = min(all_t) if all_t else 0
            for cd in by_ctr.values():
                cd["t"] = [t - t0 for t in cd["t"]]

            for cname in sorted(by_ctr.keys()):
                if cname not in container_cmap:
                    container_cmap[cname] = ccycle[
                        len(container_cmap) % len(ccycle)
                    ]

            for m_idx, (mk, mlabel) in enumerate(metrics):
                ax = fig.add_subplot(gs_ts[m_idx, eng_idx])
                for cname in sorted(by_ctr.keys()):
                    cd = by_ctr[cname]
                    ax.plot(
                        cd["t"], cd[mk],
                        marker=".", markersize=3, linewidth=1,
                        color=container_cmap[cname], label=cname,
                    )
                ax.set_ylabel(mlabel, fontsize=7)
                ax.tick_params(labelsize=7)
                if m_idx == 0:
                    ax.set_title(engine, fontsize=10, fontweight="bold")
                if m_idx == len(metrics) - 1:
                    ax.set_xlabel("Time (s)", fontsize=8)
                ax.legend(fontsize=6, loc="upper left")
                ax.grid(alpha=0.3)

    # ===== Title =====
    pct_parts = [
        f"B{b}={batch_pcts.get(b, '?')}%"
        for b in batches if batch_pcts.get(b)
    ]
    pct_label = f" ({', '.join(pct_parts)})" if pct_parts else ""
    fig.suptitle(
        f"dbt Model Execution Time by Engine (SF={sf}{pct_label})",
        fontsize=14, y=0.995,
    )
    if engine_resources:
        parts = []
        for eng in sorted(engine_resources.keys()):
            r = engine_resources[eng]
            parts.append(f"{eng}: {r['cpus']} CPU / {r['memory_gb']} GB")
        subtitle = "  |  ".join(parts)
        fig.text(
            0.5, 0.985, subtitle,
            ha="center", va="top", fontsize=9, color="gray",
        )

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# ==========================================================================
# Heuristics Chart
# ==========================================================================

# --- Incrementalization-coverage helpers ----------------------------------

_INC_OK_COLOR = "#27ae60"
_INC_BAD_COLOR = "#c0392b"
_INC_NA_COLOR = "#bdbdbd"
_NOT_INC_PATTERN = re.compile(
    r"MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE[^:]*:\s*(.+?)(?:\.\s*SQLSTATE|$)",
    re.IGNORECASE | re.DOTALL,
)


def _short_reason(reason: str, max_len: int = 90) -> str:
    """Collapse whitespace and truncate so footnote text stays readable."""
    if not reason:
        return ""
    one_line = re.sub(r"\s+", " ", reason).strip()
    return (one_line[: max_len - 1] + "\u2026") if len(one_line) > max_len else one_line


def _parse_databricks_reason(error: str) -> str:
    """Pull the human-readable reason out of a Databricks EXPLAIN error."""
    if not error:
        return ""
    m = _NOT_INC_PATTERN.search(error)
    if m:
        return _short_reason(m.group(1))
    return _short_reason(error)


def _collect_openivm_views(
    state_dir: str, engine: str,
) -> Tuple[set, Optional[str]]:
    """Return the set of view_names with a `total_refresh` step in batch>=2.

    OpenIVM's profile only records `total_refresh` once the IVM CTAS rewriter
    has produced an incremental refresh plan and Spark/DuckDB has executed
    it. A view that fell back to FULL recompute (or that openivm couldn't
    rewrite) never gets a `total_refresh` row in batch 2/3 — exactly the
    signal we want for "did openivm incrementalize this model".
    """
    for batch in (2, 3, 1):
        for fname in (
            f"openivm-profile-by-view-step-batch{batch}.csv",
            f"openivm-profile-by-view-step-{engine}-batch{batch}.csv",
        ):
            fp = os.path.join(state_dir, fname)
            if not os.path.exists(fp):
                continue
            views = set()
            try:
                import csv
                with open(fp, newline="") as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        if row.get("step_name") == "total_refresh":
                            v = (row.get("view_name") or "").strip()
                            if v and v != "view_name":
                                views.add(v)
            except Exception as e:
                logger.warning(
                    "[chart] openivm CSV parse failed for %s: %s", fp, e,
                )
                continue
            if views:
                return views, os.path.basename(fp)
    return set(), None


def _collect_feldera_status(state_dir: str) -> Tuple[bool, Optional[str]]:
    """Did feldera's incremental batch (batch>=2) finish successfully?

    Feldera writes one run-feldera-batch<N>.json per batch with a
    per-model `nodes` list and per-node `status`. There is no top-level
    `status` field; the batch is considered successful if every node
    finished with `status=='success'` (which is feldera's contract:
    every dbt model maps to a dataflow operator in the running DBSP
    circuit — if any failed the pipeline would not have advanced).
    """
    for batch in (2, 3):
        fp = os.path.join(state_dir, f"run-feldera-batch{batch}.json")
        if not os.path.exists(fp):
            continue
        try:
            with open(fp) as fh:
                data = json.load(fh)
            nodes = data.get("nodes") or []
            if not nodes:
                continue
            if all((n.get("status") or "").lower() == "success" for n in nodes):
                return True, os.path.basename(fp)
        except Exception as e:
            logger.warning("[chart] feldera run JSON parse failed for %s: %s", fp, e)
    return False, None


def _collect_databricks_summary(
    repo_dir: str, sf: str,
) -> Optional[Dict[str, Any]]:
    """Load the EXPLAIN STRICT pre-flight summary written by engine_runner."""
    fp = os.path.join(
        repo_dir, "mount", "query-plan", str(sf), "databricks-enzyme",
        "explain-create-materialized-view", "summary.json",
    )
    if not os.path.exists(fp):
        return None
    try:
        with open(fp) as fh:
            return json.load(fh)
    except Exception as e:
        logger.warning("[chart] databricks summary parse failed for %s: %s", fp, e)
        return None


def _infer_sf_from_state_dir(state_dir: str) -> Optional[str]:
    """state_dir is `<repo>/mount/results/<sf>/dbt-server` — pull <sf> out."""
    parts = os.path.normpath(state_dir).split(os.sep)
    try:
        i = parts.index("results")
        if i + 1 < len(parts):
            return parts[i + 1]
    except ValueError:
        pass
    return None


def _infer_repo_from_state_dir(state_dir: str) -> str:
    """state_dir is `<repo>/mount/results/<sf>/dbt-server`."""
    parts = os.path.normpath(state_dir).split(os.sep)
    try:
        i = parts.index("mount")
        return os.sep + os.path.join(*parts[:i]) if i > 0 else "/repo"
    except ValueError:
        return "/repo"


def _collect_incrementalization(
    engines: list,
    sql_data: Dict[str, Any],
    state_dir: str,
) -> Dict[str, Dict[str, Any]]:
    """Build {engine: {total, ok_count, models: {name: {ok, reason}}, source}}.

    Per-engine rules:
      databricks-enzyme: read EXPLAIN STRICT summary.json. Models in
                        `failures` are NOT incrementalizable, reason comes
                        from the `MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE: …`
                        prefix on the error string.
      spark-openivm:    a view with a `total_refresh` step in
                        openivm-profile-by-view-step-batch2.csv (or 3) was
                        incrementalized by the IVM engine. Missing means it
                        fell back / wasn't rewritten.
      duckdb-openivm:   same as spark-openivm.
      feldera:          if run-feldera-batch2.json status==ok then 100%.
                        Feldera contract: all SQL compiles to DBSP or the
                        pipeline never starts.
      spark, duckdb:    no IVM layer → 0%.
    """
    repo_dir = _infer_repo_from_state_dir(state_dir)
    sf = _infer_sf_from_state_dir(state_dir) or "?"

    out: Dict[str, Dict[str, Any]] = {}
    for engine in engines:
        models = [m["model_name"] for m in sql_data.get(engine, {}).get("models", [])]
        per_model: Dict[str, Dict[str, Any]] = {
            n: {"ok": False, "reason": ""} for n in models
        }
        source = "n/a"

        if engine == "databricks-enzyme":
            summary = _collect_databricks_summary(repo_dir, sf)
            if summary is not None:
                source = f"mount/query-plan/{sf}/databricks-enzyme/.../summary.json"
                # Strip ephemeral models from the count — they're never
                # materialized as MVs so there's nothing to incrementalize.
                ephemeral = {
                    uid.split(".")[-1]
                    for uid in (summary.get("skipped_ephemeral") or [])
                }
                for eph in ephemeral:
                    per_model.pop(eph, None)
                failures = {
                    (f.get("model") or "").split(".")[-1]:
                    _parse_databricks_reason(f.get("error") or "")
                    for f in (summary.get("failures") or [])
                }
                # Every model the EXPLAIN sweep saw: default ok unless in failures.
                seen = set()
                for plan in (summary.get("plans") or []):
                    bare = (plan.get("model") or plan.get("name") or "").split(".")[-1]
                    if not bare or bare in ephemeral:
                        continue
                    seen.add(bare)
                    if bare in per_model:
                        if plan.get("error"):
                            per_model[bare] = {
                                "ok": False,
                                "reason": _parse_databricks_reason(plan["error"]),
                            }
                        else:
                            per_model[bare] = {"ok": True, "reason": ""}
                # Models from sql_data the EXPLAIN sweep didn't see (e.g. summary
                # without per-plan detail) — fall back to failures-list intersection.
                for name in list(per_model.keys()):
                    if name in seen:
                        continue
                    if name in failures:
                        per_model[name] = {"ok": False, "reason": failures[name]}
                    else:
                        per_model[name] = {"ok": True, "reason": ""}
            else:
                source = "summary.json missing — defaulting to ok"
                for name in models:
                    per_model[name] = {"ok": True, "reason": ""}

        elif engine in ("spark-openivm", "duckdb-openivm"):
            views, csv_name = _collect_openivm_views(state_dir, engine)
            if csv_name:
                source = csv_name
                for name in models:
                    if name in views:
                        per_model[name] = {"ok": True, "reason": ""}
                    else:
                        per_model[name] = {
                            "ok": False,
                            "reason": "no total_refresh step in IVM profile",
                        }
            else:
                source = "openivm profile CSV not found"
                for name in models:
                    per_model[name] = {
                        "ok": False, "reason": "no IVM profile available",
                    }

        elif engine == "feldera":
            ok, src_name = _collect_feldera_status(state_dir)
            if ok:
                source = f"{src_name} (DBSP contract: all SQL → incremental circuit)"
                for name in models:
                    per_model[name] = {"ok": True, "reason": ""}
            else:
                source = "run-feldera-batch>=2 missing/non-ok"
                for name in models:
                    per_model[name] = {
                        "ok": False, "reason": "feldera incremental batch did not complete",
                    }

        else:
            source = f"{engine}: no IVM layer (vanilla recompute every batch)"
            for name in models:
                per_model[name] = {
                    "ok": False, "reason": "engine has no IVM layer",
                }

        ok_count = sum(1 for v in per_model.values() if v["ok"])
        out[engine] = {
            "total": len(per_model),
            "ok_count": ok_count,
            "models": per_model,
            "source": source,
        }
    return out


def _render_incrementalization_section(
    fig,
    gs_slot,
    engines: list,
    inc_data: Dict[str, Dict[str, Any]],
    canonical_models: list,
) -> None:
    """Two-panel section: per-engine bar + per-model matrix.

    Bar:    horizontal stack (green=ok, red=not), label `X/N (P%)`.
    Matrix: rows=engines, cols=canonical_models; green/red/gray cells.
    """
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np

    sub = gs_slot.subgridspec(
        2, 1, height_ratios=[1.2, max(2.0, 0.35 * len(engines) + 1.0)],
        hspace=0.55,
    )

    # ---- Panel A: stacked horizontal bar per engine ----
    ax_bar = fig.add_subplot(sub[0, 0])
    y_pos = np.arange(len(engines))
    bar_height = 0.6
    max_total = max((inc_data[e]["total"] for e in engines), default=1) or 1

    for i, engine in enumerate(engines):
        d = inc_data[engine]
        ok = d["ok_count"]
        total = d["total"]
        not_ok = total - ok
        pct = (100.0 * ok / total) if total else 0.0

        ax_bar.barh(
            y_pos[i], ok, height=bar_height,
            color=_INC_OK_COLOR, edgecolor="white", linewidth=0.5,
        )
        ax_bar.barh(
            y_pos[i], not_ok, left=ok, height=bar_height,
            color=_INC_BAD_COLOR, edgecolor="white", linewidth=0.5,
        )
        label = f"{ok} / {total}  ({pct:.0f}%)"
        ax_bar.text(
            total + max_total * 0.01, y_pos[i], label,
            va="center", ha="left", fontsize=8, fontweight="bold",
        )

    ax_bar.set_yticks(y_pos)
    ax_bar.set_yticklabels(engines, fontsize=8)
    ax_bar.invert_yaxis()
    ax_bar.set_xlim(0, max_total * 1.25)
    ax_bar.set_xlabel("# dbt models", fontsize=8)
    ax_bar.set_title(
        "Incrementalization Coverage \u2014 per engine\n"
        "(green=incrementalizable, red=full/fallback)",
        fontsize=11, fontweight="bold",
    )
    ax_bar.grid(axis="x", alpha=0.3, linestyle="--")

    legend_handles = [
        mpatches.Patch(color=_INC_OK_COLOR, label="Incrementalizable"),
        mpatches.Patch(color=_INC_BAD_COLOR, label="Not incrementalizable"),
        mpatches.Patch(color=_INC_NA_COLOR, label="Model not present"),
    ]
    ax_bar.legend(handles=legend_handles, fontsize=7, loc="lower right")

    # ---- Panel B: per-model matrix ----
    ax_mat = fig.add_subplot(sub[1, 0])
    n_e = len(engines)
    n_m = len(canonical_models)
    if n_m == 0:
        ax_mat.set_axis_off()
        return

    color_grid = np.zeros((n_e, n_m, 3))
    for i, engine in enumerate(engines):
        per = inc_data[engine]["models"]
        for j, name in enumerate(canonical_models):
            entry = per.get(name)
            if entry is None:
                rgb = (0.745, 0.745, 0.745)
            elif entry["ok"]:
                rgb = (0.153, 0.682, 0.376)
            else:
                rgb = (0.753, 0.224, 0.169)
            color_grid[i, j] = rgb

    ax_mat.imshow(color_grid, aspect="auto", interpolation="nearest")
    ax_mat.set_yticks(range(n_e))
    ax_mat.set_yticklabels(engines, fontsize=7)
    ax_mat.set_xticks(range(n_m))
    ax_mat.set_xticklabels(canonical_models, fontsize=4, rotation=90)

    # Annotate cells with ✓ / ✗ / •
    for i, engine in enumerate(engines):
        per = inc_data[engine]["models"]
        for j, name in enumerate(canonical_models):
            entry = per.get(name)
            if entry is None:
                glyph = "\u00b7"
            elif entry["ok"]:
                glyph = "\u2713"
            else:
                glyph = "\u2717"
            ax_mat.text(
                j, i, glyph,
                ha="center", va="center", fontsize=4.5,
                color="white", fontweight="bold",
            )

    ax_mat.set_title(
        "Per-model incrementalization matrix (\u2713 ok, \u2717 not, \u00b7 not present)",
        fontsize=10, fontweight="bold",
    )

    # Combined footer: verdict sources + top non-incremental reasons per engine.
    # Placed via fig.text (figure-relative) so it sits below the matrix
    # axes rather than overlapping the bar panel above.
    src_lines = [f"  {e:<22s} <- {inc_data[e]['source']}" for e in engines]
    reason_lines = []
    for engine in engines:
        per = inc_data[engine]["models"]
        counts: Dict[str, int] = {}
        for entry in per.values():
            if entry["ok"] or not entry["reason"]:
                continue
            counts[entry["reason"]] = counts.get(entry["reason"], 0) + 1
        if not counts:
            continue
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:3]
        for reason, n in top:
            reason_lines.append(f"  {engine:<22s} x{n:<3d} {reason}")

    footer = "Verdict sources:\n" + "\n".join(src_lines)
    if reason_lines:
        footer += "\n\nTop non-incremental reasons:\n" + "\n".join(reason_lines)
    bbox = ax_mat.get_position()
    fig.text(
        bbox.x0, bbox.y0 - 0.012,
        footer,
        fontsize=6, va="top", ha="left", family="monospace", color="#444",
    )


def generate_heuristics_png(state_dir: str) -> Optional[bytes]:
    """Generate the heuristics PNG from lineage and SQL analysis data.

    Sections (top to bottom, engines as columns unless noted):
    1. Operator heatmaps
    2. Lineage DAGs
    3. Operator chains per query
    4. Incrementalization coverage (per-engine bar + per-model matrix)
    """
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    import numpy as np
    import networkx as nx

    # --- Discover engines ---
    lineage_data = {}
    for fp in sorted(_glob.glob(os.path.join(state_dir, "lineage-*.json"))):
        match = re.match(r"lineage-(.+)\.json", os.path.basename(fp))
        if match:
            engine = match.group(1)
            with open(fp) as f:
                lineage_data[engine] = json.load(f)

    sql_data = {}
    for fp in sorted(_glob.glob(os.path.join(state_dir, "sql-analysis-*.json"))):
        match = re.match(r"sql-analysis-(.+)\.json", os.path.basename(fp))
        if match:
            engine = match.group(1)
            with open(fp) as f:
                sql_data[engine] = json.load(f)

    engines = _sort_engines([e for e in lineage_data if e in sql_data])
    if not engines:
        return None
    n_engines = len(engines)

    # --- Canonical model list (from first engine) ---
    canonical_models = [m["model_name"] for m in sql_data[engines[0]]["models"]]
    n_models = len(canonical_models)

    # --- Shared heatmap scale ---
    global_max = 0
    for engine in engines:
        for m in sql_data[engine]["models"]:
            for op in OPERATOR_ORDER:
                global_max = max(global_max, m["operators"].get(op, 0))
    vmax = max(global_max, 1)

    # --- Active operators (at least one non-zero across all engines) ---
    active_ops = []
    for op in OPERATOR_ORDER:
        found = False
        for engine in engines:
            for m in sql_data[engine]["models"]:
                if m["operators"].get(op, 0) > 0:
                    found = True
                    break
            if found:
                break
        if found:
            active_ops.append(op)
    if not active_ops:
        active_ops = OPERATOR_ORDER
    n_ops = len(active_ops)

    # --- Pre-collect incrementalization verdicts (Section 4) ---
    inc_data = _collect_incrementalization(engines, sql_data, state_dir)

    # --- Section heights ---
    heatmap_h = max(8, n_models * 0.2 + 3)
    dag_h = max(10, n_models * 0.25)
    chain_h = max(8, n_models * 0.25 + 2)
    inc_h = max(6, 2 + 0.4 * n_engines + 0.05 * n_models + 2)
    total_h = heatmap_h + dag_h + chain_h + inc_h
    fig_width = max(8, 6 * n_engines + 1)

    fig = plt.figure(figsize=(fig_width, total_h))
    gs = fig.add_gridspec(
        4, 1,
        height_ratios=[heatmap_h, dag_h, chain_h, inc_h],
        hspace=0.35,
    )

    # ===== Section 1: Operator Heatmaps =====
    from matplotlib.colors import LinearSegmentedColormap
    cmap = LinearSegmentedColormap.from_list("white_red", ["#ffffff", "#ff4444"])

    gs_heat = gs[0].subgridspec(
        1, n_engines + 1, width_ratios=[1] * n_engines + [0.05], wspace=0.4,
    )

    im = None
    for eng_idx, engine in enumerate(engines):
        ax = fig.add_subplot(gs_heat[0, eng_idx])
        model_lookup = {m["model_name"]: m for m in sql_data[engine]["models"]}

        data = np.zeros((n_models, n_ops))
        for i, mname in enumerate(canonical_models):
            m = model_lookup.get(mname)
            if m:
                for j, op in enumerate(active_ops):
                    data[i, j] = m["operators"].get(op, 0)

        im = ax.imshow(
            data, aspect="auto", cmap=cmap, vmin=0, vmax=vmax,
            interpolation="nearest",
        )

        # Annotate non-zero cells
        for i in range(n_models):
            for j in range(n_ops):
                val = int(data[i, j])
                if val > 0:
                    ax.text(
                        j, i, str(val),
                        ha="center", va="center", fontsize=4, color="#333",
                    )

        ax.set_yticks(range(n_models))
        ax.set_yticklabels(canonical_models, fontsize=4)
        ax.set_xticks(range(n_ops))
        ax.set_xticklabels(
            [op.replace("_", "\n") for op in active_ops],
            fontsize=5, rotation=45, ha="right",
        )
        ax.set_title(
            f"{engine}\nOperator Heatmap", fontsize=10, fontweight="bold",
        )

    if im is not None:
        cbar_ax = fig.add_subplot(gs_heat[0, n_engines])
        fig.colorbar(im, cax=cbar_ax, label="Count")

    # ===== Section 2: Lineage DAGs =====
    gs_dag = gs[1].subgridspec(1, n_engines, wspace=0.3)

    for eng_idx, engine in enumerate(engines):
        ax = fig.add_subplot(gs_dag[0, eng_idx])
        ld = lineage_data[engine]

        G = nx.DiGraph()
        node_roles = {}
        for node in ld["nodes"]:
            uid = node["unique_id"]
            G.add_node(uid)
            node_roles[uid] = node.get("role", "intermediate")
        for edge in ld["edges"]:
            if edge["from"] in G and edge["to"] in G:
                G.add_edge(edge["from"], edge["to"])

        # Layered layout with barycenter heuristic
        pos = {}
        try:
            layers = list(nx.topological_generations(G))
        except nx.NetworkXError:
            layers = None

        if layers:
            # Forward pass: sort each layer by average predecessor y
            for layer_idx, layer_nodes in enumerate(layers):
                if layer_idx == 0:
                    sorted_nodes = sorted(layer_nodes)
                else:
                    def _avg_pred_y(n, _pos=pos, _G=G):
                        preds = [p for p in _G.predecessors(n) if p in _pos]
                        if not preds:
                            return 0
                        return sum(_pos[p][1] for p in preds) / len(preds)
                    sorted_nodes = sorted(layer_nodes, key=_avg_pred_y)
                n_in = len(sorted_nodes)
                for ni, node in enumerate(sorted_nodes):
                    pos[node] = (layer_idx, -(ni - n_in / 2))

            # Backward pass: refine by successor positions
            for layer_idx in range(len(layers) - 2, -1, -1):
                layer_nodes = layers[layer_idx]
                def _avg_succ_y(n, _pos=pos, _G=G):
                    succs = [s for s in _G.successors(n) if s in _pos]
                    if not succs:
                        return _pos.get(n, (0, 0))[1]
                    return sum(_pos[s][1] for s in succs) / len(succs)
                sorted_nodes = sorted(layer_nodes, key=_avg_succ_y)
                n_in = len(sorted_nodes)
                for ni, node in enumerate(sorted_nodes):
                    pos[node] = (layer_idx, -(ni - n_in / 2))
        else:
            pos = nx.spring_layout(G, seed=42)

        node_colors = [
            ROLE_COLORS.get(node_roles.get(n, "intermediate"), "#95a5a6")
            for n in G.nodes()
        ]
        labels = {n: n.split(".")[-1] for n in G.nodes()}

        nx.draw_networkx_nodes(
            G, pos, ax=ax, node_color=node_colors, node_size=40, alpha=0.9,
        )
        nx.draw_networkx_edges(
            G, pos, ax=ax, edge_color="#cccccc",
            arrows=True, arrowsize=4, width=0.3, alpha=0.6,
        )
        nx.draw_networkx_labels(G, pos, labels, ax=ax, font_size=3)
        ax.set_title(
            f"{engine}\nLineage DAG", fontsize=10, fontweight="bold",
        )
        ax.set_axis_off()

        if eng_idx == 0:
            legend_handles = [
                mpatches.Patch(color=c, label=r) for r, c in ROLE_COLORS.items()
            ]
            ax.legend(handles=legend_handles, fontsize=5, loc="lower left")

    # ===== Section 3: Operator Chains =====
    gs_chain = gs[2].subgridspec(1, n_engines, wspace=0.3)

    for eng_idx, engine in enumerate(engines):
        ax = fig.add_subplot(gs_chain[0, eng_idx])
        model_lookup = {m["model_name"]: m for m in sql_data[engine]["models"]}

        row_height = 1.0
        box_w = 1.2
        box_h = 0.7
        arrow_gap = 0.3
        name_w = 3.5

        # Compute max x extent
        max_x = name_w
        for mname in canonical_models:
            m = model_lookup.get(mname)
            if m:
                n_active = sum(
                    1 for op in active_ops if m["operators"].get(op, 0) > 0
                )
                extent = name_w + n_active * (box_w + arrow_gap)
                max_x = max(max_x, extent)

        ax.set_xlim(-0.5, max_x + 0.5)
        ax.set_ylim(-n_models * row_height, row_height)

        for i, mname in enumerate(canonical_models):
            y = -i * row_height
            m = model_lookup.get(mname)

            ax.text(
                0, y, mname, fontsize=3.5, va="center", ha="left",
                fontweight="bold", family="monospace",
            )

            if not m:
                continue

            x = name_w
            ops = m["operators"]
            drawn_any = False
            for op in active_ops:
                count = ops.get(op, 0)
                if count == 0:
                    continue

                color = OPERATOR_COLORS.get(op, "#888888")
                short = OPERATOR_SHORT.get(op, op[:3].upper())
                label = f"{short}\u00d7{count}"

                rect = plt.Rectangle(
                    (x, y - box_h / 2), box_w, box_h,
                    facecolor=color, edgecolor="white",
                    linewidth=0.5, alpha=0.85,
                )
                ax.add_patch(rect)
                ax.text(
                    x + box_w / 2, y, label,
                    fontsize=3, va="center", ha="center",
                    color="white", fontweight="bold",
                )

                if drawn_any:
                    # Arrow from previous box
                    ax.annotate(
                        "", xy=(x, y),
                        xytext=(x - arrow_gap, y),
                        arrowprops=dict(
                            arrowstyle="->", color="#aaa", lw=0.5,
                        ),
                    )
                drawn_any = True
                x += box_w + arrow_gap

        ax.set_title(
            f"{engine}\nOperator Chains", fontsize=10, fontweight="bold",
        )
        ax.set_axis_off()

    # ===== Section 4: Incrementalization Coverage =====
    _render_incrementalization_section(
        fig, gs[3], engines, inc_data, canonical_models,
    )

    fig.suptitle(
        "Benchmark Heuristics \u2014 SQL Complexity, Lineage & Incrementalization",
        fontsize=14, y=0.995,
    )

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=300, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


# ==========================================================================
# Flask Endpoints
# ==========================================================================

@bp.route("/chart")
def chart():
    """Generate a PNG chart from benchmark results."""
    repo_dir = os.environ.get("REPO_DIR", "/repo")
    sf = request.args.get("sf", os.environ.get("SCALE_FACTOR", "3"))
    b1pct = request.args.get("b1pct", os.environ.get("BATCH_1_PCT", ""))
    b2pct = request.args.get("b2pct", os.environ.get("BATCH_2_PCT", ""))
    b3pct = request.args.get("b3pct", os.environ.get("BATCH_3_PCT", ""))

    results_dir = os.path.join(repo_dir, "mount", "results", str(sf), "dbt-server")
    stats_dir = os.path.join(repo_dir, "mount", "stats", str(sf))

    png_data = generate_chart_png(
        state_dir=results_dir,
        sf=str(sf),
        b1pct=b1pct,
        b2pct=b2pct,
        b3pct=b3pct,
        stats_dir=stats_dir,
    )
    if png_data is None:
        return jsonify({"error": "No result files found"}), 404

    return Response(png_data, mimetype="image/png")


@bp.route("/chart/heuristics")
def chart_heuristics():
    """Generate a heuristics PNG from lineage and SQL analysis data."""
    repo_dir = os.environ.get("REPO_DIR", "/repo")
    sf = request.args.get("sf", os.environ.get("SCALE_FACTOR", "3"))

    results_dir = os.path.join(repo_dir, "mount", "results", str(sf), "dbt-server")

    png_data = generate_heuristics_png(state_dir=results_dir)
    if png_data is None:
        return jsonify({"error": "No heuristics data found"}), 404

    return Response(png_data, mimetype="image/png")


@bp.route("/chart/openivm-ops")
def chart_openivm_ops():
    sf = request.args.get("sf", os.environ.get("SCALE_FACTOR", "3"))
    engine = request.args.get("engine", "spark-openivm")
    try:
        batch = int(request.args.get("batch", "1"))
    except ValueError:
        return jsonify({"error": "invalid batch"}), 400
    if batch < 1 or batch > 3:
        return jsonify({"error": "batch must be between 1 and 3"}), 400
    if engine not in ("spark-openivm", "duckdb-openivm"):
        return jsonify({"error": "engine must be spark-openivm or duckdb-openivm"}), 400
    repo_dir = os.environ.get("REPO_DIR", "/repo")
    from services.openivm_ops_chart import render_batch_png

    png = render_batch_png(sf=str(sf), engine=engine, batch=batch, repo_dir=repo_dir)
    if not png:
        return jsonify({"error": "no openivm telemetry found for this engine/batch"}), 404
    return Response(png, mimetype="image/png")


@bp.route("/chart/openivm-ops/compare")
def chart_openivm_ops_compare():
    sf = request.args.get("sf", os.environ.get("SCALE_FACTOR", "3"))
    engine = request.args.get("engine", "spark-openivm")
    try:
        batch = int(request.args.get("batch", "1"))
    except ValueError:
        return jsonify({"error": "invalid batch"}), 400
    if batch < 1 or batch > 3:
        return jsonify({"error": "batch must be between 1 and 3"}), 400
    if engine not in ("spark-openivm", "duckdb-openivm"):
        return jsonify({"error": "engine must be spark-openivm or duckdb-openivm"}), 400
    before_dir = request.args.get("before_dir") or os.environ.get("OPENIVM_BEFORE_DIR")
    after_dir = request.args.get("after_dir") or os.environ.get("OPENIVM_AFTER_DIR") or os.environ.get("REPO_DIR", "/repo")
    if not before_dir:
        return jsonify({"error": "before_dir query arg or OPENIVM_BEFORE_DIR env var required"}), 400
    from services.openivm_ops_chart import render_compare_png

    png = render_compare_png(sf=str(sf), engine=engine, batch=batch, after_repo_dir=after_dir, before_repo_dir=before_dir)
    if not png:
        return jsonify({"error": "no openivm telemetry found in one of the dirs"}), 404
    return Response(png, mimetype="image/png")


class ChartHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
