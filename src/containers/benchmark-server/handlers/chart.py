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
from typing import Optional

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

def generate_heuristics_png(state_dir: str) -> Optional[bytes]:
    """Generate the heuristics PNG from lineage and SQL analysis data.

    Sections (top to bottom, engines as columns):
    1. Operator heatmaps
    2. Lineage DAGs
    3. Operator chains per query
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

    # --- Section heights ---
    heatmap_h = max(8, n_models * 0.2 + 3)
    dag_h = max(10, n_models * 0.25)
    chain_h = max(8, n_models * 0.25 + 2)
    total_h = heatmap_h + dag_h + chain_h
    fig_width = max(8, 6 * n_engines + 1)

    fig = plt.figure(figsize=(fig_width, total_h))
    gs = fig.add_gridspec(
        3, 1, height_ratios=[heatmap_h, dag_h, chain_h], hspace=0.3,
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

    fig.suptitle(
        "Benchmark Heuristics \u2014 SQL Complexity & Lineage",
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
