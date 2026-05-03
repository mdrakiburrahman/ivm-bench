"""Chart handler — generates PNG visualization of benchmark results."""

import os
import re

from flask import Blueprint, Flask, Response, jsonify, request

from handlers.base import BaseHandler
from services.db import STATE_DIR

bp = Blueprint("chart", __name__)

ENGINE_COLORS = {
    "duckdb": "#f7e900",
    "spark": "#da571b",
    "feldera": "#ca46bf",
    "duckdb-openivm": "#4a90d9",
}


def _get_engine_color(engine: str) -> str:
    """Get hex color for an engine, with fallback."""
    return ENGINE_COLORS.get(engine.lower(), "#888888")


@bp.route("/chart")
def chart():
    """Generate a PNG chart with source data stats (top) and execution times (bottom)."""
    import glob as _glob
    import json
    from io import BytesIO

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    sf = request.args.get("sf", os.environ.get("SCALE_FACTOR", "?"))
    batch_pcts = {
        1: request.args.get("b1pct", os.environ.get("BATCH_1_PCT", "")),
        2: request.args.get("b2pct", os.environ.get("BATCH_2_PCT", "")),
        3: request.args.get("b3pct", os.environ.get("BATCH_3_PCT", "")),
    }

    # --- Load execution time records ---
    records = []
    for filepath in sorted(_glob.glob(os.path.join(STATE_DIR, "run-*.json"))):
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

    if not records:
        return jsonify({"error": "No result files found in STATE_DIR"}), 404

    batches = sorted(set(r["batch_num"] for r in records))
    engines = sorted(set(r["engine"] for r in records))
    all_queries = sorted(set(r["query_label"] for r in records))

    lookup = {}
    for r in records:
        lookup[(r["batch_num"], r["engine"], r["query_label"])] = r["duration_s"]

    # Filter: only show models that appear in at least 2 engines
    # (excludes engine-specific scaffolding like Feldera staging pass-throughs)
    queries = []
    for q in all_queries:
        engines_with_data = set()
        for b in batches:
            for e in engines:
                if lookup.get((b, e, q), 0) > 0:
                    engines_with_data.add(e)
        if len(engines_with_data) >= 2:
            queries.append(q)

    if not queries:
        return jsonify({"error": "All model durations are zero"}), 404

    n_batches = len(batches)
    n_engines = len(engines)
    n_queries = len(queries)

    # --- Load delta-stats for source data panels ---
    delta_stats = {}  # batch_num -> list of {name, rows, size_gb}
    for batch in batches:
        stats_file = os.path.join(STATE_DIR, f"delta-stats-batch{batch}.json")
        if os.path.exists(stats_file):
            with open(stats_file) as f:
                data = json.load(f)
            delta_stats[batch] = data.get("tables", [])

    has_delta_stats = len(delta_stats) > 0

    # --- Create figure layout ---
    if has_delta_stats:
        fig, all_axes = plt.subplots(
            2, n_batches,
            figsize=(7 * n_batches, max(10, n_queries * 0.9) + 5),
            gridspec_kw={"height_ratios": [1, 3]},
        )
        if n_batches == 1:
            top_axes = [all_axes[0]]
            bot_axes = [all_axes[1]]
        else:
            top_axes = all_axes[0]
            bot_axes = all_axes[1]
    else:
        fig, bot_axes = plt.subplots(
            1, n_batches,
            figsize=(7 * n_batches, max(10, n_queries * 0.9)),
        )
        if n_batches == 1:
            bot_axes = [bot_axes]
        top_axes = None

    # --- Top row: Source data stats (horizontal bar chart per batch) ---
    if has_delta_stats and top_axes is not None:
        for ax_idx, batch in enumerate(batches):
            ax = top_axes[ax_idx]
            tables_data = delta_stats.get(batch, [])
            if not tables_data:
                ax.set_visible(False)
                continue

            table_names = [t["name"] for t in tables_data]
            rows_vals = [t.get("rows", 0) or 0 for t in tables_data]
            size_vals = [t.get("size_gb", 0) or 0 for t in tables_data]

            n_tables = len(table_names)
            bar_h = 0.15
            group_gap = bar_h * 2 + 0.5
            y_positions = np.arange(n_tables) * group_gap

            # Bottom x-axis: rows (blue bars)
            ax.barh(y_positions - bar_h / 2, rows_vals, bar_h * 0.9, label="Rows", color="#4a90d9", alpha=0.8)
            ax.set_xlabel("Rows", color="#4a90d9", fontsize=9)
            ax.tick_params(axis="x", labelcolor="#4a90d9", labelsize=8)
            ax.set_yticks(y_positions)
            ax.set_yticklabels(table_names, fontsize=8)
            ax.invert_yaxis()
            ax.set_title(f"Batch {batch} — {sum(size_vals):.2f} GB — {batch_pcts.get(batch, '?')}%", fontsize=11)
            ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v/1e6:.1f}M" if v >= 1e6 else f"{v/1e3:.0f}K" if v >= 1e3 else f"{v:.0f}"))

            # Top x-axis: size in GB (orange bars)
            ax2 = ax.twiny()
            ax2.barh(y_positions + bar_h / 2, size_vals, bar_h * 0.9, label="Size (GB)", color="#e07b39", alpha=0.8)
            ax2.set_xlabel("Size (GB)", color="#e07b39", fontsize=9)
            ax2.tick_params(axis="x", labelcolor="#e07b39", labelsize=8)

            # Combined legend
            lines1, labels1 = ax.get_legend_handles_labels()
            lines2, labels2 = ax2.get_legend_handles_labels()
            ax.legend(lines1 + lines2, labels1 + labels2, loc="lower right", fontsize=8)
            ax.grid(axis="x", alpha=0.3)

    # --- Bottom row: Execution time chart ---
    bar_thickness = 0.25
    group_spacing = n_engines * bar_thickness + 0.4

    batch_maxes = {}
    for r in records:
        b = r["batch_num"]
        batch_maxes[b] = max(batch_maxes.get(b, 0), r["duration_s"])

    for ax_idx, batch in enumerate(batches):
        ax = bot_axes[ax_idx]
        x_max = batch_maxes.get(batch, 1)
        label_threshold = x_max * 0.15

        tick_centers = []
        for q_idx, query in enumerate(queries):
            group_base = q_idx * group_spacing
            for i, engine in enumerate(engines):
                y_pos = group_base + i * bar_thickness
                val = lookup.get((batch, engine, query), 0)
                ax.barh(y_pos, val, bar_thickness * 0.9, align='center',
                        label=engine if q_idx == 0 else None,
                        color=_get_engine_color(engine))
                if val >= label_threshold:
                    ax.text(val * 0.5, y_pos, f"{query} ({val:.1f}s)",
                            ha='center', va='center', fontsize=6,
                            color='white', fontweight='bold')
            tick_centers.append(group_base + (n_engines - 1) * bar_thickness / 2)

        ax.set_yticks(tick_centers)
        ax.set_yticklabels(queries, fontsize=9)
        ax.set_ylim(-0.4, (n_queries - 1) * group_spacing + n_engines * bar_thickness)
        ax.invert_yaxis()
        ax.set_xlabel("Duration (seconds)", fontsize=11)
        ax.set_title(f"Batch {batch}", fontsize=13)
        ax.legend(loc="lower right", fontsize=10)
        ax.grid(axis="x", alpha=0.3)

        for q_idx in range(1, n_queries):
            sep_y = q_idx * group_spacing - 0.2
            ax.axhline(y=sep_y, color='gray', linewidth=0.3, alpha=0.5)

    pct_parts = [f"B{b}={batch_pcts.get(b, '?')}%" for b in batches if batch_pcts.get(b)]
    pct_label = f" ({', '.join(pct_parts)})" if pct_parts else ""
    fig.suptitle(f"dbt Model Execution Time by Engine (SF={sf}{pct_label})", fontsize=14, y=0.99)
    plt.tight_layout(rect=[0, 0, 1, 0.97])

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="image/png")


class ChartHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
