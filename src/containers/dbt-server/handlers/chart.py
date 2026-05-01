"""Chart handler — generates PNG visualization of benchmark results."""

import os
import re

from flask import Blueprint, Flask, Response, jsonify

from handlers.base import BaseHandler
from services.db import STATE_DIR

bp = Blueprint("chart", __name__)


@bp.route("/chart")
def chart():
    """Generate a PNG chart of model execution times across engines and batches."""
    import glob as _glob
    import json
    from io import BytesIO

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    records = []
    for filepath in sorted(_glob.glob(os.path.join(STATE_DIR, "run-*.json"))):
        filename = os.path.basename(filepath)
        m = re.match(r"run-(\w+)-batch(\d+)\.json", filename)
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
    queries = sorted(set(r["query_label"] for r in records))

    lookup = {}
    for r in records:
        lookup[(r["batch_num"], r["engine"], r["query_label"])] = r["duration_s"]

    n_batches = len(batches)
    n_engines = len(engines)
    n_queries = len(queries)
    bar_thickness = 0.25
    group_spacing = n_engines * bar_thickness + 0.4
    colors = plt.cm.tab10.colors

    fig, axes = plt.subplots(
        1, n_batches,
        figsize=(7 * n_batches, max(10, n_queries * 0.9)),
    )
    if n_batches == 1:
        axes = [axes]

    batch_maxes = {}
    for r in records:
        b = r["batch_num"]
        batch_maxes[b] = max(batch_maxes.get(b, 0), r["duration_s"])

    for ax_idx, batch in enumerate(batches):
        ax = axes[ax_idx]
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
                        color=colors[i % len(colors)])
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

    fig.suptitle("dbt Model Execution Time by Engine (SF=5)", fontsize=14, y=0.98)
    plt.tight_layout(rect=[0, 0, 1, 0.96])

    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return Response(buf.getvalue(), mimetype="image/png")


class ChartHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
