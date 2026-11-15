#!/usr/bin/env python3
"""Render simple per-batch engine charts from an engine-coverage verdict."""

from __future__ import annotations

import argparse
import html
import json
import math
from pathlib import Path
from typing import Any, Dict, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

BATCH_COLORS = {1: "#3498db", 2: "#e67e22", 3: "#2ecc71"}
FALLBACK_BATCH_COLOR = "#8c959f"
INK, MUTED, BORDER = "#1f2328", "#656d76", "#d0d7de"
WIDTH_IN, ROW_IN, BOTTOM_IN = 8.0, 0.44, 0.3


def format_duration(seconds: float) -> str:
    total = int(round(seconds))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def _rows(experiment: Dict[str, Any]) -> List[Dict[str, Any]]:
    if experiment.get("timed") is not True:
        return []
    engines = experiment.get("engines")
    if not isinstance(engines, dict) or not engines:
        return []
    expected_batches = experiment.get("batch_count")
    rows = []
    for name, result in engines.items():
        if not isinstance(result, dict) or result.get("passed") is not True:
            return []
        durations = result.get("durations_s")
        if not isinstance(durations, list) or len(durations) != expected_batches:
            return []
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            for value in durations
        ):
            return []
        rows.append({"engine": name, "durations_s": durations, "total_s": sum(durations)})
    return sorted(rows, key=lambda row: row["total_s"])


def _render(rows: List[Dict[str, Any]], experiment: Dict[str, Any], output: Path) -> None:
    count = len(rows)
    height = 1.34 + count * ROW_IN + BOTTOM_IN
    fig = plt.figure(figsize=(WIDTH_IN, height), dpi=180)
    fig.patch.set_alpha(0)
    bg = fig.add_axes([0, 0, 1, 1])
    bg.set_xlim(0, WIDTH_IN)
    bg.set_ylim(0, height)
    bg.axis("off")
    bg.add_patch(FancyBboxPatch(
        (0.03, 0.03), WIDTH_IN - 0.06, height - 0.06,
        boxstyle="round,pad=0,rounding_size=0.14",
        facecolor="white", edgecolor=BORDER, linewidth=1.0,
    ))

    params = experiment.get("params") or {}
    scale_factor = params.get("scale_factor", "?")
    label = experiment.get("label") or ""
    subtitle = f"Scale factor {scale_factor}  ·  lower is better"
    if label:
        subtitle += f"  ·  {label}"
    bg.text(0.38, height - 0.42, "Time per engine, split by batch",
            fontsize=15, fontweight="bold", color=INK, va="center", ha="left")
    bg.text(0.38, height - 0.72, subtitle, fontsize=9.5, color=MUTED, va="center", ha="left")

    batch_count = len(rows[0]["durations_s"])
    legend = ["Batch 1 · initial load"] + [f"Batch {batch}" for batch in range(2, batch_count + 1)]
    x = 0.38
    for batch, text in enumerate(legend, start=1):
        color = BATCH_COLORS.get(batch, FALLBACK_BATCH_COLOR)
        bg.add_patch(FancyBboxPatch(
            (x, height - 1.1), 0.15, 0.15,
            boxstyle="round,pad=0,rounding_size=0.03", facecolor=color, edgecolor="none",
        ))
        bg.text(x + 0.23, height - 1.025, text, fontsize=9.5, color=INK, va="center", ha="left")
        x += 0.23 + len(text) * 0.075 + 0.35

    names = [row["engine"] for row in rows]
    left_in = max(len(name) for name in names) * 0.095 + 0.62
    right_in = 0.3
    axis_width = WIDTH_IN - left_in - right_in
    ax = fig.add_axes([
        left_in / WIDTH_IN, BOTTOM_IN / height,
        axis_width / WIDTH_IN, count * ROW_IN / height,
    ])
    ax.set_ylim(count - 0.5, -0.5)
    ax.set_yticks(range(count))
    ax.set_yticklabels(names, fontsize=11, fontweight="bold", color=INK)
    ax.tick_params(axis="y", length=0, pad=8)
    ax.set_xticks([])
    for side in ("top", "right", "bottom"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(BORDER)

    peak = max(row["total_s"] for row in rows)
    scale = peak if peak > 0 else 1
    ax.set_xlim(0, scale * 1.17)
    for index, row in enumerate(rows):
        left = 0.0
        for batch, seconds in enumerate(row["durations_s"], start=1):
            color = BATCH_COLORS.get(batch, FALLBACK_BATCH_COLOR)
            ax.barh(index, seconds, left=left, height=0.62, color=color,
                    edgecolor="white", linewidth=0.8)
            if seconds / (scale * 1.17) * axis_width >= 0.55:
                ax.text(left + seconds / 2, index, format_duration(seconds),
                        va="center", ha="center", fontsize=8.5,
                        fontweight="bold", color="white")
            left += seconds
        ax.text(left + scale * 0.015, index, format_duration(left),
                va="center", ha="left", fontsize=10.5, fontweight="bold", color=INK)

    output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output, format="png", dpi=180, facecolor="white")
    plt.close(fig)


def render_charts(verdict: Dict[str, Any], output_dir: Path) -> List[Dict[str, Any]]:
    if verdict.get("passed") is not True:
        return []
    charts = []
    experiments = verdict.get("experiments")
    if not isinstance(experiments, list):
        return []
    for position, experiment in enumerate(experiments, start=1):
        if not isinstance(experiment, dict) or experiment.get("status") != "completed":
            continue
        rows = _rows(experiment)
        if not rows:
            continue
        path = output_dir / f"experiment-{position:02d}.png"
        _render(rows, experiment, path)
        totals = "; ".join(
            f"{row['engine']} {format_duration(row['total_s'])}" for row in rows
        )
        alt = html.escape(f"Time per engine split by batch, fastest first: {totals}", quote=True)
        charts.append({"path": str(path), "alt": alt, "engines": rows})
    return charts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    verdict = json.loads(args.coverage.read_text(encoding="utf-8"))
    print(json.dumps({"charts": render_charts(verdict, args.output_dir)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
