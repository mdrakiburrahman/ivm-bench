"""Render OpenIVM per-operation refresh breakdown charts."""

import csv
import glob
import io
import logging
import os
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

_SPARK_LINE_RE = re.compile(r"\[openivm-perf\]\s+(?P<body>.*)$")
_KV_RE = re.compile(r"(\w+)=('([^']*)'|([^\s]+))")
_FULL_OUTCOMES = {
    "simple_projection_full_refresh",
    "simple_projection_full_refresh_fallback",
    "metadata_full_refresh",
    "full_refresh",
}
_REFRESH_STEPS = {
    "acquire_locks",
    "metadata_pre_sql",
    "execute_refresh_sql_stmt",
    "execute_refresh_sql",
    "metadata_post_sql",
    "total_refresh",
}
_CLASS_COLORS = {
    "INCR": "#2e7d32",
    "FULL": "#ef8a00",
    "NOOP": "#777777",
    "INCOMPLETE": "#777777",
    "UNKNOWN": "#777777",
    "MIXED": "#777777",
}
_RESERVED_SEGMENT_COLORS = {
    "merge_skipped": "#d62728",
    "other_elapsed": "#bdbdbd",
    "unattributed": "#bdbdbd",
}
_TOP_N = 20


def render_batch_png(
    sf: str,
    engine: str,
    batch: int,
    repo_dir: str,
    log_byte_window: Optional[Tuple[int, int]] = None,
) -> Optional[bytes]:
    refreshes = _load_refreshes(sf, engine, batch, repo_dir, log_byte_window)
    refreshes = _select_latest_by_view(refreshes)
    if not refreshes:
        return None
    return _render_png(refreshes, engine=engine, batch=batch, sf=sf)


def save_batch_png(
    sf: str,
    engine: str,
    batch: int,
    repo_dir: str,
    log_byte_window: Optional[Tuple[int, int]] = None,
) -> Optional[str]:
    """Write PNG to <repo_dir>/mount/imgs/<sf>/openivm-ops-<engine>-batch<batch>.png. Return path or None."""
    png = render_batch_png(sf, engine, batch, repo_dir, log_byte_window)
    if not png:
        return None

    out_dir = os.path.join(repo_dir, "mount", "imgs", str(sf))
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"openivm-ops-{engine}-batch{batch}.png")
    with open(out_path, "wb") as f:
        f.write(png)
    return out_path


def render_compare_png(
    sf: str,
    engine: str,
    batch: int,
    after_repo_dir: str,
    before_repo_dir: str,
) -> Optional[bytes]:
    """Side-by-side comparison: BEFORE state on left, AFTER on right, same MVs aligned, same scale."""
    before = _select_latest_by_view(_load_refreshes(sf, engine, batch, before_repo_dir, None))
    after = _select_latest_by_view(_load_refreshes(sf, engine, batch, after_repo_dir, None))
    if not before or not after:
        return None
    return _render_compare_png(before, after, engine=engine, batch=batch, sf=sf)


def _load_refreshes(
    sf: str,
    engine: str,
    batch: int,
    repo_dir: str,
    log_byte_window: Optional[Tuple[int, int]],
) -> List[dict]:
    if engine == "spark-openivm":
        return _parse_spark_openivm_perf(repo_dir, sf, log_byte_window)
    if engine == "duckdb-openivm":
        return _parse_duckdb_openivm_perf(repo_dir, sf, batch)
    return []


def _parse_spark_openivm_perf(
    repo_dir: str,
    sf: str,
    log_byte_window: Optional[Tuple[int, int]],
) -> List[dict]:
    log_dir = os.path.join(repo_dir, "mount", "logs", str(sf), "spark-openivm", "livy-logs")
    live_log = os.path.join(log_dir, "livy-server.log")
    if log_byte_window is not None:
        lines = _read_log_window(live_log, log_byte_window)
    else:
        lines = _read_all_livy_logs(log_dir)

    groups: Dict[str, dict] = {}
    order = 0
    for line in lines:
        order += 1
        match = _SPARK_LINE_RE.search(line)
        if not match:
            continue
        try:
            attrs = _parse_kv(match.group("body"))
            refresh_id = attrs.get("refresh_id")
            if not refresh_id:
                continue
            group = groups.setdefault(
                refresh_id,
                {
                    "engine": "spark-openivm",
                    "refresh_id": refresh_id,
                    "view": attrs.get("view", "unknown"),
                    "refresh_type": None,
                    "outcome": None,
                    "total_ms": None,
                    "pending_deltas": None,
                    "stmts": [],
                    "phase_times": defaultdict(float),
                    "segments": defaultdict(float),
                    "order": order,
                    "last_order": order,
                },
            )
            group["last_order"] = order
            if attrs.get("view"):
                group["view"] = attrs["view"]
            if attrs.get("refresh_type"):
                group["refresh_type"] = attrs["refresh_type"]
            if attrs.get("pending_deltas") is not None:
                group["pending_deltas"] = _safe_int(attrs.get("pending_deltas"))

            phase = attrs.get("phase")
            elapsed_ms = _safe_float(attrs.get("elapsed_ms"))
            if phase == "stmt":
                stmt = {
                    "stmt_idx": _safe_int(attrs.get("stmt_idx")),
                    "stmt_kind": attrs.get("stmt_kind", "other"),
                    "elapsed_ms": elapsed_ms or 0.0,
                }
                group["stmts"].append(stmt)
                group["segments"][stmt["stmt_kind"]] += stmt["elapsed_ms"]
            elif phase == "end":
                group["total_ms"] = _safe_float(attrs.get("total_ms"))
                group["outcome"] = attrs.get("outcome")
            elif phase and elapsed_ms is not None:
                group["phase_times"][phase] += elapsed_ms
                group["segments"][phase] += elapsed_ms
        except Exception as exc:  # pragma: no cover - defensive row skip
            logger.warning("Skipping malformed spark-openivm perf line: %s", exc)

    refreshes: List[dict] = []
    for group in groups.values():
        try:
            phase_times = dict(group.get("phase_times") or {})
            segments = dict(group.get("segments") or {})
            stmt_sum = sum(float(s.get("elapsed_ms") or 0.0) for s in group.get("stmts") or [])
            known_total = stmt_sum + sum(float(v or 0.0) for v in phase_times.values())
            if group.get("total_ms") is None:
                group["total_ms"] = known_total
                group["classification"] = "INCOMPLETE"
            else:
                group["classification"] = _classify_spark(group)
                remainder = float(group["total_ms"] or 0.0) - sum(segments.values())
                if remainder > 0.5:
                    segments["other_elapsed"] = segments.get("other_elapsed", 0.0) + remainder
            group["view"] = _normalize_view(group.get("view"))
            group["phase_times"] = phase_times
            group["segments"] = segments
            group["event_kind"] = "refresh"
            group["is_create"] = False
            refreshes.append(group)
        except Exception as exc:  # pragma: no cover - defensive group skip
            logger.warning("Skipping malformed spark-openivm refresh group: %s", exc)
    return refreshes


def _parse_duckdb_openivm_perf(repo_dir: str, sf: str, batch: int) -> List[dict]:
    csv_path = os.path.join(
        repo_dir,
        "mount",
        "results",
        str(sf),
        "dbt-server",
        f"openivm-profile-batch{batch}.csv",
    )
    if not os.path.exists(csv_path):
        return []

    groups: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    try:
        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    view = row.get("view_name") or "unknown"
                    refresh_id = row.get("refresh_id") or f"{view}-unknown"
                    groups[(view, refresh_id)].append(row)
                except Exception as exc:  # pragma: no cover - defensive row skip
                    logger.warning("Skipping malformed duckdb-openivm profile row: %s", exc)
    except Exception as exc:
        logger.warning("Unable to read duckdb-openivm profile CSV %s: %s", csv_path, exc)
        return []

    refreshes: List[dict] = []
    for (view, refresh_id), rows in groups.items():
        try:
            step_times: Dict[str, float] = defaultdict(float)
            step_names = set()
            dispatch_refresh_type = None
            create_refresh_type = None
            first_ts = None
            last_ts = None
            max_step_order = -1

            for row in rows:
                step_name = row.get("step_name") or "unknown"
                duration_ms = _safe_float(row.get("duration_ms")) or 0.0
                detail = _parse_detail(row.get("detail") or "")
                step_times[step_name] += duration_ms
                step_names.add(step_name)
                max_step_order = max(max_step_order, _safe_int(row.get("step_order")) or 0)
                ts = _parse_timestamp(row.get("profile_timestamp"))
                if ts is not None:
                    first_ts = ts if first_ts is None or ts < first_ts else first_ts
                    last_ts = ts if last_ts is None or ts > last_ts else last_ts
                if step_name == "generate_refresh_sql.dispatch" and detail.get("refresh_type"):
                    dispatch_refresh_type = detail["refresh_type"]
                if step_name == "create_compile_classification" and detail.get("refresh_type"):
                    create_refresh_type = detail["refresh_type"]

            is_create = "_create_mv_" in refresh_id or (step_names and all(s.startswith("create_") for s in step_names))
            is_refresh = any(s in _REFRESH_STEPS or s.startswith("generate_refresh_sql") for s in step_names)
            if is_create and not is_refresh:
                segments = _duckdb_create_segments(step_times)
                total_ms = step_times.get("create_mv_total") or sum(segments.values())
                total_ms = _align_total_with_segments(total_ms, segments)
                classification = "FULL"
                refresh_type = "CREATE"
                event_kind = "create"
            elif is_refresh:
                segments = _duckdb_refresh_segments(step_times)
                total_ms = step_times.get("total_refresh") or sum(segments.values())
                total_ms = _align_total_with_segments(total_ms, segments)
                refresh_type = dispatch_refresh_type
                classification = _classify_duckdb(dispatch_refresh_type, total_ms, segments)
                event_kind = "refresh"
            else:
                total_ms = sum(step_times.values())
                segments = {k: v for k, v in step_times.items() if v > 0}
                refresh_type = create_refresh_type or dispatch_refresh_type
                classification = "UNKNOWN"
                event_kind = "unknown"

            refreshes.append(
                {
                    "engine": "duckdb-openivm",
                    "refresh_id": refresh_id,
                    "view": _normalize_view(view),
                    "refresh_type": refresh_type,
                    "outcome": None,
                    "total_ms": total_ms,
                    "pending_deltas": None,
                    "stmts": [],
                    "phase_times": dict(step_times),
                    "segments": segments,
                    "classification": classification,
                    "event_kind": event_kind,
                    "is_create": event_kind == "create",
                    "first_ts": first_ts,
                    "last_ts": last_ts,
                    "last_order": int(last_ts.timestamp() * 1000) if last_ts else max_step_order,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive group skip
            logger.warning("Skipping malformed duckdb-openivm profile group %s/%s: %s", view, refresh_id, exc)
    return refreshes


def _render_png(refreshes_classified: List[dict], engine: str, batch: int, sf: str = "?") -> bytes:
    plt, Patch = _matplotlib()
    rows = _collapse_rows(sorted(refreshes_classified, key=lambda r: float(r.get("total_ms") or 0.0), reverse=True))
    segment_keys = _segment_keys(rows)
    colors = _segment_color_map(segment_keys, plt)

    height = max(7.0, 0.42 * len(rows) + 4.2)
    fig, (ax_top, ax_summary) = plt.subplots(
        2,
        1,
        figsize=(14, height),
        gridspec_kw={"height_ratios": [max(3.0, 0.38 * len(rows)), 1.35]},
    )

    _draw_stacked_bar_panel(ax_top, rows, colors, show_xlabel=True)
    handles = [Patch(facecolor=colors[k], label=k) for k in segment_keys]
    if handles:
        ax_top.legend(
            handles=handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.12),
            ncol=min(4, max(1, len(handles))),
            fontsize=8,
            frameon=False,
        )

    _draw_summary_panel(ax_summary, refreshes_classified, engine, batch)
    fig.suptitle(f"OpenIVM op breakdown — SF{sf} batch{batch} — {engine}", fontsize=16, fontweight="bold", y=0.995)
    fig.subplots_adjust(hspace=0.72, top=0.93, bottom=0.08)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def _render_compare_png(before: List[dict], after: List[dict], engine: str, batch: int, sf: str = "?") -> bytes:
    plt, Patch = _matplotlib()
    after_rows = _collapse_rows(sorted(after, key=lambda r: float(r.get("total_ms") or 0.0), reverse=True))
    before_by_view = {r.get("view"): r for r in before}
    after_views = {r.get("view") for r in after_rows if not r.get("is_other")}
    before_other = _make_other_row([r for r in before if r.get("view") not in after_views])

    before_rows = []
    for row in after_rows:
        if row.get("is_other"):
            before_rows.append(before_other or _empty_like(row))
        else:
            before_rows.append(before_by_view.get(row.get("view")) or _empty_like(row))

    segment_keys = _segment_keys(before_rows + after_rows)
    colors = _segment_color_map(segment_keys, plt)
    max_total = max([float(r.get("total_ms") or 0.0) for r in before_rows + after_rows] + [1.0])

    height = max(7.0, 0.42 * len(after_rows) + 2.8)
    fig, (ax_before, ax_after) = plt.subplots(1, 2, figsize=(18, height), sharey=True)
    _draw_stacked_bar_panel(ax_before, before_rows, colors, xlim=max_total, title="BEFORE", show_xlabel=True)
    _draw_stacked_bar_panel(ax_after, after_rows, colors, xlim=max_total, title="AFTER", show_xlabel=True)
    _annotate_deltas(ax_after, before_rows, after_rows, max_total)

    handles = [Patch(facecolor=colors[k], label=k) for k in segment_keys]
    if handles:
        fig.legend(
            handles=handles,
            loc="lower center",
            ncol=min(5, max(1, len(handles))),
            fontsize=8,
            frameon=False,
        )
    fig.suptitle(f"OpenIVM op breakdown compare — SF{sf} batch{batch} — {engine}", fontsize=16, fontweight="bold")
    fig.subplots_adjust(wspace=0.08, top=0.92, bottom=0.14)

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=150, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return buf.getvalue()


def _draw_stacked_bar_panel(ax, rows: List[dict], colors: Dict[str, str], xlim: Optional[float] = None, title: Optional[str] = None, show_xlabel: bool = False) -> None:
    y_positions = list(range(len(rows)))
    segment_keys = _segment_keys(rows)
    for y, row in zip(y_positions, rows):
        left = 0.0
        segments = row.get("segments") or {}
        for key in segment_keys:
            value = float(segments.get(key) or 0.0)
            if value <= 0:
                continue
            ax.barh(y, value, left=left, color=colors[key], edgecolor="white", linewidth=0.35)
            left += value
        total_ms = float(row.get("total_ms") or 0.0)
        if total_ms > 0:
            ax.text(total_ms, y, f"  {_format_ms(total_ms)}", va="center", ha="left", fontsize=8, color="#444444")

    labels = [_row_label(r) for r in rows]
    ax.set_yticks(y_positions)
    ax.set_yticklabels(labels, fontsize=8)
    for label, row in zip(ax.get_yticklabels(), rows):
        label.set_color(_CLASS_COLORS.get(row.get("classification"), "#777777"))
        if row.get("classification") in ("INCR", "FULL"):
            label.set_fontweight("bold")
    ax.invert_yaxis()
    ax.grid(axis="x", linestyle=":", alpha=0.35)
    ax.set_axisbelow(True)
    if show_xlabel:
        ax.set_xlabel("elapsed ms")
    if title:
        ax.set_title(title, fontweight="bold")
    max_total = xlim or max([float(r.get("total_ms") or 0.0) for r in rows] + [1.0])
    ax.set_xlim(0, max_total * 1.18)


def _draw_summary_panel(ax, refreshes: List[dict], engine: str, batch: int) -> None:
    counts = Counter(r.get("classification", "UNKNOWN") for r in refreshes)
    classes = ["INCR", "FULL", "NOOP"] + [c for c in ("INCOMPLETE", "UNKNOWN") if counts.get(c)]
    y_positions = list(range(len(classes)))
    values = [counts.get(c, 0) for c in classes]
    colors = [_CLASS_COLORS.get(c, "#777777") for c in classes]
    ax.barh(y_positions, values, color=colors)
    ax.set_yticks(y_positions)
    ax.set_yticklabels(classes)
    ax.invert_yaxis()
    ax.set_xlim(0, max(values + [1]) * 1.25)
    for y, value in zip(y_positions, values):
        ax.text(value, y, f" {value}", va="center", ha="left", fontweight="bold")
    ax.set_xlabel("MV count")
    ax.set_title(f"Refresh classification — batch {batch} — {engine} — total {len(refreshes)} MVs", fontsize=11)
    ax.grid(axis="x", linestyle=":", alpha=0.3)
    ax.set_axisbelow(True)


def _annotate_deltas(ax, before_rows: List[dict], after_rows: List[dict], max_total: float) -> None:
    for y, (before, after) in enumerate(zip(before_rows, after_rows)):
        before_ms = float(before.get("total_ms") or 0.0)
        after_ms = float(after.get("total_ms") or 0.0)
        delta = after_ms - before_ms
        if abs(delta) < 0.5:
            text = "±0ms"
            color = "#777777"
        elif delta < 0:
            text = f"Δ -{_format_ms(abs(delta))}"
            color = "#2e7d32"
        else:
            text = f"Δ +{_format_ms(delta)}"
            color = "#b00020"
        ax.text(max(after_ms, 0.0) + max_total * 0.02, y, text, va="center", ha="left", fontsize=8, color=color)


def _read_log_window(path: str, log_byte_window: Tuple[int, int]) -> List[str]:
    if not os.path.exists(path):
        return []
    start, end = log_byte_window
    start = max(0, int(start or 0))
    end = max(start, int(end or 0))
    try:
        with open(path, "rb") as f:
            f.seek(start)
            data = f.read(max(0, end - start))
        return data.decode("utf-8", errors="replace").splitlines()
    except Exception as exc:
        logger.warning("Unable to read spark-openivm log window %s: %s", path, exc)
        return []


def _read_all_livy_logs(log_dir: str) -> List[str]:
    files = sorted(glob.glob(os.path.join(log_dir, "livy-server.log*")), key=_livy_log_sort_key)
    lines: List[str] = []
    for path in files:
        try:
            with open(path, encoding="utf-8", errors="replace") as f:
                lines.extend(f.read().splitlines())
        except Exception as exc:
            logger.warning("Unable to read spark-openivm log %s: %s", path, exc)
    return lines


def _livy_log_sort_key(path: str) -> Tuple[int, int]:
    name = os.path.basename(path)
    if name == "livy-server.log":
        return (1, 0)
    match = re.match(r"livy-server\.log\.(\d+)$", name)
    if match:
        return (0, -int(match.group(1)))
    return (0, 0)


def _parse_kv(body: str) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for match in _KV_RE.finditer(body):
        attrs[match.group(1)] = match.group(3) if match.group(3) is not None else match.group(4)
    return attrs


def _parse_detail(detail: str) -> Dict[str, str]:
    attrs: Dict[str, str] = {}
    for part in detail.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        attrs[key.strip()] = value.strip()
    return attrs


def _duckdb_create_segments(step_times: Dict[str, float]) -> Dict[str, float]:
    return {k: v for k, v in step_times.items() if k != "create_mv_total" and v > 0}


def _duckdb_refresh_segments(step_times: Dict[str, float]) -> Dict[str, float]:
    segments: Dict[str, float] = {}
    for key in ("acquire_locks", "metadata_pre_sql", "metadata_post_sql"):
        if step_times.get(key, 0.0) > 0:
            segments[key] = step_times[key]

    generate_parent = step_times.get("generate_refresh_sql", 0.0)
    generate_children = sum(v for k, v in step_times.items() if k.startswith("generate_refresh_sql.") and v > 0)
    generate_total = generate_parent or generate_children
    if generate_total > 0:
        segments["generate_refresh_sql"] = generate_total

    execute_parent = step_times.get("execute_refresh_sql", 0.0)
    execute_children = sum(v for k, v in step_times.items() if k == "execute_refresh_sql_stmt" and v > 0)
    execute_total = execute_parent or execute_children
    if execute_total > 0:
        segments["execute_refresh_sql"] = execute_total

    for key, value in step_times.items():
        if key in segments or key == "total_refresh":
            continue
        if key.startswith("generate_refresh_sql") or key.startswith("execute_refresh_sql"):
            continue
        if value > 0:
            segments[key] = value
    return segments


def _align_total_with_segments(total_ms: float, segments: Dict[str, float]) -> float:
    segment_total = sum(segments.values())
    if total_ms <= 0:
        return segment_total
    remainder = total_ms - segment_total
    if remainder > 0.5:
        segments["unattributed"] = segments.get("unattributed", 0.0) + remainder
    elif remainder < -0.5:
        total_ms = segment_total
    return total_ms


def _classify_spark(group: dict) -> str:
    refresh_type = group.get("refresh_type")
    outcome = group.get("outcome")
    if outcome == "no_pending_deltas":
        return "NOOP"
    if outcome in _FULL_OUTCOMES or refresh_type == "FULL_REFRESH":
        return "FULL"
    if outcome == "incremental_executed":
        return "INCR"
    return "UNKNOWN"


def _classify_duckdb(refresh_type: Optional[str], total_ms: float, segments: Dict[str, float]) -> str:
    if refresh_type == "FULL_REFRESH":
        return "FULL"
    if refresh_type and total_ms > 0:
        return "INCR"
    if not refresh_type and not any(k.startswith("execute_refresh_sql") or k.startswith("generate_refresh_sql") for k in segments):
        return "NOOP"
    if total_ms <= 0:
        return "NOOP"
    return "UNKNOWN"


def _select_latest_by_view(refreshes: Iterable[dict]) -> List[dict]:
    by_view: Dict[str, List[dict]] = defaultdict(list)
    for refresh in refreshes:
        by_view[refresh.get("view") or "unknown"].append(refresh)

    selected: List[dict] = []
    for rows in by_view.values():
        non_create = [r for r in rows if r.get("event_kind") != "create"]
        candidates = non_create or rows
        selected.append(max(candidates, key=lambda r: int(r.get("last_order") or r.get("order") or 0)))
    return selected


def _collapse_rows(rows: List[dict], top_n: int = _TOP_N) -> List[dict]:
    if len(rows) <= top_n:
        return rows
    top = rows[:top_n]
    other = _make_other_row(rows[top_n:])
    return top + ([other] if other else [])


def _make_other_row(rows: List[dict]) -> Optional[dict]:
    if not rows:
        return None
    segments: Dict[str, float] = defaultdict(float)
    total_ms = 0.0
    for row in rows:
        total_ms += float(row.get("total_ms") or 0.0)
        for key, value in (row.get("segments") or {}).items():
            segments[key] += float(value or 0.0)
    return {
        "view": f"(other {len(rows)} MVs)",
        "refresh_type": None,
        "classification": "MIXED",
        "total_ms": total_ms,
        "segments": dict(segments),
        "is_other": True,
        "event_kind": "other",
    }


def _empty_like(row: dict) -> dict:
    return {
        "view": row.get("view"),
        "refresh_type": row.get("refresh_type"),
        "classification": "NOOP",
        "total_ms": 0.0,
        "segments": {},
        "event_kind": "missing",
        "is_other": row.get("is_other", False),
    }


def _segment_keys(rows: List[dict]) -> List[str]:
    totals: Dict[str, float] = defaultdict(float)
    for row in rows:
        for key, value in (row.get("segments") or {}).items():
            if value and value > 0:
                totals[key] += float(value)
    return sorted(totals, key=lambda k: (-totals[k], k))


def _segment_color_map(segment_keys: List[str], plt) -> Dict[str, str]:
    palettes = []
    for name in ("tab20", "tab20b", "tab20c", "Set3"):
        palettes.extend(plt.get_cmap(name).colors)
    colors: Dict[str, str] = {}
    palette_idx = 0
    for key in segment_keys:
        if key in _RESERVED_SEGMENT_COLORS:
            colors[key] = _RESERVED_SEGMENT_COLORS[key]
            continue
        colors[key] = palettes[palette_idx % len(palettes)]
        palette_idx += 1
    return colors


def _row_label(row: dict) -> str:
    classification = row.get("classification") or "UNKNOWN"
    refresh_type = row.get("refresh_type")
    if row.get("is_other"):
        badge = "[MIXED]"
    elif row.get("is_create"):
        badge = "[FULL CREATE]"
    elif classification == "NOOP":
        badge = "[NOOP]"
    elif refresh_type:
        badge = f"[{classification} {refresh_type}]"
    else:
        badge = f"[{classification}]"
    return f"{badge} {_truncate(str(row.get('view') or 'unknown'), 72)}"


def _normalize_view(view: Optional[str]) -> str:
    if not view:
        return "unknown"
    return view.replace("`", "")


def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _format_ms(ms: float) -> str:
    if ms >= 1000:
        return f"{ms / 1000:.1f}s"
    return f"{ms:.0f}ms"


def _safe_int(value) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_float(value) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _matplotlib():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    return plt, Patch
