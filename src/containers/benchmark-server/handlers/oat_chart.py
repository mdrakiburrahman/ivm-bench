"""OAT sweep chart and markdown result generation."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

try:
    from flask import Blueprint, Flask, Response, jsonify, request
except ModuleNotFoundError:  # Allows pure function smoke tests on minimal hosts.
    Flask = Any  # type: ignore

    class Response:  # type: ignore
        def __init__(self, response: Any = None, mimetype: Optional[str] = None):
            self.response = response
            self.mimetype = mimetype

    class _RequestStub:
        args: Dict[str, str] = {}

    class _BlueprintStub:
        def __init__(self, *_args: Any, **_kwargs: Any):
            pass

        def route(self, *_args: Any, **_kwargs: Any):
            def decorator(func: Any) -> Any:
                return func

            return decorator

    def jsonify(value: Any) -> Any:  # type: ignore
        return value

    request = _RequestStub()  # type: ignore
    Blueprint = _BlueprintStub  # type: ignore

try:
    from handlers.base import BaseHandler
except ModuleNotFoundError:
    class BaseHandler:  # type: ignore
        def register(self, app: Flask) -> None:
            raise NotImplementedError

from models.experiments import ExperimentInputs

bp = Blueprint("oat_chart", __name__)

_MISSING = "—"
_BATCHES = (1, 2, 3)
_ENGINE_ORDER = ("spark", "spark-openivm", "duckdb", "duckdb-openivm", "feldera", "databricks-enzyme", "fabric-jvm-35", "fabric-openivm-jvm-35")


@dataclass(frozen=True)
class _Column:
    key: str
    short: str
    description: str
    kind: str
    width: float = 0.55
    heatmap: bool = False
    text_only: bool = False
    green_high: bool = False


def _repo_dir() -> str:
    return os.environ.get("REPO_DIR", "/repo")


def _load_json(path: str) -> Optional[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _state_candidates(oat_run_id: str, state_dir: str) -> List[str]:
    repo_dir = _repo_dir()
    candidates: List[str] = []
    if state_dir:
        candidates.extend(
            [
                os.path.join(state_dir, oat_run_id, "outputs.json"),
                os.path.join(state_dir, "oat-state", oat_run_id, "outputs.json"),
                os.path.join(state_dir, "mount", "oat-state", oat_run_id, "outputs.json"),
                os.path.join(state_dir, "outputs.json"),
            ]
        )
    candidates.append(os.path.join(repo_dir, "mount", "oat-state", oat_run_id, "outputs.json"))

    out: List[str] = []
    seen = set()
    for path in candidates:
        norm = os.path.abspath(path)
        if norm not in seen:
            seen.add(norm)
            out.append(path)
    return out


def _load_oat_state(oat_run_id: str, state_dir: str = "/data/state") -> Dict[str, Any]:
    """Load the master OAT outputs.json for a run."""
    for path in _state_candidates(oat_run_id, state_dir):
        data = _load_json(path)
        if data is not None:
            return data
    raise FileNotFoundError(f"no OAT outputs.json found for run_id={oat_run_id!r}")


def _sort_experiments(state: Dict[str, Any]) -> List[Dict[str, Any]]:
    experiments = [e for e in state.get("experiments", []) if isinstance(e, dict)]
    indexed: List[Tuple[int, Dict[str, Any]]] = []
    for fallback_idx, exp in enumerate(experiments):
        try:
            idx = int(exp.get("exp_idx", fallback_idx))
        except (TypeError, ValueError):
            idx = fallback_idx
        indexed.append((idx, exp))
    return [exp for _, exp in sorted(indexed, key=lambda item: item[0])]


def _exp_idx(exp: Dict[str, Any], fallback: int = 0) -> int:
    try:
        return int(exp.get("exp_idx", fallback))
    except (TypeError, ValueError):
        return fallback


def _exp_label(exp: Dict[str, Any], fallback: int = 0) -> str:
    label = exp.get("label")
    if label is None and isinstance(exp.get("inputs"), dict):
        label = exp["inputs"].get("label")
    return str(label or f"exp-{_exp_idx(exp, fallback)}")


def _input_model(exp: Dict[str, Any]) -> ExperimentInputs:
    data = exp.get("inputs") if isinstance(exp.get("inputs"), dict) else {}
    if not data:
        data = {
            "label": exp.get("label"),
            "scale_factor": exp.get("scale_factor", 3),
            "engines": list((exp.get("engines") or {}).keys()) or ["spark", "spark-openivm"],
        }
    try:
        return ExperimentInputs.from_dict(data)
    except Exception:
        return ExperimentInputs()


def _flat_inputs(exp: Dict[str, Any]) -> Dict[str, Any]:
    return _input_model(exp).flat_values()


def _scale_factor(exp: Dict[str, Any]) -> Any:
    if exp.get("scale_factor") is not None:
        return exp.get("scale_factor")
    return _flat_inputs(exp).get("scale_factor", "")


def _sort_engines(engines: Iterable[str]) -> List[str]:
    order = {engine: idx for idx, engine in enumerate(_ENGINE_ORDER)}
    return sorted(set(engines), key=lambda e: (order.get(e, 999), e))


def _engine_union(experiments: Sequence[Dict[str, Any]]) -> List[str]:
    engines = set()
    for exp in experiments:
        exp_engines = exp.get("engines")
        if isinstance(exp_engines, dict):
            engines.update(str(e) for e in exp_engines.keys())
        elif isinstance(exp_engines, list):
            engines.update(str(e) for e in exp_engines)
        inputs = exp.get("inputs")
        if isinstance(inputs, dict):
            raw = inputs.get("engines")
            if isinstance(raw, list):
                engines.update(str(e) for e in raw)
            elif isinstance(raw, str):
                engines.update(e.strip() for e in raw.split(",") if e.strip())
    return _sort_engines(engines)


def _numeric(value: Any) -> Optional[float]:
    if value is None or value == "" or value == _MISSING:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if math.isfinite(float(value)):
            return float(value)
        return None
    if isinstance(value, str):
        text = value.strip().rstrip("%")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _format_duration(seconds: Any, *, compact: bool = False) -> str:
    value = _numeric(seconds)
    if value is None:
        return _MISSING
    total = max(0, int(round(value)))
    hours, rem = divmod(total, 3600)
    minutes, secs = divmod(rem, 60)
    if compact and hours == 0:
        return f"{minutes:02d}:{secs:02d}"
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _format_bytes(value: Any) -> str:
    numeric = _numeric(value)
    if numeric is None:
        return _MISSING
    size = float(numeric)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(size) < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.2f} {unit}"
        size /= 1024
    return _MISSING


def _cell_text(value: Any) -> str:
    if value is None or value == "":
        return _MISSING
    if isinstance(value, float):
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}".rstrip("0").rstrip(".")
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)


def _truncate(value: str, max_len: int = 18) -> str:
    return value if len(value) <= max_len else value[: max_len - 1] + "…"


def _batch_duration(exp: Dict[str, Any], engine: str, batch: int) -> Optional[float]:
    engine_data = (exp.get("engines") or {}).get(engine)
    if not isinstance(engine_data, dict):
        return None
    batches = engine_data.get("batches") or []
    for item in batches:
        if not isinstance(item, dict):
            continue
        try:
            item_batch = int(item.get("batch_num"))
        except (TypeError, ValueError):
            item_batch = None
        if item_batch == batch:
            return _numeric(item.get("duration_s"))
    if 0 <= batch - 1 < len(batches) and isinstance(batches[batch - 1], dict):
        return _numeric(batches[batch - 1].get("duration_s"))
    return None


def _storage_summary(exp: Dict[str, Any], engine: str, batch: int) -> Optional[Dict[str, Any]]:
    storage = exp.get("storage") or {}
    engine_storage = storage.get(engine)
    if not isinstance(engine_storage, dict):
        return None
    item = engine_storage.get(str(batch)) or engine_storage.get(batch)
    return item if isinstance(item, dict) else None


def _status(exp: Dict[str, Any]) -> str:
    return str(exp.get("status") or "unknown")


def _status_note(exp: Dict[str, Any], max_len: int = 80) -> str:
    """Short human-readable note for the Overview column: skip reason for
    skipped rows, truncated error for failed rows, empty otherwise. Makes
    fail-fast aborts visible in RESULTS.md without opening outputs.json."""
    status = (exp.get("status") or "").lower()
    if status == "skipped":
        reason = exp.get("skip_reason") or ""
        return _md_escape((reason[: max_len - 1] + "…") if len(reason) > max_len else reason)
    if status == "failed":
        err = exp.get("error") or ""
        return _md_escape((err[: max_len - 1] + "…") if len(err) > max_len else err)
    return ""


def _status_color(status: str) -> str:
    lower = status.lower()
    if lower == "completed":
        return "#ffffff"
    if lower == "skipped":
        return "#dddddd"
    if lower == "failed":
        return "#f6c7c3"
    return "#eeeeee"


def _relative_luminance(rgba: Tuple[float, float, float, float]) -> float:
    r, g, b = rgba[:3]
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def _text_color_for(rgba: Tuple[float, float, float, float]) -> str:
    return "white" if _relative_luminance(rgba) < 0.48 else "black"


def _png_from_fig(fig: Any) -> bytes:
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=fig.dpi, bbox_inches="tight", facecolor="white")
    data = buf.getvalue()
    buf.close()
    return data


def _empty_png(message: str) -> bytes:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(8, 2.4), dpi=120)
    ax.axis("off")
    ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=13)
    data = _png_from_fig(fig)
    plt.close(fig)
    return data


def _aggregate_columns(experiments: Sequence[Dict[str, Any]], engines: Sequence[str]) -> List[_Column]:
    columns = [_Column("#", "#", "experiment index", "META", heatmap=False)]
    for key, header in ExperimentInputs.column_headers().items():
        columns.append(
            _Column(
                key,
                header,
                f"ExperimentInputs.{key}",
                "INPUT",
                width=1.15 if key == "engines" else 0.55,
                heatmap=key != "engines",
                text_only=key == "engines",
            )
        )
    for engine in engines:
        for batch in _BATCHES:
            key = f"{engine}.b{batch}"
            columns.append(_Column(key, key, f"{engine} batch {batch} wall-clock seconds", "OUTPUT", heatmap=True))
    columns.append(
        _Column("disk_free_pct", "disk%", "free disk percentage after experiment", "OUTPUT", heatmap=True, green_high=True)
    )
    columns.append(_Column("status", "status", "experiment status", "META", width=0.8, text_only=True))
    return columns


def _aggregate_row(exp: Dict[str, Any], columns: Sequence[_Column], fallback: int) -> Tuple[List[Any], List[str]]:
    status = _status(exp)
    skipped = status.lower() == "skipped"
    flat = _flat_inputs(exp)
    values: List[Any] = []
    texts: List[str] = []
    for col in columns:
        if col.key == "#":
            value = _exp_idx(exp, fallback)
        elif col.key == "status":
            value = status
        elif col.key == "disk_free_pct":
            value = None if skipped else exp.get("disk_free_pct")
        elif ".b" in col.key and col.kind == "OUTPUT":
            engine, batch_text = col.key.rsplit(".b", 1)
            value = None if skipped else _batch_duration(exp, engine, int(batch_text))
        else:
            value = flat.get(col.key)
        values.append(value)
        if col.kind == "OUTPUT" and col.key != "disk_free_pct":
            texts.append(_format_duration(value, compact=True))
        elif col.key == "disk_free_pct":
            num = _numeric(value)
            texts.append(_MISSING if num is None else f"{num:.1f}")
        else:
            texts.append(_truncate(_cell_text(value)))
    return values, texts


def _column_color(col: _Column, value: Any, numeric_values: Sequence[float], plt: Any) -> Tuple[str, Tuple[float, float, float, float]]:
    from matplotlib.colors import to_rgba

    if col.key == "status":
        rgba = to_rgba(_status_color(str(value)))
        return _status_color(str(value)), rgba
    num = _numeric(value)
    if col.text_only or not col.heatmap:
        rgba = to_rgba("#f7f7f7")
        return "#f7f7f7", rgba
    if num is None:
        rgba = to_rgba("#d9d9d9")
        return "#d9d9d9", rgba
    if not numeric_values:
        rgba = to_rgba("#ffffff")
        return "#ffffff", rgba
    min_v = min(numeric_values)
    max_v = max(numeric_values)
    if math.isclose(min_v, max_v):
        rgba = to_rgba("#fff3b0")
        return "#fff3b0", rgba
    ratio = (num - min_v) / (max_v - min_v)
    cmap = plt.get_cmap("RdYlGn") if col.green_high else plt.get_cmap("Reds")
    rgba = cmap(ratio if col.green_high else 0.10 + 0.85 * ratio)
    return rgba, rgba


def generate_oat_aggregate_png(oat_run_id: str, state_dir: str = "/data/state") -> bytes:
    """Generate the OAT aggregate heatmap PNG."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    from matplotlib.colors import to_rgba
    from matplotlib.patches import Rectangle

    state = _load_oat_state(oat_run_id, state_dir)
    experiments = _sort_experiments(state)
    if not experiments:
        return _empty_png(f"OAT sweep — {oat_run_id[:8]} — no experiments")

    engines = _engine_union(experiments)
    columns = _aggregate_columns(experiments, engines)
    row_values: List[List[Any]] = []
    row_texts: List[List[str]] = []
    for idx, exp in enumerate(experiments):
        values, texts = _aggregate_row(exp, columns, idx)
        row_values.append(values)
        row_texts.append(texts)

    df = pd.DataFrame(row_values, columns=[c.key for c in columns])
    numeric_by_col: Dict[str, List[float]] = {}
    for col in columns:
        nums = [_numeric(v) for v in df[col.key].tolist()] if col.key in df else []
        numeric_by_col[col.key] = [n for n in nums if n is not None]

    scalar_w = 0.55
    row_h = 0.42
    header_h = 1.1
    label_gutter = 3.5
    title_h = 0.55
    legend_h = max(1.1, 0.18 * len(columns) + 0.35)
    table_h = header_h + row_h * len(experiments)
    width = label_gutter + sum(c.width or scalar_w for c in columns) + 0.25
    height = title_h + table_h + legend_h + 0.3

    fig = plt.figure(figsize=(width, height), dpi=120)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, width)
    ax.set_ylim(0, height)
    ax.axis("off")

    total = _format_duration(state.get("total_duration_s"), compact=False)
    title = f"OAT sweep — {oat_run_id[:8]} — {len(experiments)} experiment(s) — total {total}"
    ax.text(width / 2, height - 0.22, title, ha="center", va="top", fontsize=12, fontweight="bold")

    table_bottom = legend_h + 0.15
    table_top = table_bottom + table_h
    ax.text(label_gutter - 0.08, table_top - header_h + 0.05, "experiment", ha="right", va="bottom", fontsize=8)

    x = label_gutter
    for col in columns:
        col_w = col.width or scalar_w
        ax.text(x + col_w / 2, table_top - header_h + 0.08, col.short, ha="right", va="bottom", rotation=60, fontsize=6.5)
        x += col_w

    for row_idx, exp in enumerate(experiments):
        y = table_top - header_h - (row_idx + 1) * row_h
        row_status = _status(exp).lower()
        if row_status == "skipped":
            ax.add_patch(Rectangle((0.04, y), width - 0.08, row_h, facecolor="#eeeeee", edgecolor="none", alpha=0.45))
        ax.text(label_gutter - 0.08, y + row_h / 2, _truncate(_exp_label(exp, row_idx), 32), ha="right", va="center", fontsize=7)
        x = label_gutter
        for col_idx, col in enumerate(columns):
            col_w = col.width or scalar_w
            value = row_values[row_idx][col_idx]
            face, rgba = _column_color(col, value, numeric_by_col.get(col.key, []), plt)
            if row_status == "skipped" and col.key != "status":
                face = "#eeeeee"
                rgba = to_rgba(face)
            ax.add_patch(Rectangle((x, y), col_w, row_h, facecolor=face, edgecolor="#bbbbbb", linewidth=0.35))
            color = _text_color_for(rgba) if col.heatmap and _numeric(value) is not None else "black"
            ax.text(x + col_w / 2, y + row_h / 2, row_texts[row_idx][col_idx], ha="center", va="center", fontsize=5.8, color=color)
            x += col_w

    legend_y = legend_h - 0.18
    for idx, col in enumerate(columns, start=1):
        line = f"#{idx} {col.kind} {col.short} — {col.description}"
        ax.text(0.16, legend_y, line, ha="left", va="top", fontsize=6.2)
        legend_y -= 0.18

    data = _png_from_fig(fig)
    plt.close(fig)
    return data


def _resolve_relative(path: str, state_dir: str) -> Optional[str]:
    if not path:
        return None
    candidates = [path] if os.path.isabs(path) else []
    if not os.path.isabs(path):
        candidates.extend(
            [
                os.path.join(_repo_dir(), path),
                os.path.join(state_dir, path),
                os.path.join(state_dir, "..", path),
            ]
        )
    for candidate in candidates:
        norm = os.path.abspath(candidate)
        if os.path.exists(norm):
            return norm
    return os.path.abspath(candidates[0]) if candidates else None


def _dbt_run_path(exp: Dict[str, Any], engine: str, batch: int, state_dir: str) -> Optional[str]:
    files = exp.get("dbt_run_files")
    if isinstance(files, dict):
        engine_files = files.get(engine) or []
        if isinstance(engine_files, str):
            engine_files = [engine_files]
        for raw in engine_files:
            if f"batch{batch}" in os.path.basename(str(raw)):
                return _resolve_relative(str(raw), state_dir)
        if 0 <= batch - 1 < len(engine_files):
            return _resolve_relative(str(engine_files[batch - 1]), state_dir)

    sf = str(_scale_factor(exp))
    fallback = os.path.join("mount", "results", sf, "dbt-server", f"run-{engine}-batch{batch}.json")
    resolved = _resolve_relative(fallback, state_dir)
    if resolved and os.path.exists(resolved):
        return resolved
    alt = os.path.join(state_dir, "results", sf, "dbt-server", f"run-{engine}-batch{batch}.json")
    return alt


def _load_model_times(exp: Dict[str, Any], engine: str, batch: int, state_dir: str) -> Dict[str, Tuple[str, float]]:
    path = _dbt_run_path(exp, engine, batch, state_dir)
    data = _load_json(path or "")
    if not data:
        return {}
    out: Dict[str, Tuple[str, float]] = {}
    for node in data.get("nodes", []):
        if not isinstance(node, dict) or node.get("resource_type") != "model":
            continue
        unique_id = str(node.get("unique_id") or node.get("name") or "")
        if not unique_id:
            continue
        duration = _numeric(node.get("execution_time_s"))
        if duration is None or duration <= 0:
            continue
        out[unique_id] = (str(node.get("name") or unique_id.split(".")[-1]), duration)
    return out


def _exp_has_engines(exp: Dict[str, Any], required: Sequence[str]) -> bool:
    engines = exp.get("engines")
    if isinstance(engines, dict):
        return all(engine in engines for engine in required)
    inputs = exp.get("inputs")
    if isinstance(inputs, dict):
        raw = inputs.get("engines")
        if isinstance(raw, list):
            available = set(str(e) for e in raw)
            return all(engine in available for engine in required)
        if isinstance(raw, str):
            available = set(e.strip() for e in raw.split(",") if e.strip())
            return all(engine in available for engine in required)
    return False


def _ratio_value(exp: Dict[str, Any], batch: int, state_dir: str) -> Tuple[Dict[str, Optional[float]], Dict[str, str]]:
    if _status(exp).lower() == "skipped" or not _exp_has_engines(exp, ("spark", "spark-openivm")):
        return {}, {}
    spark = _load_model_times(exp, "spark", batch, state_dir)
    openivm = _load_model_times(exp, "spark-openivm", batch, state_dir)
    values: Dict[str, Optional[float]] = {}
    names: Dict[str, str] = {}
    for model in set(spark.keys()) | set(openivm.keys()):
        if model in spark:
            names[model] = spark[model][0]
        if model in openivm:
            names.setdefault(model, openivm[model][0])
        if model in spark and model in openivm:
            denom = spark[model][1]
            numer = openivm[model][1]
            values[model] = math.log(numer / denom, 2) if denom > 0 and numer > 0 else None
        else:
            values[model] = None
    return values, names


def generate_oat_per_model_png(oat_run_id: str, state_dir: str = "/data/state") -> bytes:
    """Generate the OAT per-model log2(openivm/spark) heatmap PNG."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import pandas as pd
    from matplotlib.cm import ScalarMappable
    from matplotlib.colors import Normalize, to_rgba
    from matplotlib.patches import Rectangle

    state = _load_oat_state(oat_run_id, state_dir)
    experiments = [exp for exp in _sort_experiments(state) if _status(exp).lower() != "skipped"]
    if not experiments:
        return _empty_png(f"OAT sweep — {oat_run_id[:8]} — no completed experiments")

    batch_values: Dict[int, Dict[str, Dict[str, Optional[float]]]] = {}
    model_names: Dict[str, str] = {}
    model_union: Dict[int, List[str]] = {}
    for batch in _BATCHES:
        per_exp: Dict[str, Dict[str, Optional[float]]] = {}
        models = set()
        for idx, exp in enumerate(experiments):
            key = str(_exp_idx(exp, idx))
            ratios, names = _ratio_value(exp, batch, state_dir)
            per_exp[key] = ratios
            models.update(ratios.keys())
            model_names.update(names)
        batch_values[batch] = per_exp
        model_union[batch] = sorted(models)

    max_rows = max((len(rows) for rows in model_union.values()), default=0)
    if max_rows == 0:
        return _empty_png(f"OAT sweep — {oat_run_id[:8]} — no dbt model timings")

    labels = [f"{_exp_label(exp, i)}\nSF={_scale_factor(exp)}" for i, exp in enumerate(experiments)]
    columns = [str(_exp_idx(exp, i)) for i, exp in enumerate(experiments)]
    _ = pd.DataFrame(columns=columns)

    fig_w = max(8.0, 3.0 + 0.85 * len(experiments))
    subplot_h = max(2.2, 0.22 * max_rows + 1.0)
    fig_h = subplot_h * 3 + 1.25
    fig, axes_raw = plt.subplots(3, 1, figsize=(fig_w, fig_h), dpi=120)
    try:
        axes = list(axes_raw.flat)
    except AttributeError:
        axes = [axes_raw]

    cmap = plt.get_cmap("bwr")
    norm = Normalize(vmin=-3, vmax=3)

    for ax, batch in zip(axes, _BATCHES):
        rows = model_union[batch]
        ax.set_title(f"batch {batch}: log₂(openivm / spark)", fontsize=11, pad=8)
        ax.set_xlim(0, max(1, len(experiments)))
        ax.set_ylim(0, max(1, len(rows)))
        ax.invert_yaxis()
        ax.set_xticks([i + 0.5 for i in range(len(experiments))])
        ax.set_xticklabels(labels, fontsize=7, rotation=45, ha="right")
        ax.set_yticks([i + 0.5 for i in range(len(rows))])
        ax.set_yticklabels([model_names.get(model, model) for model in rows], fontsize=6)
        ax.tick_params(length=0)
        for spine in ax.spines.values():
            spine.set_visible(False)

        for row_idx, model in enumerate(rows):
            for col_idx, exp in enumerate(experiments):
                exp_key = str(_exp_idx(exp, col_idx))
                value = batch_values[batch].get(exp_key, {}).get(model)
                if value is None:
                    face = "#d9d9d9"
                    rgba = to_rgba(face)
                    text = _MISSING
                    color = "black"
                else:
                    clipped = max(-3.0, min(3.0, value))
                    rgba = cmap(norm(clipped))
                    face = rgba
                    text = f"{value:.2f}"
                    color = _text_color_for(rgba)
                ax.add_patch(Rectangle((col_idx, row_idx), 1, 1, facecolor=face, edgecolor="#bbbbbb", linewidth=0.3))
                ax.text(col_idx + 0.5, row_idx + 0.5, text, ha="center", va="center", fontsize=5.5, color=color)

    sm = ScalarMappable(norm=norm, cmap=cmap)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=axes, orientation="horizontal", fraction=0.035, pad=0.07)
    cbar.set_ticks([-3, -2, -1, 0, 1, 2, 3])
    cbar.set_ticklabels(
        ["-3 = 8x faster", "-2 = 4x faster", "-1 = 2x faster", "0 = parity", "1 = 2x slower", "2 = 4x slower", "3 = 8x slower"]
    )
    cbar.ax.tick_params(labelsize=7)
    fig.suptitle(f"OAT per-model sweep — {oat_run_id[:8]}", fontsize=13, fontweight="bold", y=0.995)
    fig.subplots_adjust(left=0.28, right=0.98, top=0.94, bottom=0.12, hspace=0.75)

    data = _png_from_fig(fig)
    plt.close(fig)
    return data


def _md_escape(value: Any) -> str:
    text = _cell_text(value)
    return text.replace("|", "\\|").replace("\n", " ")


def _engine_batch_header(engines: Sequence[str]) -> List[str]:
    return [f"{engine} b{batch}" for engine in engines for batch in _BATCHES]


def _markdown_table(headers: Sequence[str], rows: Sequence[Sequence[Any]]) -> str:
    out = ["| " + " | ".join(_md_escape(h) for h in headers) + " |"]
    out.append("|" + "|".join("---" for _ in headers) + "|")
    for row in rows:
        out.append("| " + " | ".join(_md_escape(v) for v in row) + " |")
    return "\n".join(out)


def _engine_names_for_exp(exp: Dict[str, Any]) -> List[str]:
    engines = exp.get("engines")
    if isinstance(engines, dict):
        return _sort_engines(str(e) for e in engines.keys())
    return _sort_engines(_flat_inputs(exp).get("engines", "").split(","))


def _batch_ratio(exp: Dict[str, Any], batch: int) -> Optional[float]:
    spark = _batch_duration(exp, "spark", batch)
    openivm = _batch_duration(exp, "spark-openivm", batch)
    if spark is None or openivm is None or spark <= 0:
        return None
    return openivm / spark


def _break_even_rows(experiments: Sequence[Dict[str, Any]], state_dir: str) -> List[List[Any]]:
    sorted_exps = sorted(
        [exp for exp in experiments if _status(exp).lower() != "skipped"],
        key=lambda e: (_numeric(_scale_factor(e)) or 0, _exp_idx(e)),
    )
    model_keys = set()
    model_names: Dict[str, str] = {}
    cached: Dict[int, Tuple[Dict[str, Tuple[str, float]], Dict[str, Tuple[str, float]]]] = {}
    for pos, exp in enumerate(sorted_exps):
        spark = _load_model_times(exp, "spark", 2, state_dir)
        openivm = _load_model_times(exp, "spark-openivm", 2, state_dir)
        cached[pos] = (spark, openivm)
        model_keys.update(spark.keys())
        model_keys.update(openivm.keys())
        for key, (name, _) in {**spark, **openivm}.items():
            model_names.setdefault(key, name)

    rows: List[List[Any]] = []
    for model in sorted(model_keys, key=lambda key: model_names.get(key, key)):
        best_sf: Any = _MISSING
        best_ratio: Any = _MISSING
        for pos, exp in enumerate(sorted_exps):
            spark, openivm = cached[pos]
            if model not in spark or model not in openivm:
                continue
            spark_time = spark[model][1]
            openivm_time = openivm[model][1]
            if spark_time > 0 and openivm_time < spark_time:
                best_sf = _scale_factor(exp)
                best_ratio = f"{openivm_time / spark_time:.2f}"
                break
        rows.append([model_names.get(model, model), best_sf, best_ratio])
    return rows


def generate_oat_results_md(oat_run_id: str, state_dir: str = "/data/state") -> str:
    """Generate the OAT RESULTS.md body."""
    state = _load_oat_state(oat_run_id, state_dir)
    experiments = _sort_experiments(state)
    engines = _engine_union(experiments)

    lines: List[str] = [f"# OAT sweep results — {oat_run_id}", ""]
    lines.extend(
        [
            f"* Started:     {_md_escape(state.get('started_at', _MISSING))}",
            f"* Completed:   {_md_escape(state.get('completed_at', _MISSING))}",
            f"* Total wall:  {_format_duration(state.get('total_duration_s'))} ({len(experiments)} experiments)",
            f"* Experiments file: {_md_escape(state.get('experiments_file', _MISSING))}",
            f"* Status:      {_md_escape(state.get('status', _MISSING))}",
            "",
            "## Overview",
            "",
        ]
    )

    overview_rows = []
    for idx, exp in enumerate(experiments):
        overview_rows.append(
            [
                _exp_idx(exp, idx),
                _exp_label(exp, idx),
                _scale_factor(exp),
                ", ".join(_engine_names_for_exp(exp)),
                _status(exp),
                _format_duration(exp.get("wall_clock_s")),
                _status_note(exp),
            ]
        )
    lines.append(
        _markdown_table(
            ["#", "label", "SF", "engines", "status", "wall_clock", "note"],
            overview_rows,
        )
    )
    lines.extend(["", "## Per-engine timing (aggregate)", ""])

    timing_headers = ["#", "label", "SF"] + _engine_batch_header(engines)
    timing_rows = []
    for idx, exp in enumerate(experiments):
        row: List[Any] = [_exp_idx(exp, idx), _exp_label(exp, idx), _scale_factor(exp)]
        for engine in engines:
            for batch in _BATCHES:
                row.append(_format_duration(_batch_duration(exp, engine, batch), compact=True))
        timing_rows.append(row)
    lines.append(_markdown_table(timing_headers, timing_rows))

    lines.extend(["", "## Per-engine storage overhead", ""])
    storage_rows = []
    for idx, exp in enumerate(experiments):
        for engine in _engine_names_for_exp(exp):
            for batch in _BATCHES:
                summary = _storage_summary(exp, engine, batch)
                if not summary:
                    storage_rows.append([
                        _exp_idx(exp, idx),
                        _exp_label(exp, idx),
                        _scale_factor(exp),
                        engine,
                        batch,
                        _MISSING,
                        _MISSING,
                        _MISSING,
                        _MISSING,
                        _MISSING,
                        _MISSING,
                    ])
                    continue
                ratio = _numeric(summary.get("overhead_ratio_internal_to_visible"))
                storage_rows.append([
                    _exp_idx(exp, idx),
                    _exp_label(exp, idx),
                    _scale_factor(exp),
                    engine,
                    batch,
                    summary.get("status", _MISSING),
                    _format_bytes(summary.get("visible_output_bytes")),
                    _format_bytes(summary.get("internal_state_bytes")),
                    _format_bytes(summary.get("metadata_bytes")),
                    _format_bytes(summary.get("source_bytes")),
                    _MISSING if ratio is None else f"{ratio:.2f}",
                ])
    lines.append(_markdown_table(
        [
            "#", "label", "SF", "engine", "batch", "status",
            "visible", "internal", "metadata", "source", "internal/visible",
        ],
        storage_rows,
    ))

    lines.extend(
        [
            "",
            "## Per-engine ratio (openivm / spark)",
            "",
            "For each batch, the ratio `openivm/spark` (lower is better for openivm).",
            "",
        ]
    )
    ratio_rows = []
    for idx, exp in enumerate(experiments):
        row = [_exp_idx(exp, idx), _exp_label(exp, idx), _scale_factor(exp)]
        for batch in _BATCHES:
            ratio = _batch_ratio(exp, batch)
            row.append(_MISSING if ratio is None else f"{ratio:.2f}")
        ratio_rows.append(row)
    lines.append(_markdown_table(["#", "label", "SF", "b1 ratio", "b2 ratio", "b3 ratio"], ratio_rows))

    lines.extend(
        [
            "",
            "## Per-model break-even (batch 2)",
            "",
            "For each dbt model, the SMALLEST SF (in the sweep) at which spark-openivm < spark for batch 2. If never, \"—\".",
            "",
        ]
    )
    lines.append(_markdown_table(["model", "break-even SF", "first openivm/spark ratio"], _break_even_rows(experiments, state_dir)))

    lines.extend(["", "## Inputs legend", ""])
    if experiments:
        first = _flat_inputs(experiments[0])
        input_rows = [[key, first.get(key, _MISSING)] for key in ExperimentInputs.column_headers().keys()]
    else:
        input_rows = []
    lines.append(_markdown_table(["key", "value (baseline)"], input_rows))
    lines.append("")
    return "\n".join(lines)


@bp.route("/benchmark/oat-chart.png", methods=["GET"])
def oat_chart_png() -> Response:
    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        return jsonify({"error": "run_id query parameter is required"}), 400
    kind = request.args.get("kind", "aggregate").strip().lower()
    try:
        if kind == "aggregate":
            png = generate_oat_aggregate_png(run_id)
        elif kind == "per-model":
            png = generate_oat_per_model_png(run_id)
        else:
            return jsonify({"error": "kind must be aggregate or per-model"}), 400
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    return Response(png, mimetype="image/png")


@bp.route("/benchmark/oat-results.md", methods=["GET"])
def oat_results_md() -> Response:
    run_id = request.args.get("run_id", "").strip()
    if not run_id:
        return jsonify({"error": "run_id query parameter is required"}), 400
    try:
        md = generate_oat_results_md(run_id)
    except FileNotFoundError as exc:
        return jsonify({"error": str(exc)}), 404
    return Response(md, mimetype="text/markdown; charset=utf-8")


class OatChartHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
