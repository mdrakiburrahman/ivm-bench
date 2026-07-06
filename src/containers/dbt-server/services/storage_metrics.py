"""Per-engine storage footprint collection.

The benchmark-server calls this outside the timed batch window. Metrics are
best-effort: collectors return structured errors instead of raising whenever a
particular engine cannot expose exact table sizes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set


CATEGORIES = ("visible_output", "internal_state", "metadata", "source")

PROCESSED_ROOTS = {
    "spark": Path("/data/processed/spark"),
    "spark-openivm": Path("/data/processed/spark-openivm"),
    "duckdb": Path("/data/processed/duckdb"),
    "duckdb-openivm": Path("/data/processed/duckdb-openivm"),
    "feldera": Path("/data/processed/feldera"),
    "databricks-enzyme": Path("/data/processed/databricks-enzyme"),
}

SOURCE_ROOT = Path(os.environ.get("DELTA_BASE_DIR", "/data/raw/delta"))


def _empty_totals() -> Dict[str, int]:
    return {f"{category}_bytes": 0 for category in CATEGORIES} | {
        "total_bytes": 0,
        "file_count": 0,
    }


def _safe_rel(path: Path, root: Path) -> str:
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def _is_under(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _safe_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        # pandas NaN is the one common non-None missing value here.
        if value != value:
            return 0
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 0


def _iter_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return []
    return (p for p in root.rglob("*") if p.is_file())


def _dir_stats(root: Path, *, exclude_parts: Optional[Set[str]] = None) -> tuple[int, int]:
    exclude_parts = exclude_parts or set()
    size = 0
    count = 0
    for path in _iter_files(root):
        if exclude_parts.intersection(path.relative_to(root).parts):
            continue
        size += _file_size(path)
        count += 1
    return size, count


def _find_delta_tables(root: Path) -> List[Path]:
    tables: List[Path] = []
    if not root.exists():
        return tables
    for path, dirs, _ in os.walk(root):
        current = Path(path)
        if "_delta_log" in dirs:
            tables.append(current)
            dirs[:] = []
    return sorted(tables)


def _is_openivm_path(rel: str) -> bool:
    lowered = rel.lower()
    parts = lowered.split("/")
    return any(
        part.startswith("openivm_")
        or part == "_openivm"
        or part == "_ivm"
        or part.startswith("_ivm")
        or "rocksdb" in part
        for part in parts
    )


def _classify_processed(engine: str, rel: str, *, is_delta_table: bool) -> str:
    lowered = rel.lower()
    first = lowered.split("/", 1)[0]

    if engine == "spark-openivm" and _is_openivm_path(lowered):
        return "internal_state"
    if engine == "duckdb-openivm":
        if (
            _is_openivm_path(lowered)
            or first in {"openivm.duckdb", "openivm.ducklake", "_tmp"}
            or lowered.endswith(".ducklake")
        ):
            return "internal_state"
    if engine == "duckdb":
        if first in {"metadata.db", "_tmp"} or lowered.endswith(".db"):
            return "metadata"
    if engine == "databricks-enzyme":
        if first in {"query-plan", "pipeline-events"}:
            return "metadata"
    if "query-log" in lowered or "profile" in lowered or "lineage" in lowered:
        return "metadata"
    if first in {"_tmp", "tmp"}:
        return "internal_state"
    return "visible_output" if is_delta_table or engine in PROCESSED_ROOTS else "metadata"


def _add_item(
    items: List[Dict[str, Any]],
    totals: Dict[str, int],
    *,
    engine: str,
    name: str,
    category: str,
    bytes_: int,
    file_count: int,
    path: str,
    kind: str,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    category = category if category in CATEGORIES else "metadata"
    item = {
        "engine": engine,
        "name": name,
        "category": category,
        "bytes": int(bytes_),
        "file_count": int(file_count),
        "path": path,
        "kind": kind,
    }
    if details:
        item.update(details)
    items.append(item)
    totals[f"{category}_bytes"] += int(bytes_)
    totals["total_bytes"] += int(bytes_)
    totals["file_count"] += int(file_count)


def _collect_local_root(engine: str, root: Path, root_category: str) -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    if not root.exists():
        return items, totals

    covered: List[Path] = []
    for table in _find_delta_tables(root):
        rel = _safe_rel(table, root)
        data_bytes, data_files = _dir_stats(table, exclude_parts={"_delta_log"})
        log_bytes, log_files = _dir_stats(table / "_delta_log")
        table_category = root_category if root_category == "source" else _classify_processed(
            engine, rel, is_delta_table=True
        )
        if data_bytes or data_files:
            _add_item(
                items,
                totals,
                engine=engine,
                name=rel,
                category=table_category,
                bytes_=data_bytes,
                file_count=data_files,
                path=str(table),
                kind="delta_data",
            )
        if log_bytes or log_files:
            _add_item(
                items,
                totals,
                engine=engine,
                name=f"{rel}/_delta_log",
                category="metadata" if root_category != "source" else "source",
                bytes_=log_bytes,
                file_count=log_files,
                path=str(table / "_delta_log"),
                kind="delta_log",
            )
        covered.append(table)

    grouped: Dict[str, tuple[int, int, str, str]] = {}
    for file_path in _iter_files(root):
        if any(_is_under(file_path, table) for table in covered):
            continue
        rel = _safe_rel(file_path, root)
        group = rel.split("/", 1)[0]
        category = root_category if root_category == "source" else _classify_processed(
            engine, rel, is_delta_table=False
        )
        size, count, old_category, path = grouped.get(group, (0, 0, category, str(root / group)))
        grouped[group] = (size + _file_size(file_path), count + 1, old_category, path)

    for group, (size, count, category, path) in sorted(grouped.items()):
        _add_item(
            items,
            totals,
            engine=engine,
            name=group,
            category=category,
            bytes_=size,
            file_count=count,
            path=path,
            kind="files",
        )
    return items, totals


def _merge_totals(left: Dict[str, int], right: Dict[str, int]) -> Dict[str, int]:
    for key, value in right.items():
        left[key] = left.get(key, 0) + int(value or 0)
    return left


def _ratio(internal: int, visible: int) -> Optional[float]:
    if visible <= 0:
        return None
    return internal / visible


def _databricks_relation_storage() -> tuple[List[Dict[str, Any]], Dict[str, int]]:
    from services import databricks_enzyme_sources as src
    from services import databricks_enzyme_metrics as metrics

    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    for rel in metrics._list_relations():
        schema = rel["schema"]
        name = rel["name"]
        fqn = f"`{src.CATALOG}`.`{schema}`.`{name}`"
        try:
            df = src._execute(f"DESCRIBE DETAIL {fqn}")
        except Exception as exc:
            _add_item(
                items,
                totals,
                engine="databricks-enzyme",
                name=f"{schema}.{name}",
                category="metadata",
                bytes_=0,
                file_count=0,
                path=fqn,
                kind="databricks_relation",
                details={"error": str(exc)},
            )
            continue
        if df is None or df.empty:
            continue
        row = df.iloc[0]
        size_bytes = _safe_int(row.get("sizeInBytes", row.get("size_in_bytes", 0)))
        file_count = _safe_int(row.get("numFiles", row.get("num_files", 0)))
        if schema == src.data_schema():
            category = "source"
        elif schema == src.work_schema():
            category = "internal_state"
        else:
            category = "visible_output"
        _add_item(
            items,
            totals,
            engine="databricks-enzyme",
            name=f"{schema}.{name}",
            category=category,
            bytes_=size_bytes,
            file_count=file_count,
            path=str(row.get("location") or fqn),
            kind="databricks_relation",
            details={
                "table_type": rel.get("table_type", ""),
                "format": str(row.get("format") or ""),
            },
        )
    return items, totals


def collect_storage_metrics(engine: str, batch_num: Optional[int] = None) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []

    processed_root = PROCESSED_ROOTS.get(engine)
    if processed_root is None:
        return {
            "status": "unsupported",
            "engine": engine,
            "batch_num": batch_num,
            "error": f"unsupported engine: {engine}",
            "totals": totals,
            "items": items,
        }

    local_items, local_totals = _collect_local_root(engine, processed_root, "visible_output")
    items.extend(local_items)
    _merge_totals(totals, local_totals)

    if engine == "databricks-enzyme":
        try:
            remote_items, remote_totals = _databricks_relation_storage()
            items.extend(remote_items)
            _merge_totals(totals, remote_totals)
        except Exception as exc:
            errors.append(f"databricks remote storage unavailable: {exc}")

    source_items, source_totals = _collect_local_root(engine, SOURCE_ROOT, "source")
    items.extend(source_items)
    _merge_totals(totals, source_totals)

    visible = totals.get("visible_output_bytes", 0)
    internal = totals.get("internal_state_bytes", 0)
    result = {
        "status": "ok" if not errors else "partial",
        "engine": engine,
        "batch_num": batch_num,
        "roots": {
            "processed": str(processed_root),
            "source": str(SOURCE_ROOT),
        },
        "totals": totals,
        "overhead_ratio_internal_to_visible": _ratio(internal, visible),
        "items": sorted(items, key=lambda x: (x["category"], x["name"], x["kind"])),
    }
    if errors:
        result["errors"] = errors
    return result
