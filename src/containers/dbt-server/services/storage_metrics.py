"""Per-engine durable storage-footprint collection.

Storage is captured after a benchmark batch, outside its timed window.  A
collector never reports ``ok`` after silently losing part of the footprint:
missing roots, failed relation probes, stat failures, and deadlines are
returned as structured errors.
"""

from __future__ import annotations

import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


CATEGORIES = ("visible_output", "internal_state", "metadata", "source")
LOCAL_ENGINES = {"spark", "spark-openivm", "feldera", "databricks-enzyme"}
DUCKLAKE_ENGINES = {"duckdb", "duckdb-openivm"}
FABRIC_ENGINES = {"fabric-jvm-35", "fabric-openivm-jvm-35"}
SUPPORTED_ENGINES = LOCAL_ENGINES | DUCKLAKE_ENGINES | FABRIC_ENGINES

PROCESSED_ROOTS = {
    "spark": Path("/data/processed/spark"),
    "spark-openivm": Path("/data/processed/spark-openivm"),
    "duckdb": Path("/data/processed/duckdb"),
    "duckdb-openivm": Path("/data/processed/duckdb-openivm"),
    "feldera": Path("/data/processed/feldera"),
    "databricks-enzyme": Path("/data/processed/databricks-enzyme"),
}
RAW_DELTA_ROOT = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
RAW_BACKED_ENGINES = {"spark", "feldera"}


def _empty_totals() -> Dict[str, int]:
    return {f"{category}_bytes": 0 for category in CATEGORIES} | {
        "total_bytes": 0,
        "file_count": 0,
    }


def _safe_int(value: Any) -> int:
    try:
        if value is None or value != value:
            return 0
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return 0


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


def _merge_totals(left: Dict[str, int], right: Dict[str, int]) -> None:
    for key, value in right.items():
        left[key] = left.get(key, 0) + int(value or 0)


def _ratio(internal: int, visible: int) -> Optional[float]:
    return internal / visible if visible > 0 else None


def _collect_raw_base_reference(
    root: Path, *, deadline: float
) -> Tuple[Optional[int], int, List[str]]:
    """Measure the current logical input footprint without future batches.

    Only ``batch1``, the mutable ``staging`` snapshot, and ``audit`` belong to
    the current base tables.  The sibling ``batch2``/``batch3`` directories
    contain future inputs and must never leak into an earlier sample.  Delta
    logs and hidden sidecars are excluded so this remains comparable to the
    collectors' ``source_bytes`` data-file total.
    """
    if not root.is_dir():
        return None, 0, [f"raw base-table root does not exist: {root}"]

    sections = [root / name for name in ("batch1", "staging", "audit")]
    stack = [path for path in sections if path.is_dir()]
    if not stack:
        return None, 0, [f"raw base-table sections do not exist under: {root}"]

    total_bytes = 0
    file_count = 0
    errors: List[str] = []
    while stack:
        expired = _deadline_error(deadline)
        if expired:
            errors.append(expired)
            break
        current = stack.pop()
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            errors.append(f"cannot scan raw base-table path {current}: {exc}")
            continue
        for entry in entries:
            if entry.name.startswith((".", "_")):
                continue
            try:
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                total_bytes += entry.stat(follow_symlinks=False).st_size
                file_count += 1
            except OSError as exc:
                errors.append(f"cannot stat raw base-table path {entry.path}: {exc}")
    return total_bytes, file_count, errors


def _base_table_summary(
    engine: str, totals: Dict[str, int], *, deadline: float
) -> Dict[str, Any]:
    """Return a base-table footprint and a raw-data comparison baseline.

    ``source_bytes`` is engine-owned managed storage.  Spark and Feldera read
    the shared raw tables directly, so their actual base-table footprint is
    the raw reference instead.  The difference is a storage-overhead proxy:
    same-engine baseline/OpenIVM pairs isolate metadata/state additions best,
    while cross-format comparisons can also include encoding and file layout.
    """
    reference_bytes, reference_files, reference_errors = _collect_raw_base_reference(
        RAW_DELTA_ROOT, deadline=deadline
    )
    managed_bytes = totals["source_bytes"]
    source_mode = "shared_raw" if engine in RAW_BACKED_ENGINES else "managed"
    storage_bytes = reference_bytes if source_mode == "shared_raw" else managed_bytes
    overhead_bytes: Optional[int] = None
    overhead_ratio: Optional[float] = None
    if storage_bytes is not None and reference_bytes is not None:
        overhead_bytes = storage_bytes - reference_bytes
        if reference_bytes > 0:
            overhead_ratio = overhead_bytes / reference_bytes
    return {
        "source_mode": source_mode,
        "storage_bytes": storage_bytes,
        "managed_source_bytes": managed_bytes,
        "reference_bytes": reference_bytes,
        "reference_file_count": reference_files,
        "reference_format": "delta_data_files",
        "overhead_bytes_vs_raw": overhead_bytes,
        "overhead_ratio_vs_raw": overhead_ratio,
        "reference_status": "ok" if not reference_errors else "partial",
        **({"reference_errors": reference_errors} if reference_errors else {}),
    }


def _deadline_error(deadline: float) -> Optional[str]:
    if time.monotonic() >= deadline:
        return "storage collection deadline exceeded"
    return None


def _classify_processed(engine: str, rel: str, *, is_delta_table: bool = False) -> str:
    """Classify a file relative to an engine-owned processed root."""
    del is_delta_table  # retained for compatibility with older callers
    lowered = rel.lower().replace("\\", "/").strip("/")
    parts = lowered.split("/") if lowered else []
    first = parts[0] if parts else ""

    metadata_parts = {
        "query-log",
        "query_plan",
        "query-plan",
        "profile",
        "profiles",
        "lineage",
    }
    if "_delta_log" in parts or metadata_parts.intersection(parts):
        return "metadata"
    if first == "sources":
        return "source"

    if engine == "spark-openivm":
        if len(parts) >= 2 and parts[0] == "_ivm" and parts[1] == "views":
            return "visible_output"
        if first in {"_ivm", "_openivm", "_tmp", "tmp"}:
            return "internal_state"
    if engine == "databricks-enzyme" and first in {"query-plan", "pipeline-events"}:
        return "metadata"
    if first in {"_tmp", "tmp"}:
        return "internal_state"
    return "visible_output"


def _collect_local_root(
    engine: str,
    root: Path,
    root_category: Optional[str] = None,
    *,
    deadline: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    """Walk ``root`` once and aggregate files by owning table/category."""
    deadline = deadline if deadline is not None else time.monotonic() + 90
    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []
    if not root.is_dir():
        return items, totals, [f"storage root does not exist: {root}"]
    expired = _deadline_error(deadline)
    if expired:
        return items, totals, [expired]

    grouped: Dict[Tuple[str, str, str, str], List[int]] = {}
    stack: List[Tuple[Path, Path, Optional[Path]]] = [(root, Path(), None)]
    while stack:
        expired = _deadline_error(deadline)
        if expired:
            errors.append(expired)
            break
        current, rel_dir, parent_delta = stack.pop()
        try:
            entries = list(os.scandir(current))
        except OSError as exc:
            errors.append(f"cannot scan {current}: {exc}")
            continue
        try:
            dir_names = {
                entry.name for entry in entries if entry.is_dir(follow_symlinks=False)
            }
        except OSError as exc:
            errors.append(f"cannot inspect directory entries in {current}: {exc}")
            dir_names = set()
        active_delta = rel_dir if "_delta_log" in dir_names else parent_delta
        for entry in entries:
            rel = rel_dir / entry.name
            try:
                if entry.is_dir(follow_symlinks=False):
                    stack.append((Path(entry.path), rel, active_delta))
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
                size = entry.stat(follow_symlinks=False).st_size
            except OSError as exc:
                errors.append(f"cannot stat {entry.path}: {exc}")
                continue

            rel_text = rel.as_posix()
            rel_parts = rel.parts
            in_delta_log = "_delta_log" in rel_parts
            if in_delta_log:
                category = "metadata"
                log_idx = rel_parts.index("_delta_log")
                name = "/".join(rel_parts[: log_idx + 1])
                kind = "delta_log"
            else:
                category = root_category or _classify_processed(engine, rel_text)
                owner = active_delta or (Path(rel_parts[0]) if rel_parts else Path("."))
                name = owner.as_posix()
                kind = "delta_data" if active_delta is not None else "files"
            owner_path = str(root / name)
            key = (category, name, kind, owner_path)
            aggregate = grouped.setdefault(key, [0, 0])
            aggregate[0] += int(size)
            aggregate[1] += 1

    for (category, name, kind, path), (bytes_, file_count) in sorted(grouped.items()):
        _add_item(
            items,
            totals,
            engine=engine,
            name=name,
            category=category,
            bytes_=bytes_,
            file_count=file_count,
            path=path,
            kind=kind,
        )
    return items, totals, errors


def _ducklake_category(engine: str, schema: str, table: str) -> str:
    schema_lower = schema.lower()
    table_lower = table.lower()
    if (
        schema_lower == "tpcdi"
        or table_lower == "audit"
        or table_lower.startswith(("batch1_", "staging_"))
    ):
        return "source"
    if engine == "duckdb-openivm" and table_lower.startswith("openivm_data_"):
        return "visible_output"
    if engine == "duckdb-openivm" and table_lower.startswith("openivm_"):
        return "internal_state"
    return "visible_output"


def _relative_path(base: Path, value: Any, is_relative: Any) -> Path:
    path = Path(str(value or ""))
    return base / path if bool(is_relative) else path


def _collect_ducklake_storage(
    engine: str,
    root: Path,
    *,
    deadline: Optional[float] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    """Use the DuckLake catalog to attribute physical files to relations."""
    deadline = deadline if deadline is not None else time.monotonic() + 90
    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []
    if not root.is_dir():
        return items, totals, [f"storage root does not exist: {root}"]
    catalog_name = "openivm.ducklake" if engine == "duckdb-openivm" else "metadata.db"
    catalog = root / catalog_name
    if not catalog.is_file():
        return items, totals, [f"DuckLake catalog does not exist: {catalog}"]

    mapped: Set[Path] = set()
    relation_totals: Dict[Tuple[str, str, str], List[int]] = {}
    inline_catalog_bytes = 0
    sql = """
        SELECT s.schema_name, s.path, s.path_is_relative,
               t.table_name, t.path, t.path_is_relative,
               f.path, f.path_is_relative, f.file_size_bytes, f.file_kind
        FROM ducklake_schema s
        JOIN ducklake_table t USING (schema_id)
        JOIN (
            SELECT table_id, path, path_is_relative, file_size_bytes, 'data' AS file_kind
            FROM ducklake_data_file
            UNION ALL
            SELECT table_id, path, path_is_relative, file_size_bytes, 'delete' AS file_kind
            FROM ducklake_delete_file
        ) f USING (table_id)
    """
    try:
        connection = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
        try:
            connection.set_progress_handler(
                lambda: 1 if time.monotonic() >= deadline else 0, 1000
            )
            rows = connection.execute(sql).fetchall()
            table_lookup = {
                int(table_id): (str(schema), str(table))
                for table_id, schema, table in connection.execute(
                    "SELECT t.table_id, s.schema_name, t.table_name "
                    "FROM ducklake_table t JOIN ducklake_schema s USING (schema_id)"
                ).fetchall()
            }
            try:
                inline_tables = connection.execute(
                    "SELECT table_id, table_name FROM ducklake_inlined_data_tables"
                ).fetchall()
                page_bytes = {
                    str(name): int(bytes_ or 0)
                    for name, bytes_ in connection.execute(
                        "SELECT name, SUM(pgsize) FROM dbstat GROUP BY name"
                    ).fetchall()
                }
            except sqlite3.Error:
                # Older DuckLake catalogs may not support data inlining, and
                # some SQLite builds omit the dbstat virtual table.
                inline_tables, page_bytes = [], {}
        finally:
            connection.close()
    except (OSError, sqlite3.Error) as exc:
        return items, totals, [f"cannot read DuckLake catalog {catalog}: {exc}"]

    for table_id, inline_name in inline_tables:
        relation = table_lookup.get(int(table_id))
        if relation is None:
            errors.append(f"unmapped DuckLake inline table: {inline_name}")
            continue
        schema, table = relation
        bytes_ = page_bytes.get(str(inline_name), 0)
        if bytes_ <= 0:
            errors.append(f"cannot measure DuckLake inline table: {schema}.{table}")
            continue
        category = _ducklake_category(engine, schema, table)
        aggregate = relation_totals.setdefault((schema, table, category), [0, 0])
        aggregate[0] += bytes_
        inline_catalog_bytes += bytes_

    for row in rows:
        expired = _deadline_error(deadline)
        if expired:
            errors.append(expired)
            break
        (
            schema,
            schema_path,
            schema_relative,
            table,
            table_path,
            table_relative,
            file_path,
            file_relative,
            _,
            _,
        ) = row
        schema_dir = _relative_path(root / "data", schema_path, schema_relative)
        table_dir = _relative_path(schema_dir, table_path, table_relative)
        physical = _relative_path(table_dir, file_path, file_relative).resolve()
        if not physical.is_file():
            # DuckLake can retain historical file records after compaction.
            continue
        try:
            size = physical.stat().st_size
        except OSError as exc:
            errors.append(f"cannot stat DuckLake file {physical}: {exc}")
            continue
        mapped.add(physical)
        category = _ducklake_category(engine, str(schema), str(table))
        key = (str(schema), str(table), category)
        aggregate = relation_totals.setdefault(key, [0, 0])
        aggregate[0] += int(size)
        aggregate[1] += 1

    for (schema, table, category), (bytes_, file_count) in sorted(relation_totals.items()):
        _add_item(
            items,
            totals,
            engine=engine,
            name=f"{schema}.{table}",
            category=category,
            bytes_=bytes_,
            file_count=file_count,
            path=f"ducklake://{schema}/{table}",
            kind="ducklake_relation",
        )

    known_metadata = {catalog.resolve()}
    main_db = root / ("openivm.duckdb" if engine == "duckdb-openivm" else "duckdb.duckdb")
    if main_db.is_file():
        known_metadata.add(main_db.resolve())
    extras: Dict[Tuple[str, str], List[int]] = {}
    for current, _, names in os.walk(root):
        expired = _deadline_error(deadline)
        if expired:
            if expired not in errors:
                errors.append(expired)
            break
        for name in names:
            physical = (Path(current) / name).resolve()
            if physical in mapped:
                continue
            try:
                size = physical.stat().st_size
            except OSError as exc:
                errors.append(f"cannot stat {physical}: {exc}")
                continue
            if physical in known_metadata:
                category, kind = "metadata", "ducklake_catalog"
                if physical == catalog.resolve():
                    size = max(0, size - inline_catalog_bytes)
            elif physical.is_relative_to((root / "data").resolve()):
                category, kind = "internal_state", "unmapped_ducklake_file"
                errors.append(f"unmapped DuckLake data file: {physical}")
            else:
                rel = physical.relative_to(root.resolve()).as_posix()
                category = "internal_state" if rel.startswith("_tmp/") else "metadata"
                kind = "files"
            key = (category, kind)
            aggregate = extras.setdefault(key, [0, 0])
            aggregate[0] += int(size)
            aggregate[1] += 1

    for (category, kind), (bytes_, file_count) in sorted(extras.items()):
        _add_item(
            items,
            totals,
            engine=engine,
            name=kind,
            category=category,
            bytes_=bytes_,
            file_count=file_count,
            path=str(root),
            kind=kind,
        )
    return items, totals, errors


def _databricks_category(src: Any, schema: str, name: str) -> str:
    if name.startswith("event_log_"):
        return "metadata"
    if schema == src.data_schema():
        return "source"
    if schema == src.work_schema():
        return "internal_state"
    return "visible_output"


def _databricks_relation_storage(
    *, deadline: Optional[float] = None
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    from services import databricks_enzyme_metrics as metrics
    from services import databricks_enzyme_sources as src

    deadline = deadline if deadline is not None else time.monotonic() + 90
    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []
    relations = metrics.list_relations(deadline=deadline)
    physical = [
        rel
        for rel in relations
        if "VIEW" not in str(rel.get("table_type", "")).upper()
    ]
    configured_workers = _safe_int(os.environ.get("STORAGE_DATABRICKS_WORKERS", "8"))
    workers = max(1, min(configured_workers, len(physical) or 1))
    executor = ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="storage-databricks"
    )
    futures = {
        executor.submit(metrics.describe_storage, rel, deadline=deadline): rel
        for rel in physical
    }
    try:
        timeout = max(0.001, deadline - time.monotonic())
        try:
            completed = as_completed(futures, timeout=timeout)
            for future in completed:
                rel = futures[future]
                schema, name = str(rel["schema"]), str(rel["name"])
                table_type = str(rel.get("table_type", "") or "").upper()
                try:
                    frame = future.result()
                    if frame is None or frame.empty:
                        raise RuntimeError("DESCRIBE DETAIL returned no rows")
                    row = frame.iloc[0]
                    size_bytes = _safe_int(
                        row.get("sizeInBytes", row.get("size_in_bytes", 0))
                    )
                    file_count = _safe_int(
                        row.get("numFiles", row.get("num_files", 0))
                    )
                    _add_item(
                        items,
                        totals,
                        engine="databricks-enzyme",
                        name=f"{schema}.{name}",
                        category=_databricks_category(src, schema, name),
                        bytes_=size_bytes,
                        file_count=file_count,
                        path=str(
                            row.get("location") or f"{src.CATALOG}.{schema}.{name}"
                        ),
                        kind="databricks_relation",
                        details={
                            "table_type": table_type,
                            "format": str(row.get("format") or ""),
                        },
                    )
                except Exception as exc:
                    errors.append(
                        f"Databricks storage probe failed for {schema}.{name}: {exc}"
                    )
        except TimeoutError:
            pass
        for future, rel in futures.items():
            if not future.done():
                future.cancel()
                errors.append(
                    "Databricks storage probe deadline exceeded for "
                    f"{rel['schema']}.{rel['name']}"
                )
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return items, totals, errors


def _classify_fabric_path(engine: str, rel: str) -> str:
    lowered = rel.lower().replace("\\", "/").strip("/")
    parts = lowered.split("/")
    if "_delta_log" in parts:
        return "metadata"
    if lowered.startswith("files/_openivm/"):
        return "internal_state"
    if lowered.startswith("files/_ivm-warehouse/"):
        warehouse_rel = lowered.split("files/_ivm-warehouse/", 1)[1]
        if warehouse_rel.startswith("_ivm/views/"):
            return "visible_output"
        return "internal_state"
    if lowered.startswith("tables/"):
        relation_parts = parts[1:]
        if relation_parts and relation_parts[0] == "tpcdi":
            return "source"
        relation = relation_parts[0] if relation_parts else ""
        if relation == "audit" or relation.startswith(("batch1_", "staging_")):
            return "source"
        return "visible_output"
    return "metadata" if engine in FABRIC_ENGINES else "visible_output"


def _fabric_storage(
    engine: str, *, deadline: float
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    from services import fabric

    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []
    grouped: Dict[Tuple[str, str], List[int]] = {}
    try:
        paths = fabric.list_storage_paths(deadline=deadline)
    except Exception as exc:
        return items, totals, [f"Fabric storage listing failed: {exc}"]
    for record in paths:
        rel = str(record.get("path") or record.get("name") or "")
        if not rel:
            continue
        category = _classify_fabric_path(engine, rel)
        kind = "delta_log" if "/_delta_log/" in f"/{rel.lower()}/" else "onelake_file"
        aggregate = grouped.setdefault((category, kind), [0, 0])
        aggregate[0] += _safe_int(record.get("bytes", record.get("contentLength", 0)))
        aggregate[1] += 1
    for (category, kind), (bytes_, file_count) in sorted(grouped.items()):
        _add_item(
            items,
            totals,
            engine=engine,
            name=kind,
            category=category,
            bytes_=bytes_,
            file_count=file_count,
            path="onelake://compute-lakehouse",
            kind=kind,
        )
    return items, totals, errors


def collect_storage_metrics(engine: str, batch_num: Optional[int] = None) -> Dict[str, Any]:
    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []
    if engine not in SUPPORTED_ENGINES:
        return {
            "status": "unsupported",
            "engine": engine,
            "batch_num": batch_num,
            "error": f"unsupported engine: {engine}",
            "totals": totals,
            "items": items,
        }

    timeout_s = max(
        1, _safe_int(os.environ.get("STORAGE_COLLECTION_TIMEOUT_S", "90"))
    )
    deadline = time.monotonic() + timeout_s
    if engine in DUCKLAKE_ENGINES:
        local_items, local_totals, local_errors = _collect_ducklake_storage(
            engine, PROCESSED_ROOTS[engine], deadline=deadline
        )
    elif engine in FABRIC_ENGINES:
        local_items, local_totals, local_errors = _fabric_storage(
            engine, deadline=deadline
        )
    else:
        local_items, local_totals, local_errors = _collect_local_root(
            engine, PROCESSED_ROOTS[engine], deadline=deadline
        )
    items.extend(local_items)
    _merge_totals(totals, local_totals)
    errors.extend(local_errors)

    if engine == "databricks-enzyme" and time.monotonic() < deadline:
        try:
            remote_items, remote_totals, remote_errors = (
                _databricks_relation_storage(deadline=deadline)
            )
            items.extend(remote_items)
            _merge_totals(totals, remote_totals)
            errors.extend(remote_errors)
        except Exception as exc:
            errors.append(f"Databricks remote storage unavailable: {exc}")

    visible = totals["visible_output_bytes"]
    internal = totals["internal_state_bytes"]
    base_tables = _base_table_summary(engine, totals, deadline=deadline)
    if engine in RAW_BACKED_ENGINES and base_tables["storage_bytes"] is None:
        errors.extend(
            base_tables.get(
                "reference_errors", ["raw base-table footprint is unavailable"]
            )
        )
    if visible == 0:
        errors.append("storage collector found no visible materialized output")
    if totals["total_bytes"] == 0:
        errors.append("storage collector found no engine-owned durable files")
    status = "ok"
    if errors:
        status = "partial" if totals["total_bytes"] > 0 else "error"
    result: Dict[str, Any] = {
        "status": status,
        "engine": engine,
        "batch_num": batch_num,
        "deadline_seconds": timeout_s,
        "totals": totals,
        "overhead_ratio_internal_to_visible": _ratio(internal, visible),
        "base_tables": base_tables,
        "items": sorted(
            items,
            key=lambda item: (item["category"], item["name"], item["kind"]),
        ),
    }
    if engine in PROCESSED_ROOTS:
        result["roots"] = {"processed": str(PROCESSED_ROOTS[engine])}
    result.setdefault("roots", {})["raw_base_reference"] = str(RAW_DELTA_ROOT)
    if errors:
        result["errors"] = errors
    return result
