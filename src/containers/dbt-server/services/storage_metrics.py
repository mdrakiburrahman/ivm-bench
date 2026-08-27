"""Per-engine durable storage-footprint collection.

Storage is captured after a benchmark batch, outside its timed window.  A
collector never reports ``ok`` after silently losing part of the footprint:
missing roots, failed relation probes, stat failures, and deadlines are
returned as structured errors.
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


CATEGORIES = ("visible_output", "helper_data", "metadata", "source")
LOCAL_ENGINES = {"spark", "spark-openivm", "feldera", "databricks-enzyme"}
# RisingWave is measured through its own catalog rather than by walking a
# directory, so it is its own case rather than part of LOCAL_ENGINES.
RISINGWAVE_ENGINES = {"risingwave"}
DUCKLAKE_ENGINES = {"duckdb", "duckdb-openivm"}
FABRIC_ENGINES = {"fabric-jvm-35", "fabric-openivm-jvm-35"}
SUPPORTED_ENGINES = LOCAL_ENGINES | DUCKLAKE_ENGINES | FABRIC_ENGINES | RISINGWAVE_ENGINES

PROCESSED_ROOTS = {
    "spark": Path("/data/processed/spark"),
    "spark-openivm": Path("/data/processed/spark-openivm"),
    "duckdb": Path("/data/processed/duckdb"),
    "duckdb-openivm": Path("/data/processed/duckdb-openivm"),
    "feldera": Path("/data/processed/feldera"),
    "databricks-enzyme": Path("/data/processed/databricks-enzyme"),
    "risingwave": Path("/data/processed/risingwave"),
}
RAW_DELTA_ROOT = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
RAW_BACKED_ENGINES = {"spark", "feldera"}
RAW_REFERENCE_ENGINES = RAW_BACKED_ENGINES | {"databricks-enzyme"}
DATABRICKS_BACKING_TABLE = re.compile(
    r"^__materialization_mat_"
    r"[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}"
    r"_(?P<view_name>.+)_[0-9]+$",
    re.IGNORECASE,
)


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
    if engine in RAW_REFERENCE_ENGINES:
        reference_bytes, reference_files, reference_errors = (
            _collect_raw_base_reference(RAW_DELTA_ROOT, deadline=deadline)
        )
    else:
        # Managed engines are compared with a same-batch vanilla engine in the
        # consolidated CSV.  Their raw mount is not necessarily the source of
        # the managed tables (and can belong to another serial engine), so a
        # raw comparison here would be misleading.
        reference_bytes, reference_files, reference_errors = None, 0, []
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
        "reference_status": (
            "not_applicable"
            if engine not in RAW_REFERENCE_ENGINES
            else "ok" if not reference_errors else "partial"
        ),
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
            return "helper_data"
    if engine == "databricks-enzyme" and first in {"query-plan", "pipeline-events"}:
        return "metadata"
    if first in {"_tmp", "tmp"}:
        return "helper_data"
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


def _collect_physical_root(
    root: Path, *, deadline: float
) -> Tuple[Dict[str, Any], List[str]]:
    """Return actual allocated blocks and apparent bytes beneath ``root``.

    Unlike RisingWave's logical ``rw_table_stats``, allocated blocks measure
    the host volume consumption that can make a benchmark run out of disk.
    Directory blocks are included to match ``du``.
    """
    summary: Dict[str, Any] = {
        "path": str(root),
        "allocated_bytes": 0,
        "measurement": "filesystem_st_blocks",
    }
    if not root.is_dir():
        return summary, [f"physical storage root does not exist: {root}"]

    errors: List[str] = []
    stack = [root]
    while stack:
        expired = _deadline_error(deadline)
        if expired:
            errors.append(expired)
            break
        current = stack.pop()
        try:
            current_stat = current.stat(follow_symlinks=False)
            summary["allocated_bytes"] += int(current_stat.st_blocks) * 512
            entries = list(os.scandir(current))
        except OSError as exc:
            errors.append(f"cannot scan physical storage path {current}: {exc}")
            continue
        for entry in entries:
            try:
                entry_stat = entry.stat(follow_symlinks=False)
                if entry.is_dir(follow_symlinks=False):
                    stack.append(Path(entry.path))
                    continue
                summary["allocated_bytes"] += int(entry_stat.st_blocks) * 512
            except OSError as exc:
                errors.append(f"cannot stat physical storage path {entry.path}: {exc}")
    return summary, errors


def _collect_feldera_helper_data(
    *, deadline: float
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    """Measure Feldera's DBSP intermediate state through its stats API."""
    from services import feldera_client

    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    expired = _deadline_error(deadline)
    if expired:
        return items, totals, [expired]
    stats = feldera_client.get_stats()
    if not stats:
        return items, totals, ["Feldera pipeline storage stats are unavailable"]
    global_metrics = stats.get("global_metrics") or {}
    if global_metrics.get("storage_bytes") is None:
        return items, totals, ["Feldera pipeline stats omit storage_bytes"]
    storage_bytes = _safe_int(global_metrics["storage_bytes"])
    _add_item(
        items,
        totals,
        engine="feldera",
        name="dbsp_intermediate_state",
        category="helper_data",
        bytes_=storage_bytes,
        file_count=0,
        path=f"feldera://pipeline/{feldera_client.FELDERA_PIPELINE_NAME}/storage",
        kind="feldera_storage",
        details={"measurement": "global_metrics.storage_bytes"},
    )
    return items, totals, []


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
        return "helper_data"
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
    table_directories: List[Tuple[Path, str, str, str]] = []
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
            table_rows = connection.execute(
                "SELECT t.table_id, s.schema_name, s.path, s.path_is_relative, "
                "t.table_name, t.path, t.path_is_relative "
                "FROM ducklake_table t JOIN ducklake_schema s USING (schema_id)"
            ).fetchall()
            table_lookup = {
                int(table_id): (str(schema), str(table))
                for table_id, schema, _, _, table, _, _ in table_rows
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

    for _, schema, schema_path, schema_relative, table, table_path, table_relative in table_rows:
        schema_dir = _relative_path(root / "data", schema_path, schema_relative)
        table_dir = _relative_path(schema_dir, table_path, table_relative).resolve()
        table_directories.append(
            (
                table_dir,
                str(schema),
                str(table),
                _ducklake_category(engine, str(schema), str(table)),
            )
        )
    table_directories.sort(key=lambda entry: len(entry[0].parts), reverse=True)

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
        if physical in mapped:
            # A physical file can remain referenced by multiple DuckLake
            # snapshots.  Storage is a physical footprint, so count its path
            # once rather than once per catalog record.
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
                owner = next(
                    (
                        (schema, table, category)
                        for table_dir, schema, table, category in table_directories
                        if physical.is_relative_to(table_dir)
                    ),
                    None,
                )
                if owner is not None:
                    schema, table, category = owner
                    aggregate = relation_totals.setdefault(
                        (schema, table, category), [0, 0]
                    )
                    aggregate[0] += int(size)
                    aggregate[1] += 1
                    continue
                category, kind = "helper_data", "unmapped_ducklake_file"
                errors.append(f"unmapped DuckLake data file: {physical}")
            else:
                rel = physical.relative_to(root.resolve()).as_posix()
                category = "helper_data" if rel.startswith("_tmp/") else "metadata"
                kind = "files"
            key = (category, kind)
            aggregate = extras.setdefault(key, [0, 0])
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
        return "helper_data"
    return "visible_output"


def _databricks_metric_int(name: str, value: Any) -> int:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise RuntimeError(f"invalid {name} metric: {value!r}") from None
    if not number.is_finite() or number < 0 or number != number.to_integral_value():
        raise RuntimeError(f"invalid {name} metric: {value!r}")
    return int(number)


def _databricks_storage_values(frame: Any) -> Dict[str, int]:
    if frame is None or frame.empty:
        raise RuntimeError("ANALYZE TABLE COMPUTE STORAGE METRICS returned no rows")
    required = {
        "total_bytes",
        "num_total_files",
        "active_bytes",
        "num_active_files",
        "vacuumable_bytes",
        "num_vacuumable_files",
        "time_travel_bytes",
        "num_time_travel_files",
    }
    raw_values = {
        str(row["metric_name"]): row["metric_value"] for _, row in frame.iterrows()
    }
    values = {
        name: _databricks_metric_int(name, raw_values[name])
        for name in required & raw_values.keys()
    }
    missing = sorted(required - values.keys())
    if missing:
        raise RuntimeError(
            "ANALYZE TABLE COMPUTE STORAGE METRICS omitted required metrics: "
            + ", ".join(missing)
        )
    retained_bytes = values["total_bytes"] - values["active_bytes"]
    retained_files = values["num_total_files"] - values["num_active_files"]
    if retained_bytes < 0 or retained_files < 0:
        raise RuntimeError(
            "ANALYZE TABLE COMPUTE STORAGE METRICS returned totals smaller "
            "than the active snapshot"
        )
    retained_data_bytes = (
        values["vacuumable_bytes"] + values["time_travel_bytes"]
    )
    retained_data_files = (
        values["num_vacuumable_files"] + values["num_time_travel_files"]
    )
    if retained_data_bytes > retained_bytes or retained_data_files > retained_files:
        raise RuntimeError(
            "ANALYZE TABLE COMPUTE STORAGE METRICS returned retained data "
            "larger than total retained storage"
        )
    values["retained_bytes"] = retained_bytes
    values["num_retained_files"] = retained_files
    values["retained_data_bytes"] = retained_data_bytes
    values["num_retained_data_files"] = retained_data_files
    values["transaction_log_bytes"] = retained_bytes - retained_data_bytes
    values["num_transaction_log_files"] = retained_files - retained_data_files
    return values


def _databricks_physical_relations(
    relations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    candidates = [
        rel
        for rel in relations
        if str(rel.get("table_type", "")).upper().replace(" ", "_") != "VIEW"
    ]
    backing_aliases = set()
    for rel in candidates:
        match = DATABRICKS_BACKING_TABLE.match(str(rel.get("name", "")))
        if match:
            backing_aliases.add((str(rel.get("schema", "")), match["view_name"]))
    return [
        rel
        for rel in candidates
        if not (
            str(rel.get("table_type", "")).upper().replace(" ", "_")
            == "MATERIALIZED_VIEW"
            and (str(rel.get("schema", "")), str(rel.get("name", "")))
            in backing_aliases
        )
    ]


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
    physical = _databricks_physical_relations(relations)
    configured_workers = _safe_int(os.environ.get("STORAGE_DATABRICKS_WORKERS", "8"))
    workers = max(1, min(configured_workers, len(physical) or 1))
    executor = ThreadPoolExecutor(
        max_workers=workers, thread_name_prefix="storage-databricks"
    )
    futures = {
        executor.submit(metrics.analyze_storage, rel, deadline=deadline): rel
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
                    values = _databricks_storage_values(frame)
                    relation = f"{schema}.{name}"
                    category = _databricks_category(src, schema, name)
                    details = {
                        "table_type": table_type,
                        "storage_metric_source": (
                            "analyze_table_compute_storage_metrics"
                        ),
                        "total_bytes": values["total_bytes"],
                        "num_total_files": values["num_total_files"],
                        "active_bytes": values["active_bytes"],
                        "num_active_files": values["num_active_files"],
                        "vacuumable_bytes": values["vacuumable_bytes"],
                        "num_vacuumable_files": values["num_vacuumable_files"],
                        "time_travel_bytes": values["time_travel_bytes"],
                        "num_time_travel_files": values[
                            "num_time_travel_files"
                        ],
                        "transaction_log_bytes": values[
                            "transaction_log_bytes"
                        ],
                        "num_transaction_log_files": values[
                            "num_transaction_log_files"
                        ],
                    }
                    _add_item(
                        items,
                        totals,
                        engine="databricks-enzyme",
                        name=relation,
                        category=category,
                        bytes_=values["active_bytes"],
                        file_count=values["num_active_files"],
                        path=f"{src.CATALOG}.{relation}",
                        kind="databricks_active_snapshot",
                        details=details,
                    )
                    if (
                        values["retained_data_bytes"]
                        or values["num_retained_data_files"]
                    ):
                        _add_item(
                            items,
                            totals,
                            engine="databricks-enzyme",
                            name=f"{relation}:retained-data",
                            category=category,
                            bytes_=values["retained_data_bytes"],
                            file_count=values["num_retained_data_files"],
                            path=f"{src.CATALOG}.{relation}",
                            kind="databricks_retained_data",
                            details=details,
                        )
                    if (
                        values["transaction_log_bytes"]
                        or values["num_transaction_log_files"]
                    ):
                        _add_item(
                            items,
                            totals,
                            engine="databricks-enzyme",
                            name=f"{relation}:transaction-log",
                            category="metadata",
                            bytes_=values["transaction_log_bytes"],
                            file_count=values["num_transaction_log_files"],
                            path=f"{src.CATALOG}.{relation}",
                            kind="databricks_transaction_log",
                            details=details,
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
        return "helper_data"
    if lowered.startswith("files/_ivm-warehouse/"):
        warehouse_rel = lowered.split("files/_ivm-warehouse/", 1)[1]
        if warehouse_rel.startswith("_ivm/views/"):
            return "visible_output"
        return "helper_data"
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


def _collect_risingwave_storage(
    *, deadline: float
) -> Tuple[List[Dict[str, Any]], Dict[str, int], List[str]]:
    """Measure RisingWave's state through its own catalog, not the filesystem.

    Walking /root/.risingwave would only give one opaque Hummock blob total, and
    the interesting split is inside it. RisingWave names every state table in
    `rw_catalog`, and `rw_table_stats` carries per-table key/value bytes, so the
    three categories fall out directly:

      visible_output — the MV result tables (`rw_materialized_views`)
      helper_data    — the streaming operator state backing those MVs
                       (`rw_internal_tables`: join/agg/window state). This is the
                       IVM overhead the benchmark is trying to price.
      source         — the loaded TPC-DI base tables (`rw_tables`)

    Hummock counts logical bytes before compression, so these are an upper bound
    on what the object store actually holds; the ratio between them is the
    comparable number, not the absolute size.
    """
    from services import risingwave_sources

    items: List[Dict[str, Any]] = []
    totals = _empty_totals()
    errors: List[str] = []

    expired = _deadline_error(deadline)
    if expired:
        return items, totals, [expired]

    # (catalog view, storage category, kind) — each row becomes one item.
    sources = (
        ("rw_materialized_views", "visible_output", "materialized_view"),
        ("rw_internal_tables", "helper_data", "operator_state"),
        ("rw_tables", "source", "table"),
    )
    try:
        conn = risingwave_sources._connect()
    except Exception as exc:
        return items, totals, [f"RisingWave storage unavailable: {exc}"]
    try:
        cur = conn.cursor()
        for view, category, kind in sources:
            if _deadline_error(deadline):
                errors.append(f"deadline reached before reading {view}")
                break
            try:
                cur.execute(
                    "SELECT c.id, c.name, "
                    "       COALESCE(st.total_key_size, 0) "
                    "     + COALESCE(st.total_value_size, 0) AS bytes, "
                    "       COALESCE(st.total_key_count, 0) AS rows "
                    f"FROM rw_catalog.{view} c "
                    "JOIN rw_catalog.rw_table_stats st ON st.id = c.id"
                )
                rows = cur.fetchall()
            except Exception as exc:
                errors.append(f"cannot read rw_catalog.{view}: {exc}")
                continue
            for relation_id, name, bytes_, row_count in rows:
                _add_item(
                    items,
                    totals,
                    engine="risingwave",
                    name=str(name),
                    category=category,
                    bytes_=_safe_int(bytes_),
                    file_count=0,
                    path=f"hummock://{view}/{name}",
                    kind=kind,
                )
                items[-1]["relation_id"] = _safe_int(relation_id)
                del row_count
    finally:
        try:
            conn.close()
        except Exception:
            pass

    # The meta store is a local SQLite file, not part of Hummock.
    meta_root = PROCESSED_ROOTS["risingwave"] / "state" / "meta_store"
    if meta_root.is_dir():
        meta_items, meta_totals, meta_errors = _collect_local_root(
            "risingwave", meta_root, root_category="metadata", deadline=deadline
        )
        items.extend(meta_items)
        _merge_totals(totals, meta_totals)
        errors.extend(meta_errors)

    return items, totals, errors


def _collect_risingwave_current_ssts(
    *, deadline: float
) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """Measure the compressed SSTs referenced by the current Hummock version.

    The filesystem footprint also contains obsolete SSTs waiting for GC. Keeping
    the two numbers separate tells us whether shorter retention can recover the
    space or whether the live streaming state itself is the limiting factor.
    """
    from services import risingwave_sources

    expired = _deadline_error(deadline)
    if expired:
        return None, [expired]
    try:
        conn = risingwave_sources._connect()
    except Exception as exc:
        return None, [f"RisingWave SST storage unavailable: {exc}"]
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT CAST(table_ids AS VARCHAR), "
            "       COALESCE(SUM(file_size), 0), "
            "       COALESCE(SUM(uncompressed_file_size), 0), "
            "       COALESCE(SUM(total_key_count), 0), "
            "       COUNT(*) "
            "FROM rw_catalog.rw_hummock_sstables "
            "GROUP BY table_ids "
            "ORDER BY SUM(file_size) DESC"
        )
        rows = cur.fetchall()
        if not rows:
            return None, ["rw_catalog.rw_hummock_sstables returned no rows"]
        groups = []
        for raw_table_ids, file_bytes, uncompressed_bytes, key_count, sst_count in rows:
            try:
                parsed_table_ids = json.loads(str(raw_table_ids))
            except (TypeError, ValueError):
                parsed_table_ids = []
            if not isinstance(parsed_table_ids, list):
                parsed_table_ids = []
            groups.append(
                {
                    "table_ids": [_safe_int(value) for value in parsed_table_ids],
                    "file_bytes": _safe_int(file_bytes),
                    "uncompressed_bytes": _safe_int(uncompressed_bytes),
                    "key_count": _safe_int(key_count),
                    "file_count": _safe_int(sst_count),
                }
            )
        return {
            "file_bytes": sum(group["file_bytes"] for group in groups),
            "uncompressed_bytes": sum(
                group["uncompressed_bytes"] for group in groups
            ),
            "key_count": sum(group["key_count"] for group in groups),
            "file_count": sum(group["file_count"] for group in groups),
            "groups": groups,
        }, []
    except Exception as exc:
        return None, [f"cannot read rw_catalog.rw_hummock_sstables: {exc}"]
    finally:
        try:
            conn.close()
        except Exception:
            pass


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

    default_timeout_s = 1800 if engine == "databricks-enzyme" else 90
    configured_timeout = os.environ.get("STORAGE_COLLECTION_TIMEOUT_S", "").strip()
    timeout_s = max(
        1,
        _safe_int(configured_timeout)
        if configured_timeout
        else default_timeout_s,
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
    elif engine in RISINGWAVE_ENGINES:
        local_items, local_totals, local_errors = _collect_risingwave_storage(
            deadline=deadline
        )
    else:
        local_items, local_totals, local_errors = _collect_local_root(
            engine, PROCESSED_ROOTS[engine], deadline=deadline
        )
    items.extend(local_items)
    _merge_totals(totals, local_totals)
    errors.extend(local_errors)

    physical_state: Optional[Dict[str, Any]] = None
    if engine in RISINGWAVE_ENGINES:
        physical_state, physical_errors = _collect_physical_root(
            PROCESSED_ROOTS[engine] / "state", deadline=deadline
        )
        errors.extend(physical_errors)
        current_ssts, sst_errors = _collect_risingwave_current_ssts(
            deadline=deadline
        )
        errors.extend(sst_errors)
        if physical_state is not None and current_ssts is not None:
            physical_state["current_ssts"] = current_ssts
            physical_state["non_current_sst_allocated_bytes"] = max(
                0,
                physical_state["allocated_bytes"] - current_ssts["file_bytes"],
            )

    if engine == "feldera" and time.monotonic() < deadline:
        helper_items, helper_totals, helper_errors = _collect_feldera_helper_data(
            deadline=deadline
        )
        items.extend(helper_items)
        _merge_totals(totals, helper_totals)
        errors.extend(helper_errors)

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
    helper = totals["helper_data_bytes"]
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
        "overhead_ratio_helper_to_visible": _ratio(helper, visible),
        "base_tables": base_tables,
        "items": sorted(
            items,
            key=lambda item: (item["category"], item["name"], item["kind"]),
        ),
    }
    if physical_state is not None:
        result["physical_state"] = physical_state
    if engine in PROCESSED_ROOTS:
        result["roots"] = {"processed": str(PROCESSED_ROOTS[engine])}
    result.setdefault("roots", {})["raw_base_reference"] = str(RAW_DELTA_ROOT)
    if errors:
        result["errors"] = errors
    return result
