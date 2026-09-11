"""Read physical source-row counts from generated Delta transaction logs."""

from __future__ import annotations

import json
import os
from typing import Any, Dict


def _active_table_rows(table_dir: str) -> int:
    active_files: Dict[str, int] = {}
    log_dir = os.path.join(table_dir, "_delta_log")
    for name in sorted(os.listdir(log_dir)):
        if not name.endswith(".json"):
            continue
        with open(os.path.join(log_dir, name), encoding="utf-8") as log_file:
            for line in log_file:
                action = json.loads(line)
                add = action.get("add")
                if add is not None:
                    stats = json.loads(add.get("stats") or "{}")
                    if "numRecords" not in stats:
                        raise ValueError(f"missing numRecords in {table_dir}/{name}")
                    active_files[add["path"]] = int(stats["numRecords"])
                remove = action.get("remove")
                if remove is not None:
                    active_files.pop(remove["path"], None)
    return sum(active_files.values())


def collect_source_row_counts(delta_dir: str) -> Dict[str, Any]:
    """Return total and per-table rows for each generated benchmark batch."""
    batches: Dict[str, Any] = {}
    for batch_num in (1, 2, 3):
        batch_dir = os.path.join(delta_dir, f"batch{batch_num}")
        tables: Dict[str, int] = {}
        if os.path.isdir(batch_dir):
            for table in sorted(os.listdir(batch_dir)):
                table_dir = os.path.join(batch_dir, table)
                if os.path.isdir(os.path.join(table_dir, "_delta_log")):
                    tables[table] = _active_table_rows(table_dir)
        batches[str(batch_num)] = {
            "total_rows": sum(tables.values()),
            "tables": tables,
        }
    return {"batches": batches}
