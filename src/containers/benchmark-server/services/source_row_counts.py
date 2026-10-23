"""Read physical source-row counts from generated Delta transaction logs."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional


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


def collect_source_row_counts(
    delta_dir: str, batch_count: int = 3, *, refresh_plan: Optional[dict] = None,
) -> Dict[str, Any]:
    """Count physical payloads; a refresh plan also identifies loaded sources."""
    batches: Dict[str, Any] = {}
    for batch_num in range(1, batch_count + 1):
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
    if refresh_plan is None:
        return {"batches": batches}

    # Audit is loaded from outside batch1; time and initial Category B tables
    # are generated but not loaded. Keep both inventories explicitly.
    audit = os.path.join(delta_dir, "audit")
    if os.path.isdir(os.path.join(audit, "_delta_log")):
        batches["1"]["tables"]["audit"] = _active_table_rows(audit)
        batches["1"]["total_rows"] = sum(batches["1"]["tables"].values())
    initial = refresh_plan["initial_tables"]
    loaded = {**batches, "1": {"tables": dict(initial), "total_rows": sum(initial.values())}}
    if loaded["1"]["total_rows"] != refresh_plan["initial_rows"]:
        raise ValueError("refresh plan initial table counts do not match its initial_rows")
    for round_ in refresh_plan["rounds"]:
        batch_num = str(round_["batch_num"])
        actual = batches[batch_num]["tables"]
        planned = {table: rows["inserted_rows"] for table, rows in round_["tables"].items()}
        if any(actual.get(table, 0) != planned.get(table, 0) for table in actual.keys() | planned.keys()):
            raise ValueError(f"generated batch {batch_num} does not match its refresh plan")
    return {"batches": loaded, "generated_batches": batches, "refresh_plan": refresh_plan}
