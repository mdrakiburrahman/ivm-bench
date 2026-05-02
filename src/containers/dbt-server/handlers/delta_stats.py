"""Delta Stats handler — reports row count and size of Delta tables in staging."""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler

bp = Blueprint("delta_stats", __name__)

DELTA_BASE_DIR = os.environ.get("DELTA_BASE_DIR", "/data/raw/delta")


def _read_table_stats(table_path: str, name: str) -> dict:
    """Read row count and size for a single Delta table."""
    from deltalake import DeltaTable

    try:
        dt = DeltaTable(table_path)
        actions = dt.get_add_actions(flatten=True)
        num_records_col = actions.column("num_records")
        size_col = actions.column("size_bytes")
        rows = sum(v.as_py() for v in num_records_col)
        size_bytes = sum(v.as_py() for v in size_col)
        size_gb = size_bytes / (1024 ** 3)
        return {"name": name, "rows": rows, "size_gb": round(size_gb, 6)}
    except Exception as e:
        return {"name": name, "rows": None, "size_gb": None, "error": str(e)}


@bp.route("/delta-stats")
def delta_stats():
    """Return row count and size in GB for each Delta table in staging."""
    staging_path = os.path.join(DELTA_BASE_DIR, "staging")

    if not os.path.isdir(staging_path):
        return jsonify({"error": f"Staging directory not found: {staging_path}"}), 404

    # Collect valid Delta table directories
    table_dirs = []
    for entry in sorted(os.listdir(staging_path)):
        table_path = os.path.join(staging_path, entry)
        if not os.path.isdir(table_path):
            continue
        if not os.path.isdir(os.path.join(table_path, "_delta_log")):
            continue
        table_dirs.append((table_path, entry))

    # Read stats in parallel
    tables = []
    with ThreadPoolExecutor(max_workers=min(8, len(table_dirs) or 1)) as executor:
        futures = {
            executor.submit(_read_table_stats, path, name): name
            for path, name in table_dirs
        }
        for future in as_completed(futures):
            tables.append(future.result())

    # Sort by name for deterministic output
    tables.sort(key=lambda t: t["name"])

    sf = os.environ.get("SCALE_FACTOR", "3")
    return jsonify({"scale_factor": int(sf), "tables": tables})


class DeltaStatsHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
