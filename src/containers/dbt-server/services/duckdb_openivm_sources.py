"""DuckDB-OpenIVM source table management."""

import logging
import os
from pathlib import Path

from services.ducklake_sources import (
    append_sources as append_ducklake_sources,
    init_sources as init_ducklake_sources,
    quote_literal,
)

logger = logging.getLogger(__name__)

RAW_DELTA_DIR = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
WORK_DIR = Path(os.environ.get("DUCKDB_OPENIVM_WORK_DIR", "/data/processed/duckdb-openivm"))


OPENIVM_BIN = os.environ.get("DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")


def _run_openivm_sql(sql: str, label: str) -> str:
    """Execute SQL via the OpenIVM DuckDB binary (subprocess)."""
    db_file = str(WORK_DIR / "openivm.duckdb")
    meta_path = str(WORK_DIR / "openivm.ducklake")
    data_path = str(WORK_DIR / "data")

    prefix = ".bail on\n.timer off\n"
    attach = (
        f"LOAD openivm;\n"
        f"SET openivm_cascade_refresh='off';\n"
        f"INSTALL icu; LOAD icu;\n"
        f"INSTALL ducklake; LOAD ducklake;\n"
        f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
        f"(DATA_PATH {quote_literal(data_path)}, data_inlining_row_limit 0);\n"
    )

    import subprocess
    proc = subprocess.run(
        [OPENIVM_BIN, db_file],
        input=prefix + attach + sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{proc.stdout}")
    return proc.stdout


# ── Public API ──────────────────────────────────────────────────────────────


def init_sources() -> dict:
    """Create DuckLake schemas and batch-1 source tables."""
    result = init_ducklake_sources(RAW_DELTA_DIR, WORK_DIR, _run_openivm_sql, "duckdb-openivm")
    logger.info("[duckdb-openivm] Source init complete: %d tables", result["tables_created"])
    return result


def append_sources(batch_num: int) -> dict:
    """INSERT new batch data into staging tables."""
    result = append_ducklake_sources(
        RAW_DELTA_DIR,
        WORK_DIR,
        batch_num,
        _run_openivm_sql,
        "duckdb-openivm",
        state_files=(WORK_DIR / "openivm.ducklake",),
    )
    logger.info("[duckdb-openivm] Batch %d append: %d tables", batch_num, result["tables_appended"])
    return result
