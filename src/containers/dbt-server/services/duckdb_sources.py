"""DuckDB source table management for DuckLake-backed full refresh runs."""

import logging
import os
from pathlib import Path

import duckdb

from services.ducklake_sources import (
    append_sources as append_ducklake_sources,
    init_sources as init_ducklake_sources,
    quote_literal,
)

logger = logging.getLogger(__name__)

RAW_DELTA_DIR = Path(os.environ.get("RAW_DELTA_DIR", "/data/raw/delta"))
WORK_DIR = Path(os.environ.get("DUCKDB_WORK_DIR", "/data/processed/duckdb"))
MEM_LIMIT = os.environ.get("DUCKDB_MEM_LIMIT", "115GB")
TEMP_DIR = os.environ.get("DUCKDB_TEMP_DIR", str(WORK_DIR / "_tmp"))
THREADS = os.environ.get("DUCKDB_THREADS", "32")


def _run_duckdb_sql(sql: str, label: str) -> str:
    """Execute DuckLake source-management SQL through DuckDB Python."""
    meta_path = WORK_DIR / "metadata.db"
    data_path = WORK_DIR / "data"
    Path(TEMP_DIR).mkdir(parents=True, exist_ok=True)
    data_path.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(":memory:")
    try:
        con.execute(f"SET preserve_insertion_order=false")
        con.execute(f"SET memory_limit={quote_literal(MEM_LIMIT)}")
        con.execute(f"SET temp_directory={quote_literal(TEMP_DIR)}")
        if THREADS:
            con.execute(f"SET threads={int(THREADS)}")
        con.execute("INSTALL ducklake; LOAD ducklake")
        con.execute("INSTALL sqlite; LOAD sqlite")
        con.execute(
            f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
            f"(DATA_PATH {quote_literal(str(data_path))}, data_inlining_row_limit 0)"
        )
        con.execute(sql)
        return ""
    except Exception as exc:
        raise RuntimeError(f"{label} failed: {exc}") from exc
    finally:
        con.close()


def init_sources() -> dict:
    result = init_ducklake_sources(RAW_DELTA_DIR, WORK_DIR, _run_duckdb_sql, "duckdb")
    logger.info("[duckdb] Source init complete: %d tables", result["tables_created"])
    return result


def append_sources(batch_num: int) -> dict:
    result = append_ducklake_sources(
        RAW_DELTA_DIR,
        WORK_DIR,
        batch_num,
        _run_duckdb_sql,
        "duckdb",
        state_files=(WORK_DIR / "metadata.db",),
    )
    logger.info("[duckdb] Batch %d append: %d tables", batch_num, result["tables_appended"])
    return result
