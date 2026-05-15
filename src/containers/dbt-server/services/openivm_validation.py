"""OpenIVM materialized-view correctness validation."""

import logging
import os
import re
import subprocess
import time
from pathlib import Path

from services.db import get_db
from services.dbt_compiler import get_compiled_models

logger = logging.getLogger(__name__)

WORK_DIR = Path(os.environ.get("DUCKDB_OPENIVM_WORK_DIR", "/data/processed/duckdb-openivm"))
OPENIVM_BIN = os.environ.get("DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")
MEM_LIMIT = os.environ.get("DUCKDB_OPENIVM_MEM_LIMIT", "115GB")
TEMP_DIR = Path(os.environ.get("DUCKDB_OPENIVM_TEMP_DIR", str(WORK_DIR / "_tmp")))
THREADS = os.environ.get("DUCKDB_OPENIVM_THREADS", "")


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _run_scalar(sql: str, label: str) -> int:
    """Run a scalar OpenIVM SQL query and return the integer result."""
    db_file = WORK_DIR / "openivm.duckdb"
    meta_path = WORK_DIR / "openivm.ducklake"
    data_path = WORK_DIR / "data"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    preamble = [
        ".bail on",
        ".timer off",
        ".headers off",
        ".mode csv",
        f"SET memory_limit='{MEM_LIMIT}';",
        f"SET temp_directory='{TEMP_DIR}';",
    ]
    if THREADS:
        preamble.append(f"SET threads={int(THREADS)};")
    preamble.extend([
        "LOAD openivm;",
        "SET openivm_cascade_refresh='off';",
        "INSTALL icu; LOAD icu;",
        "INSTALL ducklake; LOAD ducklake;",
        f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
        f"(DATA_PATH '{data_path}', data_inlining_row_limit 0);",
    ])

    proc = subprocess.run(
        [OPENIVM_BIN, str(db_file)],
        input="\n".join(preamble) + "\n" + sql + "\n",
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=7200,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{proc.stdout[-4000:]}")

    values = re.findall(r"-?\d+", proc.stdout or "")
    if not values:
        raise RuntimeError(f"{label} returned no integer result:\n{proc.stdout[-1000:]}")
    return int(values[-1])


def validate_run(run_id: str) -> dict:
    """Validate successful model nodes from a dbt run with EXCEPT ALL.

    This intentionally runs after the benchmark timer stops. It compares each
    OpenIVM materialized view against the dbt-compiled full query under bag
    semantics, matching the standalone runner's historical correctness check.
    """
    conn = get_db()
    run = conn.execute("SELECT engine, status FROM runs WHERE run_id=?", (run_id,)).fetchone()
    if not run:
        conn.close()
        raise ValueError(f"run_id not found: {run_id}")
    if run["engine"] != "duckdb-openivm":
        conn.close()
        raise ValueError(f"validation only supports duckdb-openivm, got {run['engine']}")

    nodes = conn.execute(
        """
        SELECT unique_id, name, resource_type, status, compiled_sql
        FROM run_nodes
        WHERE run_id=?
        ORDER BY rowid
        """,
        (run_id,),
    ).fetchall()
    conn.close()

    compiled_models = get_compiled_models("duckdb-openivm")
    results = []
    started = time.monotonic()

    for node in nodes:
        if node["resource_type"] != "model" or node["status"] not in ("success", "pass"):
            continue
        compiled_sql = (node["compiled_sql"] or "").strip().rstrip(";")
        if not compiled_sql:
            continue

        meta = compiled_models.get(node["unique_id"], {})
        schema = meta.get("schema") or "main"
        name = node["name"]
        relation = ".".join([
            _quote_ident("ducklake"),
            _quote_ident(schema),
            _quote_ident(name),
        ])
        expected_name = _quote_ident(f"openivm_expected_{len(results)}")

        sql = f"""
CREATE OR REPLACE TEMP TABLE {expected_name} AS
{compiled_sql};

SELECT COUNT(*) FROM (
    (SELECT * FROM {relation} EXCEPT ALL SELECT * FROM {expected_name})
    UNION ALL
    (SELECT * FROM {expected_name} EXCEPT ALL SELECT * FROM {relation})
) AS openivm_diff;

DROP TABLE {expected_name};
"""
        t0 = time.monotonic()
        diff_count = _run_scalar(sql, f"validate {name}")
        elapsed = round(time.monotonic() - t0, 3)
        status = "pass" if diff_count == 0 else "fail"
        results.append({
            "unique_id": node["unique_id"],
            "name": name,
            "schema": schema,
            "status": status,
            "diff_count": diff_count,
            "validation_time_s": elapsed,
        })
        logger.info(
            "[duckdb-openivm] validation %s: %s diff_count=%d time=%.3fs",
            status,
            name,
            diff_count,
            elapsed,
        )

    failures = [r for r in results if r["status"] != "pass"]
    return {
        "run_id": run_id,
        "status": "failed" if failures else "passed",
        "models_checked": len(results),
        "failures": failures,
        "duration_s": round(time.monotonic() - started, 3),
        "results": results,
    }
