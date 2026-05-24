"""OpenIVM materialized-view correctness validation.

Optimized to amortise the per-model `duckdb-openivm` CLI cold start (load
`openivm` extension, INSTALL+LOAD `ducklake`, ATTACH the DuckLake metadata
sqlite database) across ALL models in a single long-lived subprocess.

Happy path:
  1. Build a single SQL script that, for every successful model node, emits
     three independent `;`-terminated statements:
       - `CREATE OR REPLACE TEMP TABLE openivm_expected_<safe_id> AS <compiled_sql>;`
       - `SELECT '<unique_id>' AS unique_id, COUNT(*) AS diff_count FROM (...);`
       - `DROP TABLE IF EXISTS openivm_expected_<safe_id>;`
  2. Pipe the unified preamble + all per-model blocks through ONE
     `subprocess.run([OPENIVM_BIN, db_file])` with `.bail off` so a SQL error
     on model K does not abort models K+1..N.
  3. With `.mode csv` + `.headers off`, every successful diff query emits a
     single line `<unique_id>,<diff_count>`. We parse those line-by-line.

Failure path:
  Any model whose marker row is ABSENT from stdout failed during CREATE or
  during the diff query itself. We re-run JUST those models in short
  diagnostic subprocesses (`.bail on`, single model per subprocess) to
  surface the precise DuckDB error message in the JSON, preserving the
  per-model error semantics the previous serial implementation provided.

Per-model `validation_time_s` is reported as `total / models_checked`
(estimated; flagged with `validation_time_estimated: true`) because we can
no longer wall-clock individual models when they all share one subprocess.
Top-level `duration_s` remains exact.
"""

import hashlib
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

# Per-model legacy timeout was 7200s. For the unified subprocess we scale by
# the number of models being validated (with a sensible floor and ceiling).
# Override via DUCKDB_OPENIVM_VALIDATE_TIMEOUT to a hard wall-clock seconds.
DEFAULT_PER_MODEL_TIMEOUT_S = 7200
TIMEOUT_CEILING_S = 86400


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _safe_table_suffix(unique_id: str) -> str:
    """Build a DuckDB-temp-table-safe identifier from a dbt unique_id.

    Even though only ONE model is materialised in DuckDB at a time inside
    the unified subprocess, we still hash the `unique_id` into the temp
    table name so the SQL script is easy to read in logs and so future
    parallelism is straightforward to add.
    """
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", unique_id) or "anon"
    digest = hashlib.sha1(unique_id.encode("utf-8")).hexdigest()[:8]
    return f"{sanitized}_{digest}"


def _build_preamble() -> str:
    """Build the once-per-subprocess CLI preamble (settings, extensions, ATTACH)."""
    meta_path = WORK_DIR / "openivm.ducklake"
    data_path = WORK_DIR / "data"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    lines = [
        ".bail off",
        ".timer off",
        ".headers off",
        ".mode csv",
        f"SET memory_limit='{MEM_LIMIT}';",
        f"SET temp_directory='{TEMP_DIR}';",
    ]
    if THREADS:
        lines.append(f"SET threads={int(THREADS)};")
    lines.extend([
        "LOAD openivm;",
        "SET openivm_cascade_refresh='off';",
        "INSTALL icu; LOAD icu;",
        "INSTALL ducklake; LOAD ducklake;",
        f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
        f"(DATA_PATH '{data_path}', data_inlining_row_limit 0);",
    ])
    return "\n".join(lines) + "\n"


def _build_model_block(unique_id: str, schema: str, name: str, compiled_sql: str) -> str:
    """Build the 3-statement per-model script block."""
    relation = ".".join([
        _quote_ident("ducklake"),
        _quote_ident(schema),
        _quote_ident(name),
    ])
    expected_unq = f"openivm_expected_{_safe_table_suffix(unique_id)}"
    expected_name = _quote_ident(expected_unq)
    # CSV-mode literal of `unique_id` will be unquoted (no commas/quotes in
    # any dbt unique_id), so the stdout row looks like `model.tpcdi.x,0`.
    # We embed the literal as a single-quoted SQL string; SQL string
    # escaping: replace single quotes with two single quotes.
    uid_sql_literal = "'" + unique_id.replace("'", "''") + "'"
    return (
        f"CREATE OR REPLACE TEMP TABLE {expected_name} AS\n"
        f"{compiled_sql};\n"
        f"SELECT {uid_sql_literal} AS unique_id, COUNT(*) AS diff_count FROM (\n"
        f"    (SELECT * FROM {relation} EXCEPT ALL SELECT * FROM {expected_name})\n"
        f"    UNION ALL\n"
        f"    (SELECT * FROM {expected_name} EXCEPT ALL SELECT * FROM {relation})\n"
        f") AS openivm_diff;\n"
        f"DROP TABLE IF EXISTS {expected_name};\n"
    )


# Tight parser: line consisting of `<unique_id>,<int>` and nothing else.
# Unique IDs in dbt are `[\w\.]+` so the regex is anchored and strict.
_DIFF_ROW_RE = re.compile(r"^([\w\.\-]+),(-?\d+)\s*$")


def _parse_diff_rows(stdout_text: str) -> dict[str, int]:
    """Parse `<unique_id>,<count>` lines out of unified-pass stdout."""
    diff_map: dict[str, int] = {}
    for raw in (stdout_text or "").splitlines():
        m = _DIFF_ROW_RE.match(raw.strip())
        if not m:
            continue
        diff_map[m.group(1)] = int(m.group(2))
    return diff_map


def _validate_timeout_for(n_models: int) -> int:
    """Resolve the subprocess timeout (seconds) for the unified pass."""
    env_override = os.environ.get("DUCKDB_OPENIVM_VALIDATE_TIMEOUT")
    if env_override:
        return int(env_override)
    scaled = max(DEFAULT_PER_MODEL_TIMEOUT_S, DEFAULT_PER_MODEL_TIMEOUT_S * max(n_models, 1))
    return min(scaled, TIMEOUT_CEILING_S)


def _run_unified(script: str, n_models: int) -> tuple[int, str]:
    """Run the full validation script in one subprocess; return (rc, stdout)."""
    db_file = WORK_DIR / "openivm.duckdb"
    timeout_s = _validate_timeout_for(n_models)
    proc = subprocess.run(
        [OPENIVM_BIN, str(db_file)],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_s,
    )
    return proc.returncode, proc.stdout or ""


def _run_diagnostic(unique_id: str, schema: str, name: str, compiled_sql: str) -> tuple[int | None, str]:
    """Re-run a single failing model with `.bail on` to capture the error.

    Returns (diff_count_or_None, captured_stdout). If the diff query runs
    successfully we extract the integer; if any statement errors the rc!=0
    and we return diff_count=None plus the full stdout for inclusion in
    the failure entry.
    """
    db_file = WORK_DIR / "openivm.duckdb"
    # Reuse the preamble but toggle `.bail on` so the FIRST error stops the
    # script and we get a clean error message.
    preamble = _build_preamble().replace(".bail off", ".bail on", 1)
    script = preamble + _build_model_block(unique_id, schema, name, compiled_sql)
    timeout_s = int(os.environ.get("DUCKDB_OPENIVM_VALIDATE_TIMEOUT", DEFAULT_PER_MODEL_TIMEOUT_S))
    proc = subprocess.run(
        [OPENIVM_BIN, str(db_file)],
        input=script,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=timeout_s,
    )
    stdout = proc.stdout or ""
    diff = _parse_diff_rows(stdout).get(unique_id)
    return diff, stdout


def validate_run(run_id: str) -> dict:
    """Validate successful model nodes from a dbt run with EXCEPT ALL.

    This intentionally runs after the benchmark timer stops. It compares each
    OpenIVM materialized view against the dbt-compiled full query under bag
    semantics, matching the standalone runner's historical correctness check.

    Optimised path: one long-lived `duckdb-openivm` subprocess for all models
    on the happy path; per-failure diagnostic re-runs for any model that
    fails to emit a result marker.
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
    started = time.monotonic()

    # Pre-filter to the validatable model set so the script only includes
    # eligible nodes; preserves input rowid order in the returned results.
    work: list[dict] = []
    for node in nodes:
        if node["resource_type"] != "model" or node["status"] not in ("success", "pass"):
            continue
        compiled_sql = (node["compiled_sql"] or "").strip().rstrip(";")
        if not compiled_sql:
            continue
        meta = compiled_models.get(node["unique_id"], {})
        work.append({
            "unique_id": node["unique_id"],
            "name": node["name"],
            "schema": meta.get("schema") or "main",
            "compiled_sql": compiled_sql,
        })

    if not work:
        return {
            "run_id": run_id,
            "status": "passed",
            "models_checked": 0,
            "failures": [],
            "duration_s": round(time.monotonic() - started, 3),
            "results": [],
        }

    # --- Happy path: single subprocess for all models.
    logger.info(
        "[duckdb-openivm] validating %d models in one unified subprocess",
        len(work),
    )
    script = _build_preamble() + "".join(
        _build_model_block(
            item["unique_id"], item["schema"], item["name"], item["compiled_sql"]
        )
        for item in work
    )

    rc, unified_stdout = _run_unified(script, n_models=len(work))
    diff_map = _parse_diff_rows(unified_stdout)
    if not diff_map and rc != 0:
        # Total failure (no markers AT ALL came through, subprocess errored).
        # Surface the stdout dump so the operator can diagnose, then bail.
        raise RuntimeError(
            f"duckdb-openivm unified validation produced no result markers "
            f"and the subprocess exited rc={rc}:\n{unified_stdout[-4000:]}"
        )

    # --- Build per-model entries in input order, re-running failed nodes.
    total_elapsed = time.monotonic() - started
    estimated_per_model = round(total_elapsed / max(len(work), 1), 3)
    results: list[dict] = []
    for item in work:
        uid = item["unique_id"]
        name = item["name"]
        schema = item["schema"]
        entry: dict
        if uid in diff_map:
            diff_count = diff_map[uid]
            entry = {
                "unique_id": uid,
                "name": name,
                "schema": schema,
                "status": "pass" if diff_count == 0 else "fail",
                "diff_count": diff_count,
                "validation_time_s": estimated_per_model,
                "validation_time_estimated": True,
            }
            logger.info(
                "[duckdb-openivm] validation %s: %s diff_count=%d",
                entry["status"], name, diff_count,
            )
        else:
            # Marker missing => the diff SQL never produced a row. Re-run
            # this model alone with `.bail on` to capture the error.
            logger.warning(
                "[duckdb-openivm] no result marker for %s; running diagnostic",
                uid,
            )
            error_msg: str | None = None
            diff_count = -1
            try:
                diag_diff, diag_stdout = _run_diagnostic(
                    uid, schema, name, item["compiled_sql"],
                )
                if diag_diff is not None:
                    diff_count = diag_diff
                    status_for_log = "pass" if diff_count == 0 else "fail"
                    logger.info(
                        "[duckdb-openivm] diagnostic %s: %s diff_count=%d",
                        status_for_log, name, diff_count,
                    )
                else:
                    error_msg = (
                        f"diagnostic re-run produced no result marker:\n"
                        f"{diag_stdout[-4000:]}"
                    )
                    logger.error("[duckdb-openivm] %s failed: %s", uid, error_msg)
            except Exception as exc:
                error_msg = f"diagnostic re-run crashed: {exc}"
                logger.exception(
                    "[duckdb-openivm] diagnostic crash for %s", uid,
                )
            entry = {
                "unique_id": uid,
                "name": name,
                "schema": schema,
                "status": "pass" if diff_count == 0 else "fail",
                "diff_count": diff_count,
                "validation_time_s": estimated_per_model,
                "validation_time_estimated": True,
            }
            if error_msg is not None:
                entry["error"] = error_msg[:4000]
        results.append(entry)

    failures = [r for r in results if r["status"] != "pass"]
    return {
        "run_id": run_id,
        "status": "failed" if failures else "passed",
        "models_checked": len(results),
        "failures": failures,
        "duration_s": round(time.monotonic() - started, 3),
        "results": results,
    }
