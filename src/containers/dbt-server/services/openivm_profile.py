"""OpenIVM profiling export helpers."""

import csv
import os
import subprocess
import tempfile
from pathlib import Path

WORK_DIR = Path(os.environ.get("DUCKDB_OPENIVM_WORK_DIR", "/data/processed/duckdb-openivm"))
OPENIVM_BIN = os.environ.get("DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")
MEM_LIMIT = os.environ.get("DUCKDB_OPENIVM_MEM_LIMIT", "115GB")
TEMP_DIR = Path(os.environ.get("DUCKDB_OPENIVM_TEMP_DIR", str(WORK_DIR / "_tmp")))
THREADS = os.environ.get("DUCKDB_OPENIVM_THREADS", "")
DIAGNOSTIC_VIEW = os.environ.get("OPENIVM_DIAGNOSTIC_VIEW", "")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _copy_query_to_csv(query: str, output_path: Path) -> None:
    """Run a COPY query through the OpenIVM CLI and write CSV to output_path."""
    db_file = WORK_DIR / "openivm.duckdb"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)

    preamble = [
        ".bail on",
        ".timer off",
        f"SET memory_limit='{MEM_LIMIT}';",
        f"SET temp_directory='{TEMP_DIR}';",
    ]
    if THREADS:
        preamble.append(f"SET threads={int(THREADS)};")
    preamble.append("LOAD openivm;")

    sql = (
        "\n".join(preamble)
        + "\nCOPY ("
        + query
        + ") TO "
        + _sql_literal(str(output_path))
        + " (HEADER, DELIMITER ',');\n"
    )
    proc = subprocess.run(
        [OPENIVM_BIN, str(db_file)],
        input=sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=3600,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"OpenIVM profile export failed:\n{proc.stdout[-4000:]}")


def _explain_merge(sql: str) -> str:
    """Explain archived SQL against the current lake state without running the MERGE."""
    meta_path = WORK_DIR / "openivm.ducklake"
    data_path = WORK_DIR / "data"
    preamble = [
        ".bail on", ".timer off",
        f"SET memory_limit={_sql_literal(MEM_LIMIT)};",
        f"SET temp_directory={_sql_literal(str(TEMP_DIR))};",
    ]
    if THREADS:
        preamble.append(f"SET threads={int(THREADS)};")
    preamble.append("LOAD openivm;")
    if meta_path.exists():
        preamble += [
            "INSTALL icu; LOAD icu;", "INSTALL ducklake; LOAD ducklake;",
            f"ATTACH {_sql_literal('ducklake:sqlite:' + str(meta_path))} AS ducklake "
            f"(DATA_PATH {_sql_literal(str(data_path))}, data_inlining_row_limit 0);",
        ]
    preamble.append("EXPLAIN (FORMAT JSON) " + sql.strip().rstrip(";") + ";")
    proc = subprocess.run(
        [OPENIVM_BIN, str(WORK_DIR / "openivm.duckdb")],
        input="\n".join(preamble) + "\n", text=True,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=3600,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"OpenIVM MERGE EXPLAIN failed:\n{proc.stdout[-4000:]}")
    return proc.stdout


def _diagnostic_merge(tmp_dir: Path) -> dict | None:
    if not DIAGNOSTIC_VIEW:
        return None
    parts = DIAGNOSTIC_VIEW.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError("OPENIVM_DIAGNOSTIC_VIEW must be catalog.schema.view")
    catalog, schema, name = map(_sql_literal, parts)
    path = tmp_dir / "compiled.csv"
    _copy_query_to_csv(
        "SELECT s.stmt_order, s.sql FROM openivm_compiled_statements s "
        "JOIN openivm_compiled_programs p USING (view_name, operation, version) "
        f"WHERE p.view_catalog={catalog} AND p.view_schema={schema} "
        f"AND p.view_sql_name={name} AND p.operation='refresh' "
        "AND p.version=(SELECT MAX(version) FROM openivm_compiled_programs "
        "WHERE view_name=p.view_name AND operation='refresh') ORDER BY s.stmt_order",
        path,
    )
    with path.open(newline="") as source:
        statements = list(csv.DictReader(source))
    merge = next((row["sql"] for row in statements if "MERGE INTO" in row["sql"].upper()), None)
    if merge is None:
        raise RuntimeError(f"No archived MERGE found for {DIAGNOSTIC_VIEW}")
    return {
        "view": DIAGNOSTIC_VIEW,
        "statements": statements,
        "merge_plan_after_refresh": _explain_merge(merge),
        "plan_note": "EXPLAIN is run after refresh; delta cardinalities may differ from execution time.",
    }


def export_profile(run_id: str, batch_num: int) -> dict:
    """Export OpenIVM profile rows and summaries as CSV strings.

    The profile table is cumulative for the OpenIVM database. Exporting after
    each batch gives the artifact a snapshot that can be inspected independently
    even when later benchmark phases clean or replace mount/results.
    """
    run_id_sql = _sql_literal(run_id)
    with tempfile.TemporaryDirectory(prefix="openivm-profile-") as tmp:
        tmp_dir = Path(tmp)
        profile_path = tmp_dir / "profile.csv"
        by_step_path = tmp_dir / "by_step.csv"
        by_view_step_path = tmp_dir / "by_view_step.csv"

        exported_cols = f"{batch_num} AS exported_after_batch, {run_id_sql} AS exported_after_run_id"
        _copy_query_to_csv(
            f"""
            SELECT {exported_cols}, *
            FROM openivm_refresh_profile
            ORDER BY profile_timestamp, refresh_id, step_order
            """,
            profile_path,
        )
        _copy_query_to_csv(
            f"""
            SELECT {exported_cols},
                   step_name,
                   COUNT(*) AS row_count,
                   SUM(duration_ms) AS total_ms,
                   AVG(duration_ms) AS avg_ms,
                   MAX(duration_ms) AS max_ms
            FROM openivm_refresh_profile
            GROUP BY step_name
            ORDER BY total_ms DESC
            """,
            by_step_path,
        )
        _copy_query_to_csv(
            f"""
            SELECT {exported_cols},
                   view_name,
                   step_name,
                   COUNT(*) AS row_count,
                   SUM(duration_ms) AS total_ms,
                   AVG(duration_ms) AS avg_ms,
                   MAX(duration_ms) AS max_ms
            FROM openivm_refresh_profile
            GROUP BY view_name, step_name
            ORDER BY total_ms DESC
            """,
            by_view_step_path,
        )

        cli_path = TEMP_DIR / "cli-timings.csv"
        cli_timings = "timestamp_s,sql,attempt,returncode,wall_s,setup_sql_s,operation_sql_s,outside_sql_s\n"
        if cli_path.exists():
            cli_timings += cli_path.read_text()
        profile_csv = profile_path.read_text()
        rows = list(csv.DictReader(profile_csv.splitlines()))
        return {
            "status": "ok",
            "run_id": run_id,
            "batch_num": batch_num,
            "row_count": len(rows),
            "view_count": len({row["view_name"] for row in rows if row.get("view_name")}),
            "csv": {
                "profile": profile_csv,
                "cli_timings": cli_timings,
                "by_step": by_step_path.read_text(),
                "by_view_step": by_view_step_path.read_text(),
            },
            "diagnostic": _diagnostic_merge(tmp_dir) if batch_num > 1 else None,
        }
