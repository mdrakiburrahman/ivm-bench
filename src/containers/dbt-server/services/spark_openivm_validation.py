"""OpenIVM correctness validation for the `spark-openivm` engine.

Mirrors `openivm_validation.validate_run` from the `duckdb-openivm` path but
executes the validation SQL through a Livy session against Spark, so the
correctness check covers everything `openivm.duckdb_extension` emitted into
each MV's openivm_data_* Delta table and surfaced through the user-facing
view.

For every successful dbt model node from the timed `run_id`, we:

  1. Build the validation SQL:

         CREATE OR REPLACE TEMPORARY VIEW openivm_expected_N AS <compiled_sql>;
         SELECT COUNT(*) AS diff_count FROM (
             (SELECT * FROM <relation> EXCEPT ALL
              SELECT * FROM openivm_expected_N)
             UNION ALL
             (SELECT * FROM openivm_expected_N EXCEPT ALL
              SELECT * FROM <relation>)
         ) AS openivm_diff;
         DROP VIEW IF EXISTS openivm_expected_N;

     where `<relation>` is the MV's `<schema>.<name>` Spark relation (the
     same one dbt's materialization produced via
     `CREATE MATERIALIZED VIEW`).

  2. Execute the three statements over Livy, parse the integer
     `diff_count` from the SELECT statement's tabular output.

  3. Record a per-model `pass` / `fail` row keyed by `diff_count == 0`.

This intentionally runs OUTSIDE the benchmark timer (post-batch hook in
benchmark-server). On the first failure the post-batch hook turns the
batch result red, which is the entire point of OPENIVM_VALIDATE=1.

Configuration:

  - SPARK_OPENIVM_LIVY_URL: Livy URL for spark-openivm container
    (default: http://spark-openivm:8998).
  - SPARK_OPENIVM_LAKEHOUSE: database to `USE` before validating
    (default: tpcdi). Matches the `lakehouse:` value in the
    spark-openivm dbt profile.
"""

from __future__ import annotations

import hashlib
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from services.db import get_db
from services.spark_openivm_sources import LivyClient

logger = logging.getLogger(__name__)

LAKEHOUSE = os.environ.get("SPARK_OPENIVM_LAKEHOUSE", "tpcdi")
# Worker count for the per-model validation thread pool. The single Livy SQL
# session already handles up to dbt-fabricspark's `threads: 8` of concurrent
# statement submission during the timed dbt run, so 8 is the documented safe
# default. Set to 1 to fall back to strictly serial validation against the
# same single Livy session (still benefits from the lower polling interval).
VALIDATE_THREADS = max(1, int(os.environ.get("SPARK_OPENIVM_VALIDATE_THREADS", "8")))


def _quote_ident(value: str) -> str:
    # Spark identifiers are backtick-quoted; embedded backticks are doubled.
    return "`" + value.replace("`", "``") + "`"


def _extract_diff_count(output: dict[str, Any], label: str) -> int:
    """Parse a single integer from the Livy SQL statement output."""
    data = (output or {}).get("data") or {}
    text = data.get("application/json") or data.get("text/plain") or ""

    if isinstance(text, dict):
        # `application/json` for `sql` kind looks like
        # {"schema": {...}, "data": [[...row1...], [...row2...]]}.
        rows = text.get("data") or []
        if rows and rows[0]:
            return int(rows[0][0])
        raise RuntimeError(f"{label}: Livy returned no rows in JSON output: {text}")

    # `text/plain` is the rendered ASCII table — pull the last integer.
    matches = re.findall(r"-?\d+", str(text))
    if not matches:
        raise RuntimeError(f"{label}: Livy returned no integer:\n{str(text)[-1000:]}")
    return int(matches[-1])


def _extract_columns(output: dict[str, Any], label: str) -> list[str]:
    """Parse the first column of every result row out of a Livy SQL output.

    Used to consume `DESCRIBE <relation>` / `SHOW COLUMNS …` so the
    validation SQL can project only the user-visible columns and drop the
    `openivm_*` bookkeeping that openivm-spark's incremental refresh
    types leave on the user-facing object (the AGGREGATE_HAVING path
    already creates a view that hides them; AGGREGATE_GROUP /
    SIMPLE_AGGREGATE / SIMPLE_PROJECTION do not).
    """
    data = (output or {}).get("data") or {}
    payload = data.get("application/json") or data.get("text/plain") or ""
    cols: list[str] = []

    if isinstance(payload, dict):
        for row in payload.get("data") or []:
            if row:
                cols.append(str(row[0]))
        return cols

    # Tabular text output (`+----+...|`). Walk row-by-row, take the first
    # non-divider cell. DESCRIBE in Spark renders as:
    #     +---+------+...
    #     |col|...   |
    #     +---+------+
    #     |c1 |type  |
    #     ...
    seen_header = False
    for line in str(payload).splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if not cells:
            continue
        first = cells[0]
        if not seen_header:
            # Skip the `col_name` header row.
            seen_header = True
            continue
        if not first or first.startswith("-") or first.startswith("col_name"):
            continue
        # DESCRIBE trails with `# Partition Information` etc. on Spark —
        # stop at any header-style row that starts with `#`.
        if first.startswith("#"):
            break
        cols.append(first)
    return cols


def _safe_view_suffix(unique_id: str) -> str:
    """Build a Spark-temp-view-safe identifier from a dbt unique_id.

    Sanitises everything outside `[A-Za-z0-9_]` to `_`, then appends a short
    stable hash of the original `unique_id` so collisions are impossible
    (e.g. `a-b` vs `a_b` both sanitise the same but get different hashes).
    Required because the validation runs N models concurrently against ONE
    Livy session — each model needs a unique temp-view name.
    """
    sanitized = re.sub(r"[^A-Za-z0-9_]", "_", unique_id) or "anon"
    digest = hashlib.sha1(unique_id.encode("utf-8")).hexdigest()[:8]
    return f"{sanitized}_{digest}"


def _validate_one(
    livy: LivyClient,
    *,
    unique_id: str,
    name: str,
    schema: str,
    compiled_sql: str,
) -> dict[str, Any]:
    """Validate a single model against the Spark MV via EXCEPT-ALL.

    Designed for concurrent invocation against a single open `LivyClient`:
    no shared mutable state is touched, all SQL bookkeeping uses a
    per-model-unique temp-view name.
    """
    relation = f"{_quote_ident(schema)}.{_quote_ident(name)}"
    expected_unq = f"openivm_expected_{_safe_view_suffix(unique_id)}"
    expected_name = _quote_ident(expected_unq)

    label = f"validate {schema}.{name}"
    t0 = time.monotonic()
    diff_count = -1
    error_msg: str | None = None
    sample_payload: dict | None = None
    create_expected_sql = (
        f"CREATE OR REPLACE TEMPORARY VIEW {expected_name} AS\n"
        f"{compiled_sql}"
    )
    drop_sql = f"DROP VIEW IF EXISTS {expected_name}"
    try:
        livy.execute(create_expected_sql)

        # Drop openivm_* bookkeeping columns from the MV before
        # the EXCEPT-ALL bag comparison. AGGREGATE_HAVING already
        # exposes only user-visible columns via its user-facing
        # VIEW, but AGGREGATE_GROUP / SIMPLE_AGGREGATE /
        # SIMPLE_PROJECTION leave openivm_count_star and / or
        # openivm_multiplicity on the data table itself
        # (`<schema>.<name>` IS the data table for those types
        # — see MaterializedViewCommands.scala:691).
        #
        # Align the projection order across both sides BY COLUMN
        # NAME. openivm-spark's LPTS-rendered initial-load SQL may
        # store join-USING columns in a different ORDER than
        # Spark's analyzed plan emits them (LPTS lowers USING to
        # an ON-equi-join and projects FROM-side columns first).
        # EXCEPT ALL is positional, so without name-based
        # alignment a value-correct MV would still report a diff
        # whenever the column order diverges. Use the
        # compiled-SQL side's declared column order as the
        # canonical projection: both sides project those columns
        # in that order, so EXCEPT ALL only ever reports a true
        # value-bag difference.
        describe_mv_out = livy.execute(f"DESCRIBE {relation}")
        mv_cols = _extract_columns(
            describe_mv_out.get("output") or {},
            f"describe {schema}.{name}",
        )
        describe_exp_out = livy.execute(f"DESCRIBE {expected_name}")
        expected_cols = _extract_columns(
            describe_exp_out.get("output") or {},
            f"describe {expected_name}",
        )
        expected_user_cols = [
            c for c in expected_cols
            if not c.lower().startswith("openivm_")
        ]
        mv_user_cols = set(
            c for c in mv_cols
            if not c.lower().startswith("openivm_")
        )
        user_cols = [c for c in expected_user_cols if c in mv_user_cols]
        missing_from_mv = [
            c for c in expected_user_cols if c not in mv_user_cols
        ]
        missing_from_expected = sorted(
            mv_user_cols - set(expected_user_cols)
        )
        if not user_cols:
            raise RuntimeError(
                f"{label}: no user-visible columns shared between "
                f"{relation} ({mv_cols}) and expected view "
                f"({expected_cols})"
            )
        if missing_from_mv or missing_from_expected:
            raise RuntimeError(
                f"{label}: column-name mismatch between MV and "
                f"expected. Missing on MV: {missing_from_mv}; "
                f"missing on expected: {missing_from_expected}"
            )
        proj = ", ".join(_quote_ident(c) for c in user_cols)
        mv_subquery = f"SELECT {proj} FROM {relation}"
        expected_subquery = f"SELECT {proj} FROM {expected_name}"

        diff_sql = (
            f"SELECT COUNT(*) AS diff_count FROM (\n"
            f"    (({mv_subquery}) EXCEPT ALL "
            f"({expected_subquery}))\n"
            f"    UNION ALL\n"
            f"    (({expected_subquery}) EXCEPT ALL "
            f"({mv_subquery}))\n"
            f") AS openivm_diff"
        )
        out = livy.execute(diff_sql)
        diff_count = _extract_diff_count(
            out.get("output") or {}, label
        )
        # On mismatch, capture up to 5 sample diff rows from
        # each side so the JSON report is useful for root-cause
        # work (no live cluster needed).
        if diff_count != 0:
            try:
                sample_sql = (
                    f"SELECT 'mv' AS side, * FROM ("
                    f"({mv_subquery}) EXCEPT ALL "
                    f"({expected_subquery})) LIMIT 5"
                )
                mv_extra = livy.execute(sample_sql)
                sample_payload_mv = (
                    (mv_extra.get("output") or {}).get("data") or {}
                ).get("application/json") or {}
                sample_sql2 = (
                    f"SELECT 'expected' AS side, * FROM "
                    f"(({expected_subquery}) EXCEPT ALL "
                    f"({mv_subquery})) LIMIT 5"
                )
                exp_extra = livy.execute(sample_sql2)
                sample_payload_exp = (
                    (exp_extra.get("output") or {}).get("data") or {}
                ).get("application/json") or {}
                sample_payload = {
                    "mv_extra": sample_payload_mv,
                    "expected_extra": sample_payload_exp,
                    "user_cols": user_cols,
                    "mv_cols": mv_cols,
                    "expected_cols": expected_cols,
                    "mv_count": None,
                    "expected_count": None,
                }
                try:
                    mv_cnt = livy.execute(
                        f"SELECT COUNT(*) FROM ({mv_subquery})"
                    )
                    sample_payload["mv_count"] = _extract_diff_count(
                        mv_cnt.get("output") or {}, label + " mv_count"
                    )
                except Exception:
                    pass
                try:
                    exp_cnt = livy.execute(
                        f"SELECT COUNT(*) FROM ({expected_subquery})"
                    )
                    sample_payload["expected_count"] = (
                        _extract_diff_count(
                            exp_cnt.get("output") or {},
                            label + " exp_count",
                        )
                    )
                except Exception:
                    pass
            except Exception as sample_exc:
                sample_payload = {"sample_error": str(sample_exc)}
    except Exception as exc:
        # Don't abort on a per-model error — record it as a failure
        # so the report shows every diff at once.
        error_msg = str(exc)
        logger.exception("[spark-openivm] validation error for %s.%s",
                         schema, name)
    finally:
        # Best-effort cleanup; ignore drop failures so a real
        # validation error from above is not masked.
        try:
            livy.execute(drop_sql)
        except RuntimeError:
            logger.warning(
                "[spark-openivm] cleanup DROP failed for %s",
                expected_name,
            )
    elapsed = round(time.monotonic() - t0, 3)
    status = "pass" if diff_count == 0 else "fail"
    entry: dict[str, Any] = {
        "unique_id": unique_id,
        "name": name,
        "schema": schema,
        "status": status,
        "diff_count": diff_count,
        "validation_time_s": elapsed,
    }
    if error_msg is not None:
        # Cap to keep the JSON readable; full trace lives in the
        # dbt-server logs already.
        entry["error"] = error_msg[:4000]
    if status == "fail" and sample_payload:
        entry["sample"] = sample_payload
    logger.info(
        "[spark-openivm] validation %s: %s.%s diff_count=%d time=%.3fs",
        status, schema, name, diff_count, elapsed,
    )
    return entry


def validate_run(run_id: str) -> dict:
    """Validate successful model nodes from a spark-openivm dbt run.

    Returns a dict shaped exactly like
    `services.openivm_validation.validate_run` so the benchmark-server's
    consumer code (`_validate_*_openivm`) can stay engine-agnostic.

    Models are validated concurrently via a `ThreadPoolExecutor` (default
    8 workers, override with `SPARK_OPENIVM_VALIDATE_THREADS`). Every worker
    submits its statements to the SAME single Livy `kind: sql` session —
    matching how dbt-fabricspark drives the session at `threads: 8` during
    the timed dbt run. The result list is returned in input (rowid) order
    so the per-batch JSON layout is stable across runs.
    """
    conn = get_db()
    run = conn.execute(
        "SELECT engine, status FROM runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if not run:
        conn.close()
        raise ValueError(f"run_id not found: {run_id}")
    if run["engine"] != "spark-openivm":
        conn.close()
        raise ValueError(
            f"validation only supports spark-openivm, got {run['engine']}"
        )

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

    # Load the compiled manifest for schema metadata (the dbt-server caches
    # this so repeated lookups are cheap). Imported here to keep the
    # duckdb-openivm-only `dbt_compiler` dependency optional.
    from services.dbt_compiler import get_compiled_models

    compiled_models = get_compiled_models("spark-openivm")

    # Pre-filter to the validatable model set so the thread pool only sees
    # eligible nodes; preserves input rowid order via the input list.
    work: list[dict[str, Any]] = []
    for node in nodes:
        if node["resource_type"] != "model" or node["status"] not in (
            "success", "pass",
        ):
            continue
        compiled_sql = (node["compiled_sql"] or "").strip().rstrip(";")
        if not compiled_sql:
            continue
        meta = compiled_models.get(node["unique_id"], {})
        work.append({
            "unique_id": node["unique_id"],
            "name": node["name"],
            "schema": meta.get("schema") or "default",
            "compiled_sql": compiled_sql,
        })

    started = time.monotonic()

    with LivyClient() as livy:
        # Set the lakehouse as the default catalog/database so the
        # `<schema>.<name>` references in compiled SQL resolve identically
        # to how dbt wrote them. Runs ONCE on the shared session before
        # the worker pool fans out so every concurrent statement inherits
        # the same default database.
        try:
            livy.execute(f"USE {LAKEHOUSE}")
        except RuntimeError as e:
            # Some Spark setups expose lakehouse via the default catalog
            # without an explicit USE; surface the error but keep going so
            # the per-model EXCEPT ALL can still trigger.
            logger.warning("[spark-openivm] USE %s failed (continuing): %s",
                           LAKEHOUSE, e)

        # Empty work list short-circuits cleanly.
        if not work:
            results: list[dict[str, Any]] = []
        else:
            # Bound the pool to `min(VALIDATE_THREADS, len(work))` so we
            # never overspawn workers for tiny model counts. The
            # LivyClient docstring documents that concurrent execute()
            # calls are safe on an already-open client.
            max_workers = min(VALIDATE_THREADS, len(work))
            logger.info(
                "[spark-openivm] validating %d models with %d worker(s) "
                "against Livy session %s",
                len(work), max_workers, livy.session_id,
            )
            with ThreadPoolExecutor(
                max_workers=max_workers,
                thread_name_prefix="spark-openivm-validate",
            ) as executor:
                # Submit by input index, then collect in input order
                # (so `results` stays deterministic).
                futures = [
                    executor.submit(
                        _validate_one,
                        livy,
                        unique_id=item["unique_id"],
                        name=item["name"],
                        schema=item["schema"],
                        compiled_sql=item["compiled_sql"],
                    )
                    for item in work
                ]
                results = []
                for idx, fut in enumerate(futures):
                    try:
                        results.append(fut.result())
                    except Exception as exc:
                        # _validate_one already catches per-model errors
                        # and returns a fail entry, but defensively keep
                        # one bad future from killing the rest.
                        item = work[idx]
                        logger.exception(
                            "[spark-openivm] worker crash for %s.%s",
                            item["schema"], item["name"],
                        )
                        results.append({
                            "unique_id": item["unique_id"],
                            "name": item["name"],
                            "schema": item["schema"],
                            "status": "fail",
                            "diff_count": -1,
                            "validation_time_s": 0.0,
                            "error": (f"worker crash: {exc}")[:4000],
                        })

    failures = [r for r in results if r["status"] != "pass"]
    return {
        "run_id": run_id,
        "status": "failed" if failures else "passed",
        "models_checked": len(results),
        "failures": failures,
        "duration_s": round(time.monotonic() - started, 3),
        "results": results,
    }
