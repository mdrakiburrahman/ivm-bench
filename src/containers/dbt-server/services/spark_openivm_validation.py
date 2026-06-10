"""OpenIVM correctness validation for the `spark-openivm` engine.

Mirrors `openivm_validation.validate_run` from the `duckdb-openivm` path but
executes the validation SQL through a Livy session against Spark, so the
correctness check covers everything `openivm.duckdb_extension` emitted into
each MV's openivm_data_* Delta table and surfaced through the user-facing
view.

For every successful dbt model node from the timed `run_id`, we:

  1. Build the validation SQL:

         CREATE OR REPLACE TEMPORARY VIEW openivm_expected_N AS <compiled_sql>;
         SELECT COUNT(*) AS __ivm_cnt,
                COALESCE(SUM(xxhash64(c1, ..., cN)), 0L) AS __ivm_hash
           FROM <relation>;
         SELECT COUNT(*) AS __ivm_cnt,
                COALESCE(SUM(xxhash64(c1, ..., cN)), 0L) AS __ivm_hash
           FROM openivm_expected_N;
         DROP VIEW IF EXISTS openivm_expected_N;

     where `<relation>` is the MV's `<schema>.<name>` Spark relation (the
     same one dbt's materialization produced via
     `CREATE MATERIALIZED VIEW`).

     We use an order-independent multiset digest
     (``(COUNT(*), SUM(xxhash64(*)))``) — single-pass scans on each side
     with constant driver memory. Earlier revisions ran a paired
     ``EXCEPT ALL`` followed by ``COUNT(*)``, but that shape OOMs the 12g
     Spark driver at SF>=175 (the LeftAnti build side has to hash the
     entire bag), and the resulting Livy session crash surfaces as 5x
     ``diff_count=-1`` worker-crash sentinels for every subsequent model.

  2. Execute the four statements over Livy, extract the (count, hash)
     pair from each digest SELECT.

  3. Record a per-model `pass` / `fail` row keyed by
     ``(mv_count, mv_hash) == (expected_count, expected_hash)``. On
     mismatch we set ``diff_count`` to a non-zero lower bound
     (``max(|mv_count - expected_count|, 1)``) so the existing
     ``diff_count == 0`` pass test, the orchestrator "diff=N" log line,
     and the JSON report's ``status`` field all keep their meaning.

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
# session already handles up to dbt-fabricspark's `threads: 30` of concurrent
# statement submission during the timed dbt run, so 30 is the documented safe
# default (kept in step with profiles.yml so post-batch validation drives the
# session at the same concurrency as the timed dbt run). Set to 1 to fall
# back to strictly serial validation against the same single Livy session
# (still benefits from the lower polling interval).
VALIDATE_THREADS = max(1, int(os.environ.get("SPARK_OPENIVM_VALIDATE_THREADS", "30")))


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


def _extract_two_longs(output: dict[str, Any], label: str) -> tuple[int, int]:
    """Parse a (count, hash) pair from the Livy SQL statement output.

    Used for the order-independent multiset digest
    ``SELECT COUNT(*), COALESCE(SUM(xxhash64(c1, ..., cN)), 0) FROM ...``.

    Returns ``(count, hash_sum)`` as Python ints.
    """
    data = (output or {}).get("data") or {}
    payload = data.get("application/json") or data.get("text/plain") or ""

    if isinstance(payload, dict):
        rows = payload.get("data") or []
        if rows and len(rows[0]) >= 2:
            return int(rows[0][0]), int(rows[0][1])
        raise RuntimeError(
            f"{label}: Livy returned no (count, hash) row in JSON output: {payload}"
        )

    # `text/plain` ASCII table. The data row sits between two horizontal
    # rules and is the only row whose first cell is a signed integer.
    for line in str(payload).splitlines():
        line = line.strip()
        if not line.startswith("|"):
            continue
        cells = [c.strip() for c in line.strip("|").split("|") if c.strip()]
        if len(cells) < 2:
            continue
        try:
            return int(cells[0]), int(cells[1])
        except ValueError:
            continue
    raise RuntimeError(
        f"{label}: Livy returned no (count, hash) pair:\n{str(payload)[-1000:]}"
    )



def _extract_columns(output: dict[str, Any], label: str) -> list[str]:
    """Parse the first column of every result row out of a Livy SQL output.

    Used to consume `DESCRIBE <relation>` / `SHOW COLUMNS …` so the
    validation SQL can project only the user-visible columns and drop the
    `openivm_*` bookkeeping that openivm-spark's incremental refresh
    types leave on the user-facing object (the AGGREGATE_HAVING path
    already creates a view that hides them; AGGREGATE_GROUP /
    SIMPLE_AGGREGATE / SIMPLE_PROJECTION do not).
    """
    return [name for name, _ in _extract_columns_with_types(output, label)]


def _extract_columns_with_types(
    output: dict[str, Any], label: str
) -> list[tuple[str, str]]:
    """Parse (col_name, col_type) pairs from a Livy DESCRIBE output.

    Same parsing topology as `_extract_columns`, but also captures the
    `data_type` column so the digest projection can build type-aware
    casts. Required to neutralize Spark `xxhash64` type-metadata
    sensitivity: an MV column declared `DOUBLE` and an expected-recompute
    column declared `DECIMAL(29,4)` carry behaviorally-equivalent values
    but hash differently because Catalyst folds the schema type into the
    null/value hash. Canonicalising both sides to `DOUBLE` (numerics)
    before hashing makes the digest type-insensitive without weakening
    value-level checking — see `_build_canonical_projection`.
    """
    data = (output or {}).get("data") or {}
    payload = data.get("application/json") or data.get("text/plain") or ""
    pairs: list[tuple[str, str]] = []

    if isinstance(payload, dict):
        for row in payload.get("data") or []:
            if not row:
                continue
            cname = str(row[0])
            ctype = str(row[1]) if len(row) > 1 else ""
            pairs.append((cname, ctype))
        return pairs

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
            seen_header = True
            continue
        if not first or first.startswith("-") or first.startswith("col_name"):
            continue
        if first.startswith("#"):
            break
        ctype = cells[1] if len(cells) > 1 else ""
        pairs.append((first, ctype))
    return pairs


_NUMERIC_TYPE_PREFIXES = (
    "tinyint",
    "smallint",
    "integer",
    "int",
    "bigint",
    "long",
    "short",
    "byte",
    "float",
    "double",
    "decimal",
    "numeric",
)


def _is_numeric_type(spark_type: str) -> bool:
    """True iff Spark's DESCRIBE type label denotes a numeric type.

    The DESCRIBE output uses Spark-SQL type labels like `double`,
    `bigint`, `decimal(29,4)`, `int`, etc. Match a prefix list rather
    than parse the full grammar — we only need to know whether to
    canonicalize the column to DOUBLE before hashing, so we don't
    care about the precision/scale.
    """
    t = (spark_type or "").strip().lower()
    return any(t == prefix or t.startswith(prefix + "(") or t.startswith(prefix + " ")
               or t == prefix for prefix in _NUMERIC_TYPE_PREFIXES)


def _build_canonical_projection(
    user_cols: list[str],
    mv_types: dict[str, str],
    exp_types: dict[str, str],
) -> tuple[str, list[str]]:
    """Build a canonical projection used to neutralize Spark `xxhash64`
    type-metadata sensitivity between the MV's stored schema and the
    expected-recompute schema.

    openivm-spark's CTAS path can fix a MV column's declared type to
    `DOUBLE` (e.g. when LPTS lowers a DECIMAL-arithmetic expression
    through a Spark cast at initial-load time). The user query, when
    re-analysed at validation time, may yield `DECIMAL(p,s)` for the
    same column. Both representations carry equivalent observable
    values for any application that doesn't reflect on column types,
    but Spark's `xxhash64` folds the schema type into the per-column
    hash — so a behaviorally-correct MV would otherwise be flagged.

    Strategy (narrowed per rubber-duck critique):
      * For each user column, only canonicalize when BOTH sides declare
        a numeric type AND the type labels differ. That covers the known
        `DOUBLE` vs `DECIMAL(p,s)` drift without conflating
        numeric-vs-non-numeric mismatches (which are real schema bugs
        and should surface as validation failures).
      * Casting MV-DOUBLE → DOUBLE is a no-op; casting Expected-DECIMAL
        → DOUBLE yields the same canonical 64-bit representation the MV
        would have produced via the same source expression at INSERT.
      * Non-numeric columns and matching-type columns pass through
        unchanged. Numeric-vs-non-numeric drift is left in place so the
        digest fails loudly.

    Returns:
      ``(inner_proj, hash_args)`` — ``inner_proj`` is used in the
      ``SELECT … FROM <relation>`` subquery (with ``AS alias`` to
      preserve column names through the cast), and ``hash_args`` is the
      bare comma-separated column list used as arguments to
      ``xxhash64(...)`` in the outer aggregate. They must be kept
      separate because ``AS alias`` is not legal inside a function-call
      argument list.
    """
    inner_parts: list[str] = []
    for c in user_cols:
        mt = mv_types.get(c, "")
        et = exp_types.get(c, "")
        same = mt.strip().lower() == et.strip().lower()
        both_numeric = _is_numeric_type(mt) and _is_numeric_type(et)
        if not same and both_numeric:
            inner_parts.append(
                f"CAST({_quote_ident(c)} AS DOUBLE) AS {_quote_ident(c)}"
            )
        else:
            inner_parts.append(_quote_ident(c))
    hash_args = [_quote_ident(c) for c in user_cols]
    return ", ".join(inner_parts), hash_args


def _detect_schema_drift(
    user_cols: list[str],
    mv_types: dict[str, str],
    exp_types: dict[str, str],
) -> list[dict[str, str]]:
    """Return per-column entries for every user column whose declared
    type differs between MV and expected recompute.

    Recorded in the validation JSON so a behaviorally-correct MV (digest
    pass) still surfaces schema drift for downstream consumers that
    care about types — e.g. dbt strict-typing, ORM column reflection.
    The bench treats digest failures as fatal but schema drift as
    informational; the engine fix is to make openivm-spark's CTAS
    emit the same column types Spark's analyzer infers from the
    user query.
    """
    drift: list[dict[str, str]] = []
    for c in user_cols:
        mt = mv_types.get(c, "")
        et = exp_types.get(c, "")
        if mt.strip().lower() != et.strip().lower():
            drift.append({"column": c, "mv_type": mt, "expected_type": et})
    return drift








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
    schema_drift: list[dict[str, str]] = []
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
        mv_col_pairs = _extract_columns_with_types(
            describe_mv_out.get("output") or {},
            f"describe {schema}.{name}",
        )
        mv_cols = [c for c, _ in mv_col_pairs]
        mv_types = {c: t for c, t in mv_col_pairs}
        describe_exp_out = livy.execute(f"DESCRIBE {expected_name}")
        exp_col_pairs = _extract_columns_with_types(
            describe_exp_out.get("output") or {},
            f"describe {expected_name}",
        )
        expected_cols = [c for c, _ in exp_col_pairs]
        expected_types = {c: t for c, t in exp_col_pairs}
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
        # Type-aware projection: canonicalize numeric type drift between
        # the MV's stored schema (which openivm-spark may have widened to
        # DOUBLE during initial CTAS) and the user query's recomputed
        # schema (which Spark may analyze as DECIMAL(p,s)). Spark's
        # xxhash64 includes type-metadata in its hash, so a DOUBLE NULL
        # and a DECIMAL(29,4) NULL would hash differently for the same
        # behavioral value — causing a false-positive diff on a MV that
        # is correct on every observable axis. Casting both sides' numeric
        # columns to DOUBLE neutralizes this without weakening real
        # value-level checking: ULP-level float drift, STDDEV/AVG shuffle
        # nondeterminism, and row-count mismatches all still surface.
        #
        # The inner projection carries `CAST(... AS DOUBLE) AS <col>` so
        # the subquery yields columns of canonical type and original
        # name. The outer xxhash64 takes bare column names — `AS alias`
        # is not legal inside function arguments. Keep these two SQL
        # fragments separate.
        inner_proj, hash_args = _build_canonical_projection(
            user_cols, mv_types, expected_types
        )
        hash_arg_sql = ", ".join(hash_args)
        mv_subquery = f"SELECT {inner_proj} FROM {relation}"
        expected_subquery = f"SELECT {inner_proj} FROM {expected_name}"

        # Schema drift (declared-type mismatches) is recorded separately
        # so a digest-pass MV still surfaces declared-type drift in the
        # forensics JSON. Downstream consumers that care about types
        # (dbt strict typing, ORM reflection) can fail on this report
        # even when the value digest passes.
        schema_drift = _detect_schema_drift(
            user_cols, mv_types, expected_types
        )

        # The raw column projection (no canonical casts) is used for the
        # sample-rows output on a failure so JSON forensics show the
        # actual stored types and values, not the canonical-cast view.
        raw_proj = ", ".join(_quote_ident(c) for c in user_cols)
        mv_raw_subquery = f"SELECT {raw_proj} FROM {relation}"
        expected_raw_subquery = f"SELECT {raw_proj} FROM {expected_name}"

        # Order-independent multiset equality via (COUNT(*), SUM(xxhash64(*)))
        # digest. Replaces the previous bag-EXCEPT-ALL approach, which OOMs
        # the 12g Spark driver at SF>=175 (both sides of EXCEPT ALL must hash
        # the full bag, and the SELECT COUNT(*) wrapper still materializes
        # the anti-join). The single-pass aggregation is O(scan) on each
        # side with constant memory, and xxhash64 fingerprints any ULP-level
        # column difference (so STDDEV/AVG float drift surfaces as a hash
        # mismatch just like EXCEPT ALL would have flagged).
        #
        # `xxhash64` arguments must be bare column references — `AS alias`
        # is not legal inside a function-call argument list. The inner
        # subquery does the canonicalising CASTs (with aliases that
        # preserve the original column names), and the outer aggregate
        # hashes by name.
        digest_select = (
            f"SELECT COUNT(*) AS __ivm_cnt, "
            f"COALESCE(SUM(xxhash64({hash_arg_sql})), 0L) AS __ivm_hash"
        )
        mv_digest_sql = f"{digest_select} FROM ({mv_subquery}) AS __ivm_mv"
        expected_digest_sql = (
            f"{digest_select} FROM ({expected_subquery}) AS __ivm_exp"
        )
        mv_out = livy.execute(mv_digest_sql)
        mv_cnt, mv_hash = _extract_two_longs(
            mv_out.get("output") or {}, label + " mv_digest"
        )
        exp_out = livy.execute(expected_digest_sql)
        exp_cnt, exp_hash = _extract_two_longs(
            exp_out.get("output") or {}, label + " expected_digest"
        )
        if mv_cnt == exp_cnt and mv_hash == exp_hash:
            diff_count = 0
        else:
            # Surface a non-zero "diff_count" without doing an EXCEPT ALL
            # (which would OOM at this scale). When counts differ, the
            # cardinality delta is a strict lower bound on the symmetric
            # difference; when only the hash differs, report 1 row as a
            # symbolic "at least one row differs".
            diff_count = max(abs(mv_cnt - exp_cnt), 1)
        # On mismatch, capture up to 5 sample rows from each side so the
        # JSON report is useful for root-cause work (no live cluster
        # needed). Use plain LIMIT-5 SELECTs — bounded memory, no
        # EXCEPT ALL — so a failure here cannot OOM the Livy session and
        # break validation of subsequent models.
        if diff_count != 0:
            try:
                sample_sql = (
                    f"SELECT 'mv' AS side, * FROM "
                    f"({mv_raw_subquery}) AS __ivm_mv_sample LIMIT 5"
                )
                mv_extra = livy.execute(sample_sql)
                sample_payload_mv = (
                    (mv_extra.get("output") or {}).get("data") or {}
                ).get("application/json") or {}
                sample_sql2 = (
                    f"SELECT 'expected' AS side, * FROM "
                    f"({expected_raw_subquery}) AS __ivm_exp_sample LIMIT 5"
                )
                exp_extra = livy.execute(sample_sql2)
                sample_payload_exp = (
                    (exp_extra.get("output") or {}).get("data") or {}
                ).get("application/json") or {}
                sample_payload = {
                    "mv_sample": sample_payload_mv,
                    "expected_sample": sample_payload_exp,
                    "user_cols": user_cols,
                    "mv_cols": mv_cols,
                    "mv_types": mv_types,
                    "expected_cols": expected_cols,
                    "expected_types": expected_types,
                    "schema_drift": schema_drift,
                    "mv_count": mv_cnt,
                    "expected_count": exp_cnt,
                    "mv_hash": mv_hash,
                    "expected_hash": exp_hash,
                    "diff_kind": (
                        "cardinality" if mv_cnt != exp_cnt else "values_only"
                    ),
                }
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
    if schema_drift:
        # Declared-type drift between MV and recomputed expected view
        # is informational on pass (digest still matched after numeric
        # canonicalization) and additive on fail (so the JSON shows
        # both the value diff AND the underlying schema mismatch).
        # This surfaces the engine-side defect — openivm-spark's CTAS
        # should land the same column types Spark analyzes from the
        # user query — without weakening per-MV value validation.
        entry["schema_drift"] = schema_drift
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
    30 workers, override with `SPARK_OPENIVM_VALIDATE_THREADS`). Every worker
    submits its statements to the SAME single Livy `kind: sql` session —
    matching how dbt-fabricspark drives the session at `threads: 30` during
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
