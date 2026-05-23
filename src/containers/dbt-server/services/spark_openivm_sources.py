"""spark-openivm source-table management.

Routes batch1 init + batch{2,3} append through Spark SQL DML over Livy.
Each `INSERT INTO <tracked_delta_table> SELECT * FROM delta.\\`<batchN_path>\\``
is intercepted by openivm-spark's `IvmDmlInterceptorRule`, which tees the
change set to the staging Delta — that's what `REFRESH MATERIALIZED VIEW`
consumes during batches 2/3.

Why not the spark-batch-loader's path-based writes? Because
`df.write.format("delta").mode("append").save(path)` bypasses Spark SQL DML
entirely, so the interceptor never sees the change. We MUST go through
`INSERT INTO` so the rule fires.

Why a separate Livy session from the dbt run's? The init/append happens
BEFORE the first dbt build (or BETWEEN builds), and the fabricspark adapter
manages its own per-run session.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Iterable, List

import requests

logger = logging.getLogger(__name__)

LIVY_URL = os.environ.get("SPARK_OPENIVM_LIVY_URL", "http://spark-openivm:8998")
RAW_DELTA_DIR = os.environ.get("RAW_DELTA_DIR", "/data/raw/delta")
WORK_DIR = os.environ.get(
    "SPARK_OPENIVM_WORK_DIR", "/data/processed/spark-openivm"
)
SOURCES_DIR = os.path.join(WORK_DIR, "sources")

# Tracked base tables in three categories:
#
#  1. batch1_*           — reference tables, populated once from batch1/* and
#                          never appended after that.
#  2. staging_*          — fact-like / event-like tables that grow across
#                          batches 1..3.
#  3. audit              — bookkeeping; populated once like batch1.
BATCH1_TABLES: List[str] = [
    "customer_mgmt",
    "date",
    "finwire",
    "hr",
    "industry",
    "status_type",
    "tax_rate",
    "trade_history",
    "trade_type",
]

STAGING_TABLES: List[str] = [
    "cash_transaction",
    "daily_market",
    "holding_history",
    "prospect",
    "trade",
    "watch_history",
    "account",
    "customer",
    "batch_date",
]


def _all_tables() -> List[tuple[str, str, str]]:
    """Yield (logical_name, src_delta_subdir, tpcdi_table_name)."""
    out: List[tuple[str, str, str]] = []
    for t in BATCH1_TABLES:
        # bronze model sources reference batch1_<t>; the raw Delta lives at
        # /data/raw/delta/batch1/<t>.
        out.append((t, f"batch1/{t}", f"batch1_{t}"))
    for t in STAGING_TABLES:
        # spark-batch-loader's `init` mode populates /data/raw/delta/staging/<t>
        # with the CDC-shaped initial rows already. We then point our tracked
        # table at that initial load.
        out.append((t, f"staging/{t}", f"staging_{t}"))
    out.append(("audit", "audit", "audit"))
    return out


# ---------------------------------------------------------------------------
# Livy SQL client
# ---------------------------------------------------------------------------


class LivyClient:
    """Tiny synchronous Livy SQL client.

    Opens a `sql` kind session, runs statements one-by-one (so multi-statement
    parsing isn't an issue and each `INSERT INTO` is its own AppendData plan
    for `IvmDmlInterceptorRule`), then closes the session.
    """

    def __init__(self, base_url: str = LIVY_URL, timeout_s: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session_id: int | None = None

    # ----- session lifecycle -----

    def open(self) -> None:
        body = {"kind": "sql", "name": "spark-openivm-sources"}
        resp = requests.post(
            f"{self.base_url}/sessions",
            json=body,
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        self.session_id = resp.json()["id"]
        logger.info("[spark-openivm] opened Livy SQL session %d", self.session_id)
        self._wait_for_state({"idle"}, kind="session")

    def close(self) -> None:
        if self.session_id is None:
            return
        try:
            requests.delete(
                f"{self.base_url}/sessions/{self.session_id}",
                timeout=self.timeout_s,
            )
            logger.info("[spark-openivm] closed Livy SQL session %d", self.session_id)
        except Exception:
            logger.exception("[spark-openivm] failed to close Livy session")
        self.session_id = None

    # ----- statement execution -----

    def execute(self, sql: str) -> dict:
        """Submit a SINGLE SQL statement and block until it succeeds.

        Raises RuntimeError if Livy reports the statement failed.
        """
        if self.session_id is None:
            raise RuntimeError("Livy session not opened")

        # Livy's `kind: sql` session expects plain SQL in the `code` field.
        resp = requests.post(
            f"{self.base_url}/sessions/{self.session_id}/statements",
            json={"code": sql},
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        stmt_id = resp.json()["id"]
        return self._wait_for_statement(stmt_id, sql)

    def execute_many(self, stmts: Iterable[str]) -> None:
        """Convenience: run statements sequentially, fail-fast."""
        for sql in stmts:
            sql = sql.strip().rstrip(";")
            if not sql:
                continue
            self.execute(sql)

    # ----- waiters -----

    def _wait_for_state(
        self,
        target_states: set[str],
        kind: str,
        timeout_s: float = 600.0,
    ) -> dict:
        deadline = time.time() + timeout_s
        url = f"{self.base_url}/sessions/{self.session_id}"
        while time.time() < deadline:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            data = resp.json()
            state = data.get("state")
            if state in target_states:
                return data
            if state in ("error", "dead", "killed", "shutting_down"):
                raise RuntimeError(
                    f"Livy {kind} {self.session_id} entered terminal state '{state}'"
                )
            time.sleep(1)
        raise TimeoutError(
            f"Livy {kind} {self.session_id} did not reach {target_states} within {timeout_s}s"
        )

    def _wait_for_statement(self, stmt_id: int, sql: str) -> dict:
        """Poll a statement until it succeeds; surface errors verbatim."""
        url = f"{self.base_url}/sessions/{self.session_id}/statements/{stmt_id}"
        # No fixed deadline — long INSERT INTO ... SELECT FROM delta.`path` for
        # the full staging may take minutes at SF=100. Statements that
        # genuinely hang are caught by the caller's request timeout.
        while True:
            resp = requests.get(url, timeout=self.timeout_s)
            resp.raise_for_status()
            data = resp.json()
            state = data.get("state")
            if state in ("available", "completed"):
                output = data.get("output") or {}
                if output.get("status") == "error":
                    ename = output.get("ename", "Error")
                    evalue = output.get("evalue", "")
                    traceback = "".join(output.get("traceback") or [])
                    # Keep the head of the error (the actual exception
                    # message lives there). Trim the JVM traceback tail.
                    head = f"{ename}: {evalue}"
                    if len(traceback) > 1500:
                        traceback = traceback[:1500] + "...<traceback truncated>"
                    raise RuntimeError(
                        f"Livy statement failed.\nSQL: {sql[:2000]}\n{head}\n{traceback}"
                    )
                return data
            if state in ("cancelled", "cancelling"):
                raise RuntimeError(
                    f"Livy statement cancelled.\nSQL: {sql[:500]}"
                )
            time.sleep(1)

    def __enter__(self) -> "LivyClient":
        self.open()
        return self

    def __exit__(self, *exc) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def init_sources() -> dict:
    """Create database + tracked Delta tables and load batch1 data via DML.

    Idempotent at the database+table level. Re-running after a partial failure
    will skip CREATE IF NOT EXISTS but will append duplicate rows to the
    INSERT statements — caller (benchmark orchestrator) is expected to clean
    the work dir before re-running.
    """
    statements: List[str] = ["CREATE DATABASE IF NOT EXISTS tpcdi"]

    tables_created = 0
    for _logical, src_subdir, tname in _all_tables():
        src_path = os.path.join(RAW_DELTA_DIR, src_subdir)
        dst_path = os.path.join(SOURCES_DIR, tname)
        statements.append(
            f"CREATE TABLE IF NOT EXISTS tpcdi.{tname} "
            f"USING DELTA LOCATION '{dst_path}' AS "
            f"SELECT * FROM delta.`{src_path}`"
        )
        tables_created += 1

    with LivyClient() as livy:
        livy.execute_many(statements)

    logger.info("[spark-openivm] init complete: %d tables", tables_created)
    return {"status": "ok", "tables_created": tables_created}


def append_sources(batch_num: int) -> dict:
    """INSERT new batch{N} rows into each staging table.

    Routed through Spark SQL DML so `IvmDmlInterceptorRule` fires and tees
    the change set to the staging Delta. The interceptor only tees tables
    that have at least one dependent MV (`MvCatalog.viewsForSource(...)`),
    so the dbt batch-1 full-refresh MUST have run before any append.
    """
    if batch_num not in (2, 3):
        raise ValueError(f"append_sources only supports batch 2 or 3, got {batch_num}")

    statements: List[str] = []

    # The staging_* tables are the only ones that grow. batch1_* references
    # are immutable; audit is also immutable.
    tables_appended = 0
    for t in STAGING_TABLES:
        src_path = os.path.join(RAW_DELTA_DIR, f"batch{batch_num}", t)
        # The batch-loader generates new files at this path during the
        # per-engine batch-loader append phase. If the path is absent, we
        # just skip — matches the no-op semantics in BatchLoader.scala.
        if not os.path.isdir(src_path):
            logger.info(
                "[spark-openivm] batch %d: skipping %s (no data)", batch_num, t
            )
            continue
        statements.append(
            f"INSERT INTO tpcdi.staging_{t} "
            f"SELECT * FROM delta.`{src_path}`"
        )
        tables_appended += 1

    if not statements:
        return {"status": "ok", "tables_appended": 0}

    with LivyClient() as livy:
        livy.execute_many(statements)

    logger.info(
        "[spark-openivm] batch %d append: %d tables", batch_num, tables_appended
    )
    return {"status": "ok", "tables_appended": tables_appended}
