"""spark-openivm source-table management.

Routes batch1 init + batch{2,3} append through Spark SQL DML over Livy.
The tracked Delta tables are registered with `delta.enableChangeDataFeed=true`
so each `INSERT INTO` produces Change Data Feed records. Under
`spark.openivm.changeFeed.mode=cdf` the next `REFRESH MATERIALIZED VIEW`
reads those CDF records from each source to incrementalize the dependent MVs.

Why a separate Livy session from the dbt run's? The init/append happens
BEFORE the first dbt build (or BETWEEN builds), and the fabricspark adapter
manages its own per-run session.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

LIVY_URL = os.environ.get("SPARK_OPENIVM_LIVY_URL", "http://spark-openivm:8998")
SESSION_ID_FILE = os.environ.get(
    "SPARK_OPENIVM_LIVY_SESSION_ID_FILE", "/tmp/spark-openivm-livy.session-id"
)
RAW_DELTA_DIR = os.environ.get("RAW_DELTA_DIR", "/data/raw/delta")
WORK_DIR = os.environ.get(
    "SPARK_OPENIVM_WORK_DIR", "/data/processed/spark-openivm"
)
SOURCES_DIR = os.path.join(WORK_DIR, "sources")

# Per-statement / per-state polling intervals. Defaults are tuned for the
# concurrent-validation path: 0.1s makes the per-statement floor cheap enough
# that 5 sequential statements/model (CREATE TEMP VIEW, DESCRIBE×2,
# COUNT-EXCEPT, DROP) is not dominated by polling sleeps. Overridable so we
# can dial up if Livy ever rate-limits the HTTP GETs.
LIVY_STMT_POLL_INTERVAL_S = float(
    os.environ.get("SPARK_OPENIVM_LIVY_POLL_INTERVAL", "0.1")
)
LIVY_STATE_POLL_INTERVAL_S = float(
    os.environ.get("SPARK_OPENIVM_LIVY_STATE_POLL_INTERVAL", "0.25")
)

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

    Attaches to dbt-fabricspark's long-lived Livy session via the
    session_id_file pinned in profiles.yml. This keeps all openivm RocksDB
    catalog access on ONE driver JVM, which is correctness-critical when
    `spark.openivm.rocksdb.multiProcess=false` (the fast path).

    Runs statements one-by-one so multi-statement parsing isn't an issue and
    each `INSERT INTO` produces a discrete Delta commit (and therefore a
    discrete CDF record range) for the REFRESH path to consume.

    Thread-safety: after `open()` completes, `execute()` is safe to call
    concurrently from multiple threads against a single client instance.
    `session_id`, `base_url`, and `timeout_s` are read-only once `open()`
    returns; `requests` is thread-safe per call. The validation path uses
    this to fan out per-model statements via a `ThreadPoolExecutor` while
    every call still terminates on the SAME Livy session (and therefore the
    same Spark driver / RocksDB catalog). Do NOT call `close()` while
    workers are in flight — `executor.shutdown(wait=True)` first.
    """

    def __init__(self, base_url: str = LIVY_URL, timeout_s: float = 60.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.session_id: str | None = None
        self._owned: bool = False

    # ----- session lifecycle -----

    def open(self) -> None:
        existing_id = self._read_session_id()
        if existing_id is not None and self._try_attach(existing_id):
            self.session_id = existing_id
            self._owned = False
            logger.info(
                "[spark-openivm] attached to existing Livy session %s", existing_id
            )
            return

        body = {"kind": "sql", "name": "spark-openivm"}
        resp = requests.post(
            f"{self.base_url}/sessions",
            json=body,
            timeout=self.timeout_s,
        )
        resp.raise_for_status()
        self.session_id = str(resp.json()["id"])
        self._owned = True
        self._wait_for_state({"idle"}, kind="session")
        self._write_session_id(self.session_id)
        logger.info("[spark-openivm] created Livy session %s (owned)", self.session_id)

    def close(self) -> None:
        if self.session_id is None:
            return
        if not self._owned:
            logger.info(
                "[spark-openivm] leaving attached Livy session %s alive", self.session_id
            )
            self.session_id = None
            return
        try:
            requests.delete(
                f"{self.base_url}/sessions/{self.session_id}",
                timeout=self.timeout_s,
            )
            try:
                os.unlink(SESSION_ID_FILE)
            except FileNotFoundError:
                pass
        except Exception:
            logger.exception("[spark-openivm] failed to close owned Livy session")
        self.session_id = None
        self._owned = False

    def _read_session_id(self) -> Optional[str]:
        try:
            with open(SESSION_ID_FILE, "r", encoding="utf-8") as fh:
                session_id = fh.read().strip()
        except OSError:
            return None
        return session_id or None

    def _try_attach(self, session_id: str) -> bool:
        try:
            resp = requests.get(
                f"{self.base_url}/sessions/{session_id}",
                timeout=min(self.timeout_s, 10.0),
            )
        except requests.RequestException:
            return False
        if resp.status_code != 200:
            return False
        try:
            state = resp.json().get("state")
        except ValueError:
            return False
        return state in {"idle", "busy", "starting"}

    def _write_session_id(self, session_id: str) -> None:
        tmp_path = f"{SESSION_ID_FILE}.tmp"
        parent = os.path.dirname(SESSION_ID_FILE)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as fh:
            fh.write(session_id)
        os.replace(tmp_path, SESSION_ID_FILE)

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
            time.sleep(LIVY_STATE_POLL_INTERVAL_S)
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
            time.sleep(LIVY_STMT_POLL_INTERVAL_S)

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
            f"USING DELTA LOCATION '{dst_path}' "
            f"TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true') AS "
            f"SELECT * FROM delta.`{src_path}`"
        )
        tables_created += 1

    with LivyClient() as livy:
        livy.execute_many(statements)

    logger.info("[spark-openivm] init complete: %d tables", tables_created)
    return {"status": "ok", "tables_created": tables_created}


def append_sources(batch_num: int) -> dict:
    """INSERT new batch{N} rows into each staging table.

    The benchmark engine runs in `spark.openivm.changeFeed.mode=cdf`, so the
    INSERT writes ordinary Delta rows (plus CDF records, because the staging
    tables were registered with `delta.enableChangeDataFeed=true`). The next
    REFRESH consumes those CDF records to incrementalize the MVs — no DML
    interception in this mode.
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
