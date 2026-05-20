"""Connection manager — ALL SQL executes via the OpenIVM CLI binary."""

import logging
import os
import subprocess
import time
from contextlib import contextmanager
from typing import Optional, Tuple

import agate
from dbt.adapters.contracts.connection import AdapterResponse, Connection, ConnectionState
from dbt.adapters.sql import SQLConnectionManager
from dbt_common.exceptions import DbtDatabaseError

logger = logging.getLogger(__name__)

OPENIVM_BIN = os.environ.get("DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")
WORK_DIR = os.environ.get("DUCKDB_OPENIVM_WORK_DIR", "/data/processed/duckdb-openivm")

# Per-CLI-session memory cap and spill directory. Defaults are sized for
# the serial-mode single-engine container (124 GB host, ~100 GB peak
# observed at SF=100). When running in parallel mode, the orchestrator
# overrides DUCKDB_OPENIVM_MEM_LIMIT to a fraction of the per-engine
# container so the four engines don't collectively oversubscribe RAM.
MEM_LIMIT = os.environ.get("DUCKDB_OPENIVM_MEM_LIMIT", "115GB")
TEMP_DIR = os.environ.get(
    "DUCKDB_OPENIVM_TEMP_DIR",
    os.path.join(WORK_DIR, "_tmp"),
)
THREADS = os.environ.get("DUCKDB_OPENIVM_THREADS", "")
PROFILE_REFRESH = os.environ.get("OPENIVM_PROFILE_REFRESH", "0") == "1"


MAX_RETRIES = int(os.environ.get("OPENIVM_MAX_RETRIES", "10"))
RETRY_BACKOFF = float(os.environ.get("OPENIVM_RETRY_BACKOFF", "3.0"))


def _run_cli(sql: str, expect_output: bool = False) -> str:
    """Execute SQL via the OpenIVM DuckDB CLI binary, with retry on lock errors."""
    db_file = os.path.join(WORK_DIR, "openivm.duckdb")
    meta_path = os.path.join(WORK_DIR, "openivm.ducklake")
    data_path = os.path.join(WORK_DIR, "data")
    os.makedirs(TEMP_DIR, exist_ok=True)

    preamble_lines = [
        ".bail on",
        ".timer off",
        f"SET memory_limit='{MEM_LIMIT}';",
        f"SET temp_directory='{TEMP_DIR}';",
    ]
    if THREADS:
        preamble_lines.append(f"SET threads={int(THREADS)};")
    preamble_lines.extend([
        "LOAD openivm;",
    ])
    if PROFILE_REFRESH:
        preamble_lines.append("SET openivm_profile_refresh=true;")
    preamble_lines.extend([
        "SET openivm_cascade_refresh='off';",
        "INSTALL icu; LOAD icu;",
        "INSTALL ducklake; LOAD ducklake;",
        f"ATTACH 'ducklake:sqlite:{meta_path}' AS ducklake "
        f"(DATA_PATH '{data_path}', data_inlining_row_limit 0);",
    ])
    preamble = "\n".join(preamble_lines) + "\n"

    last_error = None
    for attempt in range(MAX_RETRIES + 1):
        proc = subprocess.run(
            [OPENIVM_BIN, db_file],
            input=preamble + sql + "\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=3600,
        )
        if proc.returncode == 0:
            return proc.stdout

        output = proc.stdout or ""
        if "database is locked" in output and attempt < MAX_RETRIES:
            wait = RETRY_BACKOFF * (2 ** attempt)
            logger.warning(
                "OpenIVM CLI hit 'database is locked' (attempt %d/%d), retrying in %.1fs",
                attempt + 1, MAX_RETRIES + 1, wait,
            )
            time.sleep(wait)
            last_error = output
            continue

        raise DbtDatabaseError(f"OpenIVM CLI failed (rc={proc.returncode}):\n{output[-2000:]}")

    raise DbtDatabaseError(f"OpenIVM CLI failed after {MAX_RETRIES + 1} attempts (database locked):\n{last_error[-2000:]}")


class OpenIVMConnectionManager(SQLConnectionManager):
    """Executes all SQL through the OpenIVM CLI binary subprocess."""

    TYPE = "openivm"

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield
        except DbtDatabaseError:
            raise
        except Exception as e:
            raise DbtDatabaseError(str(e))

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection
        # No persistent handle — each execute() is a subprocess call
        connection.handle = "openivm_cli"
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def close(cls, connection: Connection) -> Connection:
        connection.state = ConnectionState.CLOSED
        return connection

    def cancel(self, connection: Connection):
        pass

    def begin(self):
        pass

    def commit(self):
        pass

    @classmethod
    def get_response(cls, cursor) -> AdapterResponse:
        return AdapterResponse(_message="OK", code="SUCCESS")

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: Optional[int] = None,
    ) -> Tuple[AdapterResponse, agate.Table]:
        sql_stripped = sql.strip()
        if not sql_stripped or sql_stripped == ";":
            return AdapterResponse(_message="OK", code="SUCCESS"), agate.Table(rows=[])

        logger.debug("OpenIVM execute: %s", sql_stripped[:120])

        t0 = time.monotonic()
        output = _run_cli(sql_stripped)
        elapsed = time.monotonic() - t0

        logger.debug("OpenIVM result (%.2fs): %s", elapsed, output[:200])

        response = AdapterResponse(
            _message=f"OK ({elapsed:.2f}s)",
            rows_affected=0,
            code="SUCCESS",
        )

        # Parse tabular output if fetch requested
        if fetch and output.strip():
            table = self._parse_cli_output(output)
        else:
            table = agate.Table(rows=[])

        return response, table

    def _parse_cli_output(self, output: str) -> agate.Table:
        """Parse pipe-delimited CLI output into an agate Table."""
        lines = [l.strip() for l in output.strip().split("\n") if l.strip()]
        if not lines:
            return agate.Table(rows=[])

        # DuckDB CLI outputs pipe-delimited tables with a header separator
        # Format: col1|col2\n---|---\nval1|val2
        if len(lines) >= 2 and all(c in "-|+ " for c in lines[1]):
            headers = [h.strip() for h in lines[0].split("|")]
            rows = []
            for line in lines[2:]:
                vals = [v.strip() for v in line.split("|")]
                rows.append(dict(zip(headers, vals)))
            return agate.Table.from_object(rows) if rows else agate.Table(rows=[], column_names=headers)

        return agate.Table(rows=[])
