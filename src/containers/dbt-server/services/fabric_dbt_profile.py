"""Run dbt with client-side Fabric timings; polling and SQL remain unchanged."""

import functools
import json
import os
import re
import sys
import threading
import time


def instrument(owner, name, emit, *, sql_argument=False):
    original = getattr(owner, name)

    @functools.wraps(original)
    def timed(*args, **kwargs):
        started = time.time_ns()
        tick = time.monotonic_ns()
        outcome = "error"
        try:
            result = original(*args, **kwargs)
            outcome = "ok"
            return result
        finally:
            ended = time.time_ns()
            elapsed = time.monotonic_ns() - tick
            row = {
                "operation": name,
                "start_epoch_ns": started,
                "end_epoch_ns": ended,
                "duration_ns": elapsed,
                "thread": threading.get_ident(),
                "outcome": outcome,
            }
            if sql_argument:
                # Emit only the verb, never SQL text, paths, tokens or headers.
                sql = args[1] if len(args) > 1 else kwargs.get("sql", "")
                sql = re.sub(r"/\*.*?\*/|--[^\n]*", "", str(sql), flags=re.S).strip().upper()
                row["statement_kind"] = next(
                    (kind for kind in (
                        "CREATE MATERIALIZED VIEW", "DROP MATERIALIZED VIEW",
                        "REFRESH MATERIALIZED VIEW", "CREATE OR REPLACE TABLE",
                        "CREATE SCHEMA", "CREATE DATABASE", "SHOW TABLES",
                        "SHOW DATABASES", "DESCRIBE", "INSERT", "SELECT", "SET",
                    ) if sql.startswith(kind)), "OTHER"
                )
            try:
                emit(row)
            except OSError:
                print("Fabric client timing record could not be written", file=sys.stderr)

    setattr(owner, name, timed)


def main():
    from dbt.adapters.fabricspark import connections, livysession
    from dbt.cli.main import dbtRunner

    lock = threading.Lock()
    with open(os.environ["FABRIC_DBT_TIMINGS_PATH"], "a", buffering=1) as output:
        def emit(row):
            # These records overlap: consumers must use intervals, not sum them.
            with lock:
                output.write(json.dumps(row) + "\n")

        for owner, names in (
            (livysession.LivySession, ("create_session", "wait_for_session_start")),
            (livysession.LivyCursor, ("_submitLivyCode", "_getLivyResult")),
        ):
            for name in names:
                instrument(owner, name, emit)
        instrument(livysession.LivyCursor, "execute", emit, sql_argument=True)
        instrument(connections, "get_lakehouse_properties", emit)
        instrument(livysession, "get_headers", emit)
        result = dbtRunner().invoke(sys.argv[1:])
        if result.exception:
            raise result.exception
        return 0 if result.success else 2


if __name__ == "__main__":
    sys.exit(main())
