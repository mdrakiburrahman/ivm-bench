"""Run dbt with client-side Fabric timings; polling and SQL remain unchanged."""

import functools
import json
import os
import re
import sys
import threading
import time


def instrument_session_polls(requests, emit):
    """Observe existing startup requests without logging the response payload."""
    original = requests.get

    @functools.wraps(original)
    def observed(url, *args, **kwargs):
        response = original(url, *args, **kwargs)
        if re.search(r"/livyapi/versions/[^/]+/sessions/[^/?]+$", str(url)):
            try:
                body = response.json()
                tags = body.get("tags") or {}
                info = body.get("livyInfo") or {}
                conf = (info.get("jobCreationRequest") or {}).get("conf") or {}
                # Messages can contain user configuration: retain setting names only.
                row = {
                    "operation": "session_poll",
                    "epoch_ns": time.time_ns(),
                    "fallback_info_present": "FallbackReasons" in tags or "FallbackMessages" in tags,
                    "fallback_reasons": re.findall(
                        r"[A-Za-z][A-Za-z0-9]+", str(tags.get("FallbackReasons", ""))
                    ),
                    "fallback_spark_settings": sorted(set(re.findall(
                        r"\bspark\.[A-Za-z0-9_.]+", str(tags.get("FallbackMessages", ""))
                    ))),
                    "idle_timeout_present": "spark.livy.session.idle.timeout" in conf if conf else None,
                    "state": body.get("state"),
                    "livy_state": info.get("currentState"),
                }
                emit(row)
            except (ValueError, TypeError, AttributeError, OSError):
                print("Fabric session diagnostic record could not be written", file=sys.stderr)
        return response

    requests.get = observed


def instrument(owner, name, emit, *, sql_argument=False):
    original = getattr(owner, name)

    @functools.wraps(original)
    def timed(*args, **kwargs):
        started = time.time_ns()
        tick = time.monotonic_ns()
        outcome = "error"
        result = None
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
            if name == "_getLivyResult" and isinstance(result, dict):
                response = result.get("output", {})
                if response.get("status") == "error":
                    row["outcome"] = "sql_error"
                    # Keep only Java frame signatures; never copy arbitrary error/SQL text.
                    trace = "\n".join(response.get("traceback") or [])
                    row["spark_stack"] = re.findall(
                        r"\bat ([\w.$]+\((?:[\w.$]+\.(?:java|scala):\d+|Unknown Source|Native Method)\))", trace
                    )
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
                if sql.startswith("SELECT SPLIT(VERSION()") and outcome == "ok":
                    rows = getattr(args[0], "_rows", None)
                    if rows and rows[0]:
                        version = re.match(r"\d+\.\d+\.\d+", str(rows[0][0]))
                        if version:
                            row["spark_version"] = version.group()
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

        instrument_session_polls(livysession.requests, emit)
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
