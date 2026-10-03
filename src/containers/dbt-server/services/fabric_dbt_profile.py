"""Run dbt with client-side Fabric timings; polling and SQL remain unchanged."""

import functools
import hashlib
import json
import os
import re
import sys
import threading
import time
from contextlib import nullcontext
from pathlib import Path


def persist_hc_sessions(backend):
    """Retain dbt-owned REPL addresses for post-build readers; never acquire one."""
    with backend._active_sessions_lock:
        sessions = list(backend._active_sessions)
    by_file = {}
    for session in sessions:
        path = session.credential.session_id_file
        if path and not session.is_dead and not session.is_new_session_required:
            by_file.setdefault(path, []).append(session)
    for path, group in by_file.items():
        if len({s.session_id for s in group}) != 1:
            raise RuntimeError("Fabric HC REPLs used multiple Spark applications; refusing incomparable results")
        session = sorted(group, key=lambda s: s.repl_id)[0]
        target = Path(path)
        temporary = target.with_suffix(".tmp")
        temporary.write_text(json.dumps({
            "hc_id": session.hc_id, "session_id": session.session_id,
            "repl_id": session.repl_id, "repl_count": len(group),
        }))
        temporary.replace(target)


def instrument_session_polls(requests, emit):
    """Observe existing startup requests without logging the response payload."""
    original = requests.get

    @functools.wraps(original)
    def observed(url, *args, **kwargs):
        response = original(url, *args, **kwargs)
        if re.search(r"/livyapi/versions/[^/]+/(?:sessions|highConcurrencySessions)/[^/?]+$", str(url)):
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
            session = getattr(args[0], "hc_session", None) if args else None
            if session:
                for key, value in (("livy_session", session.session_id), ("repl", session.repl_id)):
                    row[key] = hashlib.sha256(str(value).encode()).hexdigest()[:16]
            if name in ("_getLivyResult", "_poll") and isinstance(result, dict):
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
    from dbt.adapters.fabricspark import connections, concurrent_livy, livysession
    from dbt.cli.main import dbtRunner

    lock = threading.Lock()
    path = os.environ.get("FABRIC_DBT_TIMINGS_PATH")
    with (open(path, "a", buffering=1) if path else nullcontext()) as output:
        def emit(row):
            # These records overlap: consumers must use intervals, not sum them.
            with lock:
                output.write(json.dumps(row) + "\n")

        if output:
            instrument_session_polls(livysession.requests, emit)
            for owner, names in (
                (livysession.LivySession, ("create_session", "wait_for_session_start")),
                (livysession.LivyCursor, ("_submitLivyCode", "_getLivyResult")),
                (concurrent_livy.HighConcurrencySession, ("acquire", "_poll_until_idle")),
                (concurrent_livy.HighConcurrencyCursor, ("_submit", "_poll")),
            ):
                for name in names:
                    instrument(owner, name, emit)
            for cursor in (livysession.LivyCursor, concurrent_livy.HighConcurrencyCursor):
                instrument(cursor, "execute", emit, sql_argument=True)
            instrument(connections, "get_lakehouse_properties", emit)
            instrument(livysession, "get_headers", emit)
        try:
            result = dbtRunner().invoke(sys.argv[1:])
        finally:
            persist_hc_sessions(concurrent_livy)
        if result.exception:
            raise result.exception
        return 0 if result.success else 2


if __name__ == "__main__":
    sys.exit(main())
