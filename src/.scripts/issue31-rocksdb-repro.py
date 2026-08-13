#!/usr/bin/env python3
"""Reproduce OpenIVM issue #31 with concurrent independent Livy drivers."""

import argparse
import concurrent.futures
import json
import statistics
import sys
import time
import urllib.error
import urllib.request


def request(base_url, method, path, payload=None, timeout=60):
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=body,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            data = response.read()
            return json.loads(data) if data else {}
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path}: HTTP {error.code}: {detail}") from error


def wait_until(fetch, done, timeout, label):
    deadline = time.monotonic() + timeout
    while True:
        value = fetch()
        if done(value):
            return value
        if time.monotonic() >= deadline:
            raise TimeoutError(f"timed out waiting for {label}: {value}")
        time.sleep(1)


def create_session(
    base_url,
    backend,
    timeout,
    catalog_path=None,
    warehouse_path=None,
    multi_process=True,
    fair_scheduler=False,
):
    conf = {
        "spark.openivm.rocksdb.multiProcess": str(multi_process).lower(),
        "spark.openivm.profile.refresh": "true",
        "spark.openivm.catalog.backend": backend,
    }
    if fair_scheduler:
        conf["spark.scheduler.mode"] = "FAIR"
    if catalog_path:
        conf["spark.openivm.catalog.path"] = catalog_path
    if warehouse_path:
        conf["spark.sql.warehouse.dir"] = warehouse_path
    session = request(base_url, "POST", "/sessions", {"kind": "sql", "conf": conf})
    session_id = session["id"]
    result = wait_until(
        lambda: request(base_url, "GET", f"/sessions/{session_id}"),
        lambda item: item.get("state") in {"idle", "error", "dead", "killed"},
        timeout,
        f"session {session_id}",
    )
    if result.get("state") != "idle":
        raise RuntimeError(f"session {session_id} failed to start: {result}")
    return session_id


def run_sql(base_url, session_id, sql, timeout, kind="sql"):
    submitted_at = time.monotonic()
    statement = request(
        base_url,
        "POST",
        f"/sessions/{session_id}/statements",
        {"code": sql, "kind": kind},
    )
    statement_id = statement["id"]
    result = wait_until(
        lambda: request(
            base_url, "GET", f"/sessions/{session_id}/statements/{statement_id}"
        ),
        lambda item: item.get("state") in {"available", "error", "cancelled"},
        timeout,
        f"session {session_id} statement {statement_id}",
    )
    wall_seconds = time.monotonic() - submitted_at
    output = result.get("output") or {}
    if result.get("state") != "available" or output.get("status") != "ok":
        raise RuntimeError(
            f"session {session_id} statement {statement_id} failed after "
            f"{wall_seconds:.3f}s: {json.dumps(result)}"
        )
    return result, wall_seconds


def table_rows(statement):
    data = (statement.get("output") or {}).get("data") or {}
    table = data.get("application/vnd.livy.table.v1+json")
    if table:
        headers = [field["name"] for field in table.get("headers", [])]
    else:
        table = data.get("application/json") or {}
        headers = [field["name"] for field in (table.get("schema") or {}).get("fields", [])]
    return [dict(zip(headers, row)) for row in table.get("data", [])]


def parse_detail(detail):
    fields = {}
    for token in (detail or "").split(";"):
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = value
    return fields


def parse_admission_telemetry(statement):
    rendered = json.dumps((statement.get("output") or {}).get("data") or {})
    marker = "OPENIVM_CTAS_ADMISSION|"
    marker_at = rendered.find(marker)
    if marker_at < 0:
        return None
    payload = rendered[marker_at + len(marker) :].split("\\", 1)[0]
    fields = {}
    for token in payload.split("|"):
        if "=" in token:
            key, value = token.split("=", 1)
            fields[key] = value
    return {
        "scheduler_mode": fields.get("schedulerMode"),
        "initial_limit": int(fields["initialLimit"]),
        "learned_limit": int(fields["learnedLimit"]),
        "max_inflight": int(fields["maxInflight"]),
        "batch_wall_ms": int(fields["batchWallMs"]),
    }


def summarize_telemetry(rows, prefix):
    selected = [
        row
        for row in rows
        if prefix + "_mv_" in str(row.get("view_name", ""))
        and row.get("step_name") == "rocksdb_operation"
    ]
    totals = {
        "all_profile_rows": len(rows),
        "profile_rows": len(selected),
        "operation_count": 0,
        "failed_count": 0,
        "total_ns": 0,
        "jvm_lock_wait_ns": 0,
        "jvm_lock_held_ns": 0,
        "external_lock_wait_ns": 0,
        "native_open_ns": 0,
        "native_close_ns": 0,
        "body_ns": 0,
    }
    by_scope = {}
    for row in selected:
        detail = parse_detail(row.get("detail"))
        scope = detail.get("db_scope", "unknown")
        scope_totals = by_scope.setdefault(
            scope,
            {
                "profile_rows": 0,
                "operation_count": 0,
                "failed_count": 0,
                "total_ns": 0,
                "external_lock_wait_ns": 0,
                "native_open_ns": 0,
                "native_close_ns": 0,
                "body_ns": 0,
            },
        )
        totals["profile_rows"] = len(selected)
        scope_totals["profile_rows"] += 1
        for key in totals:
            if key in {"all_profile_rows", "profile_rows"}:
                continue
            value = int(detail.get(key, 0))
            totals[key] += value
            if key in scope_totals:
                scope_totals[key] += value
    totals["native_open_close_ns"] = totals["native_open_ns"] + totals["native_close_ns"]
    for scope_totals in by_scope.values():
        scope_totals["native_open_close_ns"] = (
            scope_totals["native_open_ns"] + scope_totals["native_close_ns"]
        )
    return totals, by_scope


def summarize_steps(rows, prefix):
    selected = [
        row for row in rows if prefix + "_mv_" in str(row.get("view_name", ""))
    ]
    by_step = {}
    for row in selected:
        step = row.get("step_name", "unknown")
        by_step.setdefault(step, []).append(int(row.get("duration_ms", 0)))
    return {
        step: {
            "count": len(durations),
            "sum_ms": sum(durations),
            "median_ms": statistics.median(durations),
            "max_ms": max(durations),
        }
        for step, durations in sorted(by_step.items())
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:8998")
    parser.add_argument("--drivers", type=int, default=10)
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--prefix", default="issue31_after")
    parser.add_argument("--backend", choices=("rocksdb", "delta"), default="rocksdb")
    parser.add_argument("--timeout", type=int, default=900)
    parser.add_argument("--output")
    parser.add_argument("--profile-only", action="store_true")
    parser.add_argument(
        "--single-process-rocksdb",
        action="store_true",
        help="reuse cached RocksDB handles when all jobs share one driver process",
    )
    topology_group = parser.add_mutually_exclusive_group()
    topology_group.add_argument(
        "--shared-session",
        action="store_true",
        help="submit all concurrent CTAS statements to one Livy/Spark session",
    )
    topology_group.add_argument(
        "--in-driver-parallel",
        action="store_true",
        help="submit one Scala statement that launches every CTAS from driver threads",
    )
    parser.add_argument(
        "--dispatch-width",
        type=int,
        help="maximum concurrent driver threads for --in-driver-parallel",
    )
    parser.add_argument(
        "--adaptive-dispatch",
        action="store_true",
        help="use optimistic AIMD admission and distinct Spark FAIR pools",
    )
    parser.add_argument(
        "--warm-source",
        action="store_true",
        help="resolve the source concurrently in every session before timing CREATE MV",
    )
    parser.add_argument("--catalog-path")
    parser.add_argument("--warehouse-path")
    args = parser.parse_args()
    if args.single_process_rocksdb and not (
        args.shared_session or args.in_driver_parallel
    ):
        parser.error("--single-process-rocksdb requires a single-session topology")
    if args.dispatch_width is not None and not args.in_driver_parallel:
        parser.error("--dispatch-width requires --in-driver-parallel")
    if args.dispatch_width is not None and args.dispatch_width < 1:
        parser.error("--dispatch-width must be positive")
    if args.adaptive_dispatch and not args.in_driver_parallel:
        parser.error("--adaptive-dispatch requires --in-driver-parallel")
    if args.adaptive_dispatch and args.dispatch_width is not None:
        parser.error("--adaptive-dispatch cannot be combined with --dispatch-width")

    sessions = []
    try:
        session_count = 1 if args.shared_session or args.in_driver_parallel else args.drivers
        if args.in_driver_parallel:
            topology = "in_driver_parallel"
        elif args.shared_session:
            topology = "shared"
        else:
            topology = "independent"
        print(
            f"starting {session_count} {topology} Livy session(s) for "
            f"{args.drivers} CTAS jobs",
            file=sys.stderr,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=session_count) as pool:
            sessions = list(
                pool.map(
                    lambda _: create_session(
                        args.base_url,
                        args.backend,
                        args.timeout,
                        args.catalog_path,
                        args.warehouse_path,
                        not args.single_process_rocksdb,
                        args.in_driver_parallel,
                    ),
                    range(session_count),
                )
            )
        print(f"sessions={sessions}", file=sys.stderr)

        if args.profile_only:
            profile_statement, _ = run_sql(
                args.base_url, sessions[0], "SHOW OPENIVM REFRESH PROFILE", args.timeout
            )
            profile_rows = table_rows(profile_statement)
            telemetry, telemetry_by_scope = summarize_telemetry(profile_rows, args.prefix)
            result = {
                "prefix": args.prefix,
                "backend": args.backend,
                "rocksdb_telemetry": telemetry,
                "rocksdb_telemetry_by_scope": telemetry_by_scope,
                "profile_steps": summarize_steps(profile_rows, args.prefix),
            }
            rendered = json.dumps(result, indent=2, sort_keys=True)
            print(rendered)
            if args.output:
                with open(args.output, "w", encoding="utf-8") as output:
                    output.write(rendered + "\n")
            return

        source = f"{args.prefix}_source"
        run_sql(args.base_url, sessions[0], f"DROP TABLE IF EXISTS {source}", args.timeout)
        _, source_wall = run_sql(
            args.base_url,
            sessions[0],
            f"CREATE TABLE {source} USING DELTA AS SELECT id FROM range({args.rows})",
            args.timeout,
        )
        print(f"source ready in {source_wall:.3f}s", file=sys.stderr)

        warm_source_wall = None
        if args.warm_source:
            warm_started_at = time.monotonic()
            with concurrent.futures.ThreadPoolExecutor(max_workers=session_count) as pool:
                list(
                    pool.map(
                        lambda session_id: run_sql(
                            args.base_url,
                            session_id,
                            f"SELECT * FROM {source} LIMIT 0",
                            args.timeout,
                        ),
                        sessions,
                    )
                )
            warm_source_wall = time.monotonic() - warm_started_at
            print(f"source warmed in {warm_source_wall:.3f}s", file=sys.stderr)

        ready_at = time.monotonic()

        def mv_sql(index):
            mv = f"{args.prefix}_mv_{index + 1}"
            return mv, f"CREATE MATERIALIZED VIEW {mv} AS SELECT id, id AS value FROM {source}"

        def create_mv(index):
            session_id = sessions[index % session_count]
            mv, sql = mv_sql(index)
            _, wall = run_sql(args.base_url, session_id, sql, args.timeout)
            return {"view": mv, "session_id": session_id, "wall_seconds": wall}

        admission_telemetry = None
        if args.in_driver_parallel:
            quoted_queries = ",\n".join(
                json.dumps(mv_sql(index)[1]) for index in range(args.drivers)
            )
            if args.adaptive_dispatch:
                scala = f"""
import org.openivm.spark.common.{{CtasAdmissionController, CtasBatchDispatcher, CtasBatchTask}}
val openivmQueries = Seq({quoted_queries})
val openivmController = CtasAdmissionController.optimistic(openivmQueries.size)
val openivmTasks = openivmQueries.zipWithIndex.map {{ case (query, index) =>
  CtasBatchTask(s"mv-${{index + 1}}", () => spark.sql(query).collect())
}}
val openivmBatch = CtasBatchDispatcher.run(spark, openivmTasks, openivmController)
val openivmTelemetry = openivmBatch.telemetry
s"OPENIVM_CTAS_ADMISSION|schedulerMode=${{openivmTelemetry.schedulerMode}}|initialLimit=${{openivmTelemetry.initialLimit}}|learnedLimit=${{openivmTelemetry.learnedLimit}}|maxInflight=${{openivmTelemetry.maxInflight}}|batchWallMs=${{openivmTelemetry.batchWallNanos / 1000000L}}"
"""
            else:
                dispatch_width = args.dispatch_width or args.drivers
                scala = f"""
import java.util.concurrent.Executors
import scala.concurrent.{{Await, ExecutionContext, Future}}
import scala.concurrent.duration.Duration
val openivmExecutor = Executors.newFixedThreadPool({dispatch_width})
implicit val openivmEc: ExecutionContext = ExecutionContext.fromExecutorService(openivmExecutor)
val openivmQueries = Seq({quoted_queries})
try {{
  Await.result(Future.sequence(openivmQueries.map(query => Future {{
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", s"openivm-ctas-${{Thread.currentThread().getId}}")
    try spark.sql(query).collect()
    finally spark.sparkContext.setLocalProperty("spark.scheduler.pool", null)
  }})), Duration.Inf)
}} finally {{
  openivmExecutor.shutdown()
}}
"""
            batch_statement, wall = run_sql(
                args.base_url, sessions[0], scala, args.timeout, kind="spark"
            )
            if args.adaptive_dispatch:
                admission_telemetry = parse_admission_telemetry(batch_statement)
                if admission_telemetry is None:
                    raise RuntimeError("adaptive dispatcher returned no admission telemetry")
            per_view = [
                {
                    "view": "in_driver_batch",
                    "session_id": sessions[0],
                    "wall_seconds": wall,
                }
            ]
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.drivers) as pool:
                per_view = list(pool.map(create_mv, range(args.drivers)))
        batch_wall = time.monotonic() - ready_at

        validation_sql = " UNION ALL ".join(
            f"SELECT '{mv_sql(index)[0]}' AS view, COUNT(*) AS row_count "
            f"FROM {mv_sql(index)[0]}"
            for index in range(args.drivers)
        )
        validation_statement, _ = run_sql(
            args.base_url, sessions[0], validation_sql, args.timeout
        )
        view_row_counts = {
            row["view"]: int(row["row_count"])
            for row in table_rows(validation_statement)
        }
        if len(view_row_counts) != args.drivers or any(
            count != args.rows for count in view_row_counts.values()
        ):
            raise RuntimeError(f"CTAS validation failed: {view_row_counts}")

        profile_statement, _ = run_sql(
            args.base_url, sessions[0], "SHOW OPENIVM REFRESH PROFILE", args.timeout
        )
        profile_rows = table_rows(profile_statement)
        telemetry, telemetry_by_scope = summarize_telemetry(profile_rows, args.prefix)
        walls = [item["wall_seconds"] for item in per_view]
        result = {
            "prefix": args.prefix,
            "backend": args.backend,
            "drivers": args.drivers,
            "adaptive_dispatch": args.adaptive_dispatch,
            "dispatch_width": (
                None
                if args.adaptive_dispatch
                else args.dispatch_width or args.drivers if args.in_driver_parallel
                else None
            ),
            "session_topology": topology,
            "rows": args.rows,
            "sessions": sessions,
            "source_wall_seconds": source_wall,
            "warm_source_wall_seconds": warm_source_wall,
            "ctas_batch_wall_seconds": batch_wall,
            "ctas_wall_seconds": {
                "min": min(walls),
                "median": statistics.median(walls),
                "max": max(walls),
                "sum": sum(walls),
            },
            "per_view": per_view,
            "rocksdb_telemetry": telemetry,
            "rocksdb_telemetry_by_scope": telemetry_by_scope,
            "profile_steps": summarize_steps(profile_rows, args.prefix),
            "admission_telemetry": admission_telemetry,
            "view_row_counts": view_row_counts,
        }
        rendered = json.dumps(result, indent=2, sort_keys=True)
        print(rendered)
        if args.output:
            with open(args.output, "w", encoding="utf-8") as output:
                output.write(rendered + "\n")
    finally:
        for session_id in sessions:
            try:
                request(args.base_url, "DELETE", f"/sessions/{session_id}")
            except Exception as error:
                print(f"failed to stop session {session_id}: {error}", file=sys.stderr)


if __name__ == "__main__":
    main()
