"""Extended TPC-DS engine batches over the shared immutable mutation plan.

Both engines use the same DuckDB CLI build and native source storage. OpenIVM
creates and refreshes materialized views; rejected CREATEs may use an explicit table
fallback. No refresh failure or correctness mismatch is converted into a fallback.
"""

import importlib.util
import hashlib
import json
import os
import re
import sys
import threading
import time
from datetime import date
from pathlib import Path

from services.compiler_bench.engines import _DuckDBSession, _verify_probe, QueryFailed


_ROOT = Path(os.environ.get("EXTENDED_TPCDS_WORKLOAD_DIR", "/data/workloads/extended-tpcds"))
_spec = importlib.util.spec_from_file_location("extended_tpcds_workload", _ROOT / "__init__.py", submodule_search_locations=[str(_ROOT)])
_package = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = _package
_spec.loader.exec_module(_package)
from extended_tpcds_workload import benchmark, mutations, pipeline
from extended_tpcds_workload.generate import quote

_LOCK = threading.Lock()


class CliConnection:
    """Minimal DuckDB result/parameter interface over the existing CLI worker."""
    def __init__(self, binary, path, *, openivm=False, timeout_s=300, memory_limit=None, threads=None):
        preamble = []
        if memory_limit:
            preamble.append("SET memory_limit=" + quote(memory_limit))
        if threads:
            preamble.append(f"SET threads={int(threads)}")
        self.session = _DuckDBSession(binary, str(path), preamble, capture_sql_timing=True)
        self.timeout_s = timeout_s
        try:
            self.session.start()
            for extension in ("json", "icu"):
                path = os.environ.get("DUCKDB_" + extension.upper() + "_EXTENSION", f"/data/bin/duckdb-openivm/{extension}.duckdb_extension")
                self.execute("LOAD " + (quote(path) if Path(path).is_file() else extension))
            if openivm:
                # Unlike the survey's tolerated preamble, failure to load the engine
                # must fail this run rather than masquerade as a vanilla baseline.
                self.execute("LOAD openivm")
                self.execute("SET openivm_cascade_refresh='off'")
            self.engine_version = self.execute("SELECT version()").fetchone()[0]
        except Exception:
            self.session.close()
            raise
        self.rows = []

    def execute(self, sql, parameters=None):
        if parameters:
            for value in parameters:
                if isinstance(value, date):
                    literal = "DATE " + quote(value)
                elif value is None:
                    literal = "NULL"
                elif isinstance(value, (int, float)):
                    literal = str(value)
                else:
                    literal = quote(value)
                sql = sql.replace("?", literal, 1)
        output = self.session.execute(sql, timeout_s=self.timeout_s)
        # CREATE MV can emit multiple JSON result sets from its internal SQL.
        decoder = json.JSONDecoder()
        self.rows = []
        remaining = output.strip()
        while remaining:
            rows, end = decoder.raw_decode(remaining)
            self.rows = rows
            remaining = remaining[end:].lstrip()
        return self

    @staticmethod
    def _value(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value)
        if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return date.fromisoformat(value)
        return value

    def fetchall(self):
        return [tuple(self._value(value) for value in row.values()) for row in self.rows]

    def fetchone(self):
        rows = self.fetchall()
        return rows[0] if rows else None

    def close(self):
        self.session.close()


def build(connection, configuration, cohort, materializations, fallback_policy):
    times, classifications = {}, {}
    for name in pipeline.selected_models():
        sql = pipeline.render_model(name, configuration, cohort)
        if not materializations.get(name):
            if materializations.get("_engine") == "duckdb-openivm":
                warning = None
                try:
                    connection.execute(f"CREATE MATERIALIZED VIEW {name} AS {sql}")
                except QueryFailed as error:
                    warning = str(error)
                duration = connection.session.last_sql_time_s
                try:
                    connection.execute(f"PRAGMA refresh_status({quote(name)})")
                    record = connection.rows[0] if connection.rows else None
                except QueryFailed:
                    record = None
                if record:
                    if warning and not ("Warning: materialized view" in warning and "Full refresh will be used." in warning):
                        raise QueryFailed(warning)
                    strategy = record.get("refresh_strategy")
                    known_incremental = ("aggregate_group", "simple_aggregate", "simple_projection", "aggregate_having", "window_partition", "group_recompute", "top_k", "distinct_incremental", "semi_anti_recompute", "count_distinct_incremental")
                    classification = "full" if strategy in ("full", "full_refresh") else "incremental" if strategy in known_incremental else "unknown"
                    materializations[name] = {"kind":"materialized_view", "classification":classification, "refresh_strategy":strategy, "create_message": warning}
                elif warning and fallback_policy == "table":
                    connection.execute(f"CREATE TABLE {name} AS {sql}")
                    duration = (duration or 0) + (connection.session.last_sql_time_s or 0)
                    materializations[name] = {"kind":"table", "classification":"unsupported_mv_table_fallback", "create_message":warning}
                else:
                    raise QueryFailed(warning or f"no materialized view registered for {name}")
            else:
                connection.execute(f"CREATE TABLE {name} AS {sql}")
                duration = connection.session.last_sql_time_s
                materializations[name] = {"kind":"table", "classification":"full", "create_message":None}
        elif materializations[name]["kind"] == "materialized_view":
            connection.execute(f"PRAGMA refresh({quote(name)})")
            duration = connection.session.last_sql_time_s
        elif materializations.get("_engine") == "duckdb-openivm":
            # Keep table identity so registered downstream MVs receive deltas.
            connection.execute("BEGIN")
            try:
                connection.execute(f"DELETE FROM {name}")
                duration = connection.session.last_sql_time_s or 0
                connection.execute(f"INSERT INTO {name} {sql}")
                duration += connection.session.last_sql_time_s or 0
                connection.execute("COMMIT")
            except Exception:
                connection.execute("ROLLBACK")
                raise
        else:
            connection.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            duration = connection.session.last_sql_time_s
        times[name] = duration
        classifications[name] = materializations[name]
    counts = {name: connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in times}
    return {"model_rows":counts, "model_sql_duration_s":times, "materializations":classifications,
            "classification_counts":{kind:sum(entry["classification"] == kind for entry in classifications.values()) for kind in {entry["classification"] for entry in classifications.values()}}}


def validate(connection, manifest, directory, batch_num, cohort):
    # A fresh plain catalog gives every intermediate its own recomputed parents;
    # validating each query against stale maintained parents would miss cascades.
    actual_catalog = connection.execute("SELECT current_database()").fetchone()[0]
    search_path = connection.execute("SELECT current_setting('search_path')").fetchone()[0]
    connection.execute("ATTACH ':memory:' AS xt_reference")
    try:
        connection.execute("SET search_path='xt_reference.main'")
        mutations.load_initial(connection, manifest, directory)
        for batch in manifest["batches"]:
            if batch["number"] > batch_num:
                break
            mutations.apply_batch(connection, manifest, directory, batch)
        # CTAS recomputation in its own catalog; it is never registered as an MV.
        pipeline.build(connection, cohort=cohort)
        checks = {}
        for name in pipeline.selected_models():
            actual = '"' + actual_catalog.replace('"','""') + f'".main.{name}'
            columns = connection.execute(f"DESCRIBE {actual}").fetchall()
            difference = connection.execute(_verify_probe(actual, f"SELECT * FROM xt_reference.main.{name}", [(row[0],row[1]) for row in columns])).fetchone()[0]
            checks[name] = difference
        return {"status":"passed" if not any(checks.values()) else "failed", "models_checked":len(checks),
                "differences":checks, "comparison":"EXCEPT ALL by position; FLOAT/DOUBLE rounded to 10 decimals; keys and nulls retained"}
    finally:
        connection.execute("SET search_path=" + quote(search_path))
        connection.execute("DETACH xt_reference")


def context(engine, manifest_path=None, work_dir=None, binary=None, options=None):
    if engine not in ("duckdb", "duckdb-openivm"):
        raise ValueError("unsupported Extended TPC-DS engine")
    options = options or json.loads(os.environ["EXTENDED_TPCDS_OPTIONS"])
    manifest_path = Path(manifest_path or (Path("/data/raw/extended-tpcds") / os.environ["EXTENDED_TPCDS_CACHE_KEY"] / "plan/manifest.json"))
    manifest = json.loads(manifest_path.read_text())
    prefix = engine.upper().replace("-", "_")
    work_dir = Path(work_dir or os.environ.get(prefix + "_WORK_DIR", "/data/processed/" + engine)) / "extended-tpcds"
    binary = binary or os.environ.get("DUCKDB_BIN" if engine == "duckdb" else "DUCKDB_OPENIVM_BIN", "/data/bin/duckdb-openivm/duckdb")
    return manifest_path, manifest, work_dir, binary, options, prefix


def validate_batch(engine, batch_num, **kwargs):
    manifest_path, manifest, work_dir, binary, options, prefix = context(engine, **kwargs)
    connection = CliConnection(binary, work_dir / "state.duckdb", openivm=engine == "duckdb-openivm",
                               timeout_s=options["statement_timeout_s"], memory_limit=os.environ.get(prefix + "_VALIDATE_MEM_LIMIT", os.environ.get(prefix + "_MEM_LIMIT")), threads=os.environ.get(prefix + "_THREADS"))
    try:
        applied, plan_id = connection.execute("SELECT number, plan_id FROM xt_applied_batch").fetchone()
        if applied != batch_num or plan_id != manifest["plan_id"]:
            raise ValueError("validation batch does not match the current source snapshot")
        result = validate(connection, manifest, manifest_path.parent, batch_num, options["query_cohort"])
        path = work_dir / f"batch{batch_num}.json"
        batch_result = json.loads(path.read_text())
        batch_result["validation"] = result
        if result["status"] != "passed":
            batch_result["status"] = "failed"
        path.write_text(json.dumps(batch_result,indent=2)+"\n")
        return result
    finally:
        connection.close()


def run_batch(engine, batch_num, *, manifest_path=None, work_dir=None, binary=None, options=None, validate_results=True):
    manifest_path, manifest, work_dir, binary, options, prefix = context(engine, manifest_path, work_dir, binary, options)
    if batch_num < 1 or batch_num > len(manifest["batches"]) + 1:
        raise ValueError("batch is outside the prepared plan")
    work_dir.mkdir(parents=True, exist_ok=True)
    database = work_dir / "state.duckdb"
    state_path = work_dir / "materializations.json"
    if batch_num == 1 and database.exists():
        raise ValueError("initial engine state already exists; start a new repetition directory")
    if batch_num > 1 and not state_path.exists():
        raise ValueError("initial construction must complete before mutation replay")
    sql_identity = hashlib.sha256(json.dumps({name:pipeline.render_model(name,manifest["source_configuration"],options["query_cohort"]) for name in pipeline.selected_models()},sort_keys=True).encode()).hexdigest()
    materializations = {"_engine":engine, "_sql_identity":sql_identity, "_plan_id":manifest["plan_id"]} if batch_num == 1 else json.loads(state_path.read_text())
    if materializations.get("_sql_identity") != sql_identity or materializations.get("_plan_id") != manifest["plan_id"]:
        raise ValueError("model definitions, query binding or mutation plan changed during the run")
    connection = CliConnection(binary, database, openivm=engine == "duckdb-openivm",
                               timeout_s=options["statement_timeout_s"], memory_limit=os.environ.get(prefix + "_MEM_LIMIT"), threads=os.environ.get(prefix + "_THREADS"))
    try:
        start = time.monotonic()
        if batch_num == 1:
            mutations.load_initial(connection, manifest, manifest_path.parent)
        else:
            mutations.apply_batch(connection, manifest, manifest_path.parent, manifest["batches"][batch_num-2])
        source_wall = time.monotonic() - start
        start = time.monotonic()
        result = build(connection, manifest["source_configuration"], options["query_cohort"], materializations, options["fallback_policy"])
        model_wall = time.monotonic() - start
        state_path.write_text(json.dumps(materializations, indent=2) + "\n")
        result.update({"engine":engine, "engine_version":connection.engine_version, "workload":"extended-tpcds", "plan_id":manifest["plan_id"],
                       "batch_num":batch_num, "source_storage":"duckdb_native", "configuration":manifest["source_configuration"], "options":options,
                       "source_replay_wall_s_including_guards":source_wall, "model_wall_s_including_cli":model_wall,
                       "duration_s":source_wall+model_wall,
                       "timing_semantics":"source replay and ordered model execution; validation excluded; source integrity checks and CLI round trips included; per-model SQL timer separate"})
        if validate_results:
            result["validation"] = validate(connection, manifest, manifest_path.parent, batch_num, options["query_cohort"])
        result["status"] = "failed" if result.get("validation",{}).get("status") == "failed" else "completed"
        (work_dir / f"batch{batch_num}.json").write_text(json.dumps(result,indent=2)+"\n")
        return result
    except Exception as error:
        failure = {"status":"failed", "engine":engine, "engine_version":connection.engine_version,
                   "batch_num":batch_num, "plan_id":manifest["plan_id"], "error":str(error),
                   "materializations":materializations, "options":options}
        (work_dir / f"batch{batch_num}.json").write_text(json.dumps(failure,indent=2)+"\n")
        raise
    finally:
        connection.close()
