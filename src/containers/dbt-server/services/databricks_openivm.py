"""OpenIVM generates maintenance SQL; Databricks SQL executes it on Delta.

The schema-only compiler shares OpenIVM Spark's compiler, classification and
rewriter. Source and model CDF ranges are pinned per model, so dependency
updates propagate without re-inserting source batches. Multi-statement failures
leave a dirty checkpoint: they must not be replayed as if they were atomic.
"""

import json
import os
import threading
import time
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import ExitStack
from datetime import datetime, timezone
from graphlib import TopologicalSorter
from pathlib import Path

import requests
import sqlglot
from sqlglot import exp
from sqlglot.dialects.databricks import Databricks

from services.db import DB_LOCK, get_db
from services.dbt_compiler import get_manifest
from services.dbt_runner import _fail_run
from services.progress import LIVE_PROGRESS, PROGRESS_LOCK, cleanup_progress, init_progress

WORK_DIR = Path(os.environ.get("DATABRICKS_OPENIVM_WORK_DIR", "/data/processed/databricks-openivm"))
COMPILER_URL = os.environ.get("OPENIVM_SQL_COMPILER_URL", "http://openivm-sql-compiler:8998")
_RUN_LOCK = threading.Lock()


class _CompilerDialect(Databricks):
    """Keep the window null-treatment syntax understood by the Spark bridge."""

    class Generator(Databricks.Generator):
        def ignorenulls_sql(self, expression):
            if isinstance(expression.this, (exp.FirstValue, exp.LastValue)):
                return self.func(expression.this.sql_name(), expression.this.this, exp.true())
            return super().ignorenulls_sql(expression)

        def respectnulls_sql(self, expression):
            if isinstance(expression.this, (exp.FirstValue, exp.LastValue)):
                return self.sql(expression, "this")
            return super().respectnulls_sql(expression)


def quote(name):
    return ".".join("`" + p.replace("`", "``") + "`" for p in name.split("."))


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def model_dependencies(manifest):
    nodes = manifest.get("nodes", {})
    selected = {uid: n for uid, n in nodes.items()
                if n.get("resource_type") == "model"
                and n.get("config", {}).get("materialized") != "ephemeral"}
    def dependencies(uid):
        deps = set()
        for dep in nodes[uid].get("depends_on", {}).get("nodes", []):
            if dep in selected:
                deps.add(dep)
            elif dep in nodes and nodes[dep].get("config", {}).get("materialized") == "ephemeral":
                deps.update(dependencies(dep))
        return deps
    return {uid: dependencies(uid) for uid in sorted(selected)}


def ordered_models(manifest):
    return [(uid, manifest["nodes"][uid])
            for uid in TopologicalSorter(model_dependencies(manifest)).static_order()]


def relation(node):
    return ".".join((node["database"], node["schema"], node.get("alias") or node["name"]))


def physical(name):
    parts = name.split(".")
    parts[-1] = "_openivm_data_" + parts[-1]
    return ".".join(parts)


def remap_sql(sql, mapping, paths=None, snapshots=None, *, for_compiler=False):
    """Change relation names, never column expressions or string literals."""
    tree = sqlglot.parse_one(sql, read="databricks")
    for table in list(tree.find_all(exp.Table)):
        key = ".".join(p.name for p in table.parts)
        snapshot = (snapshots or {}).get(key) if table.args.get("version") else None
        target = (paths or {}).get(key) or (
            snapshot.get("table") if isinstance(snapshot, dict) else snapshot
        ) or mapping.get(key)
        if target:
            alias = table.alias or table.name
            replacement = sqlglot.to_table(target if for_compiler else quote(target), dialect="databricks")
            for attr in ("this", "db", "catalog"):
                table.set(attr, replacement.args.get(attr))
            if isinstance(snapshot, dict):
                # Public views cannot be time-travelled. Reconstruct their
                # column projection and HAVING filter over the backing table.
                backing = table.copy()
                backing.set("alias", None)
                query = sqlglot.select(*(exp.column(c, quoted=True) for c in snapshot["columns"])).from_(backing)
                if snapshot.get("predicate"):
                    query = query.where(snapshot["predicate"], dialect="databricks")
                table.replace(query.subquery(alias=alias))
    return tree.sql(dialect=_CompilerDialect if for_compiler else "databricks")


def _save(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)


class SqlExecutor:
    def __init__(self):
        from services import databricks_enzyme_sources as sources
        started = time.monotonic()
        self.connection = sources.open_connection()
        self.connection_s = time.monotonic() - started
        self.statements = []
        self.model_timings = []
        self.model_id = None
        self._delta_views = {}

    def execute(self, sql, *, phase="maintenance"):
        started = time.monotonic()
        entry = {"sql": sql, "query_id": None, "status": "running",
                 "model_id": self.model_id, "phase": phase, "started_at_ms": time.time() * 1000}
        try:
            with self.connection.cursor() as cur:
                cur.execute(sql)
                entry["query_id"] = cur.query_id
                rows = [list(r) for r in cur.fetchall()] if cur.description else []
            entry["status"] = "completed"
            return rows
        except Exception as exc:
            entry.update(status="failed", error=str(exc))
            raise
        finally:
            entry["duration_s"] = time.monotonic() - started
            self.statements.append(entry)

    def close(self):
        self.connection.close()

    def ensure_delta_view(self, short, sql):
        # TEMP VIEWs belong to this worker's SQL session. Reuse only the exact
        # table/schema/CDF range, including its parent HAVING predicate.
        if self._delta_views.get(short) != sql:
            self.execute(sql, phase="cdf_setup")
            self._delta_views[short] = sql

    def schema(self, table):
        rows = self.execute(f"DESCRIBE TABLE {quote(table)}", phase="metadata")
        fields = []
        for row in rows:
            if not row[0] or row[0].startswith("#"):
                break
            # Spark parses DDL types through the schema-only compiler.
            fields.append({"name": row[0], "ddl_type": row[1]})
        return fields

    def version(self, table):
        rows = self.execute(f"DESCRIBE HISTORY {quote(table)} LIMIT 1", phase="metadata")
        if not rows:
            raise RuntimeError(f"No Delta version for {table}")
        return int(rows[0][0])


class BatchMetadata:
    """Share immutable reads within one batch; coalesce concurrent readers."""

    def __init__(self):
        self._reads = {}
        self._lock = threading.Lock()

    def read(self, executor, method, table):
        key = (method, table)
        with self._lock:
            future = self._reads.get(key)
            owner = future is None
            if owner:
                future = self._reads[key] = Future()
        if owner:
            try:
                future.set_result(getattr(executor, method)(table))
            except Exception as exc:
                future.set_exception(exc)
        return future.result()

    def invalidate_version(self, table):
        # The scheduler calls this after a writer commits and before releasing
        # its children. Tables are owned by this experiment; no cross-batch cache.
        with self._lock:
            self._reads.pop(("version", table), None)


def compiler(payload, *, timings=None):
    started = time.monotonic()
    try:
        response = requests.post(f"{COMPILER_URL}/compile", json=payload, timeout=300)
        if not response.ok:
            raise RuntimeError(f"OpenIVM SQL compiler HTTP {response.status_code}: {response.text[:6000]}")
        response.raise_for_status()
        return response.json()
    finally:
        if timings is not None:
            timings["compiler_client_s"] += time.monotonic() - started


def _cdf_sql(short, table, fields, start, end, predicate=None):
    cols = ", ".join(quote(f["name"]) for f in fields)
    if start == end:
        body = (f"SELECT {cols}, CAST(NULL AS TIMESTAMP) AS openivm_timestamp, "
                f"CAST(0 AS INT) AS openivm_multiplicity FROM {quote(table)} WHERE FALSE")
    else:
        body = (f"SELECT {cols}, _commit_timestamp AS openivm_timestamp, "
                "CASE WHEN _change_type IN ('insert', 'update_postimage') THEN 1 ELSE -1 END "
                f"AS openivm_multiplicity FROM table_changes({literal(table)}, {start + 1}, {end}) "
                "WHERE _change_type IN ('insert', 'delete', 'update_preimage', 'update_postimage')")
        if predicate:
            body += " AND (" + predicate + ")"
    return f"CREATE OR REPLACE TEMP VIEW {quote('openivm_delta_' + short)} AS {body}"


def build_models(manifest, checkpoint, *, full_refresh, batch_num, threads=None, executor_factory=None):
    threads = int(threads if threads is not None else os.environ.get("DATABRICKS_DBT_THREADS") or "8")
    if threads < 1:
        raise ValueError("DATABRICKS_DBT_THREADS must be positive")
    executor_factory = executor_factory or SqlExecutor
    dependencies = model_dependencies(manifest)
    graph = TopologicalSorter(dependencies)
    graph.prepare()
    models = [(uid, manifest["nodes"][uid]) for uid in dependencies]
    known = {relation(n): physical(relation(n)) for _, n in models}
    known.update({relation(n): relation(n) for n in manifest.get("sources", {}).values()})
    state = checkpoint.setdefault("models", {})
    by_relation = {relation(n): uid for uid, n in models}
    metadata = BatchMetadata()
    sessions = threading.local()
    executors = []
    executors_lock = threading.Lock()
    with PROGRESS_LOCK:
        progress = LIVE_PROGRESS.get(checkpoint["run_id"])
        if progress is not None:
            progress["total"] = len(models)

    def run_model(uid):
        executor = getattr(sessions, "executor", None)
        if executor is None:
            executor = sessions.executor = executor_factory()
            with executors_lock:
                executors.append(executor)
        executor.model_id = uid
        started = time.monotonic()
        timings = {"model_id": uid, "compiler_client_s": 0.0, "status": "failed",
                   "started_at_ms": time.time() * 1000}
        try:
            saved = _build_model(executor, metadata, uid, manifest["nodes"][uid], known,
                                 state, by_relation, checkpoint["run_id"], full_refresh, timings)
            timings["status"] = "completed"
            timings["refresh_type"] = saved.get("refresh_type")
            timings["refresh_type_name"] = saved.get("refresh_type_name")
            return saved
        except Exception as exc:
            _record_node(checkpoint["run_id"], uid, manifest["nodes"][uid],
                         time.monotonic() - started, "error", str(exc))
            with PROGRESS_LOCK:
                progress = LIVE_PROGRESS.get(checkpoint["run_id"])
                if progress is not None:
                    progress["events"].setdefault(uid, {"unique_id": uid, "name": manifest["nodes"][uid]["name"],
                                                       "resource_type": "model"}).update(status="error", message=str(exc))
            raise
        finally:
            timings["model_wall_s"] = time.monotonic() - started
            timings["completed_at_ms"] = time.time() * 1000
            executor.model_timings.append(timings)

    started = time.monotonic()
    checkpoint_s = 0.0
    pending = {}
    ready = deque()
    pool = ThreadPoolExecutor(max_workers=threads)
    try:
        while graph.is_active():
            ready.extend(sorted(graph.get_ready()))
            while ready and len(pending) < threads:
                uid = ready.popleft()
                pending[pool.submit(run_model, uid)] = uid
            completed, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in sorted(completed, key=lambda f: pending[f]):
                uid = pending.pop(future)
                # Only this coordinator publishes state and writes checkpoints.
                # Workers read immutable state of already-completed parents.
                state[uid] = future.result()
                metadata.invalidate_version(known[relation(manifest["nodes"][uid])])
                save_started = time.monotonic()
                _save(WORK_DIR / "checkpoint.json", checkpoint)
                checkpoint_s += time.monotonic() - save_started
                graph.done(uid)
    except Exception:
        for future in pending:
            future.cancel()
        raise
    finally:
        # Drain active writes before reporting failure or closing SQL sessions.
        pool.shutdown(wait=True, cancel_futures=True)
        statements = sorted((s for e in executors for s in e.statements),
                            key=lambda s: s["started_at_ms"])
        model_timings = [m for e in executors for m in e.model_timings]
        for model in model_timings:
            model["sql_client_s"] = sum(s["duration_s"] for s in statements if s["model_id"] == model["model_id"])
            model["other_client_s"] = model["model_wall_s"] - model["sql_client_s"] - model["compiler_client_s"]
        timing = {
            "threads": threads, "models_wall_s": time.monotonic() - started,
            "connection_work_s": sum(e.connection_s for e in executors),
            "checkpoint_write_s": checkpoint_s,
            # These are sums of concurrent client spans, not elapsed batch time.
            "sql_client_work_s": sum(s["duration_s"] for s in statements),
            "metadata_client_work_s": sum(s["duration_s"] for s in statements if s["phase"] == "metadata"),
            "compiler_client_work_s": sum(m["compiler_client_s"] for e in executors for m in e.model_timings),
            "model_work_s": sum(m["model_wall_s"] for m in model_timings),
            "models": model_timings,
        }
        try:
            close_started = time.monotonic()
            # ExitStack closes every session even when one close operation fails.
            with ExitStack() as sessions_to_close:
                for executor in executors:
                    sessions_to_close.callback(executor.close)
        finally:
            timing["connection_close_wall_s"] = time.monotonic() - close_started
            _save(WORK_DIR / f"sql-batch{batch_num}.json", {
                "run_id": checkpoint["run_id"], "timing": timing, "statements": statements,
            })
            (WORK_DIR / f"sql-batch{batch_num}.sql").write_text(";\n\n".join(
                s["sql"].rstrip(";") for s in statements) + ";\n")


def _build_model(executor, metadata, uid, node, known, state, by_relation, run_id,
                 full_refresh, timings):
    name = node.get("alias") or node["name"]
    public = relation(node)
    target = known[public]
    query = node.get("compiled_code") or node.get("compiled_sql")
    if not query:
        raise RuntimeError(f"Missing compiled SQL for {uid}")
    deps = sorted({".".join(p.name for p in t.parts)
                   for t in sqlglot.parse_one(query, read="databricks").find_all(exp.Table)} & known.keys())
    mapping = {dep: "openivm_compile." + dep.split(".")[-1] for dep in deps}
    if len(set(mapping.values())) != len(mapping):
        raise RuntimeError(f"Ambiguous short source names in {uid}")
    started = time.monotonic()
    with PROGRESS_LOCK:
        progress = LIVE_PROGRESS.get(run_id)
        if progress is not None:
            progress["events"][uid] = {"unique_id": uid, "name": name, "resource_type": "model",
                                        "status": "running", "execution_time_s": None}
    if full_refresh:
        schemas = {dep.split(".")[-1]: metadata.read(executor, "schema", dep) for dep in deps}
        compiled = compiler({"operation": "compile", "name": name,
                             # The Spark-to-DuckDB bridge strips plain db.table
                             # names; it cannot bind backtick-quoted UC refs.
                             "query": remap_sql(query, mapping, for_compiler=True), "schemas": schemas}, timings=timings)
        initial = remap_sql(compiled["initial_sql"], {v: k for k, v in mapping.items()})
        executor.execute(f"CREATE TABLE {quote(target)} USING DELTA "
                         "TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS " + initial)
        columns = ", ".join(quote(f["name"]) for f in compiled["output_schema"]["fields"])
        having = compiled.get("having_predicate")
        executor.execute(f"CREATE VIEW {quote(public)} AS SELECT {columns} FROM {quote(target)}"
                         + (" WHERE " + having if having else ""))
        saved = {**compiled, "query": query, "schemas": schemas,
                      "source_versions": {dep: metadata.read(executor, "version", known[dep]) for dep in deps}}
    else:
        saved = dict(state[uid])
        if saved["query"] != query:
            raise RuntimeError(f"Model changed after initialization: {uid}")
        ends = {dep: metadata.read(executor, "version", known[dep]) for dep in deps}
        if any(ends[dep] < saved["source_versions"][dep] for dep in deps):
            raise RuntimeError(f"Source version moved backwards for {uid}")
        if saved["refresh_type"] == 3:
            executor.execute(f"INSERT OVERWRITE {quote(target)} " + query)
        elif any(ends[d] != saved["source_versions"][d] for d in deps):
            for dep in deps:
                short = dep.split(".")[-1]
                parent = state.get(by_relation.get(dep), {})
                executor.ensure_delta_view(short, _cdf_sql(short, known[dep], saved["schemas"][short],
                                          saved["source_versions"][dep], ends[dep], parent.get("having_predicate")))
            rewritten = compiler({"operation": "rewrite", "name": name,
                                  "compiled_sql": saved["compiled_sql"], "schemas": saved["schemas"],
                                  "source_versions": {d.split(".")[-1]: v for d, v in saved["source_versions"].items()},
                                  "mv_version": metadata.read(executor, "version", target)}, timings=timings)
            scratch = ".".join(target.split(".")[:-1] + ["_openivm_delta_" + name])
            executor.execute(f"DROP TABLE IF EXISTS {quote(scratch)}")
            remote = {v: k for k, v in mapping.items()}
            remote["openivm_compile." + name] = target
            paths = {"delta.openivm-mv": target, "delta.openivm-view-delta": scratch}
            snapshots = {
                v: {"table": known[k], "columns": [f["name"] for f in saved["schemas"][k.split(".")[-1]]],
                    "predicate": state.get(by_relation.get(k), {}).get("having_predicate")}
                for k, v in mapping.items()
            }
            for sql in rewritten["statements"]:
                executor.execute(remap_sql(sql, remote, paths, snapshots))
        saved["source_versions"] = ends
    _record_node(run_id, uid, node, time.monotonic() - started)
    with PROGRESS_LOCK:
        progress = LIVE_PROGRESS.get(run_id)
        if progress is not None:
            progress["events"][uid].update(status="success", execution_time_s=time.monotonic() - started)
    return saved


def _record_node(run_id, uid, node, duration, status="success", message=""):
    with DB_LOCK:
        conn = get_db()
        conn.execute("""INSERT OR REPLACE INTO run_nodes
            (run_id, unique_id, name, resource_type, execution_time_s, status, compiled_sql, depends_on, message)
            VALUES (?,?,?,?,?,?,?,?,?)""", (run_id, uid, node["name"], "model", duration, status,
            node.get("compiled_code") or node.get("compiled_sql"), json.dumps(node.get("depends_on", {}).get("nodes", [])), message))
        conn.commit()
        conn.close()


def run(run_id, scale_factor, full_refresh, batch_num):
    started = time.monotonic()
    timing = {}
    with _RUN_LOCK:
        init_progress(run_id)
        try:
            with DB_LOCK:
                conn = get_db()
                conn.execute("UPDATE runs SET status='running', started_at=? WHERE run_id=?",
                             (datetime.now(timezone.utc).isoformat(), run_id))
                conn.commit()
                conn.close()
            path = WORK_DIR / "checkpoint.json"
            if full_refresh != (batch_num == 1):
                raise RuntimeError("Full refresh is only valid for initialization")
            if full_refresh:
                checkpoint = {"batch_num": 0, "models": {}}
            else:
                checkpoint = json.loads(path.read_text())
                if checkpoint.get("dirty") or checkpoint["batch_num"] != batch_num - 1:
                    raise RuntimeError("OpenIVM checkpoint is dirty or refresh is out of sequence; reinitialize")
            checkpoint.update(dirty=True, run_id=run_id)
            _save(path, checkpoint)
            manifest_started = time.monotonic()
            manifest = get_manifest("databricks-enzyme", force_compile=full_refresh)
            timing["manifest_compile_s"] = time.monotonic() - manifest_started
            if not manifest or not ordered_models(manifest):
                raise RuntimeError("Could not compile Databricks TPC-DI models")
            build_models(manifest, checkpoint, full_refresh=full_refresh, batch_num=batch_num)
            checkpoint.update(dirty=False, batch_num=batch_num)
            _save(path, checkpoint)
            with DB_LOCK:
                conn = get_db()
                conn.execute("UPDATE runs SET status='completed', completed_at=?, duration_s=? WHERE run_id=?",
                             (datetime.now(timezone.utc).isoformat(), time.monotonic() - started, run_id))
                conn.commit()
                conn.close()
        except Exception as exc:
            _fail_run(run_id, str(exc), time.monotonic() - started)
        finally:
            timing["run_wall_s"] = time.monotonic() - started
            _save(WORK_DIR / f"timing-batch{batch_num}.json", timing)
            cleanup_progress(run_id)


def _validation_diagnostics(executor, query, node):
    # Freeze two independent recomputations before sampling. Floating-point
    # aggregates can otherwise change between the count and sample queries.
    tables = [physical(relation(node)) + "_validation_" + suffix for suffix in ("expected", "repeat")]
    with ExitStack() as cleanup:
        for table in tables:
            cleanup.callback(executor.execute, f"DROP TABLE IF EXISTS {quote(table)}")
            executor.execute(f"CREATE TABLE {quote(table)} USING DELTA AS {query}")
        expected = "SELECT * FROM " + quote(tables[0])
        comparisons = {}
        for label, actual in (("expected_vs_actual", "SELECT * FROM " + quote(relation(node))),
                              ("expected_vs_repeat", "SELECT * FROM " + quote(tables[1]))):
            differences = {}
            for direction, left, right in (("missing", expected, actual), ("extra", actual, expected)):
                difference = f"(({left}) EXCEPT ALL ({right}))"
                differences[direction + "_rows"] = executor.execute(f"SELECT COUNT(*) FROM {difference}")[0][0]
                differences[direction + "_samples"] = [r[0] for r in executor.execute(
                    "SELECT to_json(struct(*), map('ignoreNullFields', 'false')) FROM " + difference + " LIMIT 5")]
            comparisons[label] = differences
        return comparisons


def validate(batch_num):
    checkpoint = json.loads((WORK_DIR / "checkpoint.json").read_text())
    if checkpoint.get("dirty") or checkpoint["batch_num"] != batch_num:
        raise RuntimeError("Cannot validate an incomplete refresh")
    manifest = get_manifest("databricks-enzyme")
    models = ordered_models(manifest) if manifest else []
    if not models or {uid for uid, _ in models} != set(checkpoint["models"]):
        raise RuntimeError("Validation model set is empty or differs from the initialized checkpoint")
    executor = SqlExecutor()
    results = []
    try:
        for uid, node in models:
            query = checkpoint["models"][uid]["query"]
            actual = "SELECT * FROM " + quote(relation(node))
            missing = executor.execute(f"SELECT COUNT(*) FROM (({query}) EXCEPT ALL ({actual}))")[0][0]
            extra = executor.execute(f"SELECT COUNT(*) FROM (({actual}) EXCEPT ALL ({query}))")[0][0]
            result = {"model": node["name"], "missing_rows": missing, "extra_rows": extra}
            results.append(result)
            if missing or extra:
                try:
                    result["diagnostics"] = _validation_diagnostics(executor, query, node)
                except Exception as exc:
                    result["diagnostics_error"] = str(exc)
    finally:
        executor.close()
    payload = {"status": "ok" if all(r["missing_rows"] == r["extra_rows"] == 0 for r in results) else "error",
               "batch_num": batch_num, "models": results}
    _save(WORK_DIR / f"validation-batch{batch_num}.json", payload)
    return payload


def photon_metrics(batch_num):
    """Collect observed Photon task time outside the measured refresh."""
    from services import databricks_enzyme_sources as sources
    from databricks.sdk.service.sql import QueryFilter
    sql = json.loads((WORK_DIR / f"sql-batch{batch_num}.json").read_text())
    ids = list(dict.fromkeys(s["query_id"] for s in sql["statements"] if s.get("query_id")))
    by_id = {s["query_id"]: s for s in sql["statements"] if s.get("query_id")}
    maintenance_ids = {q for q, s in by_id.items() if s.get("phase") != "metadata"}
    client = sources._workspace_client()
    results = []
    for offset in range(0, len(ids), 100):
        response = client.query_history.list(filter_by=QueryFilter(statement_ids=ids[offset:offset + 100]),
                                             include_metrics=True, max_results=100)
        results.extend(q.as_dict() for q in (response.res or []))
    for q in results:
        entry = by_id.get(q.get("query_id"), {})
        q.update(model_id=entry.get("model_id"), phase=entry.get("phase", "maintenance"),
                 client_duration_s=entry.get("duration_s"), generated_sql=entry.get("sql"))
    observed = {q.get("query_id") for q in results} & maintenance_ids
    measured = sum(q.get("query_id") in maintenance_ids and
                   (q.get("metrics") or {}).get("photon_total_time_ms") is not None for q in results)
    payload = {"status": "ok" if maintenance_ids and maintenance_ids <= observed and measured == len(maintenance_ids) else "partial", "batch_num": batch_num,
               "statement_count": len(maintenance_ids), "observed_statement_count": len(observed), "queries": results,
               "metadata_statement_count": len(set(ids) - maintenance_ids),
               "client_timing": sql.get("timing", {}),
               "metrics_statement_count": measured,
               "photon_total_time_ms": sum((q.get("metrics") or {}).get("photon_total_time_ms") or 0 for q in results if q.get("query_id") in maintenance_ids),
               "task_total_time_ms": sum((q.get("metrics") or {}).get("task_total_time_ms") or 0 for q in results if q.get("query_id") in maintenance_ids)}
    run_timing = WORK_DIR / f"timing-batch{batch_num}.json"
    if run_timing.exists():
        payload["run_timing"] = json.loads(run_timing.read_text())
    _save(WORK_DIR / f"photon-batch{batch_num}.json", payload)
    return payload
