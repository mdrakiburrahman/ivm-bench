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
from datetime import datetime, timezone
from pathlib import Path

import requests
import sqlglot
from sqlglot import exp

from services.db import DB_LOCK, get_db
from services.dbt_compiler import get_manifest
from services.dbt_runner import _fail_run
from services.progress import LIVE_PROGRESS, PROGRESS_LOCK, cleanup_progress, init_progress

WORK_DIR = Path(os.environ.get("DATABRICKS_OPENIVM_WORK_DIR", "/data/processed/databricks-openivm"))
COMPILER_URL = os.environ.get("OPENIVM_SQL_COMPILER_URL", "http://openivm-sql-compiler:8998")
_RUN_LOCK = threading.Lock()


def quote(name):
    return ".".join("`" + p.replace("`", "``") + "`" for p in name.split("."))


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def ordered_models(manifest):
    nodes = manifest.get("nodes", {})
    selected = {uid: n for uid, n in nodes.items()
                if n.get("resource_type") == "model"
                and n.get("config", {}).get("materialized") != "ephemeral"}
    ordered = []
    remaining = dict(selected)
    def dependencies(uid):
        deps = set()
        for dep in nodes[uid].get("depends_on", {}).get("nodes", []):
            if dep in selected:
                deps.add(dep)
            elif dep in nodes and nodes[dep].get("config", {}).get("materialized") == "ephemeral":
                deps.update(dependencies(dep))
        return deps
    while remaining:
        ready = sorted(uid for uid, n in remaining.items()
                       if not dependencies(uid) & remaining.keys())
        if not ready:
            raise RuntimeError("Model dependency graph contains a cycle")
        for uid in ready:
            ordered.append((uid, remaining.pop(uid)))
    return ordered


def relation(node):
    return ".".join((node["database"], node["schema"], node.get("alias") or node["name"]))


def physical(name):
    parts = name.split(".")
    parts[-1] = "_openivm_data_" + parts[-1]
    return ".".join(parts)


def remap_sql(sql, mapping, paths=None, snapshots=None):
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
            replacement = sqlglot.to_table(quote(target), dialect="databricks")
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
    return tree.sql(dialect="databricks")


def _save(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, indent=2))
    tmp.replace(path)


class SqlExecutor:
    def __init__(self):
        from services import databricks_enzyme_sources as sources
        self.connection = sources._get_connection()
        self.statements = []

    def execute(self, sql, *, record=True):
        started = time.monotonic()
        entry = {"sql": sql, "query_id": None, "status": "running"}
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
            if record:
                entry["duration_s"] = time.monotonic() - started
                self.statements.append(entry)

    def schema(self, table):
        rows = self.execute(f"DESCRIBE TABLE {quote(table)}", record=False)
        fields = []
        for row in rows:
            if not row[0] or row[0].startswith("#"):
                break
            # Spark parses DDL types through the schema-only compiler.
            fields.append({"name": row[0], "ddl_type": row[1]})
        return fields

    def version(self, table):
        rows = self.execute(f"DESCRIBE HISTORY {quote(table)} LIMIT 1", record=False)
        if not rows:
            raise RuntimeError(f"No Delta version for {table}")
        return int(rows[0][0])


def compiler(payload):
    response = requests.post(f"{COMPILER_URL}/compile", json=payload, timeout=300)
    response.raise_for_status()
    return response.json()


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


def build_models(executor, manifest, checkpoint, *, full_refresh, batch_num):
    models = ordered_models(manifest)
    known = {relation(n): physical(relation(n)) for _, n in models}
    known.update({relation(n): relation(n) for n in manifest.get("sources", {}).values()})
    state = checkpoint.setdefault("models", {})
    by_relation = {relation(n): uid for uid, n in models}
    for uid, node in models:
        checkpoint["active_model"] = uid
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
            progress = LIVE_PROGRESS.get(checkpoint["run_id"])
            if progress is not None:
                progress["total"] = len(models)
                progress["events"][uid] = {"unique_id": uid, "name": name, "resource_type": "model",
                                            "status": "running", "execution_time_s": None}
        if full_refresh:
            schemas = {dep.split(".")[-1]: executor.schema(dep) for dep in deps}
            compiled = compiler({"operation": "compile", "name": name,
                                 "query": remap_sql(query, mapping), "schemas": schemas})
            initial = remap_sql(compiled["initial_sql"], {v: k for k, v in mapping.items()})
            executor.execute(f"CREATE TABLE {quote(target)} USING DELTA "
                             "TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS " + initial)
            columns = ", ".join(quote(f["name"]) for f in compiled["output_schema"]["fields"])
            having = compiled.get("having_predicate")
            executor.execute(f"CREATE VIEW {quote(public)} AS SELECT {columns} FROM {quote(target)}"
                             + (" WHERE " + having if having else ""))
            state[uid] = {**compiled, "query": query, "schemas": schemas,
                          "source_versions": {dep: executor.version(known[dep]) for dep in deps}}
        else:
            saved = state[uid]
            if saved["query"] != query:
                raise RuntimeError(f"Model changed after initialization: {uid}")
            ends = {dep: executor.version(known[dep]) for dep in deps}
            if any(ends[dep] < saved["source_versions"][dep] for dep in deps):
                raise RuntimeError(f"Source version moved backwards for {uid}")
            if saved["refresh_type"] == 3:
                executor.execute(f"INSERT OVERWRITE {quote(target)} " + query)
            elif any(ends[d] != saved["source_versions"][d] for d in deps):
                for dep in deps:
                    short = dep.split(".")[-1]
                    parent = state.get(by_relation.get(dep), {})
                    executor.execute(_cdf_sql(short, known[dep], saved["schemas"][short],
                                              saved["source_versions"][dep], ends[dep], parent.get("having_predicate")))
                rewritten = compiler({"operation": "rewrite", "name": name,
                                      "compiled_sql": saved["compiled_sql"], "schemas": saved["schemas"],
                                      "source_versions": {d.split(".")[-1]: v for d, v in saved["source_versions"].items()},
                                      "mv_version": executor.version(target)})
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
        _record_node(checkpoint["run_id"], uid, node, time.monotonic() - started)
        with PROGRESS_LOCK:
            progress = LIVE_PROGRESS.get(checkpoint["run_id"])
            if progress is not None:
                progress["events"][uid].update(status="success", execution_time_s=time.monotonic() - started)
        _save(WORK_DIR / "checkpoint.json", checkpoint)


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
    executor = None
    manifest = None
    checkpoint = {}
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
            manifest = get_manifest("databricks-enzyme", force_compile=full_refresh)
            if not manifest or not ordered_models(manifest):
                raise RuntimeError("Could not compile Databricks TPC-DI models")
            executor = SqlExecutor()
            build_models(executor, manifest, checkpoint, full_refresh=full_refresh, batch_num=batch_num)
            checkpoint.update(dirty=False, batch_num=batch_num)
            checkpoint.pop("active_model", None)
            _save(path, checkpoint)
            with DB_LOCK:
                conn = get_db()
                conn.execute("UPDATE runs SET status='completed', completed_at=?, duration_s=? WHERE run_id=?",
                             (datetime.now(timezone.utc).isoformat(), time.monotonic() - started, run_id))
                conn.commit()
                conn.close()
        except Exception as exc:
            if manifest and checkpoint.get("active_model"):
                uid = checkpoint["active_model"]
                _record_node(run_id, uid, manifest["nodes"][uid], 0, "error", str(exc))
            _fail_run(run_id, str(exc), time.monotonic() - started)
        finally:
            if executor:
                _save(WORK_DIR / f"sql-batch{batch_num}.json", {"run_id": run_id, "statements": executor.statements})
                (WORK_DIR / f"sql-batch{batch_num}.sql").write_text(";\n\n".join(
                    s["sql"].rstrip(";") for s in executor.statements) + ";\n")
            cleanup_progress(run_id)


def validate(batch_num):
    checkpoint = json.loads((WORK_DIR / "checkpoint.json").read_text())
    if checkpoint.get("dirty") or checkpoint["batch_num"] != batch_num:
        raise RuntimeError("Cannot validate an incomplete refresh")
    manifest = get_manifest("databricks-enzyme")
    executor = SqlExecutor()
    results = []
    for uid, node in ordered_models(manifest):
        query = checkpoint["models"][uid]["query"]
        actual = "SELECT * FROM " + quote(relation(node))
        missing = executor.execute(f"SELECT COUNT(*) FROM (({query}) EXCEPT ALL ({actual}))")[0][0]
        extra = executor.execute(f"SELECT COUNT(*) FROM (({actual}) EXCEPT ALL ({query}))")[0][0]
        results.append({"model": node["name"], "missing_rows": missing, "extra_rows": extra})
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
    client = sources._workspace_client()
    results = []
    for offset in range(0, len(ids), 100):
        response = client.query_history.list(filter_by=QueryFilter(statement_ids=ids[offset:offset + 100]),
                                             include_metrics=True, max_results=100)
        results.extend(q.as_dict() for q in (response.res or []))
    observed = {q.get("query_id") for q in results}
    measured = sum((q.get("metrics") or {}).get("photon_total_time_ms") is not None for q in results)
    payload = {"status": "ok" if ids and set(ids) <= observed and measured == len(ids) else "partial", "batch_num": batch_num,
               "statement_count": len(ids), "observed_statement_count": len(observed), "queries": results,
               "metrics_statement_count": measured,
               "photon_total_time_ms": sum((q.get("metrics") or {}).get("photon_total_time_ms") or 0 for q in results),
               "task_total_time_ms": sum((q.get("metrics") or {}).get("task_total_time_ms") or 0 for q in results)}
    _save(WORK_DIR / f"photon-batch{batch_num}.json", payload)
    return payload
