#!/usr/bin/env python3
import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path


BATCH1_ONLY_TABLES = [
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

STAGING_CATEGORY_A = [
    "cash_transaction",
    "daily_market",
    "holding_history",
    "prospect",
    "trade",
    "watch_history",
]

STAGING_CATEGORY_B = ["account", "customer", "batch_date"]

CDC_TABLES = {
    "cash_transaction",
    "daily_market",
    "holding_history",
    "trade",
    "watch_history",
}

CATEGORY_B_SCHEMAS = {
    "account": [
        ("cdc_flag", "VARCHAR"),
        ("cdc_dsn", "BIGINT"),
        ("accountid", "BIGINT"),
        ("ca_b_id", "BIGINT"),
        ("ca_c_id", "BIGINT"),
        ("accountdesc", "VARCHAR"),
        ("taxstatus", "TINYINT"),
        ("ca_st_id", "VARCHAR"),
    ],
    "customer": [
        ("cdc_flag", "VARCHAR"),
        ("cdc_dsn", "BIGINT"),
        ("customerid", "BIGINT"),
        ("taxid", "VARCHAR"),
        ("status", "VARCHAR"),
        ("lastname", "VARCHAR"),
        ("firstname", "VARCHAR"),
        ("middleinitial", "VARCHAR"),
        ("gender", "VARCHAR"),
        ("tier", "TINYINT"),
        ("dob", "DATE"),
        ("addressline1", "VARCHAR"),
        ("addressline2", "VARCHAR"),
        ("postalcode", "VARCHAR"),
        ("city", "VARCHAR"),
        ("stateprov", "VARCHAR"),
        ("country", "VARCHAR"),
        ("c_ctry_1", "VARCHAR"),
        ("c_area_1", "VARCHAR"),
        ("c_local_1", "VARCHAR"),
        ("c_ext_1", "VARCHAR"),
        ("c_ctry_2", "VARCHAR"),
        ("c_area_2", "VARCHAR"),
        ("c_local_2", "VARCHAR"),
        ("c_ext_2", "VARCHAR"),
        ("c_ctry_3", "VARCHAR"),
        ("c_area_3", "VARCHAR"),
        ("c_local_3", "VARCHAR"),
        ("c_ext_3", "VARCHAR"),
        ("email1", "VARCHAR"),
        ("email2", "VARCHAR"),
        ("lcl_tx_id", "VARCHAR"),
        ("nat_tx_id", "VARCHAR"),
    ],
    "batch_date": [("batchdate", "DATE")],
}


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def relation(catalog, schema, name):
    return ".".join(quote_ident(part) for part in [catalog, schema, name])


def strip_semicolon(sql):
    sql = sql.strip()
    while sql.endswith(";"):
        sql = sql[:-1].strip()
    return sql


def parquet_files(path):
    files = sorted(
        file for file in path.rglob("*.parquet")
        if "_delta_log" not in file.parts
    )
    if not files:
        raise FileNotFoundError(f"No data parquet files found under {path}")
    return files


def read_parquet_sql(path):
    files = ", ".join(quote_literal(file) for file in parquet_files(path))
    return f"read_parquet([{files}], union_by_name = true)"


def run_duckdb(duckdb_bin, db_file, sql, label, echo=False):
    prefix = f".bail on\n.timer off\n"
    proc = subprocess.run(
        [str(duckdb_bin), str(db_file)],
        input=prefix + sql,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    if echo and proc.stdout:
        print(proc.stdout)
    if proc.returncode != 0:
        raise RuntimeError(f"{label} failed:\n{proc.stdout}")
    return proc.stdout


def attach_sql(meta_path, data_path):
    return f"""
LOAD openivm;
SET ivm_cascade_refresh='off';
INSTALL icu;
LOAD icu;
INSTALL ducklake;
LOAD ducklake;
ATTACH {quote_literal(meta_path)} AS ducklake (
    TYPE ducklake,
    DATA_PATH {quote_literal(data_path)}
);
"""


def source_path(raw_delta_dir, batch, table):
    return raw_delta_dir / f"batch{batch}" / table


def ensure_source_exists(path, label):
    if not path.exists():
        raise FileNotFoundError(f"Missing source for {label}: {path}")


def create_empty_table_sql(schema, table_name):
    cols = ", ".join(f"{quote_ident(name)} {typ}" for name, typ in schema)
    return f"CREATE TABLE {relation('ducklake', 'tpcdi', table_name)} ({cols});"


def source_init_sql(raw_delta_dir):
    statements = [
        "CREATE SCHEMA IF NOT EXISTS ducklake.tpcdi;",
        "CREATE SCHEMA IF NOT EXISTS ducklake.bronze;",
        "CREATE SCHEMA IF NOT EXISTS ducklake.silver;",
        "CREATE SCHEMA IF NOT EXISTS ducklake.gold;",
    ]

    for table in BATCH1_ONLY_TABLES:
        path = source_path(raw_delta_dir, 1, table)
        ensure_source_exists(path, f"batch1/{table}")
        statements.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', f'batch1_{table}')} AS "
            f"SELECT * FROM {read_parquet_sql(path)};"
        )

    audit_path = raw_delta_dir / "audit"
    if audit_path.exists():
        statements.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', 'audit')} AS "
            f"SELECT * FROM {read_parquet_sql(audit_path)};"
        )

    for table in STAGING_CATEGORY_A:
        path = source_path(raw_delta_dir, 1, table)
        ensure_source_exists(path, f"batch1/{table}")
        select_prefix = ""
        if table in CDC_TABLES:
            select_prefix = "NULL::VARCHAR AS cdc_flag, NULL::BIGINT AS cdc_dsn, "
        statements.append(
            f"CREATE TABLE {relation('ducklake', 'tpcdi', f'staging_{table}')} AS "
            f"SELECT {select_prefix}* FROM {read_parquet_sql(path)};"
        )

    for table in STAGING_CATEGORY_B:
        statements.append(create_empty_table_sql(CATEGORY_B_SCHEMAS[table], f"staging_{table}"))

    return "\n".join(statements)


def source_append_sql(raw_delta_dir, batch_num):
    statements = []
    for table in STAGING_CATEGORY_A + STAGING_CATEGORY_B:
        path = source_path(raw_delta_dir, batch_num, table)
        if not path.exists():
            continue
        statements.append(
            f"INSERT INTO {relation('ducklake', 'tpcdi', f'staging_{table}')} "
            f"SELECT * FROM {read_parquet_sql(path)};"
        )
    return "\n".join(statements)


def model_schema_map(models_dir):
    result = {}
    for path in models_dir.rglob("*.sql"):
        rel = path.relative_to(models_dir)
        name = path.stem
        top = rel.parts[0]
        if top in {"bronze", "silver", "gold"}:
            result[name] = top
        elif top == "work":
            result[name] = "work"
    return result


def load_model_nodes(duckdb_batch1_json, models_dir):
    with open(duckdb_batch1_json) as f:
        data = json.load(f)
    schemas = model_schema_map(models_dir)
    nodes = []
    for node in data.get("nodes", []):
        if node.get("resource_type") != "model":
            continue
        name = node["name"]
        schema = schemas.get(name)
        if not schema or schema == "work":
            continue
        compiled_sql = strip_semicolon(node.get("compiled_sql") or "")
        if not compiled_sql:
            raise ValueError(f"Missing compiled_sql for model {name}")
        item = dict(node)
        item["schema"] = schema
        item["compiled_sql"] = compiled_sql
        nodes.append(item)
    if not nodes:
        raise ValueError(f"No model nodes found in {duckdb_batch1_json}")
    return topological_sort(nodes)


def topological_sort(nodes):
    by_id = {n["unique_id"]: n for n in nodes}
    indegree = {uid: 0 for uid in by_id}
    children = defaultdict(list)
    for node in nodes:
        for dep in node.get("depends_on") or []:
            if dep in by_id:
                indegree[node["unique_id"]] += 1
                children[dep].append(node["unique_id"])
    queue = deque(sorted([uid for uid, degree in indegree.items() if degree == 0]))
    ordered = []
    while queue:
        uid = queue.popleft()
        ordered.append(by_id[uid])
        for child in sorted(children[uid]):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if len(ordered) != len(nodes):
        raise ValueError("Cycle detected in dbt model graph")
    return ordered


def create_mv_sql(node):
    target = relation("ducklake", node["schema"], node["name"])
    return f"CREATE MATERIALIZED VIEW {target} AS (\n{node['compiled_sql']}\n);"


def refresh_mv_sql(node):
    return (
        f"PRAGMA ivm_options("
        f"{quote_literal('ducklake')}, {quote_literal(node['schema'])}, {quote_literal(node['name'])}"
        f");"
    )


def compare_sql(node):
    target = relation("ducklake", node["schema"], node["name"])
    query = node["compiled_sql"]
    return f"""
CREATE TEMP TABLE openivm_actual AS
    SELECT * FROM {target};

CREATE TEMP TABLE openivm_expected AS
{query};

CREATE TEMP TABLE openivm_diff AS
    (SELECT * FROM openivm_actual
     EXCEPT ALL
     SELECT * FROM openivm_expected)
    UNION ALL
    (SELECT * FROM openivm_expected
     EXCEPT ALL
     SELECT * FROM openivm_actual);

SELECT CASE
    WHEN (SELECT COUNT(*) FROM openivm_diff) = 0 THEN 'ok'
    ELSE error('OpenIVM comparison failed for {node["schema"]}.{node["name"]}')
END AS verification;
"""


def run_model(duckdb_bin, db_file, attach, node, sql, label):
    start = time.monotonic()
    run_duckdb(duckdb_bin, db_file, attach + sql, label)
    return round(time.monotonic() - start, 4)


def result_node(node, duration_s, status="success"):
    return {
        "run_id": None,
        "unique_id": node["unique_id"],
        "name": node["name"],
        "resource_type": "model",
        "execution_time_s": duration_s,
        "status": status,
        "compiled_sql": node["compiled_sql"],
        "depends_on": node.get("depends_on") or [],
        "rows_affected": None,
    }


def write_result(state_dir, batch_num, scale_factor, duration_s, nodes, comparisons, full_refresh):
    created = datetime.now(timezone.utc).isoformat()
    result = {
        "engine": "openivm",
        "scale_factor": scale_factor,
        "full_refresh": full_refresh,
        "status": "completed",
        "created_at": created,
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(duration_s, 4),
        "nodes": nodes,
        "edges": [
            {"from": dep, "to": node["unique_id"]}
            for node in nodes
            for dep in node.get("depends_on", [])
        ],
        "comparisons": comparisons,
    }
    state_dir.mkdir(parents=True, exist_ok=True)
    path = state_dir / f"run-openivm-batch{batch_num}.json"
    with open(path, "w") as f:
        json.dump(result, f, indent=2)
    return path


def timed_duration(nodes):
    return round(sum(node.get("execution_time_s") or 0 for node in nodes), 4)


def build_openivm_if_needed(root, openivm_dir, duckdb_bin):
    if duckdb_bin.exists():
        return
    try:
        submodule_path = openivm_dir.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"OpenIVM directory must be inside the repository: {openivm_dir}") from exc
    subprocess.run(["git", "submodule", "update", "--init", "--recursive", str(submodule_path)], check=True)
    subprocess.run(["bash", "-lc", "GEN=ninja make"], cwd=openivm_dir, check=True)


def main():
    parser = argparse.ArgumentParser(description="Run the OpenIVM TPC-DI benchmark path.")
    parser.add_argument("--scale-factor", type=int, required=True)
    parser.add_argument("--raw-delta-dir", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--duckdb-batch1-json", type=Path, required=True)
    parser.add_argument("--models-dir", type=Path, default=Path("src/containers/dbt-server/dbt-projects/duckdb/models"))
    parser.add_argument("--openivm-dir", type=Path, default=Path("third_party/openivm"))
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args()

    root = Path.cwd()
    openivm_dir = (root / args.openivm_dir).resolve()
    duckdb_bin = openivm_dir / "build/release/duckdb"
    build_openivm_if_needed(root, openivm_dir, duckdb_bin)

    raw_delta_dir = args.raw_delta_dir.resolve()
    if not raw_delta_dir.exists():
        raise FileNotFoundError(f"Raw Delta directory does not exist: {raw_delta_dir}")
    if not args.duckdb_batch1_json.exists():
        raise FileNotFoundError(f"DuckDB batch1 result JSON is required: {args.duckdb_batch1_json}")

    models = load_model_nodes(args.duckdb_batch1_json, args.models_dir)

    if args.work_dir.exists():
        shutil.rmtree(args.work_dir)
    args.work_dir.mkdir(parents=True)
    db_file = args.work_dir / "openivm.duckdb"
    meta_path = args.work_dir / "openivm.ducklake"
    data_path = args.work_dir / "data"
    attach = attach_sql(meta_path, data_path)

    print("=== OpenIVM benchmark: batch 1 create ===")
    run_duckdb(duckdb_bin, db_file, attach + source_init_sql(raw_delta_dir), "openivm source init")
    nodes = []
    comparisons = []
    # Timers cover only OpenIVM CREATE/REFRESH execution. This path reuses DuckDB dbt's
    # batch-1 compiled SQL and intentionally excludes source loading, dbt orchestration,
    # and correctness comparisons so the JSON is comparable to other engine timings.
    for node in models:
        label = f"create {node['schema']}.{node['name']}"
        duration = run_model(duckdb_bin, db_file, attach, node, create_mv_sql(node), label)
        run_duckdb(duckdb_bin, db_file, attach + compare_sql(node), f"compare {node['schema']}.{node['name']}")
        nodes.append(result_node(node, duration))
        comparisons.append({"name": node["name"], "schema": node["schema"], "batch": 1, "status": "ok"})
        print(f"  {label}: {duration:.4f}s")
    path = write_result(args.results_dir, 1, args.scale_factor, timed_duration(nodes), nodes, comparisons, True)
    print(f"  saved {path}")

    for batch_num in [2, 3]:
        print(f"=== OpenIVM benchmark: batch {batch_num} append + refresh ===")
        append = source_append_sql(raw_delta_dir, batch_num)
        if append.strip():
            run_duckdb(duckdb_bin, db_file, attach + append, f"openivm source append batch {batch_num}")
        nodes = []
        comparisons = []
        for node in models:
            label = f"refresh {node['schema']}.{node['name']}"
            duration = run_model(duckdb_bin, db_file, attach, node, refresh_mv_sql(node), label)
            run_duckdb(duckdb_bin, db_file, attach + compare_sql(node), f"compare {node['schema']}.{node['name']}")
            nodes.append(result_node(node, duration))
            comparisons.append({"name": node["name"], "schema": node["schema"], "batch": batch_num, "status": "ok"})
            print(f"  {label}: {duration:.4f}s")
        path = write_result(args.results_dir, batch_num, args.scale_factor, timed_duration(nodes), nodes, comparisons, False)
        print(f"  saved {path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"OpenIVM benchmark failed: {exc}", file=sys.stderr)
        sys.exit(1)
