"""Plan deterministic source mutations and export typed before/after row images.

Updates/deletes affect existing source rows before inserts, matching ivm-bench.
The source pool and selection work are outside measured engine execution.
"""

import argparse
import hashlib
import json
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path

import duckdb

if __package__:
    from .generate import HERE, quote
else:
    from generate import HERE, quote

SCORE_MODULUS = 1_000_000
MODES = ("insertion", "updates", "deletes", "days", "repeated_insertions", "mixed")
# These feeds have independently arriving rows; the other sources are loaded
# upfront. Insert copies retain business keys/timestamps and get new physical IDs.
EVENT_DATES = {
    "ext_purchase_usage_events": "try_strptime(purchase_date_key, '%Y%m%d')::DATE",
    "ext_engagement_status_events": "observation_time::DATE",
    "ext_customer_status_events": "try_strptime(event_date_key, '%Y%m%d')::DATE",
    "ext_purchase_receipt_events": "receipt_time::DATE",
    "ext_engagement_observation_events": "observation_time::DATE",
}
UPDATES = {
    "ext_purchase_usage_events": "purchase_quantity=coalesce(purchase_quantity,0)+1",
    "ext_engagement_status_events": "assessment_key=md5('updated:' || coalesce(assessment_key,''))",
    "ext_customer_status_events": "customer_status=CASE WHEN customer_status='inactive' THEN 'active' ELSE 'inactive' END",
    "ext_purchase_receipt_events": "receipt_payload=json_merge_patch(receipt_payload, to_json(struct_pack(ReportedUnits := coalesce(try_cast(json_extract_string(receipt_payload,'$.ReportedUnits') AS INTEGER),0)+1)))",
    "ext_engagement_observation_events": "reported_units=(coalesce(try_cast(reported_units AS INTEGER),0)+1)::VARCHAR",
    "ext_engagement_capacity_history": "reported_units=coalesce(reported_units,0)+1, allocated_units=coalesce(allocated_units,0)+1",
    "ext_engagement_channel_history": "store_eligibility=CASE WHEN store_eligibility IS NULL THEN 'ready' ELSE NULL END",
    "ext_customer_profile_history": "parent_capacity=coalesce(parent_capacity,0)+1, parent_limit=coalesce(parent_limit,0)+2",
    "ext_account_profile": "offer_type=CASE WHEN offer_type='Member' THEN 'Guest' ELSE 'Member' END",
}


def percentage(value):
    try:
        number = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("percentage must be a number") from exc
    if not number.is_finite() or number < 0 or number > 100:
        raise ValueError("percentage must be finite and between 0 and 100")
    return number


def bucket_width(value):
    return int((percentage(value) * SCORE_MODULUS / 100).to_integral_value(rounding=ROUND_HALF_UP))


def score(seed, batch):
    return f"(CAST('0x' || substr(md5({quote(str(seed) + ':' + str(batch))} || ':' || _source_row_id),1,8) AS UBIGINT) % {SCORE_MODULUS})"


def sources():
    return json.loads((HERE / "source-schema.json").read_text())


def default_percentages(mode):
    return {"insert_pct": "1" if mode in ("insertion", "repeated_insertions", "mixed") else "0",
            "update_pct": "1" if mode in ("updates", "mixed") else "0",
            "delete_pct": "1" if mode in ("deletes", "mixed") else "0"}


def cohort_predicate(table, cohort):
    if cohort == "all":
        return "TRUE"
    if cohort not in ("hot", "cold"):
        raise ValueError("mutation cohort must be all, hot or cold")
    if table == "ext_purchase_usage_events":
        key, lookup = "registry_key", "SELECT engagement_registry_key FROM xt_engagements"
    elif table == "ext_engagement_status_events":
        key, lookup = "engagement_registry_key", "SELECT engagement_registry_key FROM xt_engagements"
    elif table == "ext_engagement_channel_history":
        key, lookup = "assessment_key", "SELECT md5('assessment:' || engagement_path) FROM xt_engagements"
    elif table in ("ext_purchase_receipt_events", "ext_engagement_observation_events", "ext_engagement_capacity_history"):
        key, lookup = "engagement_path", "SELECT engagement_path FROM xt_engagements"
    elif table == "ext_customer_status_events":
        key, lookup = "customer_registry_key", "SELECT md5(customer_path) FROM xt_customers"
    elif table == "ext_customer_profile_history":
        key, lookup = "customer_path", "SELECT customer_path FROM xt_customers"
    elif table == "ext_account_profile":
        key, lookup = "account_id", "SELECT account_id FROM xt_accounts"
    else:
        raise ValueError(f"no mutation cohort mapping for {table}")
    return f"{key} IN ({lookup} WHERE household_cohort={quote(cohort)})"


def file_checksum(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def export_rows(connection, sql, path, root):
    connection.execute(f"COPY ({sql} ORDER BY _source_row_id) TO {quote(path)} (FORMAT PARQUET)")
    rows = connection.execute(f"SELECT count(*) FROM read_parquet({quote(path)})").fetchone()[0]
    return {"path": str(path.relative_to(root)), "rows": rows,
            "sha256": file_checksum(path)}


def plan(connection, directory, *, mode="insertion", initial_pct="100", steps=None,
         days=3, seed=17, mutation_cohort="all"):
    if mode not in MODES:
        raise ValueError("unknown mutation mode")
    if type(days) is not int or days < 1:
        raise ValueError("days must be a positive integer")
    if type(seed) is not int:
        raise ValueError("mutation seed must be an integer")
    if mutation_cohort not in ("all", "hot", "cold"):
        raise ValueError("mutation cohort must be all, hot or cold")
    initial_width = bucket_width(initial_pct)
    default = default_percentages(mode)
    steps = [dict(default) for _ in range(days if mode == "days" else 2)] if steps is None else steps
    if mode == "days" and len(steps) != days:
        raise ValueError("days mode requires one step per day")
    for step in steps:
        if set(step) - set(default):
            raise ValueError("unknown batch percentage field")
        if any(key not in step for key in default):
            raise ValueError("each batch must specify insert, update and delete percentages")
        insert, update, delete = [percentage(step[key]) for key in default]
        if update + delete > 100:
            raise ValueError("update and delete percentages must sum to at most 100")
        if mode in ("insertion", "repeated_insertions") and (update or delete):
            raise ValueError("insert modes cannot update or delete; use mixed")
        if mode == "updates" and (insert or delete):
            raise ValueError("updates mode cannot insert or delete; use mixed")
        if mode == "deletes" and (insert or update):
            raise ValueError("deletes mode cannot insert or update; use mixed")
        if mode == "days" and insert:
            raise ValueError("daily arrival sizes follow event dates; insert percentage must be zero")
    directory = Path(directory)
    if directory.exists():
        raise ValueError("plan output must be a new directory")
    configuration = json.loads(connection.execute("SELECT configuration FROM xt_run").fetchone()[0])
    report = date.fromisoformat(configuration["report_date"])
    start = report - timedelta(days=days - 1)
    settings = {"mode": mode, "initial_pct": str(initial_pct), "steps": steps, "days": days,
                "seed": seed, "mutation_cohort": mutation_cohort, "clock": "fixed_report_date",
                "insert_semantics": "source_pool_copies_with_fresh_physical_ids",
                "physical_insert_identity": "md5(insert:source_dataset_id:mutation_seed:batch:original_source_row_id)",
                "repeat_selection": "same_source_selection_each_round" if mode == "repeated_insertions" else "seeded_per_batch"}
    identity = {"mutation_version": 1, "source_configuration": configuration, "settings": settings,
                "update_assignments": UPDATES, "event_date_expressions": EVENT_DATES}
    manifest = {**identity, "workload": "extended-tpcds",
                "plan_id": hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()[:16],
                "schemas": {name: {"_source_row_id": "VARCHAR", **columns} for name, columns in sources().items()},
                "initial": {}, "batches": []}
    directory.mkdir(parents=True)
    (directory / "initial").mkdir()
    # A frozen source pool gives repetitions a fixed denominator and lets days
    # replay withheld arrivals instead of inventing a moving reporting clock.
    for table in EVENT_DATES:
        connection.execute(f"CREATE TEMP TABLE pool_{table} AS SELECT * FROM {table}")
        predicate = f"{score(seed, 1)} < {initial_width}"
        if mode == "days":
            predicate += f" AND {EVENT_DATES[table]} < DATE {quote(start)}"
        connection.execute(f"DELETE FROM {table} WHERE NOT coalesce(({predicate}),FALSE)")
    for table in sources():
        manifest["initial"][table] = export_rows(connection, f"SELECT * FROM {table}", directory / "initial" / f"{table}.parquet", directory)
    for number, step in enumerate(steps, 2):
        batch_path = directory / f"batch{number}"
        batch_path.mkdir()
        summary = {"number": number, "percentages": step, "arrival_date": str(start + timedelta(days=number-2)) if mode == "days" else None,
                   "operations": []}
        update_width = bucket_width(step["update_pct"])
        delete_width = min(bucket_width(step["delete_pct"]), SCORE_MODULUS - update_width)
        for table, assignments in UPDATES.items():
            predicate = cohort_predicate(table, mutation_cohort)
            if table not in EVENT_DATES:
                predicate += " AND is_current"
            for operation, lower, upper in [("update", 0, update_width), ("delete", update_width, update_width + delete_width)]:
                if lower == upper:
                    continue
                # Plan both disjoint buckets against existing physical keys.
                selected = f"({predicate}) AND {score(seed, number)} >= {lower} AND {score(seed, number)} < {upper}"
                connection.execute(f"CREATE OR REPLACE TEMP TABLE changes AS SELECT * FROM {table} WHERE {selected}")
                before = export_rows(connection, "SELECT * FROM changes", batch_path / f"{table}.{operation}.before.parquet", directory)
                if operation == "delete":
                    connection.execute(f"DELETE FROM {table} WHERE _source_row_id IN (SELECT _source_row_id FROM changes)")
                    after = None
                else:
                    connection.execute(f"UPDATE {table} SET {assignments} WHERE _source_row_id IN (SELECT _source_row_id FROM changes)")
                    after = export_rows(connection, f"SELECT * FROM {table} WHERE _source_row_id IN (SELECT _source_row_id FROM changes)", batch_path / f"{table}.{operation}.after.parquet", directory)
                summary["operations"].append({"table": table, "kind": operation, "before": before, "after": after})
        insert_width = bucket_width(step["insert_pct"])
        for table, day in EVENT_DATES.items():
            if mode == "days":
                selected = f"{day}=DATE {quote(summary['arrival_date'])}"
                # Original physical IDs were withheld; no synthetic copies.
                sql = f"SELECT * FROM pool_{table} WHERE {selected}"
            elif insert_width:
                selection_batch = 2 if mode == "repeated_insertions" else number
                selected = f"{score(seed, selection_batch)} < {insert_width}"
                # IDs depend on this arrival, not on future schedule length.
                identity_prefix = f"insert:{configuration['dataset_id']}:{seed}:{number}"
                sql = f"SELECT md5({quote(identity_prefix)} || ':' || _source_row_id) AS _source_row_id, * EXCLUDE(_source_row_id) FROM pool_{table} WHERE {selected}"
            else:
                continue
            # Restrict both synthetic copies and original daily arrivals by the
            # mutation binding; query bindings are independent of this setting.
            sql += " AND " + cohort_predicate(table, mutation_cohort)
            connection.execute("CREATE OR REPLACE TEMP TABLE changes AS " + sql)
            after = export_rows(connection, "SELECT * FROM changes", batch_path / f"{table}.insert.after.parquet", directory)
            connection.execute(f"INSERT INTO {table} SELECT * FROM changes")
            summary["operations"].append({"table": table, "kind": "insert", "before": None, "after": after})
        summary["source_rows"] = {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in sources()}
        manifest["batches"].append(summary)
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def load_initial(connection, manifest, directory):
    if set(manifest["initial"]) != set(sources()):
        raise ValueError("initial snapshot must contain all extension sources")
    connection.execute("BEGIN")
    try:
        for table, image in manifest["initial"].items():
            path = Path(directory) / image["path"]
            if file_checksum(path) != image["sha256"]:
                raise ValueError(f"initial image checksum mismatch: {path.name}")
            connection.execute(f"CREATE TABLE {table} AS SELECT * FROM read_parquet({quote(path)})")
        connection.execute("CREATE TABLE xt_run(configuration JSON)")
        connection.execute("INSERT INTO xt_run VALUES (?)", [json.dumps(manifest["source_configuration"])])
        connection.execute("CREATE TABLE xt_applied_batch(number INTEGER, plan_id VARCHAR)")
        connection.execute("INSERT INTO xt_applied_batch VALUES (1, ?)", [manifest["plan_id"]])
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def apply_batch(connection, manifest, directory, batch):
    """Apply explicit row images atomically; reject replay or out-of-order batches."""
    current, plan_id = connection.execute("SELECT number,plan_id FROM xt_applied_batch").fetchone()
    if manifest["plan_id"] != plan_id:
        raise ValueError("mutation plan does not match the loaded initial snapshot")
    if batch["number"] != current + 1:
        raise ValueError("batches must be applied once, in order")
    connection.execute("BEGIN")
    try:
        for operation in batch["operations"]:
            table = operation["table"]
            if table not in sources():
                raise ValueError("unknown source in mutation batch")
            for kind in ("before", "after"):
                image = operation[kind]
                if image is not None:
                    path = Path(directory) / image["path"]
                    if file_checksum(path) != image["sha256"]:
                        raise ValueError(f"mutation image checksum mismatch: {path.name}")
            if operation["before"] is not None:
                before_path = Path(directory) / operation["before"]["path"]
                # Compare complete before-images, not just keys. Missing rows,
                # duplicate IDs and payload drift must fail before state changes.
                connection.execute(f"CREATE OR REPLACE TEMP TABLE expected_before AS SELECT * FROM read_parquet({quote(before_path)})")
                mismatch = connection.execute(f"SELECT count(*) FROM ((SELECT * FROM expected_before EXCEPT ALL SELECT * FROM {table} WHERE _source_row_id IN (SELECT _source_row_id FROM expected_before)) UNION ALL (SELECT * FROM {table} WHERE _source_row_id IN (SELECT _source_row_id FROM expected_before) EXCEPT ALL SELECT * FROM expected_before))").fetchone()[0]
                if mismatch:
                    raise ValueError(f"before-image mismatch in {table}")
                if operation["kind"] == "delete":
                    connection.execute(f"DELETE FROM {table} WHERE _source_row_id IN (SELECT _source_row_id FROM expected_before)")
            if operation["after"] is not None:
                after_path = Path(directory) / operation["after"]["path"]
                if operation["kind"] == "update":
                    assignments = ", ".join(f"{column}=a.{column}" for column in sources()[table])
                    connection.execute(f"UPDATE {table} SET {assignments} FROM read_parquet({quote(after_path)}) a WHERE {table}._source_row_id=a._source_row_id")
                else:
                    connection.execute(f"INSERT INTO {table} SELECT * FROM read_parquet({quote(after_path)})")
                duplicate = connection.execute(f"SELECT count(*)-count(DISTINCT _source_row_id) FROM {table}").fetchone()[0]
                if duplicate:
                    raise ValueError(f"duplicate physical source keys in {table}")
        connection.execute("UPDATE xt_applied_batch SET number=?", [batch["number"]])
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True, help="generated extension database, read-only")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--mode", choices=MODES, default="insertion")
    parser.add_argument("--batches", type=int, default=3, help="including initial construction; days mode uses --days")
    parser.add_argument("--days", type=int, default=3)
    parser.add_argument("--initial-pct", default="100")
    parser.add_argument("--insert-pct")
    parser.add_argument("--update-pct")
    parser.add_argument("--delete-pct")
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--mutation-cohort", choices=["all", "hot", "cold"], default="all")
    parser.add_argument("--experiment", type=Path, help="one ivm-bench experiment object with batch_N percentages")
    args = parser.parse_args()
    if not args.database.is_file() or args.batches < 1:
        parser.error("input database must exist and batches must be positive")
    defaults = default_percentages(args.mode)
    step = {key: getattr(args, key) if getattr(args, key) is not None else value for key, value in defaults.items()}
    steps = [dict(step) for _ in range(args.days if args.mode == "days" else args.batches - 1)]
    initial_pct = args.initial_pct
    if args.experiment:
        experiment = json.loads(args.experiment.read_text())
        if "experiments" in experiment:
            if len(experiment["experiments"]) != 1:
                parser.error("choose one experiment per mutation plan")
            experiment = experiment["experiments"][0]
        initial_pct = experiment.get("batch_1_pct", experiment.get("batch_1_insert_pct", initial_pct))
        for number, batch in enumerate(steps, 2):
            for key in defaults:
                canonical = f"batch_{number}_pct" if key == "insert_pct" else f"batch_{number}_{key}"
                batch[key] = experiment.get(canonical, experiment.get(f"batch_{number}_{key}", batch[key]))
    with duckdb.connect() as connection:
        connection.execute("ATTACH " + quote(args.database.resolve()) + " AS raw (READ_ONLY)")
        for table in [*sources(), "xt_run", "xt_engagements", "xt_customers", "xt_accounts"]:
            connection.execute(f"CREATE TABLE {table} AS SELECT * FROM raw.main.{table}")
        if args.experiment and "scale_factor" in experiment:
            config = json.loads(connection.execute("SELECT configuration FROM xt_run").fetchone()[0])
            if config.get("base_scale_factor") is not None and float(experiment["scale_factor"]) != config["base_scale_factor"]:
                parser.error("experiment scale factor differs from the generated input")
        manifest = plan(connection, args.output_dir, mode=args.mode, initial_pct=initial_pct,
                        steps=steps, days=args.days, seed=args.seed, mutation_cohort=args.mutation_cohort)
    print(json.dumps({"plan_id": manifest["plan_id"], "mode": args.mode, "batches": len(manifest["batches"]) + 1,
                      "manifest": str(args.output_dir / "manifest.json")}, indent=2))


if __name__ == "__main__":
    main()
