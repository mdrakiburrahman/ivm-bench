"""Replay an Extended TPC-DS mutation plan with DuckDB full recomputation.

The same immutable plan is the input contract for incremental engine adapters.
Every batch is checked against a fresh replay and full-DAG recomputation.
"""

import argparse
import json
import time
from pathlib import Path

import duckdb

if __package__:
    from . import mutations, pipeline
    from .generate import quote
else:
    import mutations
    import pipeline
    from generate import quote


def reference_check(connection, manifest, directory, through_batch, targets, cohort):
    actual_catalog = connection.execute("SELECT current_database()").fetchone()[0]
    search_path = connection.execute("SELECT current_setting('search_path')").fetchone()[0]
    connection.execute("ATTACH ':memory:' AS xt_reference")
    try:
        connection.execute("SET search_path='xt_reference.main'")
        mutations.load_initial(connection, manifest, directory)
        for batch in manifest["batches"]:
            if batch["number"] > through_batch:
                break
            mutations.apply_batch(connection, manifest, directory, batch)
        # Reverse physical input order to expose unspecified latest-row ties.
        for source in mutations.sources():
            connection.execute(f"CREATE OR REPLACE TABLE {source} AS SELECT * FROM {source} ORDER BY _source_row_id DESC")
        result = pipeline.build(connection, targets, cohort)
        differences = {}
        for name in result["model_rows"]:
            actual = '"' + actual_catalog.replace('"', '""') + f'".main.{name}'
            reference = f"xt_reference.main.{name}"
            differences[name] = connection.execute(f"SELECT count(*) FROM ((SELECT * FROM {actual} EXCEPT ALL SELECT * FROM {reference}) UNION ALL (SELECT * FROM {reference} EXCEPT ALL SELECT * FROM {actual}))").fetchone()[0]
        failed = {name: rows for name, rows in differences.items() if rows}
        if failed:
            raise ValueError(f"full-recomputation multiset mismatch: {failed}")
        return {"comparison": "EXCEPT ALL, including primary keys, nulls and duplicates",
                "models_checked": len(differences), "differences": differences}
    finally:
        connection.execute("SET search_path=" + quote(search_path))
        connection.execute("DETACH xt_reference")


def run(manifest_path, output_directory, *, targets=None, cohort="all", validate=True):
    manifest_path, output_directory = Path(manifest_path), Path(output_directory)
    if output_directory.exists():
        raise ValueError("benchmark output must be a new directory")
    if cohort not in ("all", "hot", "cold"):
        raise ValueError("query cohort must be all, hot or cold")
    manifest = json.loads(manifest_path.read_text())
    if manifest["workload"] != "extended-tpcds" or manifest["mutation_version"] != 1:
        raise ValueError("unsupported mutation plan")
    directory = manifest_path.parent
    output_directory.mkdir(parents=True)
    results = {"workload": "extended-tpcds", "engine": "duckdb", "execution": "full_recomputation",
               "plan_id": manifest["plan_id"], "source_configuration": manifest["source_configuration"],
               "schedule": manifest["settings"], "query_cohort": cohort, "validation_enabled": validate,
               "timing_semantics": "model SQL only; source replay includes integrity checks; reference checks excluded",
               "batches": []}
    path = output_directory / "results.json"
    with duckdb.connect(str(output_directory / "work.duckdb")) as connection:
        for number in [1, *[batch["number"] for batch in manifest["batches"]]]:
            start = time.monotonic()
            if number == 1:
                mutations.load_initial(connection, manifest, directory)
            else:
                batch = manifest["batches"][number - 2]
                mutations.apply_batch(connection, manifest, directory, batch)
            source_duration = time.monotonic() - start
            result = pipeline.build(connection, targets, cohort)
            result["number"] = number
            result["source_replay_wall_s_including_guards"] = source_duration
            result["source_rows"] = {table: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in mutations.sources()}
            if number > 1 and result["source_rows"] != batch["source_rows"]:
                raise ValueError("source counts disagree with planned snapshot")
            if validate:
                result["cardinality_checks"] = pipeline.validate(connection, result["model_rows"])
                result["reference_check"] = reference_check(connection, manifest, directory, number, targets, cohort)
            results["batches"].append(result)
            path.write_text(json.dumps(results, indent=2) + "\n")
            print(json.dumps({"batch": number, "models": len(result["model_rows"]),
                              "checked": result.get("reference_check", {}).get("models_checked", 0)}), flush=True)
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pilot", action="store_true", help="only the two pilot facts and their ancestry")
    parser.add_argument("--cohort", choices=["all", "hot", "cold"], default="all")
    parser.add_argument("--no-validate", action="store_true")
    args = parser.parse_args()
    run(args.manifest, args.output_dir, targets=pipeline.PILOT_TARGETS if args.pilot else None,
        cohort=args.cohort, validate=not args.no_validate)


if __name__ == "__main__":
    main()
