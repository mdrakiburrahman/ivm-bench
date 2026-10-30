"""Prepare one OAT experiment's immutable data and mutation plan."""

import json
import os
from pathlib import Path

import duckdb

if __package__:
    from . import generate, mutations
else:
    import generate
    import mutations


def prepare(directory, scale_factor, options, initial_pct, steps):
    directory = Path(directory)
    resources = {
        "memory_limit": os.environ.get("DUCKDB_MEM_LIMIT", "4GB"),
        "threads": int(os.environ.get("DUCKDB_THREADS", "4")),
        "max_temp_directory_size": os.environ.get("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "4GB"),
    }
    recipe = {"scale_factor": scale_factor, "options": options, "initial_pct": initial_pct,
              "steps": steps, "duckdb_generator_version": duckdb.__version__}
    metadata = directory / "preparation.json"
    if metadata.is_file():
        previous = json.loads(metadata.read_text())
        if previous["recipe"] != recipe:
            raise ValueError("cached source preparation has a different recipe")
        return previous
    directory.mkdir(parents=True, exist_ok=True)
    base_path = directory / "base.duckdb"
    extension_path = directory / "extension.duckdb"
    if base_path.exists() or extension_path.exists():
        raise ValueError("incomplete source cache; choose a new output directory")
    with duckdb.connect(str(base_path), config=resources) as connection:
        connection.execute("SET custom_extension_repository='https://extensions.duckdb.org'")
        connection.execute("INSTALL tpcds")
        connection.execute("LOAD tpcds")
        connection.execute(f"CALL dsdgen(sf={float(scale_factor)})")
    with duckdb.connect(str(extension_path), config=resources) as connection:
        connection.execute("ATTACH " + generate.quote(base_path) + " AS raw (READ_ONLY)")
        connection.execute("BEGIN")
        try:
            config = generate.prepare(connection, "raw.main", profile=options["profile"], seed=options["seed"],
                                      report_date=options["report_date"], history_months=options["history_months"],
                                      base_scale_factor=scale_factor,
                                      base_generator=f"DuckDB tpcds extension {duckdb.__version__}")
            achieved = generate.statistics(connection)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    with duckdb.connect(config=resources) as connection:
        connection.execute("ATTACH " + generate.quote(extension_path) + " AS canonical (READ_ONLY)")
        for table in [*mutations.sources(), "xt_run", "xt_engagements", "xt_customers", "xt_accounts"]:
            connection.execute(f"CREATE TABLE {table} AS SELECT * FROM canonical.main.{table}")
        manifest = mutations.plan(connection, directory / "plan", mode=options["mode"],
                                  initial_pct=initial_pct, steps=steps, days=options["days"],
                                  seed=options["seed"], mutation_cohort=options["mutation_cohort"])
    result = {"recipe": recipe, "configuration": config, "achieved": achieved,
              "plan_id": manifest["plan_id"], "batches": len(manifest["batches"]) + 1}
    metadata.write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    print(json.dumps(prepare("/data/output", float(os.environ["SCALE_FACTOR"]),
                            json.loads(os.environ["EXTENDED_TPCDS_OPTIONS"]), os.environ["BATCH_1_PCT"],
                            json.loads(os.environ["EXTENDED_TPCDS_STEPS"])), indent=2))
