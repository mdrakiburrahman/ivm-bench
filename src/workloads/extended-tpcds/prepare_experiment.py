"""Prepare one OAT experiment's immutable data and mutation plan."""

import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

import duckdb

if __package__:
    from . import generate, mutations
else:
    import generate
    import mutations


@contextmanager
def stage(name):
    started = time.monotonic()
    print(f"Preparation started: {name}", flush=True)
    yield
    print(f"Preparation completed: {name} ({time.monotonic() - started:.1f}s)", flush=True)


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
    print("Extended TPC-DS preparation resources: " + json.dumps(resources), flush=True)
    with stage("base TPC-DS generation"), duckdb.connect(str(base_path), config=resources) as connection:
        connection.execute("SET custom_extension_repository='https://extensions.duckdb.org'")
        connection.execute("INSTALL tpcds")
        connection.execute("LOAD tpcds")
        connection.execute(f"CALL dsdgen(sf={float(scale_factor)})")
    with stage("extended history generation and statistics"), duckdb.connect(str(extension_path), config=resources) as connection:
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
    # Persistent working tables can be evicted while COPY sorts the row images.
    # An in-memory database keeps the complete source copy inside the RAM cap.
    with duckdb.connect(str(directory / "planning.duckdb"), config=resources) as connection:
        connection.execute("ATTACH " + generate.quote(extension_path) + " AS canonical (READ_ONLY)")
        for table in [*mutations.sources(), "xt_run", "xt_engagements", "xt_customers", "xt_accounts"]:
            with stage(f"copy source {table}"):
                connection.execute(f"CREATE TABLE {table} AS SELECT * FROM canonical.main.{table}")
        with stage("refresh planning and sorted exports"):
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
