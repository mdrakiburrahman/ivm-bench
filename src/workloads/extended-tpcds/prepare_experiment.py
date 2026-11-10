"""Prepare one OAT experiment's immutable data and mutation plan."""

import json
import math
import os
import shutil
import tempfile
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


def source_storage(connection, directory, manifest):
    """Measure the complete initial input, independently of engine storage."""
    tables = {}
    for name, image in manifest["initial"].items():
        path = directory / image["path"]
        uncompressed = connection.execute(
            "SELECT coalesce(sum(total_uncompressed_size),0) FROM parquet_metadata(?)", [str(path)]
        ).fetchone()[0]
        tables[name] = {"rows": image["rows"], "parquet_bytes": path.stat().st_size,
                        "parquet_uncompressed_bytes": int(uncompressed)}
    return {"tables": tables, **{key: sum(table[key] for table in tables.values())
                                for key in ("rows", "parquet_bytes", "parquet_uncompressed_bytes")}}


def prepare(directory, scale_factor, options, initial_pct, steps):
    target = options.get("source_size_gb")
    if target is None:
        return _prepare(directory, scale_factor, options, initial_pct, steps)
    if isinstance(target, bool) or not isinstance(target, (int, float)) or not math.isfinite(target) or target <= 0:
        raise ValueError("source_size_gb must be positive and finite")
    if options["mode"] == "days" or mutations.bucket_width(initial_pct) != mutations.SCORE_MODULUS:
        raise ValueError("source_size_gb requires a complete initial snapshot")
    directory = Path(directory)
    recipe = preparation_recipe(scale_factor, options, initial_pct, steps)
    metadata = directory / "preparation.json"
    if metadata.is_file():
        previous = json.loads(metadata.read_text())
        if previous["recipe"] != recipe:
            raise ValueError("cached source preparation has a different recipe")
        return previous
    if directory.exists() and any(directory.iterdir()):
        raise ValueError("incomplete source cache; choose a new output directory")
    directory.mkdir(parents=True, exist_ok=True)
    target_bytes = target * 1_000_000_000
    # Measure a small real export, then correct the base SF from actual bytes.
    # Never trim individual tables: regenerate the relational dataset together.
    base_sf = min(0.1, float(scale_factor))
    attempts = []
    with tempfile.TemporaryDirectory(prefix="sizing-", dir=directory) as scratch:
        for attempt in range(4):
            candidate = Path(scratch) / str(attempt)
            result = _prepare(candidate, base_sf, options, initial_pct, steps)
            measured = result["source_storage"]["parquet_uncompressed_bytes"]
            attempts.append({"base_scale_factor": base_sf, "source_bytes": measured})
            print("Extended TPC-DS source sizing: " + json.dumps(attempts[-1]), flush=True)
            if abs(measured / target_bytes - 1) <= 0.10:
                result["recipe"] = recipe
                result["sizing"] = {"target_bytes": target_bytes, "relative_tolerance": 0.10,
                                    "basis": "initial Parquet column chunks before compression",
                                    "attempts": attempts}
                (candidate / "plan").rename(directory / "plan")
                metadata.write_text(json.dumps(result, indent=2) + "\n")
                return result
            if measured <= 0:
                raise ValueError("cannot size an empty source export")
            shutil.rmtree(candidate)
            base_sf *= target_bytes / measured
            # DuckDB's pinned dsdgen truncates SF >= 1 to whole-number
            # row-count scales. Round the estimate rather than undershooting.
            if base_sf >= 1:
                base_sf = float(round(base_sf))
    raise ValueError("source size did not converge within 10%; engine execution refused")


def preparation_recipe(scale_factor, options, initial_pct, steps):
    return {"preparation_version": 8, "scale_factor": scale_factor, "options": options, "initial_pct": initial_pct,
            "steps": steps, "duckdb_generator_version": duckdb.__version__}


def _prepare(directory, scale_factor, options, initial_pct, steps):
    directory = Path(directory)
    resources = {
        "memory_limit": os.environ.get("DUCKDB_MEM_LIMIT", "4GB"),
        "threads": int(os.environ.get("DUCKDB_THREADS", "4")),
        "max_temp_directory_size": os.environ.get("DUCKDB_MAX_TEMP_DIRECTORY_SIZE", "4GB"),
    }
    recipe = preparation_recipe(scale_factor, options, initial_pct, steps)
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
    full_initial = options["mode"] != "days" and mutations.bucket_width(initial_pct) == mutations.SCORE_MODULUS
    with stage("extended history generation and statistics"), duckdb.connect(str(extension_path), config=resources) as connection:
        connection.execute("ATTACH " + generate.quote(base_path) + " AS raw (READ_ONLY)")
        connection.execute("BEGIN")
        try:
            config = generate.prepare(connection, "raw.main", profile=options["profile"], seed=options["seed"],
                                      report_date=options["report_date"], history_months=options["history_months"],
                                      base_scale_factor=scale_factor,
                                      base_generator=f"DuckDB tpcds extension {duckdb.__version__}", source_views=True, validate_source_ids=not full_initial)
            achieved = generate.statistics(connection)
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
    # Insert-only planning reads immutable projections. Updates/deletes still
    # need disk-backed working tables so planning can evict them during replay.
    insert_only = options["mode"] in ("insertion", "repeated_insertions")
    with duckdb.connect(str(directory / "planning.duckdb"), config=resources) as connection:
        connection.execute("ATTACH " + generate.quote(base_path) + " AS raw (READ_ONLY)")
        connection.execute("ATTACH " + generate.quote(extension_path) + " AS canonical (READ_ONLY)")
        for table in [*mutations.sources(), "xt_run", "xt_engagements", "xt_customers", "xt_accounts"]:
            with stage(f"{'bind' if insert_only else 'copy'} source {table}"):
                kind = "VIEW" if insert_only else "TABLE"
                connection.execute(f"CREATE {kind} {table} AS SELECT * FROM canonical.main.{table}")
        with stage("refresh planning and exports"):
            manifest = mutations.plan(connection, directory / "plan", mode=options["mode"],
                                      initial_pct=initial_pct, steps=steps, days=options["days"],
                                      seed=options["seed"], mutation_cohort=options["mutation_cohort"],
                                      mutate_source=not insert_only,
                                      expected_initial_rows=achieved["extension_rows"] if full_initial else None)
        storage = source_storage(connection, directory / "plan", manifest)
    result = {"recipe": recipe, "configuration": config, "achieved": achieved,
              "plan_id": manifest["plan_id"], "batches": len(manifest["batches"]) + 1,
              "source_storage": storage}
    # Engines replay the immutable Parquet plan; these databases are preparation
    # scratch, not engine inputs. Release their space before any engine starts.
    for path in (base_path, extension_path, directory / "planning.duckdb"):
        path.unlink(missing_ok=True)
    metadata.write_text(json.dumps(result, indent=2) + "\n")
    return result


if __name__ == "__main__":
    print(json.dumps(prepare("/data/output", float(os.environ["SCALE_FACTOR"]),
                            json.loads(os.environ["EXTENDED_TPCDS_OPTIONS"]), os.environ["BATCH_1_PCT"],
                            json.loads(os.environ["EXTENDED_TPCDS_STEPS"])), indent=2))
