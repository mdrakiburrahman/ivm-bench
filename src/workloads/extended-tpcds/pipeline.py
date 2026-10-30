"""Execute the translated materialized DAG with DuckDB full recomputation."""

import argparse
import csv
import json
import math
import time
from datetime import date
from pathlib import Path

import duckdb

if __package__:
    from .cardinality import count_windows
else:
    from cardinality import count_windows

HERE = Path(__file__).resolve().parent
PILOT_TARGETS = ["fact_engagement_status_channel_eligibility_28d_rolling_hist_12m",
                 "fact_purchase_quantity_monthly_snapshot"]


def mapping():
    with (HERE / "model-mapping.csv").open(newline="") as handle:
        return {row["target_model"]: row for row in csv.DictReader(handle)}


def selected_models(targets=None):
    models = mapping()
    selected = set()

    def include(name):
        if name not in models:
            raise ValueError(f"unknown model: {name}")
        if name in selected:
            return
        selected.add(name)
        for parent in filter(None, models[name]["parent_models"].split(";")):
            include(parent)

    for target in models if targets is None else targets:
        include(target)
    return sorted(selected, key=lambda name: (int(models[name]["level"]), name))


def render_model(name, configuration, cohort="all"):
    if cohort not in ("all", "hot", "cold"):
        raise ValueError("cohort must be all, hot or cold")
    report_date = date.fromisoformat(configuration["report_date"])
    months = configuration["history_months"]
    if type(months) is not int or months < 1:
        raise ValueError("history months must be a positive integer")
    return ((HERE / "models" / f"{name}.sql").read_text()
            .replace("__REPORT_DATE__", str(report_date))
            .replace("__history_months__", str(months))
            .replace("__COHORT__", cohort))


def build(connection, targets=None, cohort="all"):
    config = json.loads(connection.execute("SELECT configuration FROM xt_run").fetchone()[0])
    models = selected_models(targets)
    durations = {}
    connection.execute("BEGIN")
    try:
        for name in models:
            sql = render_model(name, config, cohort)
            start = time.monotonic()
            connection.execute(f"CREATE OR REPLACE TABLE {name} AS {sql}")
            durations[name] = time.monotonic() - start
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    # These scans are validation artifacts and are outside the recorded build time.
    counts = {name: connection.execute(f"SELECT count(*) FROM {name}").fetchone()[0] for name in models}
    return {"mode": "full_recomputation", "configuration": config, "cohort": cohort,
            "binding_scope": "eligible_engagement_family", "model_rows": counts,
            "model_duration_s": durations, "total_model_duration_s": sum(durations.values())}


def validate(connection, model_rows):
    checks = {}
    if "int_hist_eligible_status" in model_rows:
        first, last = connection.execute("SELECT min(report_as_of_date)::DATE, max(report_as_of_date)::DATE FROM int_hist_date_spine").fetchone()
        if first is None:
            raise ValueError("historical report spine is empty")
        events = connection.execute("SELECT engagement_registry_key, effective_date::DATE FROM int_hist_eligible_status WHERE effective_date IS NOT NULL").fetchall()
        expected = count_windows(events, first, last, [1, 7, 28])
        for width, suffix in [(1, "daily"), (7, "7d"), (28, "28d")]:
            counts = expected["windows"][str(width)]
            for model, key in [(f"int_hist_active_candidates_{suffix}", "candidate_rows"),
                               (f"int_hist_active_{suffix}", "selected_entity_dates"),
                               (f"int_hist_unpivoted_{suffix}", "unpivot_rows_without_enrichment_fanout")]:
                if model in model_rows:
                    if model_rows[model] != counts[key]:
                        raise ValueError(f"cardinality mismatch for {model}: {model_rows[model]} != {counts[key]}")
                    checks[model] = {"actual": model_rows[model], "expected": counts[key]}
    if "fact_purchase_quantity_monthly_snapshot" in model_rows:
        config = json.loads(connection.execute("SELECT configuration FROM xt_run").fetchone()[0])
        first = date.fromisoformat(config["report_date"])
        dimensions = "household_key, account_offer_key, account_usage_key, platform_key, preference_key, product_tier_key, product_version_key, engagement_type_key, price_catalog_key"
        group = "date_trunc('month', event_time), engagement_path, price_catalog_id, " + dimensions
        expected_rows = connection.execute(f"SELECT count(*) FROM (SELECT {group} FROM int_purchase_usage_transaction WHERE event_time >= ? - INTERVAL 365 DAY GROUP BY ALL)", [first]).fetchone()[0]
        actual = model_rows["fact_purchase_quantity_monthly_snapshot"]
        if actual != expected_rows:
            raise ValueError(f"monthly hidden-grain count mismatch: {actual} != {expected_rows}")
        expected_sum = connection.execute("SELECT sum(purchase_quantity) FROM int_purchase_usage_transaction WHERE event_time >= ? - INTERVAL 365 DAY", [first]).fetchone()[0]
        actual_sum = connection.execute("SELECT sum(purchase_quantity_count) FROM fact_purchase_quantity_monthly_snapshot").fetchone()[0]
        if (expected_sum is None) != (actual_sum is None) or (expected_sum is not None and not math.isclose(actual_sum, expected_sum, rel_tol=1e-10, abs_tol=1e-10)):
            raise ValueError("monthly quantity sum mismatch")
        checks["monthly_quantity"] = {"actual_rows": actual, "expected_rows": expected_rows,
                                      "actual_sum": actual_sum, "expected_sum": expected_sum}
    return checks


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--all-models", action="store_true")
    parser.add_argument("--cohort", choices=["all", "hot", "cold"], default="all")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not args.database.is_file():
        parser.error("database must already contain generated extension data")
    with duckdb.connect(str(args.database)) as connection:
        result = build(connection, None if args.all_models else PILOT_TARGETS, args.cohort)
        result["validation"] = validate(connection, result["model_rows"])
    path = args.output or args.database.with_suffix(f".{args.cohort}.pipeline.json")
    path.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({"models": len(result["model_rows"]), "duration_s": result["total_model_duration_s"],
                      "validation_checks": len(result["validation"]), "artifact": str(path)}, indent=2))


if __name__ == "__main__":
    main()
