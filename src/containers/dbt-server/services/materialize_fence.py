"""Strict-serializable completion fence for every maintained dbt output."""

import json
import time
from pathlib import Path

from psycopg2 import sql

from sql_catalog import load_sql

PROJECT = Path("/app/dbt-projects/materialize-emulator")
TARGETS = Path("/app/sql_catalog/project_targets.json")


def expected_models(manifest_path=PROJECT / "target" / "manifest.json", targets_path=TARGETS):
    with targets_path.open(encoding="utf-8") as file:
        targets = json.load(file)
    expected = {
        path
        for group in targets["models"]
        if "materialize-emulator" in group["engines"]
        for path in group["paths"]
    }
    standard = {
        path
        for group in targets["models"]
        if "spark" in group["engines"]
        for path in group["paths"]
    }
    if len(expected) != 50 or expected != standard or "work/wrk_company_financials" not in expected:
        raise RuntimeError(
            f"Materialize catalog must contain the 50 standard model targets, got {len(expected)}; "
            f"missing={sorted(standard - expected)}, unexpected={sorted(expected - standard)}"
        )
    with manifest_path.open(encoding="utf-8") as file:
        manifest = json.load(file)
    models = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] != "model" or node["package_name"] != "tpcdi":
            continue
        path = node["original_file_path"]
        if not path.startswith("models/") or not path.endswith(".sql"):
            raise RuntimeError(f"Unexpected dbt model path: {path}")
        path = path[len("models/"):-len(".sql")]
        if path in models:
            raise RuntimeError(f"Duplicate Materialize model: {path}")
        models[path] = node
    if set(models) != expected:
        raise RuntimeError(
            f"Materialize manifest mismatch: missing={sorted(expected - set(models))}, "
            f"unexpected={sorted(set(models) - expected)}"
        )
    outputs = []
    for path, node in models.items():
        kind = node["config"]["materialized"]
        if path == "work/wrk_company_financials" and kind == "ephemeral":
            continue
        if kind != "materialized_view":
            raise RuntimeError(f"Materialize model {path} is {kind}, not a materialized view")
        outputs.append(node)
    identities = [(node["schema"], node["alias"]) for node in outputs]
    if len(outputs) != 49 or len(set(identities)) != len(identities):
        raise RuntimeError("Materialize manifest must contain 49 distinct materialized views")
    return sorted(outputs, key=lambda node: (node["schema"], node["alias"]))


def fence_outputs(connection, timeout_s=7200):
    """Wait for each output to be readable at a timestamp after committed writes."""
    outputs = [(node["schema"], node["alias"]) for node in expected_models()]
    deadline = time.monotonic() + timeout_s
    with connection.cursor() as cursor:
        def statement(name, **params):
            return load_sql(
                "materialize-emulator", "inline",
                f"dbt_server/services_materialize_fence/{name}", **params,
            )

        cursor.execute(statement("set_isolation"))
        cursor.execute(statement("show_isolation"))
        isolation = cursor.fetchone()[0]
        if isolation.lower() != "strict serializable":
            raise RuntimeError(f"Materialize fence has unexpected isolation: {isolation}")
        cursor.execute(statement("materialized_views"))
        missing = set(outputs) - set(cursor.fetchall())
        if missing:
            raise RuntimeError(f"Missing Materialize materialized views: {sorted(missing)}")
        for schema, name in outputs:
            remaining = int(deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("Materialize all-output fence timed out")
            cursor.execute(statement("set_timeout", seconds=remaining))
            relation = sql.Identifier(schema, name).as_string(connection)
            cursor.execute(statement("read_relation", relation=relation))
            cursor.fetchone()
    return {"outputs_fenced": len(outputs), "isolation": isolation}
