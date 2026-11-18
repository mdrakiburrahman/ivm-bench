"""Check maintained outputs against their own compiled Materialize definitions."""

import time

from psycopg2 import sql

from services.materialize_fence import expected_models
from sql_catalog import load_sql


def validate_outputs(connection, timeout_s=7200):
    models = expected_models()
    deadline = time.monotonic() + timeout_s
    with connection.cursor() as cursor:
        cursor.execute(load_sql("materialize-emulator", "inline", "dbt_server/services_materialize_fence/set_isolation"))
        cursor.execute(load_sql("materialize-emulator", "inline", "dbt_server/services_materialize_fence/show_isolation"))
        if cursor.fetchone()[0].lower() != "strict serializable":
            raise RuntimeError("Materialize validation requires strict serializable isolation")
        for node in models:
            remaining = int(deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("Materialize all-output correctness validation timed out")
            cursor.execute(load_sql(
                "materialize-emulator", "inline", "dbt_server/services_materialize_fence/set_timeout",
                seconds=remaining,
            ))
            query = (node.get("compiled_code") or "").strip().rstrip(";")
            if not query:
                raise RuntimeError(f"No compiled SQL for Materialize model {node['name']}")
            relation = sql.Identifier(node["schema"], node["alias"]).as_string(connection)
            cursor.execute(load_sql(
                "materialize-emulator", "inline", "dbt_server/services_materialize_validation/describe_expected",
                query=query,
            ))
            expected_columns = [(column.name, column.type_code) for column in cursor.description]
            cursor.execute(load_sql(
                "materialize-emulator", "inline", "dbt_server/services_materialize_validation/describe_output",
                relation=relation,
            ))
            actual_columns = [(column.name, column.type_code) for column in cursor.description]
            if actual_columns != expected_columns:
                raise RuntimeError(f"Materialize output schema differs from compiled model {node['name']}")
            cursor.execute(load_sql(
                "materialize-emulator", "inline", "dbt_server/services_materialize_validation/multiset_difference",
                query=query, relation=relation,
            ))
            if cursor.fetchone() is not None:
                raise RuntimeError(f"Materialize output differs from recomputed model {node['name']}")
    return {
        "status": "passed",
        "models_compared": len(models),
        "comparison": "bidirectional EXCEPT ALL",
        "scope": "same-engine compiled-definition consistency; not independent reference parity",
    }
