import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from jinja2 import Environment


class DatabricksMaterializationTest(unittest.TestCase):
    def render(self, full_refresh, partitions):
        path = Path(__file__).resolve().parents[1] / "dbt-projects/databricks-enzyme/macros/materializations/materialized_view.sql"
        template = path.read_text().replace(
            "{% materialization materialized_view, adapter='databricks' %}",
            "{% macro materialized_view() %}",
        ).replace("{% endmaterialization %}", "{% endmacro %}")
        statements = {}

        def statement(name, caller):
            statements[name] = " ".join(caller().split())
            return ""

        partition_clause = Mock(return_value="PARTITIONED BY (sk_date_id)" if partitions else "")
        values = {"refresh_policy": "AUTO", "partition_by": partitions}
        module = Environment(extensions=["jinja2.ext.do"]).from_string(template).make_module({
            "config": SimpleNamespace(get=values.get),
            "flags": SimpleNamespace(FULL_REFRESH=full_refresh),
            "this": SimpleNamespace(incorporate=lambda **_kwargs: "test.market_history"),
            "get_create_sql_partition_by": partition_clause,
            "statement": statement,
            "run_hooks": lambda *_args, **_kwargs: "",
            "persist_docs": lambda *_args: "",
            "return": lambda _value: "",
            "pre_hooks": [], "post_hooks": [], "model": {},
            "sql": "SELECT * FROM source_market_history",
        })
        module.materialized_view()
        return statements, partition_clause

    def test_initial_create_emits_partitioning_and_keeps_query_and_policy(self):
        statements, helper = self.render(True, ["sk_date_id"])
        helper.assert_called_once_with(["sk_date_id"])
        self.assertEqual(statements["main"], "CREATE MATERIALIZED VIEW test.market_history PARTITIONED BY (sk_date_id) REFRESH POLICY AUTO AS SELECT * FROM source_market_history")
        self.assertEqual(statements["drop"], "DROP MATERIALIZED VIEW IF EXISTS test.market_history")

    def test_unpartitioned_create_keeps_legacy_ddl(self):
        statements, helper = self.render(True, [])
        helper.assert_called_once_with([])
        self.assertEqual(statements["main"], "CREATE MATERIALIZED VIEW test.market_history REFRESH POLICY AUTO AS SELECT * FROM source_market_history")

    def test_refresh_reuses_partitioned_materialization(self):
        statements, helper = self.render(False, ["sk_date_id"])
        helper.assert_not_called()
        self.assertEqual(statements, {"main": "REFRESH MATERIALIZED VIEW test.market_history"})
