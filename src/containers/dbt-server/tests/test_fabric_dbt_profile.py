import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

from jinja2 import Environment

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.dbt_runner import _inject_fabric_resolved
from services.fabric_dbt_profile import instrument


class FabricFreshBuildTest(unittest.TestCase):
    def setUp(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(root) / "resolved.json"
        self.enterContext(patch.dict(os.environ, {"FABRIC_RESOLVED_PATH": str(self.path)}))
        self.resolved = {"lakehouse_id": "fresh-id", "lakehouse_name": "fresh-name",
                         "fresh_compute": True, "openivm": True}
        self.path.write_text(json.dumps(self.resolved))

    def test_only_first_attempt_can_skip_drop(self):
        env = {}
        _inject_fabric_resolved(env, claim_fresh=True)
        self.assertEqual(env["FABRIC_OPENIVM_FRESH_BUILD"], "1")
        self.assertEqual(env["FABRIC_LAKEHOUSE_ID"], "fresh-id")
        # The flag must be consumed even if that invocation failed midway.
        _inject_fabric_resolved(env, claim_fresh=True)
        self.assertNotIn("FABRIC_OPENIVM_FRESH_BUILD", env)

    def test_ordinary_build_does_not_consume_claim(self):
        env = {"FABRIC_OPENIVM_FRESH_BUILD": "1"}
        _inject_fabric_resolved(env)
        self.assertNotIn("FABRIC_OPENIVM_FRESH_BUILD", env)
        _inject_fabric_resolved(env, claim_fresh=True)
        self.assertEqual(env["FABRIC_OPENIVM_FRESH_BUILD"], "1")

    def test_old_or_baseline_resolved_state_cannot_skip_drop(self):
        for key in ("fresh_compute", "openivm", "lakehouse_id"):
            state = dict(self.resolved)
            state.pop(key)
            self.path.write_text(json.dumps(state))
            env = {}
            _inject_fabric_resolved(env, claim_fresh=True)
            self.assertNotIn("FABRIC_OPENIVM_FRESH_BUILD", env)

    def test_materialization_keeps_drop_on_retry_and_ordinary_full_refresh(self):
        path = Path(__file__).resolve().parents[1] / "dbt-projects/fabric-openivm-jvm-35/macros/materializations/materialized_view.sql"
        template = path.read_text().replace(
            "{% materialization materialized_view, adapter='fabricspark' %}", "{% macro build() %}"
        ).replace("{% endmaterialization %}", "{% endmacro %}") + "{{ build() }}"
        for fresh, full_refresh, expected in (
            ("1", True, ["CREATE"]), ("0", True, ["DROP", "CREATE"]),
            ("0", False, ["REFRESH"]),
        ):
            statements = []

            def statement(name, caller):
                statements.append(caller().strip().split()[0])
                return ""

            Environment(extensions=["jinja2.ext.do"]).from_string(template).render(
                this=SimpleNamespace(incorporate=lambda **kw: "schema.mv"),
                run_hooks=lambda *a, **kw: "", pre_hooks=[], post_hooks=[],
                flags=SimpleNamespace(FULL_REFRESH=full_refresh), sql="SELECT 1",
                env_var=lambda name, default: fresh, statement=statement,
                persist_docs=lambda *a: "", model={}, **{"return": lambda value: ""},
            )
            self.assertEqual(statements, expected)


class FabricClientTimingTest(unittest.TestCase):
    def test_spark_error_frames_are_preserved_without_query_or_message(self):
        class Cursor:
            def _getLivyResult(self):
                return {"output": {"status": "error", "evalue": "secret query", "traceback": [
                    "secret query\n at org.apache.spark.sql.SparkSession.active(SparkSession.scala:123)"
                ]}}

        rows = []
        instrument(Cursor, "_getLivyResult", rows.append)
        self.assertEqual(Cursor()._getLivyResult()["output"]["status"], "error")
        self.assertEqual(rows[0]["outcome"], "sql_error")
        self.assertEqual(rows[0]["spark_stack"], ["org.apache.spark.sql.SparkSession.active(SparkSession.scala:123)"])
        self.assertNotIn("secret", json.dumps(rows))

    def test_version_probe_records_actual_version_without_changing_rows(self):
        class Cursor:
            def execute(self, sql):
                self._rows = [["3.5.5"]]

        rows = []
        instrument(Cursor, "execute", rows.append, sql_argument=True)
        cursor = Cursor()
        cursor.execute("SELECT split(version(), ' ')[0] as version")
        self.assertEqual(cursor._rows, [["3.5.5"]])
        self.assertEqual(rows[0]["spark_version"], "3.5.5")

    def test_nested_timings_preserve_return_and_exclude_sql(self):
        class Cursor:
            def submit(self):
                return "result"

            def execute(self, sql):
                return self.submit()

        rows = []
        instrument(Cursor, "submit", rows.append)
        instrument(Cursor, "execute", rows.append, sql_argument=True)
        self.assertEqual(Cursor().execute("SELECT 'secret'"), "result")
        self.assertEqual([r["operation"] for r in rows], ["submit", "execute"])
        self.assertEqual(rows[1]["statement_kind"], "SELECT")
        self.assertNotIn("secret", json.dumps(rows))
        self.assertLessEqual(rows[1]["start_epoch_ns"], rows[0]["start_epoch_ns"])
        self.assertGreaterEqual(rows[1]["end_epoch_ns"], rows[0]["end_epoch_ns"])

    def test_original_exception_is_preserved(self):
        class Cursor:
            def execute(self, sql):
                raise ValueError("original")

        rows = []
        instrument(Cursor, "execute", rows.append, sql_argument=True)
        with self.assertRaisesRegex(ValueError, "original"):
            Cursor().execute("CREATE TABLE x")
        self.assertEqual(rows[0]["outcome"], "error")

    def test_timing_write_failure_does_not_fail_sql(self):
        class Cursor:
            def execute(self, sql):
                return 42

        def broken(row):
            raise OSError("disk full")

        instrument(Cursor, "execute", broken)
        with patch("sys.stderr"):
            self.assertEqual(Cursor().execute("SELECT 42"), 42)
