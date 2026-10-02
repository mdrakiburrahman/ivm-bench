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

from services.dbt_runner import _inject_fabric_resolved, warm_fabric_session
from services.fabric_dbt_profile import instrument


class FabricFreshBuildTest(unittest.TestCase):
    def setUp(self):
        root = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(root) / "resolved.json"
        self.enterContext(patch.dict(os.environ, {"FABRIC_RESOLVED_PATH": str(self.path), "BATCH_1_INSERT_PCT": "100", "SCALE_FACTOR": "100"}))
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

    @patch("services.dbt_runner.subprocess.run")
    def test_warmup_uses_provisioned_profile_without_consuming_fresh_claim(self, run):
        run.return_value.returncode = 0
        with patch.dict(os.environ, {"OPENIVM_PROFILE_REFRESH": "0", "FABRIC_OPENIVM_FRESH_BUILD": "1"}):
            self.assertEqual(warm_fabric_session("fabric-openivm-jvm-35")["status"], "ok")
        args = run.call_args
        self.assertEqual(args.args[0][:3], ["dbt", "run-operation", "warm_fabric_session"])
        self.assertEqual(args.kwargs["env"]["FABRIC_LAKEHOUSE_ID"], "fresh-id")
        self.assertNotIn("FABRIC_OPENIVM_FRESH_BUILD", args.kwargs["env"])
        env = {}
        _inject_fabric_resolved(env, claim_fresh=True)
        self.assertEqual(env["FABRIC_OPENIVM_FRESH_BUILD"], "1")

    @patch("services.dbt_runner.subprocess.run")
    def test_failed_warmup_is_fatal_and_does_not_return_diagnostics(self, run):
        run.return_value.returncode = 2
        run.return_value.stderr = "secret"
        with patch.dict(os.environ, {"OPENIVM_PROFILE_REFRESH": "0"}):
            with self.assertRaisesRegex(RuntimeError, "dbt exit 2") as error:
                warm_fabric_session("fabric-openivm-jvm-35")
        self.assertNotIn("secret", str(error.exception))

    @patch("services.dbt_runner.subprocess.run")
    def test_baseline_warmup_uses_baseline_profile_and_claim(self, run):
        run.return_value.returncode = 0
        self.resolved["openivm"] = False
        self.path.write_text(json.dumps(self.resolved))
        with patch.dict(os.environ, {"OPENIVM_PROFILE_REFRESH": "0"}):
            warm_fabric_session("fabric-jvm-35")
        self.assertEqual(run.call_args.args[0][-1], "fabric-jvm-35")
        env = {}
        _inject_fabric_resolved(env, claim_fresh=True)
        self.assertEqual(env["FABRIC_FRESH_BUILD"], "1")
        self.assertNotIn("FABRIC_OPENIVM_FRESH_BUILD", env)

    def test_spark_openivm_initial_sources_clone_to_independent_locations(self):
        from services import spark_openivm_sources as sources
        with patch.object(sources, "LivyClient") as client:
            sources.init_sources()
        statements = client.return_value.__enter__.return_value.execute_many.call_args.args[0]
        self.assertEqual(len(statements), 20)
        for sql in statements[1:]:
            self.assertIn("SHALLOW CLONE delta.`", sql)
            self.assertIn("LOCATION '", sql)
            self.assertIn("delta.enableChangeDataFeed", sql)
            self.assertNotIn("AS SELECT", sql)

    def test_fabric_baseline_and_openivm_have_identical_source_and_warmup_hooks(self):
        projects = Path(__file__).resolve().parents[1] / "dbt-projects"
        for macro in ("load_fabric_sources.sql", "warm_fabric_session.sql"):
            self.assertEqual((projects / "fabric-jvm-35/macros" / macro).read_text(),
                             (projects / "fabric-openivm-jvm-35/macros" / macro).read_text())

    def render_sources(self, batch, fresh):
        path = Path(__file__).resolve().parents[1] / "dbt-projects/fabric-openivm-jvm-35/macros/load_fabric_sources.sql"
        template = path.read_text() + "{{ load_fabric_sources() }}"
        env = {"FABRIC_BATCH_NUM": str(batch), "SCALE_FACTOR": "100",
               "FABRIC_WORKSPACE_ID": "workspace", "FABRIC_CACHE_LAKEHOUSE_ID": "cache",
               "FABRIC_CACHE_ROOT": "Files/cache", "FABRIC_INCREMENTAL_STAGING_TABLES": "trade,account",
               "FABRIC_FRESH_BUILD": fresh}
        statements = []
        def run_query(sql):
            sql = sql.strip()
            statements.append(sql)
            if sql.startswith("SHOW TABLES"):
                return SimpleNamespace(rows=[["lakehouse", "old_table", False]])
        Environment(extensions=["jinja2.ext.do"]).from_string(template).render(
            target=SimpleNamespace(type="fabricspark", schema="lakehouse"), execute=True,
            env_var=lambda name, default="": env.get(name, default), run_query=run_query,
            log=lambda *args, **kwargs: "", **{"return": lambda value: value},
        )
        return statements

    def test_fresh_sources_clone_all_19_tables_with_cdf(self):
        statements = self.render_sources(1, "1")
        self.assertEqual(len(statements), 19)
        for sql in statements:
            self.assertIn("SHALLOW CLONE delta.`abfss://workspace@", sql)
            self.assertIn("TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')", sql)
            self.assertNotIn("AS SELECT", sql)
        self.assertTrue(any("lakehouse.audit " in sql for sql in statements))

    def test_source_retry_still_cleans_catalog_before_cloning(self):
        statements = self.render_sources(1, "0")
        self.assertEqual(statements[:2], ["SHOW TABLES IN lakehouse", "DROP TABLE IF EXISTS lakehouse.`old_table`"])
        self.assertEqual(len(statements), 21)

    def test_incremental_sources_keep_insert_by_name_without_clone_properties(self):
        for batch in (2, 3):
            statements = self.render_sources(batch, "0")
            self.assertEqual(len(statements), 2)
            for sql in statements:
                self.assertTrue(sql.startswith("INSERT INTO lakehouse.staging_"))
                self.assertIn(f"staging_batch{batch}", sql)
                self.assertNotIn("TBLPROPERTIES", sql)
                self.assertNotIn("CLONE", sql)


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
