import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import sqlglot

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import databricks_openivm as backend


class DatabricksOpenivmTest(unittest.TestCase):
    def model(self, name, deps=(), materialized="materialized_view"):
        return {"name": name, "alias": name, "database": "cat", "schema": "exp_gold",
                "resource_type": "model", "config": {"materialized": materialized},
                "depends_on": {"nodes": list(deps)}, "compiled_code": "SELECT id FROM cat.exp_data.events"}

    def manifest(self):
        return {"nodes": {"model.a": self.model("a")}, "sources": {
            "source.events": {"database": "cat", "schema": "exp_data", "name": "events"}}}

    def test_dependencies_through_ephemeral_models_are_ordered(self):
        manifest = {"nodes": {
            "model.a": self.model("a", ["model.e"]),
            "model.e": self.model("e", ["model.z"], "ephemeral"),
            "model.z": self.model("z"),
        }}
        self.assertEqual([uid for uid, _ in backend.ordered_models(manifest)], ["model.z", "model.a"])

    def test_relation_remapping_preserves_literals_columns_and_versions(self):
        sql = "SELECT 'compiler.x' AS text, x.id FROM compiler.x VERSION AS OF 4 x"
        result = backend.remap_sql(sql, {"compiler.x": "cat.exp.public"},
                                   snapshots={"compiler.x": "cat.exp.data"})
        self.assertIn("'compiler.x'", result)
        self.assertIn("`cat`.`exp`.`data` VERSION AS OF 4", result)
        self.assertIn("x.id", result)

    def test_virtual_delta_paths_become_managed_tables(self):
        sql = "CREATE TABLE delta.`openivm-view-delta` USING DELTA AS SELECT 1 AS id"
        result = backend.remap_sql(sql, {}, {"delta.openivm-view-delta": "cat.exp.scratch"})
        self.assertIn("`cat`.`exp`.`scratch`", result)
        self.assertNotIn("openivm-view-delta", result)

    def test_old_snapshot_reconstructs_public_columns_and_having_filter(self):
        result = backend.remap_sql("SELECT x.id FROM compiler.x VERSION AS OF 4 x", {},
                                   snapshots={"compiler.x": {"table": "cat.exp.data", "columns": ["id", "cnt"],
                                                             "predicate": "cnt >= 2"}})
        self.assertIn("SELECT `id`, `cnt` FROM `cat`.`exp`.`data` VERSION AS OF 4 WHERE cnt >= 2", result)
        self.assertIn("AS x", result)

    def test_cdf_reads_exact_new_versions_with_both_retraction_signs(self):
        sql = backend._cdf_sql("events", "cat.exp.events", [{"name": "id"}], 4, 7)
        self.assertIn("table_changes('cat.exp.events', 5, 7)", sql)
        self.assertIn("'update_preimage'", sql)
        self.assertIn("'update_postimage'", sql)
        self.assertIn("THEN 1 ELSE -1", sql)
        sqlglot.parse_one(sql, read="databricks")

    def test_unchanged_source_registers_typed_empty_delta_without_cdf(self):
        sql = backend._cdf_sql("events", "cat.exp.events", [{"name": "id"}], 4, 4)
        self.assertIn("WHERE FALSE", sql)
        self.assertNotIn("table_changes", sql)

    def test_having_cdf_filters_each_preimage_and_postimage(self):
        sql = backend._cdf_sql("a", "cat.exp.a", [{"name": "cnt"}], 4, 5, "cnt >= 2")
        self.assertIn("AND (cnt >= 2)", sql)
        self.assertIn("'update_preimage'", sql)

    @patch.object(backend, "_record_node")
    @patch.object(backend, "_save")
    @patch.object(backend, "compiler")
    def test_initialize_once_then_refresh_pinned_cdf_and_commit_watermark(self, compile_sql, _save, _record):
        executor = Mock()
        executor.schema.return_value = [{"name": "id", "ddl_type": "bigint"}]
        executor.version.return_value = 4
        compile_sql.return_value = {
            "refresh_type": 2, "refresh_type_name": "SIMPLE_PROJECTION",
            "initial_sql": "SELECT id FROM openivm_compile.events",
            "compiled_sql": "compiled-maintenance", "output_schema": {"fields": [{"name": "id"}]}}
        state = {"run_id": "initial"}
        backend.build_models(executor, self.manifest(), state, full_refresh=True, batch_num=1)
        self.assertEqual(state["models"]["model.a"]["source_versions"], {"cat.exp_data.events": 4})
        initial_calls = [c.args[0] for c in executor.execute.call_args_list]
        self.assertTrue(any("CREATE TABLE" in sql for sql in initial_calls))
        self.assertTrue(any("CREATE VIEW" in sql for sql in initial_calls))
        self.assertFalse(any("MATERIALIZED VIEW" in sql for sql in initial_calls))

        executor.reset_mock()
        executor.version.return_value = 5
        compile_sql.return_value = {"statements": [
            "INSERT INTO openivm_compile.a SELECT id FROM openivm_delta_events"]}
        backend.build_models(executor, self.manifest(), state, full_refresh=False, batch_num=2)
        calls = [c.args[0] for c in executor.execute.call_args_list]
        self.assertTrue(any("table_changes('cat.exp_data.events', 5, 5)" in sql for sql in calls))
        self.assertTrue(any("INSERT INTO `cat`.`exp_gold`.`_openivm_data_a`" in sql for sql in calls))
        self.assertFalse(any("CREATE TABLE" in sql or "CREATE VIEW" in sql for sql in calls))
        self.assertEqual(state["models"]["model.a"]["source_versions"], {"cat.exp_data.events": 5})
        self.assertEqual(compile_sql.call_args.args[0]["source_versions"], {"events": 4})

    @patch.object(backend, "_record_node")
    @patch.object(backend, "_save")
    @patch.object(backend, "compiler")
    def test_failed_maintenance_does_not_advance_checkpoint(self, compile_sql, _save, _record):
        executor = Mock()
        executor.version.return_value = 5
        compile_sql.return_value = {"statements": ["INSERT INTO openivm_compile.a SELECT 1"]}
        executor.execute.side_effect = [[], [], RuntimeError("failure")]
        state = {"run_id": "failed", "models": {"model.a": {
            "query": self.manifest()["nodes"]["model.a"]["compiled_code"],
            "refresh_type": 2, "compiled_sql": "compiled", "schemas": {"events": [{"name": "id"}]},
            "source_versions": {"cat.exp_data.events": 4}}}}
        with self.assertRaisesRegex(RuntimeError, "failure"):
            backend.build_models(executor, self.manifest(), state, full_refresh=False, batch_num=2)
        self.assertEqual(state["models"]["model.a"]["source_versions"], {"cat.exp_data.events": 4})

    def test_dirty_checkpoint_is_rejected_before_any_sql_or_compilation(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(backend, "WORK_DIR", Path(tmp)), \
                patch.object(backend, "get_db") as db, patch.object(backend, "DB_LOCK"), \
                patch.object(backend, "_fail_run") as fail, patch.object(backend, "SqlExecutor") as executor:
            Path(tmp, "checkpoint.json").write_text(json.dumps({"dirty": True, "batch_num": 1}))
            backend.run("retry", 1, False, 2)
            executor.assert_not_called()
            self.assertIn("dirty", fail.call_args.args[1])

    def test_photon_metrics_use_statement_ids_and_do_not_infer_missing_task_time(self):
        fake_sources = types.ModuleType("services.databricks_enzyme_sources")
        fake_sources._workspace_client = Mock()
        history = fake_sources._workspace_client.return_value.query_history
        query = Mock()
        history.list.return_value.res = [query]
        import services
        with tempfile.TemporaryDirectory() as tmp, patch.object(backend, "WORK_DIR", Path(tmp)), \
                patch.dict(sys.modules, {"services.databricks_enzyme_sources": fake_sources}), \
                patch.object(services, "databricks_enzyme_sources", fake_sources, create=True):
            Path(tmp, "sql-batch2.json").write_text(json.dumps({"statements": [{"query_id": "q1"}]}))
            query.as_dict.return_value = {"query_id": "q1", "metrics": {"photon_total_time_ms": 250, "task_total_time_ms": 500}}
            result = backend.photon_metrics(2)
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["photon_total_time_ms"], 250)
            self.assertEqual(history.list.call_args.kwargs["filter_by"].statement_ids, ["q1"])
            query.as_dict.return_value = {"query_id": "q1"}
            self.assertEqual(backend.photon_metrics(2)["status"], "partial")


if __name__ == "__main__":
    unittest.main()
