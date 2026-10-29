import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))


class ExplainObserverTest(unittest.TestCase):
    def test_post_build_mode_never_creates_or_drops_real_model_stubs(self):
        import services

        sources = Mock(CATALOG="cat")
        sources.data_schema.return_value = "data"
        spec = importlib.util.spec_from_file_location("explain_observer_test", SERVER / "services/databricks_enzyme_explain.py")
        backend = importlib.util.module_from_spec(spec)
        with patch.object(services, "databricks_enzyme_sources", sources, create=True):
            spec.loader.exec_module(backend)
        manifest = {"nodes": {
            name: {"name": name, "schema": "gold", "resource_type": "model", "config": {},
                   "compiled_code": "SELECT 1 AS id", "depends_on": {"nodes": []}}
            for name in ("a", "b")}}
        for existing in (False, True):
            with self.subTest(existing_models=existing), \
                    patch.object(backend, "get_manifest", return_value=manifest), \
                    patch.object(backend, "_explain_one", return_value=("plan", None)):
                sources.reset_mock()
                result = backend.explain_all_models(10, existing_models=existing)
                self.assertEqual(result["passed"], 2)
                sql = [c.args[0] for c in sources._execute.call_args_list]
                creates = [s for s in sql if s.startswith("CREATE OR REPLACE VIEW")]
                drops = [s for s in sql if s.startswith("DROP VIEW")]
                self.assertEqual(len(creates), 0 if existing else 2)
                self.assertEqual(len(drops), 0 if existing else 2)
                self.assertIn("DROP SCHEMA IF EXISTS `cat`.`data_validate` CASCADE", sql)

    def test_endpoint_passes_existing_model_mode_and_rejects_non_boolean(self):
        import services
        from flask import Flask

        spec = importlib.util.spec_from_file_location("handler_observer_test", SERVER / "handlers/databricks_enzyme.py")
        handler = importlib.util.module_from_spec(spec)
        with patch.object(services, "databricks_enzyme_sources", Mock(), create=True):
            spec.loader.exec_module(handler)
        app = Flask(__name__)
        app.register_blueprint(handler.bp)
        backend = Mock()
        backend.explain_all_models.return_value = {"status": "ok"}
        with patch.object(services, "databricks_enzyme_explain", backend, create=True):
            response = app.test_client().post(
                "/validate/databricks-enzyme/explain-create-materialized-view/10",
                json={"existing_models": True})
            self.assertEqual(response.status_code, 200)
            backend.explain_all_models.assert_called_once_with(10, existing_models=True)
            response = app.test_client().post(
                "/validate/databricks-enzyme/explain-create-materialized-view/10",
                json={"existing_models": "false"})
            self.assertEqual(response.status_code, 400)
            self.assertEqual(backend.explain_all_models.call_count, 1)


if __name__ == "__main__":
    unittest.main()
