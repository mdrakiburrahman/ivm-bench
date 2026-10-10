import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, mock_open, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import spark_openivm_validation as validation

try:
    import flask
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


def output(rows):
    return {"output": {"data": {"application/json": {"data": rows}}}}


class ExactValidationTest(unittest.TestCase):
    def validate(self, diff=0, error=None, exact=True):
        sql = []
        def execute(statement):
            sql.append(statement)
            if statement.startswith("DESCRIBE"):
                return output([["amount", "double"], ["id", "bigint"]])
            if statement.startswith("SELECT COUNT(*) FROM"):
                if error:
                    raise RuntimeError(error)
                return output([[diff]])
            if "xxhash64" in statement:
                return output([[2, 17]])
            return output([])
        with patch.object(validation.logger, "exception"):
            result = validation._validate_one(
                Mock(execute=execute), unique_id="model.test.example", name="example",
                schema="silver", compiled_sql="SELECT amount, id FROM source",
                exact=exact,
            )
        return result, sql

    def test_exact_comparison_preserves_bags_and_does_not_round(self):
        result, sql = self.validate()
        comparison = next(s for s in sql if s.startswith("SELECT COUNT(*) FROM"))
        self.assertEqual(comparison.count("EXCEPT ALL"), 2)
        self.assertIn("UNION ALL", comparison)
        self.assertIn("SELECT `amount`, `id` FROM `silver`.`example`", comparison)
        self.assertNotIn("ROUND", comparison)
        self.assertNotIn("xxhash64", comparison)
        self.assertEqual(result["validation_method"], "except_all")
        self.assertEqual(result["diff_count"], 0)
        self.assertEqual(result["status"], "pass")
        self.assertTrue(sql[-1].startswith("DROP VIEW"))

    def test_exact_mismatch_is_failure_and_keeps_samples(self):
        result, _ = self.validate(diff=2)
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["diff_count"], 2)
        self.assertIn("mv_sample", result["sample"])
        self.assertNotIn("sample_error", result["sample"])

    def test_query_error_fails_and_cleans_up(self):
        result, sql = self.validate(error="comparison failed")
        self.assertEqual(result["status"], "fail")
        self.assertEqual(result["diff_count"], -1)
        self.assertIn("comparison failed", result["error"])
        self.assertTrue(sql[-1].startswith("DROP VIEW"))

    def test_existing_digest_mode_is_labelled_and_retained(self):
        result, sql = self.validate(exact=False)
        self.assertEqual(result["validation_method"], "count_hash_rounded")
        self.assertEqual(result["status"], "pass")
        self.assertEqual(sum("xxhash64" in s for s in sql), 2)
        self.assertTrue(any("ROUND" in s for s in sql))


@unittest.skipUnless(HAS_FLASK, "requires dbt-server Flask dependency")
class FabricValidationRoutingTest(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        from handlers.spark_openivm import bp
        app = Flask(__name__)
        app.register_blueprint(bp)
        self.client = app.test_client()

    def test_fabric_route_uses_existing_session_and_exact_mode(self):
        with patch("handlers.spark_openivm.spark_openivm_validation.validate_run") as validate, \
             patch("handlers.spark_openivm.fabric.ProfileClient") as client:
            validate.return_value = {"status": "passed"}
            response = self.client.post("/validate/fabric-openivm-jvm-35/run", json={"exact": True})
            self.assertEqual(response.status_code, 200)
            self.assertTrue(validate.call_args.kwargs["exact"])
            validate.call_args.kwargs["client_factory"]()
            client.assert_called_once_with(require_tabular=False)

    def test_spark_route_retains_local_livy_client(self):
        with patch("handlers.spark_openivm.spark_openivm_validation.validate_run") as validate:
            validate.return_value = {"status": "passed"}
            response = self.client.post("/validate/spark-openivm/run", json={"exact": False})
            self.assertEqual(response.status_code, 200)
            self.assertIsNone(validate.call_args.kwargs["client_factory"])

    def test_invalid_boolean_does_not_start_validation(self):
        with patch("handlers.spark_openivm.spark_openivm_validation.validate_run") as validate:
            response = self.client.post("/validate/fabric-openivm-jvm-35/run", json={"exact": "false"})
            self.assertEqual(response.status_code, 400)
            validate.assert_not_called()



class FabricValidationSessionTest(unittest.TestCase):
    def test_fabric_run_uses_its_manifest_and_worker_owned_cursors(self):
        from contextlib import contextmanager
        conn = Mock()
        conn.execute.return_value.fetchone.return_value = {"engine": "fabric-openivm-jvm-35", "status": "completed"}
        conn.execute.return_value.fetchall.return_value = [
            {"unique_id": f"model.test.v{i}", "name": f"v{i}", "resource_type": "model", "status": "success", "compiled_sql": "SELECT 1"}
            for i in range(2)
        ]
        clients = []
        @contextmanager
        def factory():
            client = Mock()
            clients.append(client)
            yield client
        def validate(client, **kwargs):
            self.assertTrue(kwargs["exact"])
            self.assertEqual(kwargs["schema"], "silver")
            return {"status": "pass", "diff_count": 0, "validation_method": "except_all"}
        with patch.object(validation, "get_db", return_value=conn), \
             patch("services.dbt_compiler.get_compiled_models") as manifest, \
             patch("builtins.open", mock_open(read_data=json.dumps({"nodes": {f"model.test.v{i}": {"schema": "silver"} for i in range(2)}}))), \
             patch.object(validation, "_validate_one", side_effect=validate), \
             patch.object(validation, "LivyClient") as local:
            result = validation.validate_run("run", exact=True, client_factory=factory)
        manifest.assert_not_called()
        local.assert_not_called()
        self.assertEqual(len(clients), 2)
        self.assertIsNot(clients[0], clients[1])
        self.assertEqual(result["models_checked"], 2)
        self.assertEqual(result["validation_method"], "except_all")

    def test_empty_model_set_cannot_pass_validation(self):
        from contextlib import nullcontext
        conn = Mock()
        conn.execute.return_value.fetchone.return_value = {"engine": "fabric-openivm-jvm-35", "status": "completed"}
        conn.execute.return_value.fetchall.return_value = []
        with patch.object(validation, "get_db", return_value=conn), \
             patch("builtins.open", mock_open(read_data='{"nodes":{}}')):
            with self.assertRaisesRegex(ValueError, "no successful models"):
                validation.validate_run("run", client_factory=lambda: nullcontext())

    def test_incomplete_fabric_models_cannot_pass(self):
        from contextlib import nullcontext
        for update, schema, message in (({"status": "error"}, "silver", "not successful"),
                                         ({"compiled_sql": ""}, "silver", "lacks compiled SQL"),
                                         ({}, None, "lacks manifest schema")):
            with self.subTest(update=update, schema=schema):
                node = {"unique_id": "model.test.v", "name": "v", "resource_type": "model",
                        "status": "success", "compiled_sql": "SELECT 1"}
                node.update(update)
                conn = Mock()
                conn.execute.return_value.fetchone.return_value = {"engine": "fabric-openivm-jvm-35", "status": "completed"}
                conn.execute.return_value.fetchall.return_value = [node]
                with patch.object(validation, "get_db", return_value=conn), \
                     patch("builtins.open", mock_open(read_data=json.dumps({"nodes": {"model.test.v": {"schema": schema}}}))), \
                     patch.object(validation, "_validate_one") as compare:
                    with self.assertRaisesRegex(ValueError, message):
                        validation.validate_run("run", client_factory=lambda: nullcontext())
                    compare.assert_not_called()

    def test_wrong_route_and_failed_run_are_rejected_before_queries(self):
        for run, message in (({"engine": "spark-openivm", "status": "completed"}, "does not match"),
                             ({"engine": "fabric-openivm-jvm-35", "status": "failed"}, "requires a completed run")):
            conn = Mock()
            conn.execute.return_value.fetchone.return_value = run
            with self.subTest(run=run), patch.object(validation, "get_db", return_value=conn), \
                    patch.object(validation, "_validate_one") as compare:
                with self.assertRaisesRegex(ValueError, message):
                    validation.validate_run("run", engine="fabric-openivm-jvm-35", client_factory=Mock())
                conn.close.assert_called_once()
                compare.assert_not_called()


if __name__ == "__main__":
    unittest.main()
