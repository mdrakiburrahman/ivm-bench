"""A completed state alone does not establish that Livy's output has arrived."""
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import spark_openivm_sources as sources
from services import spark_openivm_validation as validation


class LivyStatementResultsTest(unittest.TestCase):
    def setUp(self):
        self.client = sources.LivyClient()
        self.client.session_id = "1"
        self.post = self.enterContext(patch.object(sources.requests, "post"))
        self.post.return_value.json.return_value = {"id": 42}
        self.get = self.enterContext(patch.object(sources.requests, "get"))
        self.sleep = self.enterContext(patch.object(sources.time, "sleep"))

    def responses(self, *payloads):
        self.get.side_effect = [Mock(json=Mock(return_value=p)) for p in payloads]

    @staticmethod
    def completed(output):
        return {"id": 42, "state": "available", "output": output}

    def test_waits_for_output_without_resubmitting_statement(self):
        for state in ("available", "completed"):
            for incomplete in (None, {}):
                with self.subTest(state=state, output=incomplete):
                    self.post.reset_mock()
                    self.get.reset_mock()
                    result = self.completed({"status": "ok", "data": {}})
                    self.responses({"state": state, "output": incomplete}, result)
                    self.assertEqual(self.client.execute("CREATE TEMP VIEW v AS SELECT 1"), result)
                    self.post.assert_called_once()
                    self.assertEqual(self.get.call_count, 2)
                    self.assertTrue(all(c.args[0].endswith("/statements/42")
                                        for c in self.get.call_args_list))

    def test_empty_ddl_output_is_a_completed_success(self):
        result = self.completed({"status": "ok", "data": {"application/json": {"data": []}}})
        self.responses(result)
        self.assertEqual(self.client.execute("DROP VIEW IF EXISTS v"), result)
        self.get.assert_called_once()
        self.sleep.assert_not_called()

    def test_late_error_is_not_accepted_or_reexecuted(self):
        self.responses(self.completed(None), self.completed({
            "status": "error", "ename": "AnalysisException", "evalue": "missing relation",
        }))
        with self.assertRaisesRegex(RuntimeError, "AnalysisException: missing relation"):
            self.client.execute("DESCRIBE missing")
        self.post.assert_called_once()
        self.assertEqual(self.get.call_count, 2)

    def test_missing_output_still_obeys_timeout_and_cancels(self):
        self.responses(self.completed(None))
        with patch.object(sources.time, "monotonic", side_effect=[0, 2]), \
                patch.object(sources.requests, "delete") as cancel:
            with self.assertRaisesRegex(TimeoutError, "statement 42 did not finish"):
                self.client.execute("DESCRIBE v", timeout_s=1)
        self.post.assert_called_once()
        cancel.assert_called_once()

    def test_incomplete_result_is_logged_once_and_keeps_polling(self):
        result = self.completed({"status": "ok", "data": {}})
        self.responses(self.completed(None), self.completed({}), result)
        with self.assertLogs(sources.logger, level="WARNING") as logs:
            self.assertEqual(self.client.execute("DESCRIBE v"), result)
        self.assertEqual(len(logs.output), 1)
        self.assertIn("42 is available without final output", logs.output[0])
        self.post.assert_called_once()
        self.assertEqual(self.get.call_count, 3)

    def test_cancellation_is_not_polled_or_resubmitted(self):
        self.responses({"state": "cancelled", "output": None})
        with self.assertRaisesRegex(RuntimeError, "statement cancelled"):
            self.client.execute("DESCRIBE v")
        self.post.assert_called_once()
        self.get.assert_called_once()

    def test_validation_waits_for_describe_result_then_checks_values(self):
        ddl = self.completed({"status": "ok", "data": {"application/json": {"data": []}}})
        columns = self.completed({"status": "ok", "data": {
            "application/json": {"data": [["sk_date_id", "bigint"]]},
        }})
        digest = self.completed({"status": "ok", "data": {"application/json": {"data": [[3, 17]]}}})
        self.responses(ddl, columns, self.completed(None), columns, digest, digest, ddl)
        result = validation._validate_one(
            self.client, unique_id="model.tpcdi.date", name="date", schema="silver",
            compiled_sql="SELECT sk_date_id FROM bronze.reference_date",
        )
        self.assertEqual(result["status"], "pass")
        self.assertEqual(result["diff_count"], 0)
        self.assertEqual(self.post.call_count, 6)
        self.assertEqual(self.get.call_count, 7)
        self.assertEqual(sum("xxhash64" in c.kwargs["json"]["code"]
                             for c in self.post.call_args_list), 2)

    def test_successful_but_empty_describe_still_fails_validation(self):
        empty = self.completed({"status": "ok", "data": {"application/json": {"data": []}}})
        columns = self.completed({"status": "ok", "data": {
            "application/json": {"data": [["sk_date_id", "bigint"]]},
        }})
        self.responses(empty, columns, empty, empty)
        with patch.object(validation.logger, "exception"):
            report = validation._validate_one(
                self.client, unique_id="model.tpcdi.date", name="date", schema="silver",
                compiled_sql="SELECT sk_date_id FROM bronze.reference_date",
            )
        self.assertEqual(report["status"], "fail")
        self.assertEqual(report["diff_count"], -1)
        self.assertIn("no user-visible columns", report["error"])
        self.assertIn("expected={'status': 'ok'", report["error"])
        self.assertIn("'data': []", report["error"])
