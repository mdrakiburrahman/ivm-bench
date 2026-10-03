import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.fabric_dbt_profile import instrument, instrument_session_polls


class FabricClientTimingTest(unittest.TestCase):
    def test_session_poll_preserves_response_and_records_only_diagnostics(self):
        response = Mock()
        response.json.return_value = {
            "state": "idle", "livyInfo": {"currentState": "idle", "jobCreationRequest": {
                "conf": {"spark.livy.session.idle.timeout": "30m", "secret": "secret-value"}
            }},
            "tags": {"FallbackReasons": "UserSparkConfigMismatch", "FallbackMessages":
                "secret-value incompatible: spark.livy.session.idle.timeout,"},
            "token": "secret-value",
        }
        get = Mock(return_value=response)
        requests = SimpleNamespace(get=get)
        rows = []
        instrument_session_polls(requests, rows.append)
        url = "https://example.invalid/livyapi/versions/2023-12-01/sessions/session-secret"
        self.assertIs(requests.get(url, headers={"Authorization": "secret-value"}), response)
        get.assert_called_once_with(url, headers={"Authorization": "secret-value"})
        self.assertEqual(rows[0]["fallback_reasons"], ["UserSparkConfigMismatch"])
        self.assertEqual(rows[0]["fallback_spark_settings"], ["spark.livy.session.idle.timeout"])
        self.assertTrue(rows[0]["idle_timeout_present"])
        self.assertNotIn("secret", json.dumps(rows))

    def test_non_session_requests_are_not_inspected(self):
        response = Mock()
        requests = SimpleNamespace(get=Mock(return_value=response))
        rows = []
        instrument_session_polls(requests, rows.append)
        requests.get("https://example.invalid/livyapi/versions/v1/sessions/id/statements/0")
        response.json.assert_not_called()
        self.assertEqual(rows, [])

    def test_bad_diagnostic_response_does_not_fail_session_poll(self):
        response = Mock()
        response.json.side_effect = ValueError("not json")
        requests = SimpleNamespace(get=Mock(return_value=response))
        instrument_session_polls(requests, Mock())
        with patch("sys.stderr"):
            self.assertIs(requests.get("https://example.invalid/livyapi/versions/v1/sessions/id"), response)

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
