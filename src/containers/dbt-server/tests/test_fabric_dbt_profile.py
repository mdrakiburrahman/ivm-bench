import json
import sys
import unittest
import tempfile
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.fabric_dbt_profile import instrument, instrument_session_polls, persist_hc_sessions


class FabricHcAddressTest(unittest.TestCase):
    def test_persists_owned_repl_address_and_rejects_multiple_applications(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session-id"
            def session(app, repl):
                return SimpleNamespace(credential=SimpleNamespace(session_id_file=str(path)),
                    is_dead=False, is_new_session_required=False,
                    session_id=app, repl_id=repl, hc_id="hc-" + repl)
            backend = SimpleNamespace(_active_sessions_lock=threading.Lock(),
                _active_sessions=[session("shared", "b"), session("shared", "a")])
            persist_hc_sessions(backend)
            route = json.loads(path.read_text())
            self.assertEqual(route, {"session_id": "shared", "hc_id": "hc-a", "repl_id": "a", "repl_count": 2})
            backend._active_sessions.append(session("another-app", "c"))
            with self.assertRaisesRegex(RuntimeError, "multiple Spark applications"):
                persist_hc_sessions(backend)
            self.assertEqual(json.loads(path.read_text()), route)

    def test_retired_session_is_not_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "session-id"
            backend = SimpleNamespace(_active_sessions_lock=threading.Lock(), _active_sessions=[
                SimpleNamespace(credential=SimpleNamespace(session_id_file=str(path)),
                    is_dead=True, is_new_session_required=False)])
            persist_hc_sessions(backend)
            self.assertFalse(path.exists())

    def test_addresses_are_persisted_with_profiling_disabled(self):
        from services import fabric_dbt_profile
        result = SimpleNamespace(exception=None, success=True)
        with patch.dict("os.environ", {"FABRIC_DBT_TIMINGS_PATH": ""}), \
             patch("dbt.cli.main.dbtRunner") as runner, \
             patch.object(fabric_dbt_profile, "persist_hc_sessions") as persist:
            runner.return_value.invoke.return_value = result
            self.assertEqual(fabric_dbt_profile.main(), 0)
            persist.assert_called_once()


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
