import csv
import io
import os
import re
import sys
import threading
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import fabric, spark_openivm_profile, spark_openivm_query_log

try:
    import flask
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False


class FabricCursorOwnershipTest(unittest.TestCase):
    def setUp(self):
        self.session = MagicMock(is_new_session_required=False)
        self.session.try_reuse_session.return_value = True
        self.cursors = [MagicMock(description=[("value",)]), MagicMock(description=[("value",)])]
        self.cursors[0].fetchall.return_value = [[1]]
        self.cursors[1].fetchall.return_value = [[2]]
        self.cursor_factory = Mock(side_effect=self.cursors)
        modules = {
            "dbt.adapters.fabricspark.credentials": types.SimpleNamespace(FabricSparkCredentials=Mock()),
            "dbt.adapters.fabricspark.livysession": types.SimpleNamespace(
                LivySession=Mock(return_value=self.session), LivyCursor=self.cursor_factory),
        }
        self.enterContext(patch.dict(sys.modules, modules))
        self.enterContext(patch("services.fabric.Path.read_text", return_value="saved"))
        self.enterContext(patch("services.fabric._load_resolved", return_value={"lakehouse_name": "compute", "lakehouse_id": "lake"}))
        self.enterContext(patch("services.fabric._compute_lakehouse_id", return_value="lake"))

    def test_statements_have_separate_cursors_and_keep_session_alive(self):
        with fabric.ProfileClient() as client:
            results = [client.execute("SELECT 1"), client.execute("SELECT 2")]
        self.assertEqual([r["output"]["data"]["application/json"]["data"] for r in results], [[[1]], [[2]]])
        self.session.try_reuse_session.assert_called_once_with("saved")
        for cursor, sql in zip(self.cursors, ("SELECT 1", "SELECT 2")):
            cursor.execute.assert_called_once_with(sql)
            cursor.close.assert_called_once()
        self.session.close.assert_not_called()

    def test_query_failure_closes_cursor_without_deleting_session(self):
        self.cursors[0].execute.side_effect = RuntimeError("query failed")
        with fabric.ProfileClient() as client:
            with self.assertRaisesRegex(RuntimeError, "query failed"):
                client.execute("SELECT 1")
        self.cursors[0].close.assert_called_once()
        self.session.close.assert_not_called()

    def test_parallel_statements_keep_separate_result_buffers(self):
        barrier = threading.Barrier(2)
        for cursor in self.cursors:
            cursor.execute.side_effect = lambda _sql: barrier.wait(timeout=5)
        with fabric.ProfileClient() as client, ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(client.execute, ("SELECT 1", "SELECT 2")))
        self.assertEqual(sorted(r["output"]["data"]["application/json"]["data"][0][0]
                                for r in results), [1, 2])
        self.session.try_reuse_session.assert_called_once()
        for cursor in self.cursors:
            cursor.close.assert_called_once()
        self.session.close.assert_not_called()

    def test_dead_or_lost_session_never_creates_replacement(self):
        self.session.try_reuse_session.return_value = False
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            with fabric.ProfileClient():
                self.fail("attached to dead session")
        self.cursor_factory.assert_not_called()
        self.session.try_reuse_session.return_value = True
        with fabric.ProfileClient() as client:
            self.session.is_new_session_required = True
            with self.assertRaisesRegex(RuntimeError, "session was lost"):
                client.execute("SELECT 1")
        self.cursor_factory.assert_not_called()
        self.session.close.assert_not_called()


class FabricSparkRuntimeMetricsTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch("services.fabric._compute_lakehouse_id", return_value="owned"))
        self.token = self.enterContext(patch("services.fabric.get_token", return_value="private-token"))
        self.request = self.enterContext(patch("services.fabric._fabric_req"))
        self.client = self.enterContext(patch("services.fabric.ProfileClient"))
        self.request.side_effect = self.response

    def response(self, method, url, **kwargs):
        self.assertEqual(method, "GET")
        self.assertIn("/lakehouses/owned/livySessions", url)
        self.assertLessEqual(kwargs["timeout"], 10)
        if url.endswith("/livySessions"):
            data = {"value": [{"item": {"itemId": "owned"}, "livyId": "session",
                               "sparkApplicationId": "application_1_0001"}]}
        elif url.endswith("/session"):
            data = {"executorCores": 8, "numExecutors": 4}
        elif url.endswith("/environment"):
            data = {"sparkProperties": [["spark.executor.cores", "8"],
                                         ["spark.hadoop.secret", "DO_NOT_EXPORT"]]}
        elif url.endswith("/allexecutors"):
            data = [{"id": "1", "totalCores": 8, "totalGCTime": 99,
                     "executorLogs": {"stdout": "DO_NOT_EXPORT"}}]
        elif "/stages?" in url:
            data = [{"stageId": 1, "executorCpuTime": 123, "diskBytesSpilled": 456,
                     "description": "DO_NOT_EXPORT"}]
        else:
            data = {"data": {"allocatedCores": [32], "runningCores": [24]}}
        return Mock(status_code=200, json=Mock(return_value=data))

    def test_reads_owned_session_without_sql_and_keeps_only_diagnostic_fields(self):
        result = fabric.spark_runtime_metrics()
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["requested_resources"]["numExecutors"], 4)
        self.assertEqual(result["executors"][0]["totalCores"], 8)
        self.assertEqual(result["stages"][0]["diskBytesSpilled"], 456)
        self.assertEqual(result["resource_usage"]["data"]["allocatedCores"], [32])
        self.assertNotIn("DO_NOT_EXPORT", str(result))
        self.assertNotIn("private-token", str(result))
        self.assertEqual(self.request.call_count, 6)
        self.client.assert_not_called()

    def test_expired_collection_budget_stops_without_rest_calls(self):
        with patch("services.fabric.time.monotonic", side_effect=[0, 61]):
            result = fabric.spark_runtime_metrics()
        self.assertEqual(result["status"], "partial")
        self.request.assert_not_called()

    @unittest.skipUnless(HAS_FLASK, "requires dbt-server Flask dependency")
    def test_profile_export_keeps_csv_if_runtime_capture_fails(self):
        from flask import Flask
        from handlers.spark_openivm import spark_openivm_export_profile
        app = Flask(__name__)
        for engine in ("fabric-openivm-jvm-35", "spark-openivm"):
            with self.subTest(engine=engine), app.app_context(), \
                    patch("services.spark_openivm_profile.export_profile", return_value={"status": "ok", "csv": {"profile": "rows"}}), \
                    patch("services.fabric.spark_runtime_metrics", side_effect=RuntimeError("DO_NOT_EXPORT")) as capture:
                response, status = spark_openivm_export_profile("run", 1, engine)
                result = response.get_json()
                self.assertEqual(status, 200)
                self.assertEqual(result["csv"]["profile"], "rows")
                self.assertNotIn("DO_NOT_EXPORT", str(result))
                if engine == "fabric-openivm-jvm-35":
                    self.assertEqual(result["spark_runtime"], {"status": "error", "error_type": "RuntimeError"})
                    capture.assert_called_once()
                else:
                    self.assertNotIn("spark_runtime", result)
                    capture.assert_not_called()

    def test_missing_history_is_partial_without_fabricated_resources(self):
        original = self.response
        self.request.side_effect = lambda method, url, **kw: (
            original(method, url, **kw) if url.endswith("/livySessions")
            else Mock(status_code=404))
        result = fabric.spark_runtime_metrics()
        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["executors"], [])
        self.assertEqual(result["requested_resources"], {})
        self.assertEqual(result["responses"][-1]["http_status"], 404)
        self.client.assert_not_called()

    def test_does_not_attach_to_another_lakehouse_or_guess_an_ambiguous_session(self):
        for items in (("different",), ("owned", "owned")):
            with self.subTest(items=items):
                self.request.reset_mock()
                self.request.side_effect = None
                self.request.return_value = Mock(status_code=200, json=Mock(return_value={
                    "value": [{"item": {"itemId": item}} for item in items]}))
                result = fabric.spark_runtime_metrics()
                self.assertEqual(result["status"], "partial")
                self.assertEqual(self.request.call_count, 1)
                self.assertNotIn("executors", result)
                self.client.assert_not_called()


try:
    import dbt.adapters.fabricspark.livysession
    HAS_ADAPTER = True
except ImportError:
    HAS_ADAPTER = False


@unittest.skipUnless(HAS_ADAPTER, "requires dbt-server requirements (dbt-fabricspark)")
class FabricProfileTest(unittest.TestCase):
    def setUp(self):
        self.resolved = {
            "lakehouse_id": "11111111-1111-1111-1111-111111111111",
            "lakehouse_name": "benchmark",
        }
        self.patches = [
            patch("services.fabric.Path.read_text", return_value="existing-session\n"),
            patch("services.fabric._load_resolved", return_value=self.resolved),
            patch("dbt.adapters.fabricspark.livysession.get_headers", return_value={}),
            patch("dbt.adapters.fabricspark.livysession.requests.get"),
            patch("dbt.adapters.fabricspark.livysession.requests.post"),
            patch("dbt.adapters.fabricspark.livysession.requests.delete"),
            patch("dbt.adapters.fabricspark.livysession.LivySessionManager.connect"),
        ]
        self.file, _, _, self.get, self.post, self.delete, self.connect = [
            self.enterContext(p) for p in self.patches
        ]
        self.post.return_value = Mock(status_code=200)
        self.post.return_value.json.return_value = {"id": 17}

    def output(self, columns, rows):
        session = Mock(status_code=200)
        session.json.return_value = {"state": "idle", "livyInfo": {"currentState": "idle"}}
        statement = Mock(status_code=200)
        statement.json.return_value = {"state": "available", "output": {
            "status": "ok", "data": {"application/json": {
                "schema": {"fields": [
                    {"name": c, "type": "string", "nullable": True} for c in columns
                ]}, "data": rows,
            }},
        }}
        self.get.return_value = session
        self.get.side_effect = [session, statement]
        return statement

    def test_profile_uses_existing_session_and_keeps_it_alive(self):
        cols = list(reversed(spark_openivm_profile._PROFILE_COLS))
        row = dict(zip(spark_openivm_profile._PROFILE_COLS,
                       ["r1", "v1", "2026-09-25", 1, "execute", 42, "incremental"]))
        self.output(cols, [[row[c] for c in cols]])
        result = spark_openivm_profile.export_profile("run", 2, client=fabric.ProfileClient())
        records = list(csv.DictReader(io.StringIO(result["csv"]["profile"])))
        self.assertEqual(records[0]["duration_ms"], "42")
        self.assertEqual(records[0]["exported_after_batch"], "2")
        self.assertEqual(result["row_count"], 1)
        self.assertIn("/sessions/existing-session/statements", self.post.call_args.args[0])
        self.connect.assert_not_called()
        self.delete.assert_not_called()

    def test_missing_read_result_is_reissued_once_with_a_new_statement_id(self):
        statement = self.output(["count"], [[42]])
        session = self.get.return_value
        missing = Mock(status_code=404, text='{"message":"Statement not found"}')
        self.get.side_effect = [session] + [missing] * 21 + [statement]
        responses = [Mock(status_code=200), Mock(status_code=200)]
        for response, statement_id in zip(responses, (17, 18)):
            response.json.return_value = {"id": statement_id}
        self.post.side_effect = responses
        with patch("dbt.adapters.fabricspark.livysession.time.sleep"), \
                patch("services.fabric.logger.warning"), fabric.ProfileClient() as client:
            result = client.execute_read("SELECT COUNT(*) FROM existing_model")
        self.assertEqual(result["output"]["data"]["application/json"]["data"], [[42]])
        self.assertEqual(self.post.call_count, 2)
        self.assertEqual(self.post.call_args_list[0].kwargs["data"],
                         self.post.call_args_list[1].kwargs["data"])
        self.assertTrue(self.get.call_args.args[0].endswith("/statements/18"))
        self.connect.assert_not_called()
        self.delete.assert_not_called()

    def test_persistent_missing_result_fails_after_one_read_reissue(self):
        from dbt_common.exceptions import DbtRuntimeError

        self.output(["count"], [[42]])
        session = self.get.return_value
        missing = Mock(status_code=404, text='{"message":"Statement not found"}')
        self.get.side_effect = [session] + [missing] * 42
        with patch("dbt.adapters.fabricspark.livysession.time.sleep"), \
                patch("services.fabric.logger.warning"), fabric.ProfileClient() as client:
            with self.assertRaisesRegex(DbtRuntimeError, "HTTP 404"):
                client.execute_read("SELECT COUNT(*) FROM existing_model")
        self.assertEqual(self.post.call_count, 2)
        self.connect.assert_not_called()

    def test_other_read_errors_do_not_reissue(self):
        from dbt_common.exceptions import DbtRuntimeError

        self.output(["count"], [[42]])
        session = self.get.return_value
        forbidden = Mock(status_code=403, text='{"message":"Statement not found"}')
        self.get.side_effect = [session, forbidden]
        with fabric.ProfileClient() as client:
            with self.assertRaisesRegex(DbtRuntimeError, "HTTP 403"):
                client.execute_read("SELECT COUNT(*) FROM existing_model")
        self.assertEqual(self.post.call_count, 1)

    def test_mutating_commands_are_not_reissued_after_missing_result(self):
        from dbt_common.exceptions import DbtRuntimeError

        for sql in ("CREATE TEMP VIEW expected AS SELECT 1", "DROP VIEW expected",
                    "REFRESH MATERIALIZED VIEW existing_model", "INSERT INTO target SELECT 1"):
            with self.subTest(sql=sql):
                self.post.reset_mock()
                self.output([], [])
                session = self.get.return_value
                missing = Mock(status_code=404, text='{"message":"Statement not found"}')
                self.get.side_effect = [session] + [missing] * 21
                with patch("dbt.adapters.fabricspark.livysession.time.sleep"), \
                        fabric.ProfileClient(require_tabular=False) as client:
                    with self.assertRaisesRegex(DbtRuntimeError, "HTTP 404"):
                        client.execute(sql)
                self.assertEqual(self.post.call_count, 1)
        self.connect.assert_not_called()

    def test_query_log_preserves_sql_and_refresh_identity(self):
        row = ["r1", "v1", "2026-09-25", 1, 0, "incremental", "apply", "merge", 17,
               "MERGE INTO v1 USING delta ON v1.id = delta.id WHEN MATCHED THEN DELETE"]
        self.output(spark_openivm_query_log._QUERY_LOG_COLS, [row])
        result = spark_openivm_query_log.export_query_log("run", 3, client=fabric.ProfileClient())
        self.assertEqual(result["rows"][0]["sql_text"], row[-1])
        self.assertEqual(result["rows"][0]["refresh_id"], "r1")
        self.connect.assert_not_called()

    def test_missing_session_file_does_not_create_session(self):
        self.file.side_effect = FileNotFoundError("session file")
        with self.assertRaises(FileNotFoundError):
            with fabric.ProfileClient():
                self.fail("missing session attached")
        self.post.assert_not_called()
        self.connect.assert_not_called()

    def test_dead_session_does_not_create_replacement(self):
        self.get.return_value = Mock(status_code=404)
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            with fabric.ProfileClient():
                self.fail("dead session attached")
        self.post.assert_not_called()
        self.connect.assert_not_called()

    def test_statement_failure_is_not_suppressed(self):
        statement = self.output([], [])
        statement.json.return_value = {
            "state": "available", "output": {"status": "error", "evalue": "catalog unavailable"}
        }
        with self.assertRaisesRegex(Exception, "catalog unavailable"):
            spark_openivm_profile.export_profile("run", 2, client=fabric.ProfileClient())
        self.delete.assert_not_called()
        self.connect.assert_not_called()

    def test_missing_schema_fails_export(self):
        self.output([], [])
        with self.assertRaisesRegex(RuntimeError, "no tabular schema"):
            spark_openivm_profile.export_profile("run", 2, client=fabric.ProfileClient())

    def test_validation_ddl_accepts_empty_result_and_keeps_session(self):
        self.output([], [])
        with fabric.ProfileClient(require_tabular=False) as client:
            result = client.execute("CREATE OR REPLACE TEMP VIEW expected AS SELECT 1")
        self.assertEqual(result["output"]["data"]["application/json"]["data"], [])
        self.connect.assert_not_called()
        self.delete.assert_not_called()

    def test_validation_cannot_replace_lost_driver(self):
        self.output([], [])
        with fabric.ProfileClient(require_tabular=False) as client:
            client.session.is_new_session_required = True
            with self.assertRaisesRegex(RuntimeError, "session was lost"):
                client.execute("SELECT COUNT(*) FROM expected")
        self.post.assert_not_called()
        self.connect.assert_not_called()
        self.delete.assert_not_called()


class FabricProfileConfigTest(unittest.TestCase):
    def test_window_feature_flags_match_spark_benchmark(self):
        root = Path(__file__).resolve().parents[4]
        config = (root / "src/containers/spark-openivm/config/spark-defaults.conf.tmpl").read_text()
        spark = dict(line.split("=", 1) for line in config.splitlines()
                     if line.startswith("spark.openivm.") and "=" in line)
        with patch("services.fabric.onelake_abfss", return_value="abfss://test"):
            properties = fabric.default_openivm_spark_properties()
        for key in ("spark.openivm.refresh.windowRunningIncremental.enabled",
                    "spark.openivm.refresh.groupedWindowState.enabled",
                    "spark.openivm.refresh.semiJoinPrune.enabled",
                    "spark.openivm.refresh.scd2RangeAccel.enabled",
                    "spark.openivm.stateSync.async.enabled",
                    "spark.openivm.compile.classificationCache.enabled",
                    "spark.openivm.refresh.windowClusterPrune.enabled"):
            with self.subTest(key=key):
                self.assertEqual(properties[key], spark[key])

    def test_compose_forwards_recording_flags_to_environment_publisher(self):
        root = Path(__file__).resolve().parents[4]
        compose = (root / "docker/docker-compose.benchmark.fabric-openivm-jvm-35.yml").read_text()
        # Exercise the actual flag bindings, including their disabled defaults.
        bindings = dict(re.findall(r'^      (OPENIVM_\w+): "\$\{(OPENIVM_\w+):-0\}"$',
                                   compose, re.MULTILINE))
        flags = {"OPENIVM_PROFILE_REFRESH": "spark.openivm.profile.refresh",
                 "OPENIVM_QUERY_LOG": "spark.openivm.queryLog.enabled"}
        for enabled in (None, *flags, "both"):
            host = {key: "1" for key in flags if enabled in (key, "both")}
            container = {key: host.get(source, "0") for key, source in bindings.items()}
            with self.subTest(enabled=enabled), patch.dict(os.environ, container, clear=True), \
                    patch("services.fabric.onelake_abfss", return_value="abfss://test"):
                properties = fabric.default_openivm_spark_properties()
                for env_key, spark_key in flags.items():
                    self.assertEqual(properties[spark_key],
                                     "true" if env_key in host else "false")


class ProfileRowLimitTest(unittest.TestCase):
    def test_batch_one_metadata_contains_effective_settings(self):
        client = MagicMock()
        client.__enter__.return_value = client
        client.execute.side_effect = [
            {"output": {"data": {"application/json": {"data": []}}}},
            {"output": {"data": {"application/json": {"data": [
                ["spark.sql.autoBroadcastJoinThreshold", "10485760"]
            ]}}}},
        ]
        result = spark_openivm_profile.export_profile("run", 1, client=client)
        self.assertEqual(result["runtime_sql_settings"], {
            "spark.sql.autoBroadcastJoinThreshold": "10485760"
        })
        self.assertEqual([call.args[0] for call in client.execute.call_args_list],
                         ["SHOW OPENIVM REFRESH PROFILE", "SET -v"])

    def test_configuration_snapshot_excludes_unrelated_and_sensitive_settings(self):
        client = Mock()
        client.execute.return_value = {"output": {"data": {"application/json": {"data": [
            ["spark.sql.shuffle.partitions", "200", "description"],
            ["spark.databricks.delta.merge.materializeSource", "auto", "description"],
            ["spark.hadoop.fs.azure.account.key", "secret", "description"],
        ]}}}}
        self.assertEqual(spark_openivm_profile._runtime_sql_settings(client), {
            "spark.sql.shuffle.partitions": "200",
            "spark.databricks.delta.merge.materializeSource": "auto",
        })
        client.execute.assert_called_once_with("SET -v")

    def test_row_caps_are_not_reported_as_complete_exports(self):
        for count in (1000, 100000):
            with self.subTest(count=count):
                output = {"data": {"application/json": {"data": [[1]] * count}}}
                with self.assertRaisesRegex(RuntimeError, "Livy row limit"):
                    spark_openivm_profile._extract_rows(output, "profile")

    def test_more_than_default_cap_can_be_exported(self):
        output = {"data": {"application/json": {"data": [[1]] * 1001}}}
        self.assertEqual(len(spark_openivm_profile._extract_rows(output, "profile")), 1001)


if __name__ == "__main__":
    unittest.main()
