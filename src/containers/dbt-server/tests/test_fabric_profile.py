import csv
import io
import os
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services import fabric, spark_openivm_profile, spark_openivm_query_log


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


class FabricProfileConfigTest(unittest.TestCase):
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
