import json
import importlib.util
import sqlite3
import sys
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import spark_metrics


class SparkExecutionEvidenceTest(unittest.TestCase):
    def events(self, model):
        return [
            {"Event": "SparkListenerSQLExecutionStart", "executionId": 1, "time": 1000,
             "description": "Job group for statement 1",
             "physicalPlanDescription": f"AtomicCreateTableAsSelect\nArguments: DeltaCatalog@1, gold.{model}"},
            {"Event": "SparkListenerSQLAdaptiveExecutionUpdate", "executionId": 1,
             "physicalPlanDescription": f"first adaptive plan for {model}"},
            {"Event": "SparkListenerSQLAdaptiveExecutionUpdate", "executionId": 1,
             "physicalPlanDescription": f"latest observed adaptive plan for {model}"},
            # Sparse/orphan events must not erase a plan or create an execution.
            {"Event": "SparkListenerSQLAdaptiveExecutionUpdate", "executionId": 1},
            {"Event": "SparkListenerSQLAdaptiveExecutionUpdate", "executionId": 99,
             "physicalPlanDescription": "orphan plan"},
            {"Event": "SparkListenerJobStart", "Job ID": 0, "Stage IDs": [0],
             "Properties": {"spark.sql.execution.id": "1"}},
            {"Event": "SparkListenerTaskEnd", "Stage ID": 0,
             "Task Info": {"Task ID": 0, "Finish Time": 1050},
             "Task Metrics": {"Executor Run Time": 25, "Executor CPU Time": 20_000_000,
                              "Input Metrics": {"Bytes Read": 128, "Records Read": 3}}},
            {"Event": "SparkListenerSQLExecutionEnd", "executionId": 1, "time": 1100},
        ]

    def write_events(self, root, app, events):
        path = Path(root) / f"eventlog_v2_{app}" / f"events_1_{app}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("".join(json.dumps(e, separators=(",", ":")) + "\n" for e in events))
        return path

    def test_initial_and_last_observed_adaptive_plans_survive_rolling_files(self):
        with tempfile.TemporaryDirectory() as root:
            events = self.events("market")
            first = self.write_events(root, "app", events[:1])
            second = first.with_name("events_2_app")
            second.write_text(json.dumps(events[1]) + "\n")
            last = first.with_name("events_10_app")
            last.write_text("".join(json.dumps(e) + "\n" for e in events[2:]))
            parsed = spark_metrics.parse_app_events(spark_metrics._iter_event_files(root))

            self.assertEqual(set(parsed["executions"]), {1})
            meta = parsed["executions"][1]
            self.assertEqual(meta["physical_plan"], events[0]["physicalPlanDescription"])
            self.assertEqual(meta["adaptive_physical_plan"], "latest observed adaptive plan for market")
            self.assertEqual(meta["model"], "market")
            self.assertEqual(len(parsed["tasks"]), 1)

    def test_no_adaptive_plan_is_reported_when_none_was_observed(self):
        with tempfile.TemporaryDirectory() as root:
            events = [e for e in self.events("plain") if not e["Event"].endswith("AdaptiveExecutionUpdate")]
            path = self.write_events(root, "plain-app", events)
            parsed = spark_metrics.parse_app_events([str(path)])
            self.assertIsNone(parsed["executions"][1]["adaptive_physical_plan"])

    def test_reused_execution_ids_keep_each_app_plan_and_model(self):
        with tempfile.TemporaryDirectory() as root:
            for app, model in [("app-a", "market"), ("app-b", "broker")]:
                self.write_events(root, app, self.events(model))
            long_rows, _, executions, n_apps, n_execs = spark_metrics.build_engine_rows(
                "spark-openivm", "100", "run", root, {1: (900, 1200)}
            )

            self.assertEqual((n_apps, n_execs), (2, 2))
            self.assertEqual(len(executions), 2)
            by_app = {e["app_id"]: e for e in executions}
            for app, model in [("app-a", "market"), ("app-b", "broker")]:
                row = by_app[app]
                self.assertEqual((row["execution_id"], row["batch"], row["dbt_model"]), (1, 1, model))
                self.assertIn(f"gold.{model}", row["physical_plan"])
                self.assertEqual(row["adaptive_physical_plan"], f"latest observed adaptive plan for {model}")
            self.assertEqual(sum(r["metric_value"] for r in long_rows if r["metric_name"] == "records_read"), 6)

    def test_archive_keeps_requested_sidecars_and_excludes_previous_engine(self):
        with tempfile.TemporaryDirectory() as root:
            processed = Path(root) / "processed"
            processed.mkdir()
            (processed / "RESULTS.md").write_text("rollup")
            sidecars = {}
            for engine in spark_metrics.ENGINES:
                path = Path(root) / f"{engine}.jsonl"
                path.write_text(json.dumps({"physical_plan": "initial", "adaptive_physical_plan": "latest"}) + "\n")
                sidecars[engine] = str(path)
            archive_path = str(Path(root) / "metrics.zip")
            spark_metrics._zip_processed(str(processed), archive_path, sidecars)
            with zipfile.ZipFile(archive_path) as archive:
                for engine, path in sidecars.items():
                    self.assertEqual(archive.read(f"{engine}-executions.jsonl"), Path(path).read_bytes())
                self.assertEqual(archive.read("RESULTS.md"), b"rollup")
            spark_metrics._zip_processed(str(processed), archive_path, {"spark": sidecars["spark"]})
            with zipfile.ZipFile(archive_path) as archive:
                self.assertEqual(set(archive.namelist()), {"RESULTS.md", "spark-executions.jsonl"})

    @unittest.skipUnless(
        importlib.util.find_spec("duckdb") and importlib.util.find_spec("pandas"),
        "Parquet integration needs runtime DuckDB/pandas; archive coverage uses only the standard library",
    )
    def test_saved_bundle_keeps_both_sidecars_without_changing_parquet_contract(self):
        import duckdb

        native_connect = duckdb.connect
        test = self

        class NoPlansInRollup:
            def __init__(self, *args, **kwargs):
                self.connection = native_connect(*args, **kwargs)

            def register(self, name, frame):
                test.assertNotIn("physical_plan", frame.columns)
                test.assertNotIn("adaptive_physical_plan", frame.columns)
                return self.connection.register(name, frame)

            def __getattr__(self, name):
                return getattr(self.connection, name)

        with tempfile.TemporaryDirectory() as root:
            metrics = Path(root) / "mount/metrics/100"
            for engine in spark_metrics.ENGINES:
                self.write_events(metrics / engine / "spark-events", "app", self.events(engine.replace("-", "_")))
            database = Path(root) / "mount/benchmark-state/benchmark.db"
            database.parent.mkdir(parents=True)
            with sqlite3.connect(database) as con:
                con.execute("CREATE TABLE engine_batches (benchmark_id TEXT, engine TEXT, batch_num INTEGER, result_json TEXT)")
                for engine in spark_metrics.ENGINES:
                    result = {"extra": {"wall_window_start_ms": 900, "wall_window_end_ms": 1200}}
                    con.execute("INSERT INTO engine_batches VALUES (?, ?, ?, ?)", ("benchmark", engine, 1, json.dumps(result)))

            with patch.object(spark_metrics, "_render_diff_png"), patch.object(duckdb, "connect", NoPlansInRollup):
                result = spark_metrics.process(root, "100", "benchmark", "run", emit=lambda _: None)
            self.assertEqual(result["status"], "ok")
            self.assertTrue(result["both_engines"])
            with zipfile.ZipFile(result["zip"]) as archive:
                for engine in spark_metrics.ENGINES:
                    name = f"{engine}-executions.jsonl"
                    self.assertEqual(archive.namelist().count(name), 1)
                    bundled = archive.read(name).decode()
                    self.assertEqual(bundled, (metrics / engine / "executions.jsonl").read_text())
                    rows = [json.loads(line) for line in bundled.splitlines()]
                    self.assertEqual(len(rows), 1)
                    self.assertIn("AtomicCreateTableAsSelect", rows[0]["physical_plan"])
                    self.assertIn("latest observed", rows[0]["adaptive_physical_plan"])

            with duckdb.connect() as con:
                path = str(metrics / "processed/metrics_long.parquet")
                columns = [r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchall()]
                self.assertEqual(columns, ["engine", "sf", "run_id", "batch", "dbt_model", "execution_id", "job_id", "stage_id", "task_id", "metric_name", "metric_value", "event_ts"])
                self.assertEqual(con.execute("SELECT sum(metric_value) FROM read_parquet(?) WHERE metric_name='records_read'", [path]).fetchone()[0], 6)

            # Reusing the processed directory must not archive the old engine's
            # sidecar when only one engine was requested for the next run.
            with patch.object(spark_metrics, "_render_diff_png"):
                single = spark_metrics.process(root, "100", "benchmark", "next", engines=["spark"], emit=lambda _: None)
            with zipfile.ZipFile(single["zip"]) as archive:
                self.assertIn("spark-executions.jsonl", archive.namelist())
                self.assertNotIn("spark-openivm-executions.jsonl", archive.namelist())


if __name__ == "__main__":
    unittest.main()
