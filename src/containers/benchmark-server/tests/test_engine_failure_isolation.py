import sys
from copy import deepcopy
import unittest
from pathlib import Path
from threading import Barrier, BrokenBarrierError
from types import SimpleNamespace
from unittest.mock import Mock, patch


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.config import BenchmarkConfig  # noqa: E402
from models.result import BenchmarkResult, EngineResult  # noqa: E402
from services.engine_runner import EngineRunner
from services.orchestrator import Orchestrator  # noqa: E402
from services.storage_sync import storage_snapshot_barrier  # noqa: E402


class EngineFailureIsolationTest(unittest.TestCase):
    def test_broken_storage_barrier_does_not_fail_surviving_engine(self):
        barrier = Barrier(2)
        barrier.abort()

        entered = False
        with storage_snapshot_barrier(barrier):
            entered = True

        self.assertTrue(entered)

    def test_barrier_break_after_snapshot_still_preserves_result(self):
        class BreakAfterSnapshotStarts:
            calls = 0

            def wait(self):
                self.calls += 1
                if self.calls == 2:
                    raise BrokenBarrierError

        barrier = BreakAfterSnapshotStarts()

        with storage_snapshot_barrier(barrier):
            pass

        self.assertEqual(barrier.calls, 2)

    @patch("services.orchestrator.EngineRunner")
    def test_serial_failure_does_not_skip_next_engine(self, engine_runner):
        failed_runner = Mock()
        failed_runner.run.return_value = EngineResult(
            engine="failed", status="failed", error="boom"
        )
        successful_runner = Mock()
        successful_runner.run.return_value = EngineResult(
            engine="successful", status="completed"
        )
        engine_runner.side_effect = [failed_runner, successful_runner]

        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator._config = SimpleNamespace(engines=["failed", "successful"])
        orchestrator._result = BenchmarkResult()
        orchestrator._benchmark_id = "test"

        failed = orchestrator._run_engines_serial(
            {"failed": Mock(), "successful": Mock()}
        )

        self.assertEqual(failed, ["failed"])
        successful_runner.run.assert_called_once_with()
        self.assertEqual(orchestrator._result.engines["successful"].status, "completed")

    @patch("services.orchestrator.compute_engine_configs", return_value={})
    def test_host_failure_does_not_skip_cloud_wave(self, _compute_configs):
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator._config = BenchmarkConfig(
            engines=["duckdb", "databricks-enzyme"],
            schedule="serial-host-parallel-cloud",
        )
        orchestrator._result = BenchmarkResult()
        orchestrator.emit = Mock()
        orchestrator._init_parallel_staging = Mock()
        orchestrator._run_engines_serial = Mock(side_effect=lambda _configs: ["duckdb"])

        def run_cloud(_engines, _configs):
            orchestrator._result.engines["databricks-enzyme"] = EngineResult(
                engine="databricks-enzyme", status="completed"
            )

        orchestrator._run_engine_wave = Mock(side_effect=run_cloud)

        with self.assertRaisesRegex(RuntimeError, "Engines failed: duckdb"):
            orchestrator._run_serial_host_parallel_cloud()

        orchestrator._run_engine_wave.assert_called_once()


class DatabricksFailureDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        self.run = {
            "started_at": "2026-10-13T19:40:00+00:00",
            "full_refresh": False,
            "nodes": [{"name": "fact_market_history", "status": "error",
                       "message": "[TASK_FAILED_EXECUTOR_LOSS] Command exited with code 52, oom"}],
        }
        self.events = {"pipelines": [{
            "table": "fact_market_history", "updates": [{
                "creation_time": 1791920401000, "state": "UpdateInfoState.FAILED",
                "full_refresh": False,
                "events": [
                    {"details": {"abnormal_termination": {"cause": "OUT_OF_MEMORY"}}},
                    {"details": {"resource_info": {"vertical_autoscaling_info": [
                        {"scaling_decision": "WILL_SCALE_UP"}
                    ]}}},
                ],
            }],
        }]}

    def classifier(self, batch_num=2):
        runner = EngineRunner.__new__(EngineRunner)
        runner._dbt_url = "http://dbt"
        runner._emit = Mock()
        runner._export_databricks_enzyme_pipeline_events = Mock(return_value=self.events)
        with patch("services.engine_runner.requests.get") as get:
            get.return_value.json.return_value = self.run
            result = runner._is_databricks_transient("failed-run", batch_num)
        return result, runner

    def test_oom_with_explicit_scale_up_retries_refresh(self):
        result, runner = self.classifier()
        self.assertTrue(result[0])
        self.assertIn("OUT_OF_MEMORY", result[1])
        runner._export_databricks_enzyme_pipeline_events.assert_called_once_with(2)

    def test_driver_disconnect_is_retryable_but_mixed_sql_failure_is_not(self):
        self.run["nodes"] = [{
            "name": "trades_history", "status": "error",
            "message": "Communication lost with driver. Cluster "
                       "1016-223934-sjcqo5z-v2n was not reachable for 120 seconds.",
        }]
        result, runner = self.classifier()
        self.assertTrue(result[0])
        runner._export_databricks_enzyme_pipeline_events.assert_not_called()
        self.run["nodes"].append({
            "name": "other_model", "status": "error", "message": "UNRESOLVED_COLUMN"
        })
        result, _runner = self.classifier()
        self.assertFalse(result[0])

    @patch("services.engine_runner.time.sleep")
    @patch("services.engine_runner.requests.get")
    @patch("services.engine_runner.requests.post")
    def test_driver_disconnect_retries_refresh_without_reappending_sources(self, post, get, sleep):
        runner = EngineRunner.__new__(EngineRunner)
        runner._config = SimpleNamespace(scale_factor=10)
        runner._dbt_url = "http://dbt"
        runner._result = EngineResult(engine="databricks-enzyme")
        batch = runner._result.batches[1]
        for name in ("_emit", "_stream_dbt_progress", "_save_run_result"):
            setattr(runner, name, Mock())
        message = ("Communication lost with driver. Cluster "
                   "1016-223934-sjcqo5z-v2n was not reachable for 120 seconds.")
        get.return_value.json.return_value = {
            "nodes": [{"name": "trades_history", "status": "error", "message": message}]
        }
        append_response, failed_response, success_response = Mock(), Mock(), Mock()
        append_response.status_code = 200
        append_response.json.return_value = {"status": "ok"}
        failed_response.json.return_value = {"run_id": "unreachable"}
        success_response.json.return_value = {"run_id": "recovered"}
        post.side_effect = [append_response, failed_response, success_response]

        def check(run_id, batch_num):
            self.assertEqual(batch_num, 2)
            batch.status = "failed" if run_id == "unreachable" else "completed"
            batch.error = message if run_id == "unreachable" else ""

        runner._check_run_result = Mock(side_effect=check)
        self.assertEqual(runner._run_databricks_enzyme(2), "recovered")
        self.assertEqual([call.args[0] for call in post.call_args_list], [
            "http://dbt/sources/databricks-enzyme/append/2/10",
            "http://dbt/run/databricks-enzyme",
            "http://dbt/run/databricks-enzyme",
        ])
        for call in post.call_args_list[1:]:
            self.assertEqual(call.kwargs["json"], {"scale_factor": 10, "full_refresh": False})
        runner._save_run_result.assert_any_call("unreachable", 2, suffix="-attempt1")
        runner._save_run_result.assert_any_call("recovered", 2)
        sleep.assert_called_once()
        self.assertEqual(batch.status, "completed")

    def test_scale_up_requires_matching_latest_failed_update(self):
        for case in ("no_oom", "no_scale_up", "old", "completed", "full_refresh",
                     "newer_update", "ambiguous", "missing_started_at"):
            with self.subTest(case=case):
                events = deepcopy(self.events)
                run = deepcopy(self.run)
                update = events["pipelines"][0]["updates"][0]
                if case == "no_oom":
                    update["events"].pop(0)
                elif case == "no_scale_up":
                    update["events"].pop()
                elif case == "old":
                    update["creation_time"] -= 2000
                elif case == "completed":
                    update["state"] = "UpdateInfoState.COMPLETED"
                elif case == "full_refresh":
                    update["full_refresh"] = True
                elif case == "newer_update":
                    events["pipelines"][0]["updates"].append({
                        "creation_time": update["creation_time"] + 1,
                        "state": "COMPLETED", "full_refresh": False,
                    })
                elif case == "ambiguous":
                    events["pipelines"].append(deepcopy(events["pipelines"][0]))
                else:
                    run.pop("started_at")
                self.assertEqual(EngineRunner._databricks_scale_up_models(run, events), set())

    def test_initialization_does_not_use_scale_up_recovery(self):
        result, runner = self.classifier(batch_num=1)
        self.assertFalse(result[0])
        runner._export_databricks_enzyme_pipeline_events.assert_not_called()
        self.run["full_refresh"] = True
        result, runner = self.classifier()
        self.assertFalse(result[0])
        runner._export_databricks_enzyme_pipeline_events.assert_not_called()

    def test_sql_error_prevents_retry_even_when_another_node_can_scale(self):
        self.run["nodes"].append({
            "name": "other_model", "status": "error", "message": "UNRESOLVED_COLUMN"
        })
        result, _runner = self.classifier()
        self.assertFalse(result[0])

    def test_oom_without_scale_up_does_not_retry(self):
        self.events["pipelines"][0]["updates"][0]["events"].pop()
        result, _runner = self.classifier()
        self.assertFalse(result[0])

    @patch("services.engine_runner.time.sleep")
    @patch("services.engine_runner.requests.post")
    def test_scale_up_retries_are_bounded_and_preserve_final_error(self, post, sleep):
        runner = EngineRunner.__new__(EngineRunner)
        runner._config = SimpleNamespace(scale_factor=10)
        runner._dbt_url = "http://dbt"
        runner._result = EngineResult(engine="databricks-enzyme")
        batch = runner._result.batches[1]
        for name in ("_emit", "_stream_dbt_progress", "_save_run_result"):
            setattr(runner, name, Mock())
        runner._is_databricks_transient = Mock(return_value=(True, "WILL_SCALE_UP"))
        post.return_value.json.return_value = {"run_id": "still-oom"}

        def fail(*_args):
            batch.status = "failed"
            batch.error = "oom"

        runner._check_run_result = Mock(side_effect=fail)
        with patch("services.engine_runner.DATABRICKS_ENZYME_MAX_RETRIES", 2):
            self.assertEqual(runner._run_databricks_enzyme_dbt_with_retry(2, False), "still-oom")
        self.assertEqual(post.call_count, 3)
        self.assertEqual(sleep.call_count, 2)
        runner._save_run_result.assert_called_with("still-oom", 2)
        self.assertEqual(batch.status, "failed")
        self.assertEqual(batch.error, "oom")

    @patch("services.engine_runner.time.sleep")
    @patch("services.engine_runner.requests.post")
    def test_recovery_keeps_refresh_payload_and_archives_attempt(self, post, sleep):
        runner = EngineRunner.__new__(EngineRunner)
        runner._config = SimpleNamespace(scale_factor=10)
        runner._dbt_url = "http://dbt"
        runner._result = EngineResult(engine="databricks-enzyme")
        batch = runner._result.batches[1]
        for name in ("_emit", "_stream_dbt_progress", "_save_run_result"):
            setattr(runner, name, Mock())
        runner._is_databricks_transient = Mock(return_value=(True, "WILL_SCALE_UP"))
        post.return_value.json.side_effect = [{"run_id": "oom"}, {"run_id": "scaled"}]

        def check(run_id, batch_num):
            self.assertEqual(batch_num, 2)
            batch.status = "failed" if run_id == "oom" else "completed"
            batch.error = "oom" if run_id == "oom" else ""

        runner._check_run_result = Mock(side_effect=check)
        self.assertEqual(runner._run_databricks_enzyme_dbt_with_retry(2, False), "scaled")
        self.assertEqual(post.call_count, 2)
        for call in post.call_args_list:
            self.assertEqual(call.kwargs["json"], {"scale_factor": 10, "full_refresh": False})
            self.assertEqual(call.args[0], "http://dbt/run/databricks-enzyme")
        runner._save_run_result.assert_any_call("oom", 2, suffix="-attempt1")
        runner._save_run_result.assert_any_call("scaled", 2)
        self.assertEqual(batch.status, "completed")
        sleep.assert_called_once()

    @patch("services.engine_runner.time.time", side_effect=[100, 103, 103])
    def test_failed_refresh_captures_events_and_retains_error_and_timing(self, _time):
        runner = EngineRunner.__new__(EngineRunner)
        runner._engine = SimpleNamespace(name="databricks-enzyme")
        runner._result = EngineResult(engine="databricks-enzyme")
        batch = runner._result.batches[1]
        for name in (
            "_emit", "_persist_batch_result", "_batch_loader_append",
            "_capture_delta_stats", "_databricks_enzyme_warmup",
            "_ensure_cpu_measurement_status", "_export_databricks_enzyme_metrics",
            "_save_openivm_ops_chart", "_capture_storage_metrics",
        ):
            setattr(runner, name, Mock())

        def failed_refresh(_batch_num):
            batch.status = "failed"
            batch.error = "JVM_OUT_OF_MEMORY"
            return "failed-run"

        def capture_events(_batch_num, result):
            self.assertEqual(result.duration_s, 3)
            self.assertEqual(result.extra["wall_window_start_ms"], 100000)
            self.assertEqual(result.extra["wall_window_end_ms"], 103000)

        runner._run_databricks_enzyme = Mock(side_effect=failed_refresh)
        runner._collect_databricks_enzyme_flow_metrics = Mock(side_effect=capture_events)
        runner._run_batch(2)

        runner._collect_databricks_enzyme_flow_metrics.assert_called_once_with(2, batch)
        runner._export_databricks_enzyme_metrics.assert_not_called()
        runner._capture_storage_metrics.assert_not_called()
        runner._save_openivm_ops_chart.assert_not_called()
        self.assertEqual(batch.status, "failed")
        self.assertEqual(batch.error, "JVM_OUT_OF_MEMORY")
        self.assertEqual(batch.duration_s, 3)
        self.assertEqual(runner._persist_batch_result.call_count, 2)


if __name__ == "__main__":
    unittest.main()
