import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.config import BenchmarkConfig
from models.experiments import ExperimentInputs, parse_experiments_json
from models.result import BatchResult, EngineResult
from services.engine_runner import EngineRunner
from services.oat_runner import generate_results_csv
from services.source_row_counts import collect_source_row_counts
from services.orchestrator import Orchestrator
from services.resource_calc import compute_engine_configs


class RepeatedRefreshTest(unittest.TestCase):
    def test_real_engine_configuration_and_runner_initialization(self):
        for repeated, workload, count in (
            (False, "standard", 3), (True, "standard", 21),
            (True, "databricks", 21),
        ):
            with self.subTest(repeated=repeated, workload=workload):
                config = BenchmarkConfig(
                    repeated_refresh=repeated, workload=workload,
                    engines=["duckdb-openivm"], host_cores=8, host_memory_gb=32,
                )
                engine = compute_engine_configs(config)["duckdb-openivm"]
                runner = EngineRunner(config, engine, Mock())
                self.assertEqual(len(runner.result.batches), count)
                self.assertEqual(config.batch_2_days, 1 if workload == "databricks" else 0)
        with self.assertRaises(ValueError):
            BenchmarkConfig(repeated_refresh=True, refresh_pct="NaN")

    def test_datagen_failure_keeps_generator_logs_and_stops_containers(self):
        from contextlib import nullcontext
        with tempfile.TemporaryDirectory() as root:
            orchestrator = Orchestrator.__new__(Orchestrator)
            orchestrator._config = BenchmarkConfig(repo_dir=root, repeated_refresh=True)
            orchestrator.emit = Mock()
            orchestrator._heartbeat = Mock(side_effect=lambda _: nullcontext())
            manager = Mock()
            manager.logs.return_value = "generator thread failed writing HoldingHistory"
            def fail(**kwargs):
                self.assertEqual(kwargs["services"], ["tpc-di-gen", "spark-digen-delta"])
                kwargs["stream_callback"]("generator started")
                raise RuntimeError("compose timed out")
            manager.up.side_effect = fail
            with patch("services.orchestrator.DockerManager", return_value=manager):
                with self.assertRaisesRegex(RuntimeError, "generator thread failed"):
                    orchestrator._run_datagen()
            manager.down.assert_called_once()
            log = Path(root, "mount/logs/3/datagen/horizon-2.log").read_text()
            self.assertIn("generator started", log)
            self.assertIn("HoldingHistory", log)

    def test_legacy_defaults_and_both_repeated_workloads(self):
        self.assertEqual(BenchmarkConfig().batch_count, 3)
        self.assertFalse(ExperimentInputs().repeated_refresh)
        for workload, days in (("standard", 0), ("databricks", 1)):
            inputs = parse_experiments_json(json.dumps({"experiments": [{
                "repeated_refresh": True, "workload": workload,
            }]}))[0]
            self.assertEqual((inputs.refresh_count, inputs.refresh_pct), (20, "1"))
            self.assertEqual(inputs.to_compose_env()["TPCDI_BATCH_2_DAYS"], str(days))
            self.assertEqual(inputs.to_compose_env()["REPEATED_REFRESH"], "1")
            self.assertEqual(ExperimentInputs.from_dict(inputs.to_dict()), inputs)
            self.assertEqual(BenchmarkConfig(repeated_refresh=True).batch_count, 21)

    def test_invalid_knobs_fail_before_running_engines(self):
        for overrides in (
            {"workload": "unknown"}, {"refresh_count": 0}, {"refresh_count": 2.5},
            {"refresh_count": True}, {"refresh_pct": "NaN"},
            {"refresh_pct": "Infinity"}, {"refresh_pct": "0"},
            {"refresh_pct": "-1"}, {"batch_2_delete_pct": "1"},
            {"feature_flags": {"compiler_bench": True}},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(ValueError):
                ExperimentInputs.from_dict({"repeated_refresh": True, **overrides})

    def test_runner_initializes_once_and_keeps_all_refresh_results(self):
        runner = EngineRunner.__new__(EngineRunner)
        runner._config = BenchmarkConfig(repeated_refresh=True, refresh_count=4)
        runner._engine = Mock(name="spark")
        runner._engine.name = "spark"
        runner._parallel = False
        runner._result = EngineResult(engine="spark", batches=[BatchResult(i) for i in range(1, 6)])
        runner._emit = Mock()
        runner._override_file = runner._batch_override_file = None
        runner._compiler_bench_enabled = Mock(return_value=False)
        for method in ("_batch_loader_init", "_up_with_retry", "_wait_for_dbt_health",
                       "_start_stats", "_capture_delta_stats", "_fetch_sql_analysis",
                       "_fetch_lineage", "_stop_stats", "_collect_feldera_debug", "_capture_logs",
                       "_databricks_enzyme_drop_mvs", "_fabric_cleanup", "_cleanup_staging"):
            setattr(runner, method, Mock())
        runner._engine_mgr = Mock()
        runner._run_batch = Mock()
        runner.run()
        runner._batch_loader_init.assert_called_once()
        self.assertEqual([call.args[0] for call in runner._run_batch.call_args_list], [1, 2, 3, 4, 5])
        self.assertEqual(len(runner.result.batches), 5)

    def test_artifacts_keep_rounds_above_three(self):
        with tempfile.TemporaryDirectory() as root:
            log = Path(root, "batch5/trade/_delta_log")
            log.mkdir(parents=True)
            (log / "00000000000000000000.json").write_text(json.dumps({
                "add": {"path": "part.parquet", "stats": json.dumps({"numRecords": 7})},
            }) + "\n")
            self.assertEqual(collect_source_row_counts(root, 5)["batches"]["5"]["total_rows"], 7)
        csv_text = generate_results_csv({"experiments": [{
            "inputs": {"engines": ["duckdb"], "repeated_refresh": True, "refresh_count": 4},
            "source_row_counts": {"refresh_plan": {"rounds": [{
                "batch_num": 5, "before_rows": 100, "resulting_rows": 107,
            }]}},
        }]})
        rows = list(csv.DictReader(io.StringIO(csv_text)))
        self.assertEqual(len(rows), 5)
        self.assertEqual(rows[-1]["source_rows_before"], "100")
        self.assertEqual(rows[-1]["source_rows_after"], "107")

    def test_generator_sizing_remeasures_horizon_and_preserves_initial_counts(self):
        with tempfile.TemporaryDirectory() as root:
            delta = Path(root, "mount/raw/3/delta")
            delta.mkdir(parents=True)
            orchestrator = Orchestrator.__new__(Orchestrator)
            orchestrator._config = BenchmarkConfig(repo_dir=root, repeated_refresh=True)
            orchestrator.emit = Mock()
            from contextlib import nullcontext
            orchestrator._heartbeat = Mock(side_effect=lambda _: nullcontext())
            manager = Mock()
            manager.get_exit_code.return_value = "0"
            def generate(**kwargs):
                plan = ({"status": "needs_more_data", "next_horizon": 80,
                         "available_insert_rows": 500, "required_insert_rows": 20000, "initial_tables": {"trade": 100}}
                        if manager.up.call_count == 1 else {"status": "ready", "initial_tables": {"trade": 100}})
                (delta / "refresh-plan.json").write_text(json.dumps(plan))
            manager.up.side_effect = generate
            with patch("services.orchestrator.DockerManager", return_value=manager), patch("os.system"):
                orchestrator._run_datagen()
            self.assertEqual([call.args[0] for call in manager.update_env.call_args_list], [
                {"DIGEN_INCREMENTAL_BATCHES": "2"}, {"DIGEN_INCREMENTAL_BATCHES": "80"},
            ])
            self.assertEqual(manager.up.call_count, 2)
            manager.reset_mock()
            def changed_initial(**kwargs):
                plan = ({"status": "needs_more_data", "next_horizon": 80,
                         "available_insert_rows": 500, "required_insert_rows": 20000,
                         "initial_tables": {"trade": 100}}
                        if manager.up.call_count == 1 else {
                            "status": "ready", "initial_tables": {"trade": 101},
                        })
                (delta / "refresh-plan.json").write_text(json.dumps(plan))
            manager.up.side_effect = changed_initial
            with patch("services.orchestrator.DockerManager", return_value=manager), patch("os.system"):
                with self.assertRaisesRegex(RuntimeError, "changed the requested initial"):
                    orchestrator._run_datagen()



if __name__ == "__main__":
    unittest.main()
