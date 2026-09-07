import sys
import unittest
from pathlib import Path
from threading import Barrier, BrokenBarrierError
from types import SimpleNamespace
from unittest.mock import Mock, patch


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.config import BenchmarkConfig  # noqa: E402
from models.result import BenchmarkResult, EngineResult  # noqa: E402
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


if __name__ == "__main__":
    unittest.main()
