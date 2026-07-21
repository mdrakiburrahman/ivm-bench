import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock


BENCHMARK_SERVER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER_ROOT))

from models.experiments import ExperimentInputs, parse_experiments_json
from models.result import BenchmarkResult, EngineResult
from services import oat_runner
from services.storage_sync import storage_snapshot_barrier


class StorageResultTests(unittest.TestCase):
    def test_environment_disables_storage_when_json_does_not_override_it(self):
        payload = json.dumps({"experiments": [{"engines": ["spark"]}]})
        with mock.patch.dict(os.environ, {"STORAGE_METRICS": "0"}, clear=False):
            experiment = parse_experiments_json(payload)[0]
        self.assertFalse(experiment.feature_flags.storage_metrics)

        payload = json.dumps({
            "experiments": [{
                "engines": ["spark"],
                "feature_flags": {"storage_metrics": True},
            }]
        })
        with mock.patch.dict(os.environ, {"STORAGE_METRICS": "0"}, clear=False):
            experiment = parse_experiments_json(payload)[0]
        self.assertTrue(experiment.feature_flags.storage_metrics)

    def test_storage_artifacts_are_archived_per_experiment_and_repetition(self):
        with tempfile.TemporaryDirectory() as tmp:
            repo = Path(tmp)
            results = repo / "mount/results/10/dbt-server"
            results.mkdir(parents=True)
            (results / "storage-spark-batch1.json").write_text('{"run": "latest"}')
            repetition = results / "repetition-1"
            repetition.mkdir()
            (repetition / "storage-spark-batch1.json").write_text('{"run": 1}')

            current = EngineResult(engine="spark")
            current.batches[0].extra["storage"] = {
                "artifact": "mount/results/10/dbt-server/storage-spark-batch1.json"
            }
            repeated = EngineResult(engine="spark")
            repeated.batches[0].extra["storage"] = {
                "artifact": "mount/results/10/dbt-server/storage-spark-batch1.json"
            }
            result = BenchmarkResult(
                engines={"spark": current},
                repetitions=[{"spark": repeated}],
            )

            oat_runner.archive_storage_artifacts(
                repo_dir=str(repo), oat_run_id="run", exp_idx=0, result=result
            )

            latest_path = repo / result.engines["spark"].batches[0].extra["storage"]["artifact"]
            repetition_path = repo / result.repetitions[0]["spark"].batches[0].extra["storage"]["artifact"]
            self.assertEqual(latest_path.read_text(), '{"run": "latest"}')
            self.assertEqual(repetition_path.read_text(), '{"run": 1}')
            self.assertIn("exp-000/storage", str(latest_path))
            self.assertIn("exp-000/storage/repetition-1", str(repetition_path))

    def test_experiment_output_has_one_storage_representation(self):
        inputs = ExperimentInputs(scale_factor=10, engines=["spark"])
        engine = EngineResult(engine="spark")
        engine.batches[0].extra["storage"] = {
            "status": "ok",
            "artifact": "mount/oat-state/run/exp-000/storage/storage-spark-batch1.json",
        }
        result = BenchmarkResult(engines={"spark": engine})
        output = oat_runner.build_per_experiment_dict(
            exp_idx=0,
            inputs=inputs,
            result=result,
            status="completed",
            started_at="",
            ended_at="",
            wall_clock_s=0,
            disk_free_pct=100,
            error=None,
            skip_reason=None,
            repo_dir="/repo",
            benchmark_id="benchmark",
        )

        self.assertNotIn("storage", output)
        self.assertNotIn("storage_files", output)
        self.assertEqual(
            output["engines"]["spark"]["batches"][0]["extra"]["storage"]["status"],
            "ok",
        )

    def test_parallel_storage_barrier_brackets_collection(self):
        barrier = threading.Barrier(2)
        slow_started = threading.Event()
        release_slow = threading.Event()
        errors = []

        def capture(slow):
            try:
                with storage_snapshot_barrier(barrier):
                    if slow:
                        slow_started.set()
                        self.assertTrue(release_slow.wait(timeout=5))
            except Exception as exc:  # pragma: no cover - assertion reports details
                errors.append(exc)

        fast_thread = threading.Thread(target=capture, args=(False,))
        slow_thread = threading.Thread(target=capture, args=(True,))
        fast_thread.start()
        time.sleep(0.01)
        slow_thread.start()
        self.assertTrue(slow_started.wait(timeout=5))
        time.sleep(0.01)
        self.assertTrue(fast_thread.is_alive())
        release_slow.set()
        fast_thread.join(timeout=5)
        slow_thread.join(timeout=5)

        self.assertEqual(errors, [])
        self.assertFalse(fast_thread.is_alive())
        self.assertFalse(slow_thread.is_alive())


if __name__ == "__main__":
    unittest.main()
