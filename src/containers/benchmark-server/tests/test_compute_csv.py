import csv
import io
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.result import BenchmarkResult, EngineResult  # noqa: E402
from services.oat_runner import archive_compute_artifacts, generate_results_csv  # noqa: E402
from services.docker_manager import compute_cpu_usage_delta  # noqa: E402
from services.engine_runner import EngineRunner  # noqa: E402
from services.orchestrator import _average_compute_metrics  # noqa: E402


class ComputeMetricsCsvTest(unittest.TestCase):
    def test_local_cpu_fields_are_flattened(self):
        state = {
            "oat_run_id": "test",
            "experiments": [
                {
                    "inputs": {"engines": ["spark"]},
                    "engines": {
                        "spark": {
                            "batches": [
                                {
                                    "batch_num": 1,
                                    "status": "completed",
                                    "extra": {
                                        "compute_metrics": {
                                            "status": "ok",
                                            "cpu_time_s": 4.25,
                                            "cpu_time_stddev_s": 0.5,
                                            "repetition_count": 3,
                                            "source": "docker_stats_api",
                                            "semantics": "test semantics",
                                            "artifact": "container_stats.jsonl",
                                        }
                                    },
                                }
                            ]
                        }
                    },
                }
            ],
        }

        rows = list(csv.DictReader(io.StringIO(generate_results_csv(state))))

        self.assertEqual(rows[0]["compute_status"], "ok")
        self.assertEqual(rows[0]["compute_cpu_time_s"], "4.25")
        self.assertEqual(rows[0]["compute_cpu_stddev_s"], "0.5")
        self.assertEqual(rows[0]["compute_repetition_count"], "3")
        self.assertEqual(rows[0]["compute_source"], "docker_stats_api")
        self.assertEqual(rows[0]["compute_semantics"], "test semantics")
        self.assertEqual(rows[0]["compute_artifact"], "container_stats.jsonl")

    def test_cumulative_cpu_counters_are_subtracted_exactly(self):
        start = {
            "containers": {
                "spark": {
                    "service": "spark",
                    "cpu_usage_ns": 1_000_000_000,
                },
                "metastore": {
                    "service": "mssql-metastore",
                    "cpu_usage_ns": 5_000_000_000,
                },
            }
        }
        end = {
            "containers": {
                "spark": {
                    "service": "spark",
                    "cpu_usage_ns": 4_000_000_000,
                },
                "metastore": {
                    "service": "mssql-metastore",
                    "cpu_usage_ns": 5_500_000_000,
                },
            }
        }

        result = compute_cpu_usage_delta(start, end)

        self.assertEqual(result["cpu_time_s"], 3.5)
        self.assertEqual(result["service_cpu_time_s"]["spark"], 3.0)
        self.assertEqual(
            result["service_cpu_time_s"]["mssql-metastore"], 0.5
        )

    def test_container_restart_rejects_cpu_delta(self):
        start = {
            "containers": {
                "old": {"service": "spark", "cpu_usage_ns": 1},
            }
        }
        end = {
            "containers": {
                "new": {"service": "spark", "cpu_usage_ns": 2},
            }
        }

        with self.assertRaisesRegex(ValueError, "container set changed"):
            compute_cpu_usage_delta(start, end)

    def test_repetition_cpu_is_averaged_with_dispersion(self):
        averaged = _average_compute_metrics([
            {
                "compute_metrics": {
                    "status": "ok",
                    "cpu_time_s": 2.0,
                    "artifact": "first.jsonl",
                    "samples_status": "ok",
                }
            },
            {
                "compute_metrics": {
                    "status": "ok",
                    "cpu_time_s": 4.0,
                    "artifact": "second.jsonl",
                    "samples_status": "ok",
                }
            },
        ])

        self.assertEqual(averaged["cpu_time_s"], 3.0)
        self.assertEqual(averaged["cpu_time_stddev_s"], 1.0)
        self.assertEqual(averaged["repetition_count"], 2)
        self.assertEqual(averaged["repetition_cpu_time_s"], [2.0, 4.0])
        self.assertEqual(averaged["samples_status"], "ok")

    def test_missing_repetition_metric_fails_closed(self):
        averaged = _average_compute_metrics([
            {"compute_metrics": {"status": "ok", "cpu_time_s": 2.0}},
            {},
        ])

        self.assertEqual(averaged["status"], "error")
        self.assertIsNone(averaged["cpu_time_s"])

    def test_compute_artifact_is_archived_per_experiment(self):
        with tempfile.TemporaryDirectory() as repo:
            source = os.path.join(
                repo, "mount", "stats", "10", "spark", "container_stats.jsonl"
            )
            os.makedirs(os.path.dirname(source), exist_ok=True)
            with open(source, "w") as handle:
                handle.write("{}\n")

            engine = EngineResult(engine="spark")
            engine.batches[0].extra["compute_metrics"] = {
                "status": "ok",
                "cpu_time_s": 1.0,
                "artifact": os.path.relpath(source, repo),
            }
            result = BenchmarkResult(engines={"spark": engine})

            archive_compute_artifacts(
                repo_dir=repo,
                oat_run_id="run",
                exp_idx=2,
                result=result,
            )

            archived = engine.batches[0].extra["compute_metrics"]["artifact"]
            self.assertIn("mount/oat-state/run/exp-002/compute", archived)
            self.assertTrue(os.path.isfile(os.path.join(repo, archived)))

    @patch("services.oat_runner.shutil.copy2", side_effect=OSError("disk full"))
    def test_compute_artifact_archive_failure_is_explicit(self, _copy):
        with tempfile.TemporaryDirectory() as repo:
            source = os.path.join(
                repo, "mount", "stats", "10", "spark", "container_stats.jsonl"
            )
            os.makedirs(os.path.dirname(source), exist_ok=True)
            with open(source, "w") as handle:
                handle.write("{}\n")
            engine = EngineResult(engine="spark")
            metric = {
                "status": "ok",
                "cpu_time_s": 1.0,
                "artifact": os.path.relpath(source, repo),
            }
            engine.batches[0].extra["compute_metrics"] = metric

            archive_compute_artifacts(
                repo_dir=repo,
                oat_run_id="run",
                exp_idx=1,
                result=BenchmarkResult(engines={"spark": engine}),
            )

            self.assertEqual(metric["status"], "ok")
            self.assertEqual(metric["samples_status"], "error")
            self.assertNotIn("artifact", metric)

    def test_repetition_artifacts_are_archived_without_duplicate_files(self):
        with tempfile.TemporaryDirectory() as repo:
            repetitions = []
            source_paths = []
            for repetition in (1, 2):
                source = os.path.join(
                    repo,
                    "mount",
                    "results",
                    "10",
                    "dbt-server",
                    f"repetition-{repetition}",
                    "container-stats-spark.jsonl",
                )
                os.makedirs(os.path.dirname(source), exist_ok=True)
                with open(source, "w") as handle:
                    handle.write(f'{{"repetition": {repetition}}}\n')
                relative = os.path.relpath(source, repo)
                source_paths.append(relative)
                engine = EngineResult(engine="spark")
                for batch in engine.batches:
                    batch.extra["compute_metrics"] = {
                        "status": "ok",
                        "cpu_time_s": float(repetition),
                        "artifact": relative,
                    }
                repetitions.append({"spark": engine})

            averaged = EngineResult(engine="spark")
            for batch in averaged.batches:
                batch.extra["compute_metrics"] = {
                    "status": "ok",
                    "cpu_time_s": 1.5,
                    "artifact": source_paths[-1],
                    "repetition_artifacts": list(source_paths),
                }
            result = BenchmarkResult(
                engines={"spark": averaged}, repetitions=repetitions
            )

            archive_compute_artifacts(
                repo_dir=repo,
                oat_run_id="run",
                exp_idx=1,
                result=result,
            )

            archived_paths = set()
            for batch in averaged.batches:
                metric = batch.extra["compute_metrics"]
                archived_paths.add(metric["artifact"])
                self.assertEqual(len(metric["repetition_artifacts"]), 2)
                for artifact in metric["repetition_artifacts"]:
                    self.assertTrue(os.path.isfile(os.path.join(repo, artifact)))
            self.assertEqual(len(archived_paths), 1)

    def test_cloud_compute_is_explicitly_excluded(self):
        runner = EngineRunner.__new__(EngineRunner)
        runner._engine = SimpleNamespace(name="databricks-enzyme")
        runner._result = EngineResult(engine="databricks-enzyme")

        runner._start_stats()

        for batch in runner._result.batches:
            metrics = batch.extra["compute_metrics"]
            self.assertEqual(metrics["status"], "excluded")
            self.assertIsNone(metrics["cpu_time_s"])

    @patch("services.engine_runner.requests.post")
    def test_stats_start_http_error_is_recorded(self, post):
        response = Mock()
        response.raise_for_status.side_effect = RuntimeError("HTTP 500")
        post.return_value = response
        runner = EngineRunner.__new__(EngineRunner)
        runner._engine = SimpleNamespace(name="spark")
        runner._result = EngineResult(engine="spark")
        runner._dbt_url = "http://dbt"
        runner._config = SimpleNamespace(scale_factor=10)
        runner._stats_started = False
        runner._stats_error = None
        runner._emit = lambda _: None

        runner._start_stats()

        self.assertFalse(runner._stats_started)
        self.assertEqual(runner._stats_error, "HTTP 500")

    @patch("services.engine_runner.requests.post")
    def test_zero_diagnostic_samples_do_not_erase_exact_cpu(self, post):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"sample_count": 0}
        post.return_value = response
        runner = EngineRunner.__new__(EngineRunner)
        runner._engine = SimpleNamespace(name="spark")
        runner._result = EngineResult(engine="spark")
        runner._config = SimpleNamespace(scale_factor=10)
        runner._result.batches[0].extra["compute_metrics"] = {
            "status": "ok",
            "cpu_time_s": 2.0,
        }
        runner._dbt_url = "http://dbt"
        runner._stats_started = True
        runner._stats_error = None
        runner._emit = lambda _: None
        runner._persist_batch_result = Mock()

        runner._stop_stats()

        metrics = runner._result.batches[0].extra["compute_metrics"]
        self.assertEqual(metrics["status"], "ok")
        self.assertEqual(metrics["samples_status"], "error")
        self.assertIn("zero samples", metrics["samples_error"])

    @patch("services.databricks_enzyme_compute.summarize_for_persistence")
    @patch("services.databricks_enzyme_compute.compute_batch_summary")
    def test_databricks_flow_diagnostics_do_not_replace_duration(
        self, compute_summary, summarize
    ):
        compute_summary.return_value = {
            "batch": {
                "compute_wall_ms": 5_000,
                "compute_work_ms": 9_000,
                "tables_with_compute": 2,
                "updates_in_window": 2,
                "segments_total": 2,
                "segments_fallback": 0,
            },
            "tables": {},
        }
        summarize.return_value = {"batch": {}, "tables": {}}
        with tempfile.TemporaryDirectory() as repo:
            runner = EngineRunner.__new__(EngineRunner)
            runner._config = SimpleNamespace(repo_dir=repo, scale_factor=10)
            runner._emit = lambda _: None
            runner._export_databricks_enzyme_pipeline_events = Mock()
            batch = SimpleNamespace(
                duration_s=42.0,
                extra={
                    "wall_window_start_ms": 1_000,
                    "wall_window_end_ms": 43_000,
                },
            )

            runner._collect_databricks_enzyme_flow_metrics(2, batch)

            self.assertEqual(batch.duration_s, 42.0)
            self.assertEqual(batch.extra["flow_coverage_s"], 5.0)
            self.assertEqual(batch.extra["flow_work_s"], 9.0)
            sidecar = os.path.join(
                repo,
                "mount",
                "results",
                "10",
                "dbt-server",
                "databricks-flow-metrics-batch2.json",
            )
            self.assertTrue(os.path.isfile(sidecar))


if __name__ == "__main__":
    unittest.main()
