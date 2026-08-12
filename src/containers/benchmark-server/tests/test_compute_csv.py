import csv
import io
import sys
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.oat_runner import generate_results_csv  # noqa: E402


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
        self.assertEqual(rows[0]["compute_source"], "docker_stats_api")
        self.assertEqual(rows[0]["compute_semantics"], "test semantics")
        self.assertEqual(rows[0]["compute_artifact"], "container_stats.jsonl")


if __name__ == "__main__":
    unittest.main()
