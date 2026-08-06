import csv
import io
import sys
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.oat_runner import generate_results_csv  # noqa: E402


class CloudComputeCsvTest(unittest.TestCase):
    def test_cloud_compute_fields_are_flattened(self):
        state = {
            "oat_run_id": "test",
            "experiments": [
                {
                    "inputs": {"engines": ["fabric-jvm-35"]},
                    "engines": {
                        "fabric-jvm-35": {
                            "batches": [
                                {
                                    "batch_num": 1,
                                    "status": "completed",
                                    "extra": {
                                        "cloud_compute": {
                                            "status": "ok",
                                            "task_time_s": 12.5,
                                            "cpu_time_s": 4.25,
                                            "source": "fabric_spark_monitoring_api",
                                            "artifact": "cloud.json",
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

        self.assertEqual(rows[0]["cloud_compute_status"], "ok")
        self.assertEqual(rows[0]["cloud_task_time_s"], "12.5")
        self.assertEqual(rows[0]["cloud_cpu_time_s"], "4.25")
        self.assertEqual(rows[0]["cloud_compute_artifact"], "cloud.json")


if __name__ == "__main__":
    unittest.main()
