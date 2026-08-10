import csv
import io
import sys
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.oat_runner import generate_results_csv  # noqa: E402


class ComputeMetricsCsvTest(unittest.TestCase):
    def test_compute_fields_are_flattened(self):
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
                                        "compute_metrics": {
                                            "status": "ok",
                                            "task_time_s": 12.5,
                                            "cpu_time_s": 4.25,
                                            "billing_status": "ok",
                                            "billing_quantity": 0.75,
                                            "billing_unit": "DBU",
                                            "source": "fabric_spark_monitoring_api",
                                            "semantics": "test semantics",
                                            "artifact": "compute.json",
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
        self.assertEqual(rows[0]["compute_task_time_s"], "12.5")
        self.assertEqual(rows[0]["compute_cpu_time_s"], "4.25")
        self.assertEqual(rows[0]["compute_billing_status"], "ok")
        self.assertEqual(rows[0]["compute_billing_quantity"], "0.75")
        self.assertEqual(rows[0]["compute_billing_unit"], "DBU")
        self.assertEqual(rows[0]["compute_semantics"], "test semantics")
        self.assertEqual(rows[0]["compute_artifact"], "compute.json")


if __name__ == "__main__":
    unittest.main()
