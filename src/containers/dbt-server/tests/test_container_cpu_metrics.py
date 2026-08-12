import sys
import unittest
from pathlib import Path


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services.container_stats import summarize_cpu_seconds  # noqa: E402


class ContainerCpuMetricsTest(unittest.TestCase):
    def test_cpu_percentage_is_integrated_for_selected_service(self):
        result = summarize_cpu_seconds(
            [
                {"timestamp_s": 0, "container": "engine", "cpu_pct": 100},
                {"timestamp_s": 10, "container": "engine", "cpu_pct": 200},
                {"timestamp_s": 0, "container": "dbt-server", "cpu_pct": 900},
                {"timestamp_s": 10, "container": "dbt-server", "cpu_pct": 900},
            ],
            start_ms=0,
            end_ms=10_000,
            included_services=["engine"],
        )

        self.assertEqual(result["cpu_time_s"], 15.0)
        self.assertEqual(result["included_services"], ["engine"])
        self.assertNotIn("dbt-server", result["service_cpu_time_s"])

    def test_samples_outside_batch_window_are_unavailable(self):
        result = summarize_cpu_seconds(
            [{"timestamp_s": 20, "container": "engine", "cpu_pct": 100}],
            start_ms=0,
            end_ms=10_000,
            included_services=["engine"],
        )

        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["cpu_time_s"])


if __name__ == "__main__":
    unittest.main()
