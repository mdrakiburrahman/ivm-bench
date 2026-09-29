import sys
import unittest
from pathlib import Path
from unittest.mock import patch


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services.container_stats import _get_container_stats_snapshot  # noqa: E402


class ContainerCpuMetricsTest(unittest.TestCase):
    @patch("services.container_stats._docker_get")
    def test_raw_samples_retain_cumulative_cpu_counter(self, docker_get):
        docker_get.return_value = {
            "cpu_stats": {
                "cpu_usage": {"total_usage": 7_500_000_000},
                "system_cpu_usage": 200,
                "online_cpus": 2,
                "throttling_data": {"throttled_time": 500_000, "throttled_periods": 3},
            },
            "precpu_stats": {
                "cpu_usage": {"total_usage": 100},
                "system_cpu_usage": 100,
            },
            "blkio_stats": {"io_service_bytes_recursive": [
                {"major": 8, "minor": 0, "op": "Read", "value": 400},
                {"major": 8, "minor": 0, "op": "Write", "value": 200},
                {"major": 8, "minor": 0, "op": "Total", "value": 600},
                {"major": 8, "minor": 1, "op": "Read", "value": 100},
            ]},
            "memory_stats": {},
            "networks": {},
        }

        result = _get_container_stats_snapshot("container-id")

        self.assertEqual(result["cpu_usage_ns"], 7_500_000_000)
        self.assertEqual(result["cpu_throttled_time_ns"], 500_000)
        self.assertEqual(result["cpu_throttled_periods"], 3)
        self.assertEqual(result["io_read_bytes"], 500)
        self.assertEqual(result["io_write_bytes"], 200)

    @patch("services.container_stats._docker_get")
    def test_unavailable_wait_counters_are_not_reported_as_zero(self, docker_get):
        docker_get.return_value = {"cpu_stats": {"cpu_usage": {"total_usage": 1}}}
        result = _get_container_stats_snapshot("container-id")
        for key in ("cpu_throttled_time_ns", "cpu_throttled_periods", "io_read_bytes", "io_write_bytes"):
            self.assertIsNone(result[key])


if __name__ == "__main__":
    unittest.main()
