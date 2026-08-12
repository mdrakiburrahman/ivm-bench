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
            },
            "precpu_stats": {
                "cpu_usage": {"total_usage": 100},
                "system_cpu_usage": 100,
            },
            "memory_stats": {},
            "networks": {},
        }

        result = _get_container_stats_snapshot("container-id")

        self.assertEqual(result["cpu_usage_ns"], 7_500_000_000)


if __name__ == "__main__":
    unittest.main()
