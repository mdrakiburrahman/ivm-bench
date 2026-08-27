"""Behavior tests for RisingWave benchmark orchestration."""

import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.engine_runner import EngineRunner  # noqa: E402


class RisingWaveEngineRunnerTest(unittest.TestCase):
    def test_initial_load_metrics_are_emitted_for_artifacts(self):
        response = Mock()
        response.json.return_value = {
            "tables_created": 2,
            "load_metrics": [
                {
                    "table": "staging_trade",
                    "rows": 42,
                    "mode": "minio-parquet",
                    "duration_s": 1.25,
                }
            ],
        }
        runner = EngineRunner.__new__(EngineRunner)
        runner._dbt_url = "http://dbt-server:5000"
        runner._emit = Mock()
        runner._run_dbt = Mock(return_value="done")

        with patch("services.engine_runner.requests.post", return_value=response):
            result = runner._run_risingwave(1)

        self.assertEqual(result, "done")
        runner._emit.assert_any_call(
            "[risingwave] Source load staging_trade: "
            "42 rows via minio-parquet in 1.25s"
        )
        response.raise_for_status.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
