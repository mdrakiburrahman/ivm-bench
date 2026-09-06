import json
import sys
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.experiments import parse_experiments_json  # noqa: E402
class AugmentedTpcdiTest(unittest.TestCase):
    def test_days_are_forwarded_to_datagen(self):
        experiments = parse_experiments_json(json.dumps({
            "experiments": [{"scale_factor": 3, "batch_2_days": 37}],
        }))
        self.assertEqual(experiments[0].to_compose_env()["TPCDI_BATCH_2_DAYS"], "37")
        self.assertEqual(experiments[0].to_dict()["batch_2_days"], 37)

    def test_negative_daily_batch_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "between 0 and 365"):
            parse_experiments_json(json.dumps({
                "experiments": [{"batch_2_days": -1}],
            }))

    def test_daily_window_above_databricks_horizon_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "between 0 and 365"):
            parse_experiments_json(json.dumps({
                "experiments": [{"batch_2_days": 366}],
            }))

    def test_sf3_sweep_uses_nearest_whole_days(self):
        config = BENCHMARK_SERVER / "experiments" / "sf3-augmented-percent-sweep.json"
        experiments = parse_experiments_json(config.read_text())
        self.assertEqual(
            [experiment.batch_2_days for experiment in experiments],
            [37, 73, 110, 146, 183],
        )

    def test_sf10_gci_sweep_uses_5_and_50_percent_windows(self):
        config = BENCHMARK_SERVER / "experiments" / "sf10-augmented-5-50.json"
        experiments = parse_experiments_json(config.read_text())
        self.assertEqual(
            [(experiment.scale_factor, experiment.batch_2_days) for experiment in experiments],
            [(10, 18), (10, 183)],
        )


if __name__ == "__main__":
    unittest.main()
