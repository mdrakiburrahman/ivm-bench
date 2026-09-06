import json
import sys
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.experiments import parse_experiments_json  # noqa: E402
from services.augmented_tpcdi import (  # noqa: E402
    digen_horizons_by_scale_factor,
    required_incremental_batches,
)


class AugmentedTpcdiTest(unittest.TestCase):
    def test_standard_workload_keeps_two_incremental_batches(self):
        self.assertEqual(required_incremental_batches(0), 2)

    def test_accumulated_batch_reserves_following_batch_three_day(self):
        self.assertEqual(required_incremental_batches(1), 2)
        self.assertEqual(required_incremental_batches(90), 91)
        self.assertEqual(required_incremental_batches(365), 366)

    def test_largest_horizon_is_shared_per_scale_factor(self):
        experiments = parse_experiments_json(json.dumps({
            "baseline": {"batch_1_pct": "100", "batch_2_pct": "100"},
            "experiments": [
                {"scale_factor": 100, "batch_2_days": 1},
                {"scale_factor": 100, "batch_2_days": 90},
                {"scale_factor": 10, "batch_2_days": 7},
            ],
        }))

        self.assertEqual(
            digen_horizons_by_scale_factor(experiments),
            {100: 91, 10: 8},
        )
        self.assertEqual(experiments[1].to_compose_env()["TPCDI_BATCH_2_DAYS"], "90")
        self.assertEqual(experiments[1].to_dict()["batch_2_days"], 90)

    def test_negative_daily_batch_count_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "non-negative"):
            parse_experiments_json(json.dumps({
                "experiments": [{"batch_2_days": -1}],
            }))


if __name__ == "__main__":
    unittest.main()
