import sys
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.storage_sync import (  # noqa: E402
    StorageMetricsError,
    require_complete_storage_metrics,
)


class StorageMetricsStatusTest(unittest.TestCase):
    def test_ok_snapshot_is_accepted(self):
        require_complete_storage_metrics("ok", "databricks-enzyme", 1)

    def test_partial_snapshot_is_rejected(self):
        with self.assertRaisesRegex(StorageMetricsError, "status=partial"):
            require_complete_storage_metrics("partial", "databricks-enzyme", 2)

    def test_error_snapshot_is_rejected(self):
        with self.assertRaisesRegex(StorageMetricsError, "status=error"):
            require_complete_storage_metrics("error", "databricks-enzyme", 3)


if __name__ == "__main__":
    unittest.main()
