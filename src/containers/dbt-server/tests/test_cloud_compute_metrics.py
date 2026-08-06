import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services.cloud_compute_metrics import (  # noqa: E402
    _overlaps_window,
    summarize_databricks_rows,
    summarize_fabric_tasks,
)


class CloudComputeMetricsTest(unittest.TestCase):
    def test_databricks_sums_task_time_without_claiming_cpu_time(self):
        result = summarize_databricks_rows(
            [
                {"statement_id": "a", "total_task_duration_ms": 1250},
                {"statement_id": "b", "total_task_duration_ms": 2750},
            ]
        )

        self.assertEqual(result["task_time_s"], 4.0)
        self.assertIsNone(result["cpu_time_s"])
        self.assertEqual(result["query_count"], 2)

    def test_empty_databricks_history_is_not_reported_as_zero_work(self):
        result = summarize_databricks_rows([])

        self.assertEqual(result["status"], "unavailable")
        self.assertIn("no rows", result["error"])

    def test_fabric_sums_executor_runtime_and_cpu_time(self):
        result = summarize_fabric_tasks(
            [
                {
                    "taskMetrics": {
                        "executorRunTime": 2500,
                        "executorCpuTime": 1_500_000_000,
                    }
                },
                {
                    "taskMetrics": {
                        "executorRunTime": 500,
                        "executorCpuTime": 250_000_000,
                    }
                },
            ],
            stage_count=2,
        )

        self.assertEqual(result["task_time_s"], 3.0)
        self.assertEqual(result["cpu_time_s"], 1.75)
        self.assertEqual(result["task_count"], 2)

    def test_empty_fabric_tasks_are_not_reported_as_zero_work(self):
        result = summarize_fabric_tasks([], stage_count=0)

        self.assertEqual(result["status"], "unavailable")
        self.assertIn("no completed tasks", result["error"])

    def test_stage_must_overlap_batch_window(self):
        start_ms = int(
            datetime(2026, 8, 6, 10, 1, tzinfo=timezone.utc).timestamp() * 1000
        )
        end_ms = int(
            datetime(2026, 8, 6, 10, 3, tzinfo=timezone.utc).timestamp() * 1000
        )
        self.assertTrue(
            _overlaps_window(
                {
                    "submissionTime": "2026-08-06T10:00:00Z",
                    "completionTime": "2026-08-06T10:02:00Z",
                },
                start_ms,
                end_ms,
            )
        )
        self.assertFalse(
            _overlaps_window(
                {
                    "submissionTime": "2026-08-06T09:00:00Z",
                    "completionTime": "2026-08-06T09:02:00Z",
                },
                start_ms,
                end_ms,
            )
        )

    def test_spark_gmt_timestamp_is_supported(self):
        start_ms = int(
            datetime(2026, 8, 6, 10, 1, tzinfo=timezone.utc).timestamp() * 1000
        )
        end_ms = int(
            datetime(2026, 8, 6, 10, 3, tzinfo=timezone.utc).timestamp() * 1000
        )

        self.assertTrue(
            _overlaps_window(
                {
                    "submissionTime": "2026-08-06T10:00:00.000GMT",
                    "completionTime": "2026-08-06T10:02:00.000GMT",
                },
                start_ms,
                end_ms,
            )
        )


if __name__ == "__main__":
    unittest.main()
