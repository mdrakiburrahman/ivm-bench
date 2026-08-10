import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services.cloud_compute_metrics import (  # noqa: E402
    _overlaps_window,
    _valid_update_ids,
    _valid_updates,
    summarize_databricks_billing,
    summarize_fabric_stages,
)
from services.container_stats import summarize_cpu_seconds  # noqa: E402


class CloudComputeMetricsTest(unittest.TestCase):
    def test_databricks_reports_pipeline_work_and_billed_dbus(self):
        result = summarize_databricks_billing(
            [
                {"update_id": "a", "usage_quantity": 1.25},
                {"update_id": "b", "usage_quantity": 2.75},
            ],
            pipeline_work_s=42.0,
            event_rows=[
                {
                    "executor_cpu_time_ms": 1250,
                    "executor_time_ms": 2500,
                    "output_bytes": 1024,
                },
                {
                    "executor_cpu_time_ms": 500,
                    "executor_time_ms": 1000,
                    "output_bytes": 512,
                },
            ],
        )

        self.assertEqual(result["task_time_s"], 42.0)
        self.assertEqual(result["cpu_time_s"], 1.75)
        self.assertEqual(result["executor_time_s"], 3.5)
        self.assertEqual(result["output_bytes"], 1536)
        self.assertEqual(result["billing_quantity"], 4.0)
        self.assertEqual(result["billing_unit"], "DBU")

    def test_delayed_databricks_billing_is_pending_not_zero(self):
        result = summarize_databricks_billing([], pipeline_work_s=42.0)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["billing_status"], "pending")
        self.assertEqual(result["task_time_s"], 42.0)
        self.assertIsNone(result["billing_quantity"])
        self.assertIn("not published", result["error"])

    def test_only_uuid_shaped_update_ids_can_enter_billing_query(self):
        valid = "18929dd8-1f4b-426d-bec7-0075802fea08"
        self.assertEqual(
            _valid_update_ids([valid, "x' OR 1=1 --", "not-a-uuid"]),
            [valid],
        )

    def test_only_safe_mv_updates_can_enter_event_log_query(self):
        valid = "18929dd8-1f4b-426d-bec7-0075802fea08"
        self.assertEqual(
            _valid_updates(
                [
                    {
                        "schema": "exp_1_gold",
                        "table": "fact_trade",
                        "update_id": valid,
                    },
                    {
                        "schema": "exp_1_gold`; DROP SCHEMA system; --",
                        "table": "fact_trade",
                        "update_id": valid,
                    },
                    {
                        "schema": "exp_1_gold",
                        "table": "fact_trade",
                        "update_id": "not-a-uuid",
                    },
                ]
            ),
            [
                {
                    "schema": "exp_1_gold",
                    "table": "fact_trade",
                    "update_id": valid,
                }
            ],
        )

    def test_docker_cpu_percentage_is_integrated_per_selected_service(self):
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

    def test_docker_samples_outside_window_are_unavailable(self):
        result = summarize_cpu_seconds(
            [{"timestamp_s": 20, "container": "engine", "cpu_pct": 100}],
            start_ms=0,
            end_ms=10_000,
            included_services=["engine"],
        )

        self.assertEqual(result["status"], "unavailable")
        self.assertIsNone(result["cpu_time_s"])

    def test_fabric_sums_executor_runtime_and_cpu_time(self):
        result = summarize_fabric_stages(
            [
                {
                    "executorRunTime": 2500,
                    "executorCpuTime": 1_500_000_000,
                    "numCompleteTasks": 4,
                },
                {
                    "executorRunTime": 500,
                    "executorCpuTime": 250_000_000,
                    "numCompleteTasks": 2,
                },
            ]
        )

        self.assertEqual(result["task_time_s"], 3.0)
        self.assertEqual(result["cpu_time_s"], 1.75)
        self.assertEqual(result["task_count"], 6)

    def test_empty_fabric_tasks_are_not_reported_as_zero_work(self):
        result = summarize_fabric_stages([])

        self.assertEqual(result["status"], "unavailable")
        self.assertIn("no completed stages", result["error"])

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
