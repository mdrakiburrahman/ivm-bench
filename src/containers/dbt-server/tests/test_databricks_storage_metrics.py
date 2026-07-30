import importlib
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, patch


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))


class FakeFrame:
    def __init__(self, rows):
        self.rows = rows
        self.empty = not rows

    def iterrows(self):
        return enumerate(self.rows)


class DatabricksStorageMetricsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fake_sources = types.ModuleType(
            "services.databricks_enzyme_sources"
        )
        fake_sources.CATALOG = "benchmark"
        fake_sources.execute_isolated = Mock()
        fake_sources.data_schema = Mock(return_value="exp_1_data")
        fake_sources.work_schema = Mock(return_value="exp_1_work")
        sys.modules[
            "services.databricks_enzyme_sources"
        ] = fake_sources
        cls.fake_sources = fake_sources
        cls.metrics = importlib.import_module(
            "services.databricks_enzyme_metrics"
        )
        cls.storage = importlib.import_module("services.storage_metrics")

    def setUp(self):
        self.fake_sources.execute_isolated.reset_mock()

    def test_analyze_storage_uses_complete_storage_metrics(self):
        expected = FakeFrame([])
        self.fake_sources.execute_isolated.return_value = expected

        result = self.metrics.analyze_storage(
            {
                "schema": "exp_1_gold",
                "name": "customer_summary",
                "table_type": "MATERIALIZED_VIEW",
            }
        )

        self.assertIs(result, expected)
        self.fake_sources.execute_isolated.assert_called_once_with(
            "ANALYZE TABLE `benchmark`.`exp_1_gold`.`customer_summary` "
            "COMPUTE STORAGE METRICS",
            timeout_s=1800.0,
        )

    def test_storage_values_include_active_and_retained_bytes(self):
        frame = FakeFrame(
            [
                {"metric_name": "total_bytes", "metric_value": "120"},
                {"metric_name": "num_total_files", "metric_value": "12"},
                {"metric_name": "active_bytes", "metric_value": "80"},
                {"metric_name": "num_active_files", "metric_value": "8"},
                {"metric_name": "vacuumable_bytes", "metric_value": "20"},
                {"metric_name": "num_vacuumable_files", "metric_value": "2"},
                {"metric_name": "time_travel_bytes", "metric_value": "10"},
                {"metric_name": "num_time_travel_files", "metric_value": "1"},
            ]
        )

        values = self.storage._databricks_storage_values(frame)

        self.assertEqual(values["active_bytes"], 80)
        self.assertEqual(values["retained_bytes"], 40)
        self.assertEqual(values["num_retained_files"], 4)

    def test_storage_values_require_complete_metrics(self):
        frame = FakeFrame(
            [{"metric_name": "total_bytes", "metric_value": "120"}]
        )

        with self.assertRaisesRegex(
            RuntimeError, "omitted required metrics"
        ):
            self.storage._databricks_storage_values(frame)

    def test_relation_storage_includes_materialized_views(self):
        relations = [
            {
                "schema": "exp_1_gold",
                "name": "customer_summary",
                "table_type": "MATERIALIZED_VIEW",
            },
            {
                "schema": "exp_1_gold",
                "name": "logical_view",
                "table_type": "VIEW",
            },
        ]
        frame = FakeFrame(
            [
                {"metric_name": "total_bytes", "metric_value": "120"},
                {"metric_name": "num_total_files", "metric_value": "12"},
                {"metric_name": "active_bytes", "metric_value": "80"},
                {"metric_name": "num_active_files", "metric_value": "8"},
                {"metric_name": "vacuumable_bytes", "metric_value": "20"},
                {"metric_name": "num_vacuumable_files", "metric_value": "2"},
                {"metric_name": "time_travel_bytes", "metric_value": "10"},
                {"metric_name": "num_time_travel_files", "metric_value": "1"},
            ]
        )
        with (
            patch.object(
                self.metrics, "list_relations", return_value=relations
            ),
            patch.object(
                self.metrics, "analyze_storage", return_value=frame
            ) as analyze,
        ):
            items, totals, errors = (
                self.storage._databricks_relation_storage(
                    deadline=time.monotonic() + 10
                )
            )

        self.assertEqual(errors, [])
        analyze.assert_called_once_with(relations[0], deadline=ANY)
        self.assertEqual(totals["visible_output_bytes"], 80)
        self.assertEqual(totals["metadata_bytes"], 40)
        self.assertEqual(totals["total_bytes"], 120)
        self.assertEqual(
            [item["kind"] for item in items],
            [
                "databricks_active_snapshot",
                "databricks_retained_storage",
            ],
        )


if __name__ == "__main__":
    unittest.main()
