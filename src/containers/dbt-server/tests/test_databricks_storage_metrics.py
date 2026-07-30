import importlib
import sys
import time
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


DBT_SERVER = Path(__file__).resolve().parents[1]
ORIGINAL_PATH = list(sys.path)
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
        cls.original_modules = {
            name: sys.modules.get(name)
            for name in (
                "services.databricks_enzyme_sources",
                "services.databricks_enzyme_metrics",
                "services.storage_metrics",
            )
        }
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

    @classmethod
    def tearDownClass(cls):
        sys.path[:] = ORIGINAL_PATH
        for name, module in cls.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

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
        self.assertEqual(values["retained_data_bytes"], 30)
        self.assertEqual(values["transaction_log_bytes"], 10)

    def test_storage_values_require_complete_metrics(self):
        frame = FakeFrame(
            [{"metric_name": "total_bytes", "metric_value": "120"}]
        )

        with self.assertRaisesRegex(
            RuntimeError, "omitted required metrics"
        ):
            self.storage._databricks_storage_values(frame)

    def test_storage_values_reject_malformed_metrics(self):
        frame = FakeFrame(
            [
                {"metric_name": "total_bytes", "metric_value": "not-a-number"},
                {"metric_name": "num_total_files", "metric_value": "12"},
                {"metric_name": "active_bytes", "metric_value": "80"},
                {"metric_name": "num_active_files", "metric_value": "8"},
                {"metric_name": "vacuumable_bytes", "metric_value": "20"},
                {"metric_name": "num_vacuumable_files", "metric_value": "2"},
                {"metric_name": "time_travel_bytes", "metric_value": "10"},
                {"metric_name": "num_time_travel_files", "metric_value": "1"},
            ]
        )

        with self.assertRaisesRegex(
            RuntimeError, "invalid total_bytes metric"
        ):
            self.storage._databricks_storage_values(frame)

    def test_relation_storage_deduplicates_mv_alias_and_tracks_event_log(self):
        backing_name = (
            "__materialization_mat_01234567_89ab_cdef_0123_456789abcdef"
            "_customer_summary_1"
        )
        relations = [
            {
                "schema": "exp_1_gold",
                "name": "customer_summary",
                "table_type": "MATERIALIZED_VIEW",
            },
            {
                "schema": "exp_1_gold",
                "name": backing_name,
                "table_type": "MANAGED",
            },
            {
                "schema": "exp_1_gold",
                "name": "event_log_01234567_89ab_cdef_0123_456789abcdef",
                "table_type": "MANAGED",
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
        event_log_frame = FakeFrame(
            [
                {"metric_name": "total_bytes", "metric_value": "15"},
                {"metric_name": "num_total_files", "metric_value": "2"},
                {"metric_name": "active_bytes", "metric_value": "10"},
                {"metric_name": "num_active_files", "metric_value": "1"},
                {"metric_name": "vacuumable_bytes", "metric_value": "0"},
                {"metric_name": "num_vacuumable_files", "metric_value": "0"},
                {"metric_name": "time_travel_bytes", "metric_value": "0"},
                {"metric_name": "num_time_travel_files", "metric_value": "0"},
            ]
        )

        def analyze_storage(relation, deadline):
            if relation["name"].startswith("event_log_"):
                return event_log_frame
            return frame

        with (
            patch.object(
                self.metrics, "list_relations", return_value=relations
            ),
            patch.object(
                self.metrics,
                "analyze_storage",
                side_effect=analyze_storage,
            ) as analyze,
        ):
            items, totals, errors = (
                self.storage._databricks_relation_storage(
                    deadline=time.monotonic() + 10
                )
            )

        self.assertEqual(errors, [])
        self.assertEqual(analyze.call_count, 2)
        analyzed_names = {
            call.args[0]["name"] for call in analyze.call_args_list
        }
        self.assertEqual(analyzed_names, {backing_name, relations[2]["name"]})
        self.assertEqual(totals["visible_output_bytes"], 110)
        self.assertEqual(totals["helper_data_bytes"], 10)
        self.assertEqual(totals["metadata_bytes"], 15)
        self.assertEqual(totals["total_bytes"], 135)
        kinds = {item["kind"] for item in items}
        self.assertEqual(
            kinds,
            {
                "databricks_active_snapshot",
                "databricks_retained_data",
                "databricks_transaction_log",
            },
        )


if __name__ == "__main__":
    unittest.main()
