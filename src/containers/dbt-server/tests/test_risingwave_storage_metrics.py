"""Unit tests for the RisingWave storage collector.

The collector's whole job is the three-way split: an MV result table is
user-visible output, the operator state behind it is IVM overhead, and a loaded
base table is neither. Getting a row into the wrong bucket silently changes the
headline `helper_data / visible_output` ratio, which is the number this engine
is being added to produce — so the categorisation is what these tests pin.
"""

import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services import storage_metrics  # noqa: E402


class FakeCursor:
    """Returns rows keyed by which rw_catalog view the query mentions."""

    def __init__(self, by_view, fail_on=()):
        self.by_view = by_view
        self.fail_on = set(fail_on)
        self._rows = []

    def execute(self, sql, params=None):
        for view, rows in self.by_view.items():
            if f"rw_catalog.{view}" in sql:
                if view in self.fail_on:
                    raise RuntimeError(f"boom reading {view}")
                self._rows = rows
                return
        self._rows = []

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, cursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


def run_collector(by_view, fail_on=(), deadline_offset=60):
    cursor = FakeCursor(by_view, fail_on=fail_on)
    conn = FakeConn(cursor)
    fake_sources = Mock()
    fake_sources._connect = Mock(return_value=conn)
    with patch.dict(sys.modules, {"services.risingwave_sources": fake_sources}):
        # The meta-store directory does not exist under test, so only the
        # catalog-derived items are produced.
        return storage_metrics._collect_risingwave_storage(
            deadline=time.monotonic() + deadline_offset
        )


CATALOG = {
    "rw_materialized_views": [("dim_customer", 300, 10), ("fact_trade", 700, 20)],
    "rw_internal_tables": [("__internal_join_0", 1500, 40)],
    "rw_tables": [("staging_trade", 250, 5)],
}


class RisingWaveStorageTest(unittest.TestCase):
    def test_splits_bytes_across_the_three_categories(self):
        items, totals, errors = run_collector(CATALOG)
        self.assertEqual(errors, [])
        self.assertEqual(totals["visible_output_bytes"], 1000)
        self.assertEqual(totals["helper_data_bytes"], 1500)
        self.assertEqual(totals["source_bytes"], 250)
        self.assertEqual(totals["total_bytes"], 2750)
        self.assertEqual(len(items), 4)

    def test_operator_state_is_helper_data_not_visible_output(self):
        # Miscounting internal state as visible output would understate the
        # overhead ratio, which is the point of the measurement.
        items, _, _ = run_collector(CATALOG)
        internal = next(i for i in items if i["name"] == "__internal_join_0")
        self.assertEqual(internal["category"], "helper_data")
        self.assertEqual(internal["kind"], "operator_state")

    def test_base_tables_are_source_not_visible_output(self):
        items, _, _ = run_collector(CATALOG)
        base = next(i for i in items if i["name"] == "staging_trade")
        self.assertEqual(base["category"], "source")

    def test_overhead_ratio_is_helper_over_visible(self):
        _, totals, _ = run_collector(CATALOG)
        ratio = storage_metrics._ratio(
            totals["helper_data_bytes"], totals["visible_output_bytes"]
        )
        self.assertAlmostEqual(ratio, 1.5)

    def test_a_failed_view_is_reported_not_silently_zeroed(self):
        # A partial read must not look like "this engine has no hidden state".
        items, totals, errors = run_collector(CATALOG, fail_on=("rw_internal_tables",))
        self.assertTrue(any("rw_internal_tables" in e for e in errors))
        self.assertEqual(totals["helper_data_bytes"], 0)
        self.assertEqual(totals["visible_output_bytes"], 1000)

    def test_unreachable_engine_reports_an_error(self):
        fake_sources = Mock()
        fake_sources._connect = Mock(side_effect=RuntimeError("connection refused"))
        with patch.dict(sys.modules, {"services.risingwave_sources": fake_sources}):
            items, totals, errors = storage_metrics._collect_risingwave_storage(
                deadline=time.monotonic() + 60
            )
        self.assertEqual(items, [])
        self.assertEqual(totals["total_bytes"], 0)
        self.assertTrue(any("connection refused" in e for e in errors))

    def test_expired_deadline_short_circuits(self):
        items, totals, errors = run_collector(CATALOG, deadline_offset=-1)
        self.assertEqual(items, [])
        self.assertTrue(errors)

    def test_risingwave_is_a_supported_engine(self):
        self.assertIn("risingwave", storage_metrics.SUPPORTED_ENGINES)

    def test_physical_state_counts_allocated_blocks_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            nested = root / "state_store"
            nested.mkdir()
            state_file = nested / "1.data"
            state_file.write_bytes(b"state" * 1000)

            summary, errors = storage_metrics._collect_physical_root(
                root, deadline=time.monotonic() + 60
            )
            expected_allocated = sum(
                path.stat(follow_symlinks=False).st_blocks * 512
                for path in (root, nested, state_file)
            )

        self.assertEqual(errors, [])
        self.assertEqual(summary["allocated_bytes"], expected_allocated)
        self.assertEqual(summary["measurement"], "filesystem_st_blocks")


if __name__ == "__main__":
    unittest.main()
