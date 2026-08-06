"""Unit tests for the compiler-bench adapters.

These cover the pure parsing/verdict logic — the parts that failed *silently* on
a live run. The Livy row extractor returning nothing did not raise: it surfaced
as "classification unknown" and "verification produced no comparable result",
which read like engine properties rather than a harness bug. A payload-shape
test is the cheapest thing that would have caught it.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.compiler_bench.engines import (  # noqa: E402
    FULL,
    INCREMENTAL,
    UNKNOWN,
    SparkOpenIVMAdapter,
    _is_float_type,
    _is_fatal,
    _tpcc_table_names,
    _verify_probe,
    _LivyAdapter,
)


def livy_response(columns, rows):
    """A Livy statement response of the shape LivyClient.execute returns.

    The table payload lives under "output", which is the level the first
    implementation dropped.
    """
    return {
        "id": 1,
        "state": "available",
        "output": {
            "status": "ok",
            "data": {
                "application/json": {
                    "schema": {"fields": [{"name": c} for c in columns]},
                    "data": [list(r) for r in rows],
                }
            },
        },
    }


class LivyRowExtractionTest(unittest.TestCase):
    def test_extracts_rows_from_statement_response(self):
        response = livy_response(["a", "b"], [[1, "x"], [2, "y"]])
        self.assertEqual(_LivyAdapter._rows(response), [[1, "x"], [2, "y"]])

    def test_does_not_read_the_top_level_data_key(self):
        # Regression: reading result["data"] instead of result["output"]["data"]
        # returned [] for every row-returning statement.
        response = livy_response(["a"], [[1]])
        self.assertNotEqual(_LivyAdapter._rows(response), [])

    def test_empty_result_set_is_empty_not_an_error(self):
        self.assertEqual(_LivyAdapter._rows(livy_response(["a"], [])), [])

    def test_text_plain_output_yields_no_rows(self):
        # Livy falls back to text/plain for some statements; no rows is the
        # right answer, and callers turn that into `unknown` rather than a fake
        # verdict.
        response = {"output": {"data": {"text/plain": "+---+\n| a |\n+---+"}}}
        self.assertEqual(_LivyAdapter._rows(response), [])

    def test_missing_and_malformed_responses(self):
        for response in ({}, {"output": {}}, {"output": {"data": {}}}, None):
            self.assertEqual(_LivyAdapter._rows(response), [])


class SparkOpenIVMClassifyTest(unittest.TestCase):
    """The EXPLAIN verdict, which the engine returns as one JSON string."""

    def _classify(self, response):
        adapter = SparkOpenIVMAdapter.__new__(SparkOpenIVMAdapter)
        adapter._execute = lambda sql, *, timeout_s: response  # type: ignore[assignment]
        return SparkOpenIVMAdapter.classify(adapter, "mv1", "SELECT 1", timeout_s=10)

    def test_eligible_true_is_incremental(self):
        payload = '{"view":"mv1","eligible":true,"refresh_type":0}'
        self.assertEqual(self._classify(livy_response(["explain"], [[payload]])), INCREMENTAL)

    def test_eligible_false_is_full(self):
        payload = '{"view":"mv1","eligible":false,"refresh_type":3}'
        self.assertEqual(self._classify(livy_response(["explain"], [[payload]])), FULL)

    def test_refresh_type_used_when_eligible_absent(self):
        # refresh_type 3 is FULL_REFRESH, matching openivm's DuckDB catalog.
        self.assertEqual(
            self._classify(livy_response(["explain"], [['{"refresh_type":3}']])), FULL
        )
        self.assertEqual(
            self._classify(livy_response(["explain"], [['{"refresh_type":1}']])), INCREMENTAL
        )

    def test_unparseable_verdict_is_unknown_never_full(self):
        # Reporting `full` here would make an engine we failed to interrogate
        # look like a well-behaved full-refresh engine.
        for payload in ("not json", "", "{}"):
            self.assertEqual(
                self._classify(livy_response(["explain"], [[payload]])), UNKNOWN
            )

    def test_no_rows_is_unknown(self):
        self.assertEqual(self._classify(livy_response(["explain"], [])), UNKNOWN)


class VerifyProbeTest(unittest.TestCase):
    def test_float_columns_are_rounded_and_others_are_not(self):
        probe = _verify_probe("mv", "SELECT 1", [("a", "INTEGER"), ("b", "DOUBLE")])
        self.assertIn("round(CAST(c1 AS DOUBLE), 10)", probe)
        self.assertNotIn("round(CAST(c0", probe)

    def test_decimal_is_exact_and_not_rounded(self):
        # DECIMAL is exact; only DOUBLE/FLOAT/REAL drift between an incremental
        # view and a re-run of the query.
        self.assertFalse(_is_float_type("DECIMAL(12,2)"))
        for t in ("DOUBLE", "FLOAT", "REAL"):
            self.assertTrue(_is_float_type(t))

    def test_probe_compares_both_directions(self):
        probe = _verify_probe("mv", "SELECT 1", [("a", "INTEGER")])
        self.assertEqual(probe.count("EXCEPT ALL"), 2)

    def test_falls_back_when_columns_unknown(self):
        probe = _verify_probe("mv", "SELECT 1", [])
        self.assertIn("EXCEPT ALL", probe)
        self.assertNotIn("round(", probe)


class ErrorClassificationTest(unittest.TestCase):
    def test_engine_death_is_fatal(self):
        for message in ("Segmentation fault", "out of memory", "Connection refused"):
            self.assertTrue(_is_fatal(message))

    def test_ordinary_sql_errors_are_not_fatal(self):
        # Over-matching here would reclassify bad queries as crashes and inflate
        # the crash rate.
        for message in (
            "Catalog Error: Table with name x does not exist",
            "Binder Error: No function matches",
            "Parser Error: syntax error at or near",
        ):
            self.assertFalse(_is_fatal(message))


class SchemaHelperTest(unittest.TestCase):
    def test_table_names_parsed_from_ddl(self):
        ddl = ["CREATE TABLE WAREHOUSE (W_ID INT)", "CREATE TABLE ORDER_LINE (OL_W_ID INT)"]
        self.assertEqual(_tpcc_table_names(ddl), ["WAREHOUSE", "ORDER_LINE"])


if __name__ == "__main__":
    unittest.main()


class FelderaChunkedProbeTest(unittest.TestCase):
    """Chunked pre-filter: accept whole chunks, bisect only what fails.

    Per-view probing measured ~5s/query (one full SQL compilation each), which
    projects to ~3h for the 2186-query corpus. These tests pin the two
    properties that make chunking correct: every view still gets an individual
    verdict, and a passing chunk costs one compile.
    """

    def _adapter(self, bad_names):
        from services.compiler_bench.engines_cloud import FelderaAdapter

        adapter = FelderaAdapter.__new__(FelderaAdapter)
        adapter._accepted = {}
        adapter._rejected = {}
        adapter.compiles = []

        def chunk_compiles(views, *, timeout_s):
            adapter.compiles.append([n for n, _ in views])
            bad = [n for n, _ in views if n in bad_names]
            return f"cannot compile {bad[0]}" if bad else None

        adapter._chunk_compiles = chunk_compiles
        return adapter

    @staticmethod
    def _views(n):
        return [(f"cb_mv_{i}", f"SELECT {i}") for i in range(1, n + 1)]

    def test_all_good_chunk_costs_one_compile(self):
        adapter = self._adapter(bad_names=set())
        adapter._partition(self._views(50), timeout_s=60)
        self.assertEqual(len(adapter.compiles), 1)
        self.assertEqual(len(adapter._accepted), 50)
        self.assertEqual(adapter._rejected, {})

    def test_single_bad_view_is_isolated_and_others_accepted(self):
        adapter = self._adapter(bad_names={"cb_mv_7"})
        adapter._partition(self._views(16), timeout_s=60)
        self.assertEqual(set(adapter._rejected), {"cb_mv_7"})
        self.assertEqual(len(adapter._accepted), 15)
        # Bisection, not a per-view sweep: far fewer compiles than 16.
        self.assertLess(len(adapter.compiles), 12)

    def test_rejected_view_keeps_the_compiler_message(self):
        adapter = self._adapter(bad_names={"cb_mv_3"})
        adapter._partition(self._views(4), timeout_s=60)
        self.assertIn("cannot compile cb_mv_3", adapter._rejected["cb_mv_3"])

    def test_every_view_gets_a_verdict(self):
        adapter = self._adapter(bad_names={"cb_mv_2", "cb_mv_9"})
        views = self._views(12)
        adapter._partition(views, timeout_s=60)
        decided = set(adapter._accepted) | set(adapter._rejected)
        self.assertEqual(decided, {n for n, _ in views})

    def test_all_bad_rejects_each_individually(self):
        adapter = self._adapter(bad_names={f"cb_mv_{i}" for i in range(1, 5)})
        adapter._partition(self._views(4), timeout_s=60)
        self.assertEqual(len(adapter._rejected), 4)
        self.assertEqual(adapter._accepted, {})

    def test_empty_input_does_nothing(self):
        adapter = self._adapter(bad_names=set())
        adapter._partition([], timeout_s=60)
        self.assertEqual(adapter.compiles, [])
