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


class FelderaAdHocResponseTest(unittest.TestCase):
    """The ad-hoc endpoint returns newline-delimited JSON.

    Shapes captured from a live pipeline. Calling response.json() on NDJSON and
    looking for a "rows" key returned nothing, which surfaced as "verification
    produced no comparable result" for every query rather than as an error.
    """

    def _rows(self, text):
        from services.compiler_bench.engines_cloud import FelderaAdapter

        return FelderaAdapter._ndjson_rows(text)

    def test_multiple_rows_one_json_object_per_line(self):
        self.assertEqual(
            self._rows('{"a":1,"s":30}\n{"a":2,"s":5}'),
            [{"a": 1, "s": 30}, {"a": 2, "s": 5}],
        )

    def test_single_row_probe_result(self):
        self.assertEqual(self._rows('{"diff":0}'), [{"diff": 0}])

    def test_blank_lines_and_empty_body(self):
        self.assertEqual(self._rows('\n{"diff":2}\n\n'), [{"diff": 2}])
        self.assertEqual(self._rows(""), [])

    def test_a_json_array_is_not_the_shape_returned(self):
        # Guard against reverting to the array assumption: an array body yields
        # one element, not the rows inside it.
        self.assertEqual(self._rows('[{"diff":0}]'), [[{"diff": 0}]])


class FelderaDeltaParsingTest(unittest.TestCase):
    """DML -> ingress records.

    Feldera has no UPDATE: a change arrives as insert/delete records, so an
    UPDATE becomes delete-old + insert-new. Mis-parsing here would apply the
    wrong changes and still verify "successfully" against them, so the parser
    returns None for anything it does not recognise rather than guessing.
    """

    def _parse(self, sql):
        from services.compiler_bench.engines_cloud import FelderaAdapter

        return FelderaAdapter.parse_delta(sql)

    def test_update_splits_assignments_and_predicate(self):
        got = self._parse(
            "UPDATE CUSTOMER SET C_BALANCE = -329.0, C_PAYMENT_CNT = 1 "
            "WHERE C_W_ID = 1 AND C_D_ID = 2"
        )
        self.assertEqual(got["op"], "update")
        self.assertEqual(got["table"], "CUSTOMER")
        self.assertEqual(got["set"], {"c_balance": -329.0, "c_payment_cnt": 1})
        self.assertEqual(got["where"], "C_W_ID = 1 AND C_D_ID = 2")

    def test_delete_keeps_predicate(self):
        got = self._parse("DELETE FROM NEW_ORDER WHERE NO_W_ID = 1 AND NO_D_ID = 2")
        self.assertEqual((got["op"], got["table"]), ("delete", "NEW_ORDER"))
        self.assertEqual(got["where"], "NO_W_ID = 1 AND NO_D_ID = 2")

    def test_insert_values_are_typed(self):
        got = self._parse(
            "INSERT INTO HISTORY VALUES (24, 1, 1, 1, 1, '2026-01-01 00:00:00', 175.00, 'Payment')"
        )
        self.assertEqual(got["op"], "insert")
        self.assertEqual(got["values"][0], 24)
        self.assertEqual(got["values"][5], "2026-01-01 00:00:00")
        self.assertAlmostEqual(got["values"][6], 175.0)
        self.assertEqual(got["values"][7], "Payment")

    def test_timestamp_prefixed_literal(self):
        got = self._parse("INSERT INTO T VALUES (TIMESTAMP'2026-01-01 00:00:00', 1)")
        self.assertEqual(got["values"][0], "2026-01-01 00:00:00")

    def test_comma_inside_a_string_does_not_split_values(self):
        got = self._parse("INSERT INTO T VALUES ('a,b', 2)")
        self.assertEqual(got["values"], ["a,b", 2])

    def test_unrecognised_statement_is_none_not_a_guess(self):
        for sql in ("SELECT 1", "MERGE INTO T USING S ON (1=1)", "", "UPDATE T SET a = 1"):
            self.assertIsNone(self._parse(sql))

    def test_every_generated_pool_statement_parses(self):
        # The pool this must handle is generated by the benchmark-server; if its
        # shapes change, this fails rather than silently skipping deltas.
        import importlib.util
        from pathlib import Path

        path = (Path(__file__).resolve().parents[2]
                / "benchmark-server" / "services" / "compiler_bench_corpus.py")
        spec = importlib.util.spec_from_file_location("cbc", path)
        module = importlib.util.module_from_spec(spec)
        # dataclasses in the loaded module resolve their annotations through
        # sys.modules, so it has to be registered before exec.
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        for dialect in ("duckdb", "spark"):
            for stmt in module.tpcc_delta_pool(3, dialect):
                self.assertIsNotNone(self._parse(stmt), f"unparsed: {stmt}")


class FelderaDiffReadTest(unittest.TestCase):
    """Reading the probe's count out of a result row."""

    def _diff(self, row):
        from services.compiler_bench.engines_cloud import FelderaAdapter

        return FelderaAdapter._diff_value(row)

    def test_named_diff_column(self):
        self.assertEqual(self._diff({"diff": 0}), 0)
        self.assertEqual(self._diff({"diff": 2}), 2)

    def test_case_variants(self):
        self.assertEqual(self._diff({"DIFF": 3}), 3)

    def test_single_column_row_regardless_of_name(self):
        # Unambiguous: one column, one value — a naming difference should not
        # fail the whole verification.
        self.assertEqual(self._diff({"count(*)": 7}), 7)

    def test_multi_column_row_without_diff_is_unreadable(self):
        # Ambiguous — guessing which column is the count would risk a wrong
        # verdict, so this must report rather than pick one.
        self.assertIsNone(self._diff({"a": 1, "b": 2}))

    def test_null_and_empty(self):
        self.assertIsNone(self._diff({"diff": None}))
        self.assertIsNone(self._diff({}))
