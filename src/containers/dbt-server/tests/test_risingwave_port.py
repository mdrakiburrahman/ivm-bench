"""Unit tests for the RisingWave port.

These pin the SQL rewrites that make the port work at all. Each one stands in
for a failure that RisingWave reports only at runtime, as a bind or parser
error, on a query that reads as perfectly ordinary SQL — so a regression here
would look like "RisingWave cannot maintain this model" rather than "we emitted
a type parameter it does not accept".
"""

import os
import re
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.compiler_bench.engines_risingwave import (  # noqa: E402
    RisingWaveAdapter,
    strip_type_params,
    to_risingwave_sql,
)


class StripTypeParamsTest(unittest.TestCase):
    """RisingWave rejects every length/precision-parameterised type."""

    def test_drops_varchar_length(self):
        self.assertEqual(
            strip_type_params("CREATE TABLE t (a VARCHAR(10), b CHAR(2))"),
            "CREATE TABLE t (a VARCHAR, b VARCHAR)",
        )

    def test_drops_decimal_precision(self):
        self.assertEqual(
            strip_type_params("CREATE TABLE t (a DECIMAL(12,2), b NUMERIC(4,4))"),
            "CREATE TABLE t (a DECIMAL, b DECIMAL)",
        )

    def test_drops_precision_in_a_cast_not_just_ddl(self):
        # 55 of the 499 corpus queries fail on exactly this before the MV is
        # ever attempted, so the rewrite has to reach query text too.
        self.assertEqual(
            strip_type_params("select cast(x as NUMERIC(8,3)) from t"),
            "select cast(x as DECIMAL) from t",
        )

    def test_leaves_unparameterised_types_alone(self):
        sql = "select cast(x as DECIMAL), cast(y as VARCHAR) from t"
        self.assertEqual(strip_type_params(sql), sql)

    def test_is_case_insensitive(self):
        self.assertEqual(strip_type_params("a varchar(9)"), "a VARCHAR")


class RisingWaveDialectTest(unittest.TestCase):
    """Places RisingWave lacks something PostgreSQL has.

    These belong in the adapter, not in LPTS: the corpus is rendered as correct
    PostgreSQL, and "fixing" these in the postgres renderer would make its
    output wrong for real Postgres. Counts below are from the 2,505-query run.
    """

    def test_stddev_becomes_stddev_samp(self):
        # 47 queries. PostgreSQL's STDDEV is the sample form; RisingWave only
        # implements the explicit spelling.
        self.assertEqual(to_risingwave_sql("select STDDEV(x) from t"),
                         "select STDDEV_SAMP(x) from t")

    def test_variance_becomes_var_samp(self):
        # 22 queries.
        self.assertEqual(to_risingwave_sql("select variance(x) from t"),
                         "select VAR_SAMP(x) from t")

    def test_two_arg_round_gets_a_numeric_cast(self):
        # 11 queries, reported as the internal name: "function
        # round_digit(double precision, integer) does not exist".
        self.assertEqual(to_risingwave_sql("select round(x, 4) from t"),
                         "select ROUND(CAST(x AS NUMERIC), 4) from t")

    def test_single_arg_round_untouched(self):
        self.assertEqual(to_risingwave_sql("select round(x) from t"),
                         "select round(x) from t")

    def test_round_handles_nested_parens(self):
        self.assertEqual(
            to_risingwave_sql("select round(sum(a) / nullif(count(b), 0), 6) from t"),
            "select ROUND(CAST(sum(a) / nullif(count(b), 0) AS NUMERIC), 6) from t")

    def test_round_handles_two_calls_in_one_query(self):
        self.assertEqual(to_risingwave_sql("select round(a,1), round(b,2) from t"),
                         "select ROUND(CAST(a AS NUMERIC), 1), ROUND(CAST(b AS NUMERIC), 2) from t")

    def test_still_strips_type_params(self):
        self.assertEqual(to_risingwave_sql("cast(x as NUMERIC(38, 6))"),
                         "cast(x as DECIMAL)")

    def test_leaves_unrelated_sql_alone(self):
        sql = "select a, b from t where c = 1 group by a"
        self.assertEqual(to_risingwave_sql(sql), sql)


class MultisetVerifyTest(unittest.TestCase):
    """RisingWave has EXCEPT but not EXCEPT ALL, so verification compares here."""

    def test_equal_multisets_match(self):
        ms = RisingWaveAdapter._multiset
        self.assertEqual(ms([(1, "a"), (1, "a")]), ms([(1, "a"), (1, "a")]))

    def test_duplicate_counts_are_significant(self):
        ms = RisingWaveAdapter._multiset
        self.assertNotEqual(ms([(1,), (1,)]), ms([(1,)]))

    def test_row_order_is_not_significant(self):
        ms = RisingWaveAdapter._multiset
        self.assertEqual(ms([(1,), (2,)]), ms([(2,), (1,)]))

    def test_array_columns_do_not_crash_the_comparison(self):
        # RisingWave returns ARRAY columns as lists, which are unhashable;
        # Counter then raised "unhashable type: 'list'" and the runner recorded
        # it as an engine CRASH even though the engine had answered correctly.
        ms = RisingWaveAdapter._multiset
        self.assertEqual(ms([([1, 2],)]), ms([([1, 2],)]))
        self.assertNotEqual(ms([([1, 2],)]), ms([([2, 1],)]))

    def test_nested_arrays_normalise(self):
        ms = RisingWaveAdapter._multiset
        self.assertEqual(ms([([[0.1 + 0.2]],)]), ms([([[0.3]],)]))

    def test_composite_columns_do_not_crash_the_comparison(self):
        ms = RisingWaveAdapter._multiset
        self.assertEqual(ms([({"a": 1},)]), ms([({"a": 1},)]))

    def test_floats_compare_with_tolerance(self):
        # Mirrors the runner's SQL probe: an incrementally maintained SUM/COUNT
        # cannot reproduce a batch AVG bit-for-bit.
        ms = RisingWaveAdapter._multiset
        self.assertEqual(ms([(0.1 + 0.2,)]), ms([(0.3,)]))


class ProjectPortRewritesTest(unittest.TestCase):
    """The dbt project rewrites, applied by dbt-projects/.rw_port.py."""

    @staticmethod
    def _port_module():
        import importlib.util

        path = os.path.join(
            os.path.dirname(__file__), "..", "dbt-projects", ".rw_port.py"
        )
        spec = importlib.util.spec_from_file_location("rw_port", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _apply(self, text):
        module = self._port_module()
        for pattern, replacement in module.REWRITES:
            text = pattern.sub(replacement, text)
        return text

    def test_millisecond_interval_becomes_a_division(self):
        # "Bind error: Invalid unit: millisecond" — RisingWave has no such unit.
        self.assertIn(
            "INTERVAL '1 second' / 1000",
            self._apply("        ) - INTERVAL 1 MILLISECOND,"),
        )

    def test_strptime_becomes_to_date(self):
        self.assertEqual(
            self._apply("strptime(trim(x), '%Y%m%d')::DATE"),
            "to_date(trim(x), 'YYYYMMDD')",
        )

    def test_empty_partition_by_gets_a_constant(self):
        # "Window function with empty PARTITION BY is not supported"
        self.assertEqual(
            self._apply("RANK() OVER (ORDER BY a DESC) AS r"),
            "RANK() OVER (PARTITION BY 1 ORDER BY a DESC) AS r",
        )

    def test_stddev_becomes_stddev_samp(self):
        # DuckDB's and PostgreSQL's STDDEV is STDDEV_SAMP; RisingWave has only
        # the explicit spelling.
        self.assertEqual(self._apply("STDDEV(x)"), "STDDEV_SAMP(x)")

    def test_cross_join_becomes_an_equi_join_on_a_constant(self):
        # "Not supported: streaming nested-loop join" — a CROSS JOIN against a
        # one-row aggregate has to be given a join key.
        out = self._apply("    FROM symbol_volatility sv\n    CROSS JOIN global_market gm")
        self.assertIn("1 AS join_key FROM symbol_volatility", out)
        self.assertIn("1 AS join_key FROM global_market", out)
        self.assertIn("ON sv.join_key = gm.join_key", out)
        self.assertNotIn("CROSS JOIN", out)

    def test_is_cash_case_compares_integers(self):
        # 'cannot cast type "smallint" to "boolean"' — DuckDB coerces a TINYINT
        # in a CASE, RisingWave will not.
        out = self._apply(
            "case t_is_cash\n        when true then 'Cash'\n        when false then 'Margin'\n    end")
        self.assertEqual(
            out, "case when t_is_cash = 1 then 'Cash' when t_is_cash = 0 then 'Margin' end")

    def _apply_calls(self, text):
        module = self._port_module()
        for rewrite in module.CALL_REWRITES:
            text = rewrite(text)
        return text

    def test_round_gets_a_decimal_first_argument(self):
        # 'function round_digit(double precision, integer) does not exist'
        self.assertEqual(
            self._apply_calls("ROUND(sv.avg_daily_return, 4)"),
            "ROUND(CAST(sv.avg_daily_return AS NUMERIC), 4)")

    def test_round_handles_nested_parens_across_lines(self):
        out = self._apply_calls(
            "ROUND(\n  (a - b) / NULLIF(c, 0),\n  4\n)")
        self.assertEqual(out, "ROUND(CAST((a - b) / NULLIF(c, 0) AS NUMERIC), 4)")

    def test_round_wraps_an_expression_that_merely_starts_with_a_cast(self):
        # The argument here is a division whose left operand is a cast, so its
        # value is DOUBLE and ROUND still needs the NUMERIC wrap. Treating any
        # argument beginning with "CAST(" as already-cast left three analytics
        # models failing with "round_digit(double precision, integer) does not
        # exist".
        out = self._apply_calls("ROUND(CAST(SUM(x) AS DOUBLE) / NULLIF(COUNT(y), 0), 6)")
        self.assertEqual(
            out, "ROUND(CAST(CAST(SUM(x) AS DOUBLE) / NULLIF(COUNT(y), 0) AS NUMERIC), 6)")

    def test_round_leaves_a_wholly_cast_argument_alone(self):
        self.assertEqual(
            self._apply_calls("ROUND(CAST(x AS NUMERIC), 4)"),
            "ROUND(CAST(x AS NUMERIC), 4)")

    def test_round_rewrites_nested_occurrences(self):
        # Outer and inner ROUND both need the wrap; resuming the scan past the
        # whole replacement silently skipped the inner one.
        out = self._apply_calls("ROUND(SUM(CAST(ROUND(c, 6) AS DECIMAL)) / n, 6)")
        self.assertEqual(out.count("CAST(c AS NUMERIC)"), 1)
        self.assertTrue(out.startswith("ROUND(CAST(SUM("))

    def test_single_argument_round_is_left_alone(self):
        self.assertEqual(self._apply_calls("ROUND(x)"), "ROUND(x)")

    def test_regexp_matches_becomes_the_match_operator(self):
        # DuckDB returns BOOLEAN here; RisingWave returns varchar[], so a bare
        # CASE WHEN fails with 'argument of CASE WHEN must be boolean'.
        self.assertEqual(
            self._apply_calls("regexp_matches(trim(substring(line, 187, 60)), '^[0-9]+$')"),
            "(trim(substring(line, 187, 60))) ~ ('^[0-9]+$')")

    def test_strips_parameterised_decimal_from_model_sql(self):
        # The TPC-DI analytics models carry an explicit CAST(... AS
        # DECIMAL(38, 6)) that RisingWave's parser rejects. Note the space after
        # the comma: the source writes it that way, and a pattern that does not
        # allow whitespace silently misses every occurrence.
        self.assertEqual(
            self._apply("CAST(SUM(x) AS DECIMAL(38, 6))"),
            "CAST(SUM(x) AS DECIMAL)")

    def test_strips_parameterised_decimal_without_space(self):
        self.assertEqual(self._apply("cast(x as numeric(38,4))"), "cast(x as DECIMAL)")

    def test_strips_parameterised_varchar_from_model_sql(self):
        self.assertEqual(self._apply("CAST(x AS VARCHAR(50))"), "CAST(x AS VARCHAR)")

    def test_rewrites_do_not_touch_unrelated_sql(self):
        sql = "select a, b from t where c = 1 group by a, b"
        self.assertEqual(self._apply(sql), sql)


if __name__ == "__main__":
    unittest.main()
