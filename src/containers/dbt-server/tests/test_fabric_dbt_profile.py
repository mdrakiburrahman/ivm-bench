import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.fabric_dbt_profile import instrument


class FabricClientTimingTest(unittest.TestCase):
    def test_spark_error_frames_are_preserved_without_query_or_message(self):
        class Cursor:
            def _getLivyResult(self):
                return {"output": {"status": "error", "evalue": "secret query", "traceback": [
                    "secret query\n at org.apache.spark.sql.SparkSession.active(SparkSession.scala:123)"
                ]}}

        rows = []
        instrument(Cursor, "_getLivyResult", rows.append)
        self.assertEqual(Cursor()._getLivyResult()["output"]["status"], "error")
        self.assertEqual(rows[0]["outcome"], "sql_error")
        self.assertEqual(rows[0]["spark_stack"], ["org.apache.spark.sql.SparkSession.active(SparkSession.scala:123)"])
        self.assertNotIn("secret", json.dumps(rows))

    def test_version_probe_records_actual_version_without_changing_rows(self):
        class Cursor:
            def execute(self, sql):
                self._rows = [["3.5.5"]]

        rows = []
        instrument(Cursor, "execute", rows.append, sql_argument=True)
        cursor = Cursor()
        cursor.execute("SELECT split(version(), ' ')[0] as version")
        self.assertEqual(cursor._rows, [["3.5.5"]])
        self.assertEqual(rows[0]["spark_version"], "3.5.5")

    def test_nested_timings_preserve_return_and_exclude_sql(self):
        class Cursor:
            def submit(self):
                return "result"

            def execute(self, sql):
                return self.submit()

        rows = []
        instrument(Cursor, "submit", rows.append)
        instrument(Cursor, "execute", rows.append, sql_argument=True)
        self.assertEqual(Cursor().execute("SELECT 'secret'"), "result")
        self.assertEqual([r["operation"] for r in rows], ["submit", "execute"])
        self.assertEqual(rows[1]["statement_kind"], "SELECT")
        self.assertNotIn("secret", json.dumps(rows))
        self.assertLessEqual(rows[1]["start_epoch_ns"], rows[0]["start_epoch_ns"])
        self.assertGreaterEqual(rows[1]["end_epoch_ns"], rows[0]["end_epoch_ns"])

    def test_original_exception_is_preserved(self):
        class Cursor:
            def execute(self, sql):
                raise ValueError("original")

        rows = []
        instrument(Cursor, "execute", rows.append, sql_argument=True)
        with self.assertRaisesRegex(ValueError, "original"):
            Cursor().execute("CREATE TABLE x")
        self.assertEqual(rows[0]["outcome"], "error")

    def test_timing_write_failure_does_not_fail_sql(self):
        class Cursor:
            def execute(self, sql):
                return 42

        def broken(row):
            raise OSError("disk full")

        instrument(Cursor, "execute", broken)
        with patch("sys.stderr"):
            self.assertEqual(Cursor().execute("SELECT 42"), 42)
