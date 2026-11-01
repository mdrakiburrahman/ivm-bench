"""Execute the workload's monetary averages at a rounding boundary."""

import re
import unittest
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

import duckdb


PROJECTS = Path(__file__).resolve().parents[1] / "dbt-projects"
ENGINES = (
    "spark", "spark-openivm", "duckdb", "duckdb-openivm",
    "fabric-jvm-35", "fabric-openivm-jvm-35", "databricks-enzyme", "feldera",
)


class TradeVolumeStatsAverageTest(unittest.TestCase):
    def test_monetary_averages_are_order_independent_and_ignore_nulls(self):
        # Like the SF10 failure, these 32 amounts total 3009.77 and average
        # exactly 94.0553125. Binary SUM/AVG can land either side of the tie.
        cents = [6860, 3486, 9566, 5609, 9266, 8708, 5908, 13950,
                 7607, 7949, 6641, 13436, 12527, 1392, 8691, 6971,
                 8351, 9780, 2255, 6504, 4639, 4467, 14254, 1051,
                 11678, 7886, 5543, 9171, 4988, 8182, 14416, 59245]
        average = sum(Decimal(c) / 100 for c in cents) / len(cents)
        self.assertEqual(average, Decimal("94.0553125"))
        values = [c / 100 for c in cents] + [None]
        with duckdb.connect() as db:
            db.execute("SET threads = 1")
            for engine in ENGINES:
                model = (PROJECTS / engine / "models/gold/analytics/trade_volume_stats.sql").read_text()
                expressions = re.findall(r"^\s*(.+) AS (avg_price|avg_fee|avg_commission),$", model, re.M)
                self.assertEqual(len(expressions), 3)
                for expression, alias in expressions:
                    with self.subTest(engine=engine, average=alias):
                        precision = 4 if engine in ("spark", "duckdb", "duckdb-openivm", "feldera") else 6
                        expected = float(average.quantize(Decimal(1).scaleb(-precision), rounding=ROUND_HALF_UP))
                        sql = (f"SELECT {expression} FROM (SELECT x AS trade_price, x AS fee, "
                               "x AS commission FROM unnest(?) u(x)) t")
                        for ordered in (values, list(reversed(values))):
                            self.assertEqual(db.execute(sql, [ordered]).fetchone()[0], expected)
                        self.assertIsNone(db.execute(sql, [[None, None]]).fetchone()[0])
                        self.assertIsNone(db.execute(sql, [[]]).fetchone()[0])


if __name__ == "__main__":
    unittest.main()
