"""Execute the model's rank expression on a deterministic floating-point tie."""

import math
import re
import sqlite3
import unittest
from pathlib import Path


PROJECTS = Path(__file__).resolve().parents[1] / "dbt-projects"
ENGINES = (
    "spark", "spark-openivm", "duckdb", "duckdb-openivm",
    "fabric-jvm-35", "fabric-openivm-jvm-35", "databricks-enzyme", "feldera",
)


class CustomerConcentrationRankTest(unittest.TestCase):
    def test_every_engine_ranks_at_the_displayed_precision(self):
        # One ULP apart: equal at the model's six-decimal display precision,
        # but raw DOUBLE ranking splits the tie and shifts all later ranks.
        total = 323568.72
        neighbor = math.nextafter(total, math.inf)
        self.assertNotEqual(total, neighbor)
        with sqlite3.connect(":memory:") as db:
            db.execute("CREATE TABLE totals(customer TEXT, total_portfolio_value REAL)")
            db.executemany("INSERT INTO totals VALUES (?, ?)", [
                ("higher", 400000.0), ("tie_a", total), ("tie_b", neighbor),
                ("lower", 100.0),
            ])
            raw = db.execute(
                "SELECT DENSE_RANK() OVER (ORDER BY total_portfolio_value DESC) "
                "FROM totals ORDER BY customer"
            ).fetchall()
            self.assertEqual(raw, [(1,), (4,), (3,), (2,)])

            for engine in ENGINES:
                with self.subTest(engine=engine):
                    model = (PROJECTS / engine / "models/gold/analytics/customer_concentration.sql").read_text()
                    display = re.search(r"(ROUND\(uc\.total_portfolio_value,\s*\d+\)) AS total_portfolio_value", model)
                    rank = re.search(r"DENSE_RANK\(\) OVER \(ORDER BY (.*?) DESC\) AS rank_by_portfolio", model)
                    self.assertIsNotNone(display)
                    self.assertIsNotNone(rank)
                    self.assertEqual(rank[1], display[1])
                    rows = db.execute(
                        f"SELECT customer, {display[1]}, "
                        f"DENSE_RANK() OVER (ORDER BY {rank[1]} DESC) "
                        "FROM totals uc ORDER BY customer"
                    ).fetchall()
                    self.assertEqual(rows, [
                        ("higher", 400000.0, 1), ("lower", 100.0, 3),
                        ("tie_a", total, 2), ("tie_b", total, 2),
                    ])


if __name__ == "__main__":
    unittest.main()
