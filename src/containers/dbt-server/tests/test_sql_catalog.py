import json
import sys
import unittest
from pathlib import Path

import sqlglot

CONTAINERS_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CONTAINERS_DIR))
ENGINES = json.loads((CONTAINERS_DIR / "sql_catalog" / "engines.json").read_text(encoding="utf-8"))

from sql_catalog.generator import _mask_jinja, generate_model, is_sql_candidate, load_sql


class SqlCatalogTests(unittest.TestCase):
    def test_date_model_has_equivalent_ast_for_every_engine(self):
        source = load_sql("spark", "models", "silver/date")
        masked_source, tokens = _mask_jinja(source)
        expected = sqlglot.parse_one(masked_source, read="spark").sql(dialect="spark")

        for engine in (
            "spark",
            "spark-openivm",
            "databricks-enzyme",
            "databricks-openivm",
            "duckdb",
            "duckdb-openivm",
            "fabric-jvm-35",
            "fabric-openivm-jvm-35",
            "feldera",
            "dbspnet",
        ):
            with self.subTest(engine=engine):
                rendered = load_sql(engine, "models", "silver/date")
                masked, rendered_tokens = _mask_jinja(rendered)
                self.assertEqual(tokens, rendered_tokens)
                self.assertEqual(
                    expected,
                    sqlglot.parse_one(masked, read=ENGINES[engine]).sql(dialect="spark"),
                )
                self.assertIn("{{ ref('reference_date') }}", rendered)

    def test_model_generation_preserves_project_relative_path(self):
        import tempfile

        with tempfile.TemporaryDirectory() as output:
            path = generate_model("duckdb", "silver/date", output)
            self.assertEqual(Path(output) / "silver" / "date.sql", path)
            self.assertIn("{{ ref('reference_date') }}", path.read_text(encoding="utf-8"))

    def test_build_inputs_match_catalog_output_for_existing_engines(self):
        projects = CONTAINERS_DIR / "dbt-server" / "dbt-projects"
        engines = [
            engine
            for engine in ENGINES
            if (projects / engine / "models" / "silver" / "date.sql").is_file()
        ]
        self.assertTrue(engines)
        for engine in engines:
            with self.subTest(engine=engine):
                current = (projects / engine / "models" / "silver" / "date.sql").read_text(
                    encoding="utf-8"
                )
                expected, expected_tokens = _mask_jinja(load_sql(engine, "models", "silver/date"))
                actual, actual_tokens = _mask_jinja(current)
                self.assertEqual(expected_tokens, actual_tokens)
                self.assertEqual(
                    sqlglot.parse_one(expected, read=ENGINES[engine]).sql(dialect="spark"),
                    sqlglot.parse_one(actual, read=ENGINES[engine]).sql(dialect="spark"),
                )

    def test_loader_rejects_path_traversal(self):
        with self.assertRaises(ValueError):
            load_sql("spark", "models", "../outside")

    def test_loose_sql_detector_identifies_statements(self):
        self.assertTrue(is_sql_candidate('cursor.execute("SELECT id FROM accounts")'))
        self.assertFalse(is_sql_candidate('logger.info("accounts loaded")'))


if __name__ == "__main__":
    unittest.main()
