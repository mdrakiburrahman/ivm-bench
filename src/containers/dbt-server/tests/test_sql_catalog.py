import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sqlglot

CONTAINERS_DIR = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CONTAINERS_DIR))
ENGINES = json.loads((CONTAINERS_DIR / "sql_catalog" / "engines.json").read_text(encoding="utf-8"))

from sql_catalog.generator import (
    _line_marker,
    _mask_jinja,
    _normalized_sql,
    _parse_override,
    _transpile_sql,
    check_catalog,
    generate_macro,
    generate_macros,
    generate_model,
    generate_models,
    is_sql_candidate,
    load_sql,
    parity_report,
)


class SqlCatalogTests(unittest.TestCase):
    def test_date_model_has_equivalent_ast_for_every_engine(self):
        source = load_sql("spark", "models", "silver/date")
        masked_source, tokens = _mask_jinja(source)
        expected = sqlglot.parse_one(masked_source, read="spark").sql(dialect="spark")

        for engine, dialect in ENGINES.items():
            with self.subTest(engine=engine):
                rendered = load_sql(engine, "models", "silver/date")
                masked, rendered_tokens = _mask_jinja(rendered)
                self.assertEqual(tokens, rendered_tokens)
                self.assertEqual(
                    expected,
                    sqlglot.parse_one(masked, read=dialect).sql(dialect="spark"),
                )
                self.assertIn("{{ ref('reference_date') }}", rendered)

    def test_same_dialect_model_is_a_byte_for_byte_copy(self):
        source = "-- comment\nselect {{ ref('table') }}\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.sql"
            path.write_text(source, encoding="utf-8")
            with (
                patch("sql_catalog.generator._catalog_path", return_value=path),
                patch(
                    "sql_catalog.generator._override_path",
                    return_value=Path(directory) / "missing.sql",
                ),
            ):
                self.assertEqual(source, load_sql("spark", "models", "example"))

    def test_model_generation_preserves_project_relative_path(self):
        with tempfile.TemporaryDirectory() as output:
            path = generate_model("duckdb", "silver/date", output)
            self.assertEqual(Path(output) / "silver" / "date.sql", path)
            self.assertIn("{{ ref('reference_date') }}", path.read_text(encoding="utf-8"))

    def test_bulk_generation_includes_catalog_models(self):
        with tempfile.TemporaryDirectory() as output:
            paths = generate_models("duckdb", output)
            self.assertEqual([Path(output) / "silver" / "date.sql"], paths)

    def test_catalog_macros_match_existing_engine_copies(self):
        projects = CONTAINERS_DIR / "dbt-server" / "dbt-projects"
        catalog = CONTAINERS_DIR / "sql_catalog" / "sql" / "spark" / "macros"
        checked = 0
        for source in sorted(catalog.rglob("*.sql")):
            relative = source.relative_to(catalog)
            macro = relative.with_suffix("").as_posix()
            for engine in ENGINES:
                project_macro = projects / engine / "macros" / relative
                if not project_macro.is_file():
                    continue
                with self.subTest(engine=engine, macro=macro):
                    current = project_macro.read_text(encoding="utf-8")
                    self.assertEqual(current, load_sql(engine, "macros", macro))
                checked += 1
        self.assertGreater(checked, 0)

    def test_macro_generation_preserves_project_relative_path(self):
        with tempfile.TemporaryDirectory() as output:
            path = generate_macro("fabric-jvm-35", "generate_schema_name", output)
            self.assertEqual(Path(output) / "generate_schema_name.sql", path)
            self.assertIn("{{ target.schema }}", path.read_text(encoding="utf-8"))

    def test_bulk_macro_generation_applies_known_overrides(self):
        with tempfile.TemporaryDirectory() as output:
            paths = generate_macros("databricks-enzyme", output)
            self.assertEqual(
                [
                    Path(output) / "generate_schema_name.sql",
                    Path(output) / "load_fabric_sources.sql",
                    Path(output) / "materializations" / "materialized_view.sql",
                    Path(output) / "materializations" / "table.sql",
                    Path(output) / "openivm_setup.sql",
                    Path(output) / "register_sources.sql",
                ],
                paths,
            )
            rendered = {
                path.relative_to(output).as_posix(): path.read_text(encoding="utf-8")
                for path in paths
            }
            self.assertIn("DATABRICKS_EXPERIMENT_ID", rendered["generate_schema_name.sql"])
            self.assertIn("REFRESH POLICY", rendered["materializations/materialized_view.sql"])

    def test_build_inputs_match_catalog_output_for_existing_engines(self):
        projects = CONTAINERS_DIR / "dbt-server" / "dbt-projects"
        for engine, dialect in ENGINES.items():
            current = projects / engine / "models" / "silver" / "date.sql"
            if not current.is_file():
                continue
            with self.subTest(engine=engine):
                expected, expected_tokens = _mask_jinja(load_sql(engine, "models", "silver/date"))
                actual, actual_tokens = _mask_jinja(current.read_text(encoding="utf-8"))
                self.assertEqual(expected_tokens, actual_tokens)
                self.assertEqual(
                    sqlglot.parse_one(expected, read=dialect).sql(dialect="spark"),
                    sqlglot.parse_one(actual, read=dialect).sql(dialect="spark"),
                )

    def test_directory_scan_checks_and_compares_every_catalog_entry(self):
        self.assertEqual([], check_catalog(scan_project=False))
        rows, differences = parity_report()
        self.assertTrue(rows)
        self.assertEqual([], differences)

    def test_project_sql_requires_a_source_for_its_own_engine(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_path = root / "dbt-projects" / "spark" / "models" / "only-override.sql"
            duckdb_override = root / "duckdb" / "models" / "only-override.sql"
            duckdb_override.parent.mkdir(parents=True)
            duckdb_override.write_text(
                "-- catalog-skip: intentionally unsupported\n",
                encoding="utf-8",
            )
            with (
                patch(
                    "sql_catalog.generator._load_engines",
                    return_value={"spark": "spark", "duckdb": "duckdb"},
                ),
                patch("sql_catalog.generator._catalog_entries", return_value=[("models", "only-override")]),
                patch("sql_catalog.generator._override_names", return_value=[]),
                patch("sql_catalog.generator._catalog_path", return_value=root / "missing.sql"),
                patch("sql_catalog.generator.DBT_PROJECT_ROOT", root),
                patch(
                    "sql_catalog.generator._override_path",
                    side_effect=lambda engine, category, name: (
                        duckdb_override if engine == "duckdb" else root / "spark" / "missing.sql"
                    ),
                ),
                patch("sql_catalog.generator._destination", return_value=root / "generated.sql"),
                patch(
                    "sql_catalog.generator._all_project_sql",
                    return_value=[("spark", "models", "only-override", project_path)],
                ),
            ):
                errors = check_catalog()
        self.assertIn(
            f"{project_path}: project SQL has no canonical source or engine override",
            errors,
        )

    def test_inline_placeholders_and_jinja_survive_transpilation(self):
        source = (
            "SELECT {value} AS value, %s AS positional, %(named)s AS named, "
            ":bound AS bound, ? AS qmark, {{ ref('source') }} AS source\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inline.sql"
            path.write_text(source, encoding="utf-8")
            with (
                patch("sql_catalog.generator._catalog_path", return_value=path),
                patch(
                    "sql_catalog.generator._override_path",
                    return_value=Path(directory) / "missing.sql",
                ),
            ):
                rendered = load_sql("duckdb", "inline", "scripts/example", value=7)
        self.assertIn("7 AS value", rendered)
        self.assertIn("%s", rendered)
        self.assertIn("%(named)s AS named", rendered)
        self.assertIn(":bound AS bound", rendered)
        self.assertIn("? AS qmark", rendered)
        self.assertIn("{{ ref('source') }}", rendered)
        self.assertFalse(rendered.endswith("\n"))

    def test_inline_removes_only_one_trailing_newline(self):
        source = "SELECT 1\n\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inline.sql"
            path.write_text(source, encoding="utf-8")
            with (
                patch("sql_catalog.generator._catalog_path", return_value=path),
                patch(
                    "sql_catalog.generator._override_path",
                    return_value=Path(directory) / "missing.sql",
                ),
            ):
                self.assertEqual("SELECT 1\n", load_sql("spark", "inline", "scripts/example"))

    def test_multistatement_and_opaque_command_sql_are_supported(self):
        rendered = _transpile_sql("SELECT 1; SELECT 2;\n", "duckdb")
        self.assertEqual(2, rendered.count("SELECT"))
        self.assertTrue(rendered.rstrip().endswith(";"))
        self.assertIn("DESCRIBE HISTORY source", _transpile_sql("DESCRIBE HISTORY source", "duckdb"))

    def test_desc_null_order_is_preserved_for_postgres(self):
        source = "SELECT x FROM t ORDER BY x DESC"
        rendered = _transpile_sql(source, "postgres")
        self.assertIn("NULLS LAST", rendered)
        self.assertEqual(
            _normalized_sql(source, "spark"),
            _normalized_sql(rendered, "postgres"),
        )

    def test_jinja_control_blocks_require_overrides_for_other_dialects(self):
        source = "select * from source\n{% if condition %}\nwhere id = 1\n{% endif %}\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "model.sql"
            path.write_text(source, encoding="utf-8")
            with (
                patch("sql_catalog.generator._catalog_path", return_value=path),
                patch(
                    "sql_catalog.generator._override_path",
                    side_effect=lambda engine, category, name: (
                        Path(directory) / "overrides" / engine / category / f"{name}.sql"
                    ),
                ),
            ):
                self.assertEqual(source, load_sql("spark", "models", "example"))
                with self.assertRaisesRegex(ValueError, "overrides/duckdb/models/example.sql"):
                    load_sql("duckdb", "models", "example")

    def test_override_headers_and_skip_markers_are_parsed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "override.sql"
            path.write_text(
                "-- catalog-override: uses engine-specific syntax\nSELECT 1\n",
                encoding="utf-8",
            )
            override = _parse_override(path)
            self.assertEqual("uses engine-specific syntax", override["reason"])
            self.assertEqual("SELECT 1\n", override["sql"])
            path.write_text("-- catalog-skip: intentionally unsupported\n", encoding="utf-8")
            skipped = _parse_override(path)
            self.assertTrue(skipped["skip"])
            self.assertIsNone(skipped["sql"])

    def test_line_exemption_requires_a_reason_and_supports_both_positions(self):
        lines = [
            "# sql-catalog: exempt existing native statement",
            "cursor.execute('SELECT value')",
            "cursor.execute('SELECT other')  # sql-catalog: exempt inline reason",
            "# sql-catalog: exempt",
            "cursor.execute('SELECT missing_reason')",
        ]
        self.assertEqual("existing native statement", _line_marker(lines, 1))
        self.assertEqual("inline reason", _line_marker(lines, 2))
        self.assertEqual("", _line_marker(lines, 4))

    def test_loose_sql_detector_covers_engine_commands(self):
        for line in (
            "cursor.execute('DESCRIBE HISTORY source')",
            "cursor.execute('REFRESH MATERIALIZED VIEW target')",
            "cursor.execute('PRAGMA refresh_options(...)')",
            "cursor.execute('DROP TABLE IF EXISTS target')",
        ):
            with self.subTest(line=line):
                self.assertTrue(is_sql_candidate(line))

    def test_loader_rejects_path_traversal(self):
        with self.assertRaises(ValueError):
            load_sql("spark", "models", "../outside")


if __name__ == "__main__":
    unittest.main()
