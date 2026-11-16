"""Behavior tests for the RisingWave source-loading paths."""

import importlib.util
import subprocess
import sys
import unittest
from pathlib import Path
from types import ModuleType
from unittest.mock import Mock, patch

DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

SOURCE_MODULE = DBT_SERVER / "services" / "risingwave_sources.py"
SPEC = importlib.util.spec_from_file_location("risingwave_sources_under_test", SOURCE_MODULE)
sources = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sources
SPEC.loader.exec_module(sources)


class RisingWaveSourceLoaderTest(unittest.TestCase):
    def test_source_init_endpoint_is_registered(self):
        import databricks

        sql_module = ModuleType("databricks.sql")
        sql_module.connect = Mock()
        import services.db as db

        with patch.object(databricks, "sql", sql_module, create=True), patch.object(db, "init_db"):
            from app import app

        routes = {rule.rule for rule in app.url_map.iter_rules()}

        self.assertIn("/sources/risingwave/init", routes)

    def test_timestamp_projection_uses_utc_independent_of_session_timezone(self):
        cursor = Mock()
        cursor.fetchall.return_value = [("event_ts", "timestamp with time zone"), ("id", "bigint")]
        self.assertEqual(
            sources._source_projection(cursor, "fixture", False),
            ['("event_ts" AT TIME ZONE \'UTC\') AS "event_ts"', '"id"'],
        )
        self.assertIn('("_ActionTS" AT TIME ZONE \'UTC\') AS "action_ts"', sources._customer_projection())

    def test_unlicensed_locality_backfill_is_disabled_by_default(self):
        project = (DBT_SERVER / "dbt-projects" / "risingwave" / "dbt_project.yml").read_text()
        compose = (DBT_SERVER.parents[2] / "docker" / "docker-compose.benchmark.risingwave.yml").read_text()

        self.assertEqual(project.count("RISINGWAVE_LOCALITY_BACKFILL', 'false'"), 3)
        self.assertIn('RISINGWAVE_LOCALITY_BACKFILL:-false', compose)

    def test_ci_builds_exact_patched_risingwave_image(self):
        repo_root = DBT_SERVER.parents[2]
        risingwave_dir = repo_root / "src" / "containers" / "risingwave"
        compose = (repo_root / "docker" / "docker-compose.benchmark.risingwave.yml").read_text()
        workflow = (repo_root / ".github" / "workflows" / "ci.yaml").read_text()

        image = subprocess.check_output(
            ["bash", str(risingwave_dir / "build-pinned-image.sh"), "--print-image"],
            text=True,
        ).strip()

        self.assertEqual(
            image,
            "ivm-bench/risingwave:fd769195b25fb663c80b85d753d5e14bf170c74d",
        )
        self.assertEqual(compose.count(f"${{RISINGWAVE_IMAGE:-{image}}}"), 2)
        self.assertIn("build-pinned-image.sh", workflow)

    def test_snapshot_failure_is_not_hidden_by_a_fallback(self):
        spec = sources.LoadSpec("staging_trade", "batch1", "trade")
        connection = Mock()
        with (
            patch.object(sources, "_connect", return_value=connection),
            patch.object(sources, "_prepare_session"),
            patch.object(sources, "_validate_snapshot", side_effect=RuntimeError("snapshot changed")),
        ):
            with self.assertRaisesRegex(RuntimeError, "snapshot changed"):
                sources._load_one(spec, create=True)
        connection.close.assert_called_once()

    def test_parallel_loader_returns_every_result(self):
        specs = [sources.LoadSpec(f"t{idx}", "batch1", f"t{idx}") for idx in range(6)]
        with patch.object(sources, "RW_LOAD_WORKERS", 4):
            result = sources._parallel_load(specs, lambda spec: spec.table)

        self.assertCountEqual(result, [spec.table for spec in specs])

    def test_failed_append_copy_is_not_replayed(self):
        spec = sources.LoadSpec("staging_trade", "batch2", "trade")
        connection = Mock()
        connection.cursor.return_value.fetchall.return_value = [("id",)]
        operations = []

        def execute(cur, name, **params):
            operations.append(name)
            if name == "insert_iceberg":
                raise RuntimeError("copy failed after unknown progress")

        with (
            patch.object(sources, "_connect", return_value=connection),
            patch.object(sources, "_prepare_session"),
            patch.object(sources, "_validate_snapshot", return_value=123),
            patch.object(sources, "_open_source", return_value=("iceberg_source", 1, ['"id"'])),
            patch.object(sources, "_execute", side_effect=execute),
        ):
            with self.assertRaisesRegex(RuntimeError, "unknown progress"):
                sources._load_one(spec, create=False)
        self.assertEqual(operations, ["list_columns", "insert_iceberg", "drop_source"])
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
