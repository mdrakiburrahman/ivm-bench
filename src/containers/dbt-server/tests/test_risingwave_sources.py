"""Behavior tests for the RisingWave source-loading paths."""

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

SOURCE_MODULE = DBT_SERVER / "services" / "risingwave_sources.py"
SPEC = importlib.util.spec_from_file_location("risingwave_sources_under_test", SOURCE_MODULE)
sources = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sources
SPEC.loader.exec_module(sources)


class FakeInsertCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


class RisingWaveSourceLoaderTest(unittest.TestCase):
    def test_bulk_scan_materializes_cdc_prefix_columns(self):
        spec = sources.LoadSpec(
            "staging_trade",
            "SELECT t_id FROM delta_scan('/data/trade')",
            extra_cols=(("cdc_flag", "VARCHAR"), ("cdc_dsn", "BIGINT")),
            prefix_values=(None, None),
        )

        sql = sources._bulk_scan_sql(spec)

        self.assertIn('CAST(NULL AS VARCHAR) AS "cdc_flag"', sql)
        self.assertIn('CAST(NULL AS BIGINT) AS "cdc_dsn"', sql)
        self.assertIn("source.*", sql)

    def test_external_source_uses_parquet_and_s3_compatible_endpoint(self):
        sql = sources._external_source_sql(
            "load_trade",
            [("id", "BIGINT"), ("amount", "DECIMAL")],
            "tpcdi/trade/run-id",
        )

        self.assertIn("CREATE SOURCE", sql)
        self.assertIn("connector = 's3'", sql)
        self.assertIn("match_pattern = 'tpcdi/trade/run-id/*.parquet'", sql)
        self.assertIn("s3.endpoint_url = 'http://minio:9000'", sql)
        self.assertIn("FORMAT PLAIN ENCODE PARQUET", sql)

    def test_unlicensed_locality_backfill_is_disabled_by_default(self):
        project = (DBT_SERVER / "dbt-projects" / "risingwave" / "dbt_project.yml").read_text()
        compose = (DBT_SERVER.parents[2] / "docker" / "docker-compose.benchmark.risingwave.yml").read_text()

        self.assertEqual(project.count("RISINGWAVE_LOCALITY_BACKFILL', 'false'"), 3)
        self.assertIn('RISINGWAVE_LOCALITY_BACKFILL:-false', compose)

    def test_initial_load_falls_back_to_pgwire_before_returning(self):
        spec = sources.LoadSpec("staging_trade", "SELECT 1")
        with (
            patch.object(sources, "RW_BULK_LOAD", True),
            patch.object(sources, "RW_BULK_FALLBACK", True),
            patch.object(sources, "_bulk_load_spec", side_effect=RuntimeError("no s3")),
            patch.object(sources, "_pgwire_load_spec", return_value=17) as fallback,
            patch.object(sources.logger, "exception"),
        ):
            result = sources._initial_load_spec(spec)

        self.assertEqual(result[:3], ("staging_trade", 17, "pgwire"))
        self.assertGreaterEqual(result[3], 0)
        fallback.assert_called_once_with(spec)

    def test_bulk_failure_is_not_hidden_when_fallback_is_disabled(self):
        spec = sources.LoadSpec("staging_trade", "SELECT 1")
        with (
            patch.object(sources, "RW_BULK_LOAD", True),
            patch.object(sources, "RW_BULK_FALLBACK", False),
            patch.object(sources, "_bulk_load_spec", side_effect=RuntimeError("bad parquet")),
        ):
            with self.assertRaisesRegex(RuntimeError, "bad parquet"):
                sources._initial_load_spec(spec)

    def test_pgwire_fallback_respects_parameter_cap(self):
        cursor = FakeInsertCursor()
        rows = [(idx, idx + 1, idx + 2) for idx in range(5)]
        with patch.object(sources, "MAX_BIND_PARAMS", 6):
            inserted = sources._insert_rows(cursor, "t", ["a", "b", "c"], rows)

        self.assertEqual(inserted, 5)
        self.assertEqual(len(cursor.calls), 3)
        self.assertEqual([len(params) for _, params in cursor.calls], [6, 6, 3])

    def test_parallel_loader_returns_every_result(self):
        specs = [sources.LoadSpec(f"t{idx}", "SELECT 1") for idx in range(6)]
        with patch.object(sources, "RW_LOAD_WORKERS", 4):
            result = sources._parallel_load(specs, lambda spec: spec.table)

        self.assertCountEqual(result, [spec.table for spec in specs])


if __name__ == "__main__":
    unittest.main()
