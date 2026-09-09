import importlib
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services import (  # noqa: E402
    ducklake_sources,
    fabric,
    source_cache,
    spark_openivm_sources,
)
from services.source_cache import (  # noqa: E402
    AUGMENTED_STAGING_TABLES,
    batch_cache_root,
    incremental_staging_tables,
)


class CloudSourceCacheTest(unittest.TestCase):
    def test_cache_identity_includes_batch_percentage(self):
        with patch.dict(
            os.environ,
            {"BATCH_2_INSERT_PCT": "5", "TPCDI_BATCH_2_DAYS": "0"},
        ):
            five = batch_cache_root("cache", 100, 2)
        with patch.dict(
            os.environ,
            {"BATCH_2_INSERT_PCT": "25", "TPCDI_BATCH_2_DAYS": "0"},
        ):
            twenty_five = batch_cache_root("cache", 100, 2)

        self.assertEqual(five, "cache/sf=100/batch2_pct=5")
        self.assertEqual(twenty_five, "cache/sf=100/batch2_pct=25")
        self.assertNotEqual(five, twenty_five)

    def test_cache_identity_includes_augmented_window(self):
        with patch.dict(os.environ, {
            "BATCH_1_INSERT_PCT": "100",
            "BATCH_2_INSERT_PCT": "100",
            "TPCDI_BATCH_2_DAYS": "18",
        }):
            batch1_18 = batch_cache_root("cache", 10, 1)
            batch2_18 = batch_cache_root("cache", 10, 2)
        with patch.dict(os.environ, {
            "BATCH_1_INSERT_PCT": "100",
            "BATCH_2_INSERT_PCT": "100",
            "TPCDI_BATCH_2_DAYS": "183",
        }):
            batch1_183 = batch_cache_root("cache", 10, 1)
            batch2_183 = batch_cache_root("cache", 10, 2)

        self.assertEqual(batch1_18, batch1_183)
        self.assertNotEqual(batch2_18, batch2_183)
        self.assertTrue(batch1_18.endswith("batch1_augmented"))
        self.assertTrue(batch2_18.endswith("batch2_augmented_days=18"))
        self.assertTrue(batch2_183.endswith("batch2_augmented_days=183"))

    def test_augmented_workload_has_exactly_seven_daily_tables(self):
        with patch.dict(os.environ, {"TPCDI_BATCH_2_DAYS": "18"}):
            tables = incremental_staging_tables()

        self.assertEqual(tables, AUGMENTED_STAGING_TABLES)
        self.assertNotIn("prospect", tables)
        self.assertNotIn("batch_date", tables)

    def test_generated_batch_rejects_missing_expected_table(self):
        with tempfile.TemporaryDirectory() as raw, patch.dict(
            os.environ, {"TPCDI_BATCH_2_DAYS": "18"},
        ):
            for table in AUGMENTED_STAGING_TABLES[:-1]:
                (Path(raw) / "batch2" / table).mkdir(parents=True)

            with self.assertRaisesRegex(
                RuntimeError, "missing Delta tables: customer",
            ):
                source_cache.generated_batch_dirs(raw, 2)

    def test_legacy_percentage_is_used_when_insert_percentage_is_empty(self):
        with patch.dict(
            os.environ,
            {"BATCH_3_INSERT_PCT": "", "BATCH_3_PCT": "0.01"},
        ):
            root = batch_cache_root("cache/", 100, 3)

        self.assertEqual(root, "cache/sf=100/batch3_pct=0.01")


class DatabricksAppendTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.original_modules = {
            name: sys.modules.get(name)
            for name in (
                "databricks",
                "databricks.sql",
                "databricks.sdk",
                "databricks.sdk.core",
                "databricks.sdk.errors",
                "pandas",
            )
        }
        databricks = types.ModuleType("databricks")
        dbsql = types.ModuleType("databricks.sql")
        sdk = types.ModuleType("databricks.sdk")
        core = types.ModuleType("databricks.sdk.core")
        errors = types.ModuleType("databricks.sdk.errors")
        databricks.sql = dbsql
        sdk.WorkspaceClient = Mock
        core.Config = Mock
        core.oauth_service_principal = Mock
        errors.NotFound = type("NotFound", (Exception,), {})
        sys.modules.update({
            "databricks": databricks,
            "databricks.sql": dbsql,
            "databricks.sdk": sdk,
            "databricks.sdk.core": core,
            "databricks.sdk.errors": errors,
            "pandas": types.ModuleType("pandas"),
        })
        spec = importlib.util.spec_from_file_location(
            "_test_databricks_enzyme_sources",
            DBT_SERVER / "services" / "databricks_enzyme_sources.py",
        )
        if spec is None or spec.loader is None:
            raise RuntimeError("could not load databricks_enzyme_sources")
        cls.sources = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.sources)

    @classmethod
    def tearDownClass(cls):
        for name, module in cls.original_modules.items():
            if module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = module

    def test_append_uses_generated_local_table_and_percentage_cache(self):
        with tempfile.TemporaryDirectory() as raw, patch.dict(
            os.environ,
            {"BATCH_2_INSERT_PCT": "25", "DATABRICKS_EXPERIMENT_ID": "1"},
        ):
            (Path(raw) / "batch2" / "trade").mkdir(parents=True)
            execute = Mock()
            with (
                patch.object(self.sources, "RAW_DELTA_DIR", raw),
                patch.object(
                    self.sources,
                    "generated_batch_dirs",
                    return_value=(("trade", Path(raw) / "batch2" / "trade"),),
                ),
                patch.object(self.sources, "_workspace_client", return_value=Mock()),
                patch.object(self.sources, "_ensure_cache_schema"),
                patch.object(self.sources, "_seed_cache_batch", return_value=(0, True)),
                patch.object(self.sources, "data_schema", return_value="exp_data"),
                patch.object(self.sources, "_execute", execute),
            ):
                result = self.sources.append_sources(2, 100)

        self.assertEqual(result["tables_appended"], 1)
        self.assertIn(
            "INSERT INTO `ivmbenchdbrx`.`exp_data`.`staging_trade` BY NAME",
            execute.call_args.args[0],
        )
        self.assertIn("batch2_pct=25/staging_batch2/trade", execute.call_args.args[0])

    def test_append_rejects_empty_generated_batch(self):
        with tempfile.TemporaryDirectory() as raw, patch.dict(
            os.environ,
            {"BATCH_2_INSERT_PCT": "25", "DATABRICKS_EXPERIMENT_ID": "1"},
        ):
            with (
                patch.object(self.sources, "RAW_DELTA_DIR", raw),
                patch.object(self.sources, "_workspace_client", return_value=Mock()),
                patch.object(self.sources, "_ensure_cache_schema"),
                patch.object(self.sources, "_seed_cache_batch", return_value=(0, True)),
                patch.object(self.sources, "data_schema", return_value="exp_data"),
                patch.object(self.sources, "_execute"),
            ):
                with self.assertRaisesRegex(RuntimeError, "missing Delta tables"):
                    self.sources.append_sources(2, 100)

    def test_cache_seed_never_falls_back_to_cumulative_staging(self):
        with tempfile.TemporaryDirectory() as raw, patch.dict(os.environ, {
            "BATCH_2_INSERT_PCT": "100",
            "TPCDI_BATCH_2_DAYS": "18",
        }):
            for table in AUGMENTED_STAGING_TABLES:
                (Path(raw) / "batch2" / table).mkdir(parents=True)
            (Path(raw) / "staging" / "prospect").mkdir(parents=True)
            upload = Mock(return_value=1)
            with (
                patch.object(self.sources, "RAW_DELTA_DIR", raw),
                patch.object(self.sources, "_file_exists", return_value=False),
                patch.object(self.sources, "_upload_dir", upload),
                patch.object(self.sources, "_upload_bytes"),
            ):
                result = self.sources._seed_cache_batch(Mock(), 10, 2)

        self.assertEqual(result, (len(AUGMENTED_STAGING_TABLES), False))
        self.assertEqual(upload.call_count, len(AUGMENTED_STAGING_TABLES))
        self.assertTrue(all(
            call.args[1].parent.name == "batch2"
            for call in upload.call_args_list
        ))


class NamedAppendTest(unittest.TestCase):
    def test_ducklake_append_matches_columns_by_name(self):
        with tempfile.TemporaryDirectory() as raw:
            source = Path(raw) / "batch2" / "account" / "part.parquet"
            source.parent.mkdir(parents=True)
            source.touch()
            execute = Mock()

            ducklake_sources.append_sources(
                Path(raw), Path(raw) / "work", 2, execute, "duckdb", (),
            )

        sql = execute.call_args.args[0]
        self.assertIn(
            'INSERT INTO "ducklake"."tpcdi"."staging_account" BY NAME', sql,
        )

    def test_spark_openivm_append_matches_columns_by_name(self):
        with tempfile.TemporaryDirectory() as raw:
            (Path(raw) / "batch2" / "account").mkdir(parents=True)
            client = MagicMock()
            client.__enter__.return_value = client
            with (
                patch.object(spark_openivm_sources, "RAW_DELTA_DIR", raw),
                patch.object(spark_openivm_sources, "STAGING_TABLES", ["account"]),
                patch.object(spark_openivm_sources, "LivyClient", return_value=client),
            ):
                spark_openivm_sources.append_sources(2)

        self.assertEqual(
            client.execute_many.call_args.args[0],
            [
                "INSERT INTO tpcdi.staging_account BY NAME "
                f"SELECT * FROM delta.`{raw}/batch2/account`"
            ],
        )


class FabricBatchCacheTest(unittest.TestCase):
    def test_cache_seed_never_falls_back_to_cumulative_staging(self):
        with tempfile.TemporaryDirectory() as raw, patch.dict(os.environ, {
            "BATCH_2_INSERT_PCT": "100",
            "TPCDI_BATCH_2_DAYS": "18",
        }):
            for table in AUGMENTED_STAGING_TABLES:
                (Path(raw) / "batch2" / table).mkdir(parents=True)
            (Path(raw) / "staging" / "prospect").mkdir(parents=True)
            upload = Mock(return_value=1)
            with (
                patch.object(fabric, "RAW_DELTA_DIR", raw),
                patch.object(fabric, "resolve_cache_lakehouse", return_value="cache"),
                patch.object(fabric, "_dfs_exists", return_value=False),
                patch.object(fabric, "_azcopy", upload),
                patch.object(fabric, "_dfs_put_marker"),
            ):
                result = fabric.seed_cache_batch(10, 2)

        self.assertEqual(result["files_uploaded"], len(AUGMENTED_STAGING_TABLES))
        self.assertEqual(upload.call_count, len(AUGMENTED_STAGING_TABLES))
        self.assertTrue(all(
            call.args[0].parent.name == "batch2"
            for call in upload.call_args_list
        ))


if __name__ == "__main__":
    unittest.main()
