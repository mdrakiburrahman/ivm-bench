import importlib
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


DBT_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER))

from services.source_cache import batch_cache_root  # noqa: E402


class CloudSourceCacheTest(unittest.TestCase):
    def test_cache_identity_includes_batch_percentage(self):
        with patch.dict(os.environ, {"BATCH_2_INSERT_PCT": "5"}):
            five = batch_cache_root("cache", 100, 2)
        with patch.dict(os.environ, {"BATCH_2_INSERT_PCT": "25"}):
            twenty_five = batch_cache_root("cache", 100, 2)

        self.assertEqual(five, "cache/sf=100/batch2_pct=5")
        self.assertEqual(twenty_five, "cache/sf=100/batch2_pct=25")
        self.assertNotEqual(five, twenty_five)

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
                patch.object(self.sources, "STAGING_TABLES", ["trade"]),
                patch.object(self.sources, "_workspace_client", return_value=Mock()),
                patch.object(self.sources, "_ensure_cache_schema"),
                patch.object(self.sources, "_seed_cache_batch", return_value=(0, True)),
                patch.object(self.sources, "data_schema", return_value="exp_data"),
                patch.object(self.sources, "_execute", execute),
            ):
                result = self.sources.append_sources(2, 100)

        self.assertEqual(result["tables_appended"], 1)
        self.assertIn("batch2_pct=25/staging_batch2/trade", execute.call_args.args[0])

    def test_append_rejects_empty_generated_batch(self):
        with tempfile.TemporaryDirectory() as raw, patch.dict(
            os.environ,
            {"BATCH_2_INSERT_PCT": "25", "DATABRICKS_EXPERIMENT_ID": "1"},
        ):
            with (
                patch.object(self.sources, "RAW_DELTA_DIR", raw),
                patch.object(self.sources, "STAGING_TABLES", ["trade"]),
                patch.object(self.sources, "_workspace_client", return_value=Mock()),
                patch.object(self.sources, "_ensure_cache_schema"),
                patch.object(self.sources, "_seed_cache_batch", return_value=(0, True)),
                patch.object(self.sources, "data_schema", return_value="exp_data"),
                patch.object(self.sources, "_execute"),
            ):
                with self.assertRaisesRegex(RuntimeError, "found no local Delta tables"):
                    self.sources.append_sources(2, 100)


if __name__ == "__main__":
    unittest.main()
