import os
import sqlite3
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path
from unittest import mock


DBT_SERVER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(DBT_SERVER_ROOT))

from services import storage_metrics


def _write_size(path: Path, size: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)


class _Rows:
    def __init__(self, row):
        self._row = row

    def __getitem__(self, index):
        if index != 0:
            raise IndexError(index)
        return self._row


class _Frame:
    def __init__(self, row):
        self.empty = False
        self.iloc = _Rows(row)


class StorageMetricsTests(unittest.TestCase):
    def test_spark_openivm_classifies_backing_views_deltas_and_sources(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _write_size(root / "_ivm/views/gold/orders/data.parquet", 101)
            _write_size(root / "_ivm/views/gold/orders/_delta_log/1.json", 7)
            _write_size(root / "_ivm/view_deltas/gold_orders/data.parquet", 23)
            _write_size(root / "sources/tpcdi/staging_orders/data.parquet", 41)
            _write_size(root / "sources/tpcdi/staging_orders/_delta_log/1.json", 5)
            _write_size(root / "_openivm/rocksdb/state.sst", 19)

            _, totals, errors = storage_metrics._collect_local_root(
                "spark-openivm", root, deadline=time.monotonic() + 10
            )

            self.assertEqual(errors, [])
            self.assertEqual(totals["visible_output_bytes"], 101)
            self.assertEqual(totals["internal_state_bytes"], 42)
            self.assertEqual(totals["source_bytes"], 41)
            self.assertEqual(totals["metadata_bytes"], 12)
            self.assertEqual(totals["total_bytes"], 196)

    def test_ducklake_uses_relation_metadata_for_file_ownership(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data = root / "data"
            _write_size(data / "tpcdi/staging_orders/source.parquet", 11)
            _write_size(data / "gold/openivm_data_orders/output.parquet", 13)
            _write_size(data / "gold/openivm_delta_orders/delta.parquet", 17)
            metadata = root / "openivm.ducklake"
            db = sqlite3.connect(metadata)
            db.executescript(
                """
                CREATE TABLE ducklake_schema (
                    schema_id INTEGER, schema_name TEXT, path TEXT, path_is_relative INTEGER
                );
                CREATE TABLE ducklake_table (
                    table_id INTEGER, schema_id INTEGER, table_name TEXT,
                    path TEXT, path_is_relative INTEGER
                );
                CREATE TABLE ducklake_data_file (
                    table_id INTEGER, path TEXT, path_is_relative INTEGER,
                    file_size_bytes INTEGER
                );
                CREATE TABLE ducklake_delete_file (
                    table_id INTEGER, path TEXT, path_is_relative INTEGER,
                    file_size_bytes INTEGER
                );
                """
            )
            db.executemany(
                "INSERT INTO ducklake_schema VALUES (?, ?, ?, 1)",
                [(1, "tpcdi", "tpcdi/"), (2, "gold", "gold/")],
            )
            db.executemany(
                "INSERT INTO ducklake_table VALUES (?, ?, ?, ?, 1)",
                [
                    (1, 1, "staging_orders", "staging_orders/"),
                    (2, 2, "openivm_data_orders", "openivm_data_orders/"),
                    (3, 2, "openivm_delta_orders", "openivm_delta_orders/"),
                ],
            )
            db.executemany(
                "INSERT INTO ducklake_data_file VALUES (?, ?, 1, ?)",
                [
                    (1, "source.parquet", 11),
                    (2, "output.parquet", 13),
                    (3, "delta.parquet", 17),
                ],
            )
            db.commit()
            db.close()

            _, totals, errors = storage_metrics._collect_ducklake_storage(
                "duckdb-openivm", root, deadline=time.monotonic() + 10
            )

            self.assertEqual(errors, [])
            self.assertEqual(totals["source_bytes"], 11)
            self.assertEqual(totals["visible_output_bytes"], 13)
            self.assertEqual(totals["internal_state_bytes"], 17)
            self.assertEqual(totals["metadata_bytes"], metadata.stat().st_size)

    def test_ducklake_attributes_inlined_relation_pages(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            metadata = root / "metadata.db"
            db = sqlite3.connect(metadata)
            db.executescript(
                """
                CREATE TABLE ducklake_schema (
                    schema_id INTEGER, schema_name TEXT, path TEXT, path_is_relative INTEGER
                );
                CREATE TABLE ducklake_table (
                    table_id INTEGER, schema_id INTEGER, table_name TEXT,
                    path TEXT, path_is_relative INTEGER
                );
                CREATE TABLE ducklake_data_file (
                    table_id INTEGER, path TEXT, path_is_relative INTEGER,
                    file_size_bytes INTEGER
                );
                CREATE TABLE ducklake_delete_file (
                    table_id INTEGER, path TEXT, path_is_relative INTEGER,
                    file_size_bytes INTEGER
                );
                CREATE TABLE ducklake_inlined_data_tables (
                    table_id INTEGER, table_name TEXT, max_row_id INTEGER
                );
                CREATE TABLE ducklake_inlined_data_1_1 (id INTEGER);
                INSERT INTO ducklake_schema VALUES (1, 'tpcdi', 'tpcdi/', 1);
                INSERT INTO ducklake_table VALUES (1, 1, 'staging_orders', 'staging_orders/', 1);
                INSERT INTO ducklake_inlined_data_tables VALUES (1, 'ducklake_inlined_data_1_1', 1);
                INSERT INTO ducklake_inlined_data_1_1 VALUES (1);
                """
            )
            db.commit()
            inline_bytes = db.execute(
                "SELECT SUM(pgsize) FROM dbstat WHERE name = 'ducklake_inlined_data_1_1'"
            ).fetchone()[0]
            db.close()

            _, totals, errors = storage_metrics._collect_ducklake_storage(
                "duckdb", root, deadline=time.monotonic() + 10
            )

            self.assertEqual(errors, [])
            self.assertEqual(totals["source_bytes"], inline_bytes)
            self.assertEqual(totals["total_bytes"], metadata.stat().st_size)
            self.assertEqual(
                totals["metadata_bytes"], metadata.stat().st_size - inline_bytes
            )

    def test_databricks_relation_failure_marks_collection_partial(self):
        fake_src = types.SimpleNamespace(
            CATALOG="catalog",
            data_schema=lambda: "data",
            work_schema=lambda: "work",
        )
        fake_metrics = types.SimpleNamespace(
            list_relations=lambda **_: [
                {"schema": "gold", "name": "good", "table_type": "MANAGED"},
                {"schema": "gold", "name": "bad", "table_type": "MANAGED"},
                {"schema": "gold", "name": "logical", "table_type": "VIEW"},
            ],
            describe_storage=lambda rel, **_: (
                (_ for _ in ()).throw(RuntimeError("boom"))
                if rel["name"] == "bad"
                else _Frame({
                    "sizeInBytes": 10,
                    "numFiles": 1,
                    "location": "remote",
                    "format": "delta",
                })
            ),
        )
        modules = {
            "services.databricks_enzyme_sources": fake_src,
            "services.databricks_enzyme_metrics": fake_metrics,
        }
        with mock.patch.dict(sys.modules, modules):
            _, totals, errors = storage_metrics._databricks_relation_storage(
                deadline=time.monotonic() + 10
            )

        self.assertEqual(totals["visible_output_bytes"], 10)
        self.assertEqual(len(errors), 1)
        self.assertIn("gold.bad", errors[0])

    def test_databricks_relation_failure_propagates_to_endpoint_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            processed = Path(tmp)
            _write_size(processed / "query-plan/plan.json", 3)
            remote_totals = storage_metrics._empty_totals()
            remote_totals["visible_output_bytes"] = 10
            remote_totals["total_bytes"] = 10
            remote_totals["file_count"] = 1
            with mock.patch.dict(
                storage_metrics.PROCESSED_ROOTS,
                {"databricks-enzyme": processed},
            ), mock.patch.object(
                storage_metrics,
                "_databricks_relation_storage",
                return_value=([], remote_totals, ["gold.bad: boom"]),
            ):
                result = storage_metrics.collect_storage_metrics(
                    "databricks-enzyme", batch_num=2
                )

        self.assertEqual(result["status"], "partial")
        self.assertEqual(result["totals"]["visible_output_bytes"], 10)
        self.assertIn("gold.bad", result["errors"][0])

    def test_missing_root_and_expired_deadline_are_not_ok(self):
        missing = Path("/definitely/missing/storage-root")
        _, _, missing_errors = storage_metrics._collect_local_root(
            "spark", missing, deadline=time.monotonic() + 10
        )
        self.assertTrue(missing_errors)

        with tempfile.TemporaryDirectory() as tmp:
            _write_size(Path(tmp) / "table/data.parquet", 1)
            _, _, deadline_errors = storage_metrics._collect_local_root(
                "spark", Path(tmp), deadline=time.monotonic() - 1
            )
        self.assertTrue(any("deadline" in error for error in deadline_errors))

        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.dict(
                storage_metrics.PROCESSED_ROOTS, {"spark": Path(tmp)}
            ):
                empty_result = storage_metrics.collect_storage_metrics("spark")
        self.assertEqual(empty_result["status"], "error")

    def test_fabric_path_classification(self):
        classify = storage_metrics._classify_fabric_path
        self.assertEqual(
            classify("fabric-jvm-35", "Tables/tpcdi/staging_trade/data.parquet"),
            "source",
        )
        self.assertEqual(
            classify("fabric-jvm-35", "Tables/gold/fact_trade/data.parquet"),
            "visible_output",
        )
        self.assertEqual(
            classify(
                "fabric-openivm-jvm-35",
                "Files/_ivm-warehouse/_ivm/views/gold/fact_trade/data.parquet",
            ),
            "visible_output",
        )
        self.assertEqual(
            classify(
                "fabric-openivm-jvm-35",
                "Files/_ivm-warehouse/_ivm/view_deltas/gold_fact_trade/data.parquet",
            ),
            "internal_state",
        )
        self.assertEqual(
            classify("fabric-openivm-jvm-35", "Files/_openivm/state.sst"),
            "internal_state",
        )
        self.assertEqual(
            classify("fabric-jvm-35", "Tables/gold/fact_trade/_delta_log/1.json"),
            "metadata",
        )

    def test_fabric_collector_counts_all_owned_categories(self):
        fake_fabric = types.SimpleNamespace(
            list_storage_paths=lambda **_: [
                {"path": "Tables/tpcdi/staging_trade/data.parquet", "bytes": 11},
                {"path": "Tables/gold/fact_trade/data.parquet", "bytes": 13},
                {"path": "Files/_openivm/state.sst", "bytes": 17},
                {"path": "Tables/gold/fact_trade/_delta_log/1.json", "bytes": 5},
            ]
        )
        with mock.patch.dict(sys.modules, {"services.fabric": fake_fabric}):
            _, totals, errors = storage_metrics._fabric_storage(
                "fabric-jvm-35", deadline=time.monotonic() + 10
            )

        self.assertEqual(errors, [])
        self.assertEqual(totals["source_bytes"], 11)
        self.assertEqual(totals["visible_output_bytes"], 13)
        self.assertEqual(totals["internal_state_bytes"], 17)
        self.assertEqual(totals["metadata_bytes"], 5)


if __name__ == "__main__":
    unittest.main()
