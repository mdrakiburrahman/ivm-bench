import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from services import storage_metrics


class MaterializeStorageTests(unittest.TestCase):
    def test_collection_labels_live_scan_as_approximate(self):
        totals = storage_metrics._empty_totals()
        totals.update(source_bytes=2, visible_output_bytes=3, total_bytes=5)
        with (
            patch.object(storage_metrics, "_materialize_storage", return_value=([], totals, [])),
            patch.object(storage_metrics, "_base_table_summary", return_value={}),
        ):
            result = storage_metrics.collect_storage_metrics("materialize-emulator", 1)
        self.assertEqual(result["status"], "ok")
        self.assertIn("non-atomic", result["measurement_semantics"])

    def test_classifies_physical_shards_and_scratch_once(self):
        with tempfile.TemporaryDirectory() as directory:
            run_id = "a" * 32
            root = Path(directory) / run_id
            blobs = root / "mzdata" / "persist" / "blob"
            scratch = root / "scratch"
            blobs.mkdir(parents=True)
            scratch.mkdir()
            (blobs / "s-source\u2215v1").write_bytes(b"aa")
            (blobs / "s-output\u2215v1").write_bytes(b"bbb")
            (blobs / "s-output\u2215pending.tmp").write_bytes(b"fff")
            (blobs / "s-unknown\u2215v1").write_bytes(b"cccc")
            (scratch / "spill").write_bytes(b"ddddd")
            (root / "mzdata" / "catalog").write_bytes(b"eeeeee")
            connection = MagicMock()
            cursor = connection.cursor.return_value.__enter__.return_value
            cursor.fetchall.return_value = [
                ("u1", "staging_trade", "table", "tpcdi", "s-source"),
                ("u2", "dim_trade", "materialized-view", "gold", "s-output"),
            ]
            with (
                patch.dict(os.environ, {"MATERIALIZE_EMULATOR_RUN_ID": run_id}),
                patch.dict(storage_metrics.PROCESSED_ROOTS, {"materialize-emulator": Path(directory)}),
                patch("psycopg2.connect", return_value=connection),
                patch("services.materialize_fence.expected_models", return_value=[{"schema": "gold", "alias": "dim_trade"}]),
            ):
                _, totals, errors = storage_metrics._materialize_storage(time.monotonic() + 30)
            self.assertEqual(errors, [])
            self.assertEqual(totals["source_bytes"], 2)
            self.assertEqual(totals["visible_output_bytes"], 3)
            self.assertEqual(totals["helper_data_bytes"], 12)
            self.assertEqual(totals["metadata_bytes"], 6)
            self.assertEqual(totals["total_bytes"], 23)

    def test_retries_disappearing_files_without_double_counting(self):
        for failures in (1, 3):
            with self.subTest(failures=failures), tempfile.TemporaryDirectory() as directory:
                run_id = "a" * 32
                root = Path(directory) / run_id
                blobs = root / "mzdata" / "persist" / "blob"
                blobs.mkdir(parents=True)
                (root / "scratch").mkdir()
                (blobs / "s-source\u2215v1").write_bytes(b"aa")
                output = blobs / "s-output\u2215v1"
                output.write_bytes(b"bbb")
                connection = MagicMock()
                cursor = connection.cursor.return_value.__enter__.return_value
                cursor.fetchall.return_value = [
                    ("u1", "staging_trade", "table", "tpcdi", "s-source"),
                    ("u2", "dim_trade", "materialized-view", "gold", "s-output"),
                ]
                real_stat = Path.stat
                attempts = 0

                def flaky_stat(path, *args, **kwargs):
                    nonlocal attempts
                    if path == output:
                        attempts += 1
                        if attempts <= failures:
                            raise FileNotFoundError("compaction removed file")
                    return real_stat(path, *args, **kwargs)

                with (
                    patch.dict(os.environ, {"MATERIALIZE_EMULATOR_RUN_ID": run_id}),
                    patch.dict(storage_metrics.PROCESSED_ROOTS, {"materialize-emulator": Path(directory)}),
                    patch("psycopg2.connect", return_value=connection),
                    patch("services.materialize_fence.expected_models", return_value=[{"schema": "gold", "alias": "dim_trade"}]),
                    patch.object(Path, "stat", autospec=True, side_effect=flaky_stat),
                ):
                    _, totals, errors = storage_metrics._materialize_storage(time.monotonic() + 30)
                self.assertEqual(attempts, min(failures + 1, 3))
                if failures == 1:
                    self.assertEqual(errors, [])
                    self.assertEqual(totals["total_bytes"], 5)
                else:
                    self.assertEqual(len(errors), 1)
                    self.assertIn("cannot stat Materialize file", errors[0])


if __name__ == "__main__":
    unittest.main()
