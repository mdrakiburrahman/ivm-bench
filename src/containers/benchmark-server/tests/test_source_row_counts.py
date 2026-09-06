import csv
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.source_row_counts import collect_source_row_counts  # noqa: E402
from services.oat_runner import generate_results_csv  # noqa: E402


class SourceRowCountsTest(unittest.TestCase):
    def test_counts_only_active_delta_files(self):
        with tempfile.TemporaryDirectory() as root:
            log_dir = Path(root, "batch2", "trade", "_delta_log")
            log_dir.mkdir(parents=True)
            actions = [
                {
                    "add": {
                        "path": "a.parquet",
                        "stats": json.dumps({"numRecords": 5}),
                    }
                },
                {
                    "add": {
                        "path": "b.parquet",
                        "stats": json.dumps({"numRecords": 7}),
                    }
                },
            ]
            Path(log_dir, "00000000000000000000.json").write_text(
                "".join(json.dumps(action) + "\n" for action in actions),
                encoding="utf-8",
            )
            Path(log_dir, "00000000000000000001.json").write_text(
                json.dumps({"remove": {"path": "a.parquet"}}) + "\n",
                encoding="utf-8",
            )

            counts = collect_source_row_counts(root)

        self.assertEqual(counts["batches"]["1"]["total_rows"], 0)
        self.assertEqual(counts["batches"]["2"]["total_rows"], 7)
        self.assertEqual(counts["batches"]["2"]["tables"], {"trade": 7})
        self.assertEqual(counts["batches"]["3"]["total_rows"], 0)

    def test_results_csv_repeats_source_rows_for_each_engine_batch(self):
        rendered = generate_results_csv({
            "experiments": [{
                "inputs": {"engines": ["duckdb"]},
                "engines": {"duckdb": {"batches": {}}},
                "source_row_counts": {
                    "batches": {
                        "1": {"total_rows": 100},
                        "2": {"total_rows": 25},
                        "3": {"total_rows": 1},
                    }
                },
            }]
        })
        rows = list(csv.DictReader(io.StringIO(rendered)))

        self.assertEqual([row["source_rows"] for row in rows], ["100", "25", "1"])


if __name__ == "__main__":
    unittest.main()
