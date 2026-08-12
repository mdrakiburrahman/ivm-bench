import csv
import stat
import sys
import tempfile
import unittest
from pathlib import Path


BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.engine_runner import EngineRunner  # noqa: E402


class CompilerBenchSummaryCsvTest(unittest.TestCase):
    def test_combined_summary_upserts_one_sorted_row_per_result_set(self):
        columns = ["engine", "corpus", "timeout", "verify_failed"]
        with tempfile.TemporaryDirectory() as directory:
            EngineRunner._upsert_compiler_bench_summary(
                directory,
                "feldera",
                "native",
                columns,
                {"engine": "feldera", "corpus": 2505, "timeout": 0,
                 "verify_failed": 7},
            )
            EngineRunner._upsert_compiler_bench_summary(
                directory,
                "duckdb",
                "native",
                columns,
                {"engine": "duckdb", "corpus": 2505, "timeout": 1,
                 "verify_failed": 4},
            )
            # A rerun replaces its row rather than adding a duplicate.
            EngineRunner._upsert_compiler_bench_summary(
                directory,
                "duckdb",
                "native",
                columns,
                {"engine": "duckdb", "corpus": 2505, "timeout": 2,
                 "verify_failed": 3},
            )
            with (Path(directory) / "summary.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            mode = stat.S_IMODE((Path(directory) / "summary.csv").stat().st_mode)

        self.assertEqual(
            [row["result_set"] for row in rows], ["duckdb", "feldera"]
        )
        self.assertEqual(rows[0]["timeout"], "2")
        self.assertEqual(rows[0]["verify_failed"], "3")
        self.assertEqual(mode, 0o644)


if __name__ == "__main__":
    unittest.main()
