import json
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from services import dbt_runner


class DbtFailureReportingTest(unittest.TestCase):
    def test_stdout_errors_survive_nonzero_exit_with_empty_stderr(self):
        for returncode in (1, 2):
            with self.subTest(returncode=returncode):
                process = Mock(returncode=returncode)
                process.stdout = iter([json.dumps({"info": {"msg": "Database Error: No active Spark session"}}) + "\n"])
                process.stderr = iter([])
                with patch.dict(dbt_runner.os.environ, {"BATCH_1_INSERT_PCT": "100"}), \
                     patch.object(dbt_runner.subprocess, "Popen", return_value=process), \
                     patch.object(dbt_runner.os.path, "isdir", return_value=True), \
                     patch.object(dbt_runner.os, "makedirs"), \
                     patch.object(dbt_runner, "get_db", return_value=Mock()), \
                     patch.object(dbt_runner, "_parse_results", return_value={"nodes": 0, "errors": 0}), \
                     patch.object(dbt_runner, "_fail_run") as fail:
                    dbt_runner.run_dbt("test-run", "fabric-openivm-jvm-35", 10, True)
                self.assertIn("Database Error: No active Spark session", fail.call_args.args[1])
                self.assertIn(f"dbt exited {returncode}", fail.call_args.args[1])
