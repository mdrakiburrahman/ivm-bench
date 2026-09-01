import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.fabric import get_token  # noqa: E402


class FabricAuthTest(unittest.TestCase):
    @patch("services.fabric._start_keepwarm")
    @patch("services.fabric.subprocess.run")
    def test_token_failure_forces_login_and_retries(self, run, _start_keepwarm):
        run.side_effect = [
            subprocess.CompletedProcess([], 0),
            subprocess.CompletedProcess([], 1, stderr="expired"),
            subprocess.CompletedProcess([], 0),
            subprocess.CompletedProcess([], 0, stdout="token\n"),
        ]

        self.assertEqual(get_token("resource", warm_livy=False), "token")
        self.assertEqual(run.call_args_list[2].args[0][:3], ["az", "login", "--identity"])


if __name__ == "__main__":
    unittest.main()
