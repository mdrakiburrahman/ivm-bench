import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.fabric import ensure_az_login, get_token  # noqa: E402


class FabricAuthTest(unittest.TestCase):
    @patch("services.fabric.time.sleep")
    @patch("services.fabric._start_keepwarm")
    @patch("services.fabric.subprocess.run")
    def test_login_retries_transient_failures(self, run, _start_keepwarm, sleep):
        run.side_effect = [
            subprocess.CompletedProcess([], 1),
            subprocess.CompletedProcess([], 1, stderr="transient"),
            subprocess.CompletedProcess([], 1, stderr="transient"),
            subprocess.CompletedProcess([], 0),
        ]

        ensure_az_login(warm_livy=False)

        self.assertEqual(run.call_count, 4)
        self.assertEqual(sleep.call_args_list, [unittest.mock.call(5), unittest.mock.call(10)])

    @patch("services.fabric.time.sleep")
    @patch("services.fabric.subprocess.run")
    def test_login_exhausts_five_attempts(self, run, sleep):
        run.side_effect = [
            subprocess.CompletedProcess([], 1),
            *[subprocess.CompletedProcess([], 1, stderr="transient") for _ in range(5)],
        ]

        with self.assertRaisesRegex(RuntimeError, "az login --identity"):
            ensure_az_login(warm_livy=False)

        self.assertEqual(run.call_count, 6)
        self.assertEqual(
            sleep.call_args_list,
            [unittest.mock.call(delay) for delay in (5, 10, 20, 40)],
        )

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
