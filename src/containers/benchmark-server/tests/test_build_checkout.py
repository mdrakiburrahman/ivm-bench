import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.docker_manager import DockerManager


class BuildCheckoutTest(unittest.TestCase):
    def test_spark_build_fetches_a_pin_outside_branch_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source, checkout = root / "source", root / "checkout"
            source.mkdir()
            checkout.mkdir()

            def git(*args):
                return subprocess.check_output(["git", *args], cwd=source, text=True,
                                               stderr=subprocess.DEVNULL).strip()

            git("init", "-b", "main")
            git("config", "user.name", "Checkout Test")
            git("config", "user.email", "checkout@example.invalid")
            (source / "payload").write_text("main")
            git("add", "payload")
            git("commit", "-m", "main")
            git("checkout", "--detach")
            (source / "payload").write_text("pinned")
            git("commit", "-am", "pin outside main")
            pin = git("rev-parse", "HEAD")
            git("checkout", "main")
            self.assertNotEqual(git("rev-parse", "HEAD"), pin)

            # Execute the actual Docker instruction against a local Git remote.
            dockerfile = BENCHMARK_SERVER.parent / "spark-openivm-build/Dockerfile"
            stage = dockerfile.read_text().split("AS spark-ext-builder\n", 1)[1]
            instruction = stage.split("WORKDIR /src\n\n", 1)[1].split("\n\n", 1)[0]
            command = instruction.removeprefix("RUN ").replace("\\\n", "")
            subprocess.run(["sh", "-c", command], cwd=checkout, check=True,
                           env={**os.environ, "OPENIVM_SPARK_REPO": str(source),
                                "OPENIVM_SPARK_COMMIT": pin}, capture_output=True, text=True)
            head = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=checkout, text=True).strip()
            self.assertEqual(head, pin)
            self.assertEqual((checkout / "payload").read_text(), "pinned")

    def test_compose_failure_reports_stdout_and_stderr_tails(self):
        result = subprocess.CompletedProcess([], 128,
            "build chatter\n" * 1000 + "fatal: reference is not a tree: pinned-sha",
            "warning\n" * 1000 + "returned a non-zero code: 128")
        manager = DockerManager("docker-compose.yml", "build")
        with patch("services.docker_manager.subprocess.run", return_value=result):
            with self.assertRaises(RuntimeError) as caught:
                manager.build()
        message = str(caught.exception)
        self.assertIn("fatal: reference is not a tree: pinned-sha", message)
        self.assertIn("returned a non-zero code: 128", message)
        self.assertLess(len(message), 8500)


if __name__ == "__main__":
    unittest.main()
