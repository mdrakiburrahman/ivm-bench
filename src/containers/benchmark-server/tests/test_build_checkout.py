import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from services.docker_manager import DockerManager
from services.orchestrator import _spark_builder_source_hash


class BuildCheckoutTest(unittest.TestCase):
    def test_spark_builder_cache_key_handles_optional_patch(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            dockerfile = root / "Dockerfile"
            patches = root / "patches"
            patch_file = patches / "compiler.patch"
            dockerfile.write_text("FROM scratch\n")
            no_patch = _spark_builder_source_hash(str(dockerfile))
            patches.mkdir()
            patch_file.write_text("first revision\n")
            first = _spark_builder_source_hash(str(dockerfile))
            self.assertNotEqual(no_patch, first)
            patch_file.write_text("second revision\n")
            self.assertNotEqual(first, _spark_builder_source_hash(str(dockerfile)))
            patch_file.write_text("first revision\n")
            self.assertEqual(first, _spark_builder_source_hash(str(dockerfile)))
            dockerfile.write_text("FROM busybox\n")
            self.assertNotEqual(first, _spark_builder_source_hash(str(dockerfile)))
    def test_published_port_accepts_ipv4_and_ipv6_bindings(self):
        manager = DockerManager("docker-compose.yml")
        with patch.object(
            manager,
            "_run",
            return_value=subprocess.CompletedProcess(
                [], 0, "0.0.0.0:32768\n[::]:32768\n", ""
            ),
        ) as run:
            self.assertEqual(manager.published_port("dbt-server", 5000), 32768)
        run.assert_called_once_with(["port", "dbt-server", "5000"])

    def test_published_port_rejects_missing_or_ambiguous_mappings(self):
        manager = DockerManager("docker-compose.yml")
        for output in ("", "0.0.0.0:0\n", "0.0.0.0:32768\n[::]:32769\n"):
            with self.subTest(output=output), patch.object(
                manager,
                "_run",
                return_value=subprocess.CompletedProcess([], 0, output, ""),
            ):
                with self.assertRaises(RuntimeError):
                    manager.published_port("dbt-server", 5000)

    def test_run_scoped_compose_overlay(self):
        run_id = "a" * 32
        config = {"services": {"builder": {"build": {"context": "."}}, "worker": {"image": "example"}},
                  "networks": {"default": {}, "external": {"external": True}}}
        result = subprocess.CompletedProcess([], 0, json.dumps(config), "")
        manager = DockerManager("docker-compose.yml", "original", env={"IVM_BENCH_RUN_ID": run_id})
        with patch("services.docker_manager.subprocess.run", return_value=result) as run:
            cmd = manager._base_cmd
            self.assertEqual(manager._base_cmd, cmd)
        overlay_path = Path(cmd[cmd.index("-f", 4) + 1])
        try:
            overlay = json.loads(overlay_path.read_text())
            self.assertEqual(cmd[-2:], ["-p", f"ivm-{run_id}-original"])
            self.assertEqual(overlay["services"]["builder"]["image"], "original-builder")
            self.assertNotIn("image", overlay["services"]["worker"])
            self.assertEqual(overlay["services"]["worker"]["labels"]["org.ivm-bench.run-id"], run_id)
            self.assertEqual(overlay["networks"], {"default": {"labels": {"org.ivm-bench.run-id": run_id}}})
            run.assert_called_once()
        finally:
            overlay_path.unlink()
        with self.assertRaises(ValueError):
            DockerManager("docker-compose.yml", env={"IVM_BENCH_RUN_ID": "invalid"})

    def test_interrupted_server_cleans_only_run_resources_and_preserves_injected_relay_url(self):
        source_root = BENCHMARK_SERVER.parents[2]
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            root = tmp / "repo"
            (root / "src/.scripts").mkdir(parents=True)
            (root / "tools/scripts").mkdir(parents=True)
            shutil.copyfile(source_root / "src/.scripts/benchmark.sh", root / "src/.scripts/benchmark.sh")
            shutil.copyfile(source_root / "tools/scripts/cleanup-benchmark-run.sh", root / "tools/scripts/cleanup-benchmark-run.sh")
            (root / "experiments.json").write_text("{}")
            (root / ".env").write_text("IMDS_RELAY_TEMPORARY_SAS_URL='stale-sender-url'\n")
            docker = tmp / "docker"
            docker.write_text("""#!/bin/bash
echo "$*" >> "$CALLS"
printf '%s' "$IMDS_RELAY_TEMPORARY_SAS_URL" > "$RELAY_VALUE"
case "$*" in
  "ps -aq --filter label=org.ivm-bench.run-id=$IVM_BENCH_RUN_ID") echo owned-container ;;
  "network ls -q --filter label=org.ivm-bench.run-id=$IVM_BENCH_RUN_ID") echo owned-network ;;
  "rm -f owned-container") exit 1 ;;
esac
""")
            docker.chmod(0o755)
            curl = tmp / "curl"
            curl.write_text("""#!/bin/bash
if [[ "$*" == *benchmark/stream* ]]; then
  touch "$STREAMING"
  sleep 2
fi
""")
            curl.chmod(0o755)
            git = tmp / "git"
            git.write_text('#!/bin/bash\nprintf "%s\\n" "$REPO_ROOT"\n')
            git.chmod(0o755)
            env = {**os.environ, "PATH": f"{tmp}:{os.environ['PATH']}",
                   "CALLS": str(tmp / "calls"), "STREAMING": str(tmp / "streaming"),
                   "RELAY_VALUE": str(tmp / "relay_value"),
                   "IMDS_RELAY_TEMPORARY_SAS_URL": "injected-sender-url",
                   "REPO_ROOT": str(root),
                   "GITHUB_ENV": str(tmp / "github_env"), "OPENIVM_UBUNTU_MIRROR": "unused",
                   "BENCHMARK_EXPERIMENTS_FILE": str(root / "experiments.json")}
            process = subprocess.Popen(["bash", "src/.scripts/benchmark.sh"], cwd=root,
                                       env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 10
                while not (tmp / "streaming").exists() and process.poll() is None and time.monotonic() < deadline:
                    time.sleep(0.05)
                self.assertTrue((tmp / "streaming").exists(), "benchmark did not reach streaming")
                self.assertEqual((tmp / "relay_value").read_text(), "injected-sender-url")
                process.send_signal(signal.SIGTERM)
                _, stderr = process.communicate(timeout=10)
                self.assertEqual(process.returncode, 143)
                self.assertIn("Failed to remove this run's container", stderr)
                run_id = (tmp / "github_env").read_text().strip().split("=", 1)[1]
                calls = (tmp / "calls").read_text()
                self.assertIn(f"-p ivm-{run_id} down --remove-orphans", calls)
                self.assertIn(f"ps -aq --filter label=org.ivm-bench.run-id={run_id}", calls)
                self.assertIn(f"network ls -q --filter label=org.ivm-bench.run-id={run_id}", calls)
                self.assertIn("rm -f owned-container", calls)
                self.assertIn("network rm owned-network", calls)
                self.assertNotIn("unrelated", calls)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.communicate()

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
