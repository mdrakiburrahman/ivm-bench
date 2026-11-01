import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[4]
COMMANDS = Path(__file__).parent / "fixtures/datagen/bin"


class DatagenEntrypointTest(unittest.TestCase):
    def run_generator(self, root, crash=False, config_error=False):
        tools = root / "bin"
        shutil.copytree(COMMANDS, tools, dirs_exist_ok=True)
        if not (tools / "python3").exists():
            (tools / "python3").symlink_to(sys.executable)
        (root / "digen/pdgf").mkdir(parents=True, exist_ok=True)
        env = {
            **os.environ,
            "PATH": os.pathsep.join((str(tools), os.environ["PATH"])),
            "DIGEN_PATH": str(root / "output"),
            "DIGEN_DIR": str(root / "digen"),
            "TMPDIR": str(root),
            "REPEATED_REFRESH": "1",
            "DIGEN_INCREMENTAL_BATCHES": "53",
            "SCALE_FACTOR": "10",
            "FAKE_CRASH": "1" if crash else "0",
            "FAKE_CONFIG_ERROR": "1" if config_error else "0",
        }
        return subprocess.run(
            ["bash", str(REPO / "src/containers/tpc-di-gen/entrypoint.sh")],
            env=env, text=True, capture_output=True, timeout=15,
        )

    def test_only_completed_generation_is_cached(self):
        with tempfile.TemporaryDirectory(prefix="datagen 'quoted' ") as tmp:
            root = Path(tmp)
            # A reference file alone must not turn an interrupted run into a cache hit.
            partial = root / "output/horizon-53/Batch1"
            partial.mkdir(parents=True)
            (partial / "Date.txt").write_text("incomplete")
            result = self.run_generator(root)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertTrue((partial.parent / ".generation-complete").exists())
            self.assertIn("generation finished", (partial.parent / "generator.log").read_text())
            self.assertIn("Skipping", self.run_generator(root).stdout)

    def test_worker_crash_fails_promptly_and_keeps_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = self.run_generator(root, crash=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("PDGF failed", result.stdout)
            output = root / "output/horizon-53"
            self.assertFalse((output / ".generation-complete").exists())
            self.assertIn("HouseKeeper", (output / "generator.log").read_text())

    def test_config_error_before_first_file_fails_promptly_and_keeps_log(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            result = self.run_generator(root, config_error=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("PDGF failed", result.stdout)
            self.assertNotIn("timeout", result.stdout)
            output = root / "output/horizon-53"
            self.assertFalse((output / ".generation-complete").exists())
            self.assertFalse((output / "Batch1/Date.txt").exists())
            self.assertIn("minUpdateSize", (output / "generator.log").read_text())


if __name__ == "__main__":
    unittest.main()
