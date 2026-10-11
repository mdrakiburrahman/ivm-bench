import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


REPO = Path(__file__).resolve().parents[4]


class DatagenEntrypointTest(unittest.TestCase):
    def run_generator(self, root, crash=False):
        tools = root / "bin"
        tools.mkdir(exist_ok=True)
        for name, body in {
            "setsid": '#!/bin/bash\nexec "$@"\n',
            "sleep": '#!/bin/bash\n/bin/sleep 0.01\n',
            # The Linux image uses GNU find's -printf; emulate it on macOS.
            "find": f"#!{sys.executable}\n" + '''import pathlib, sys
for path in pathlib.Path(sys.argv[1]).rglob("*"):
    if path.is_file(): print(path.stat().st_mtime)
''',
            "java": f"#!{sys.executable}\n" + '''import os, pathlib, sys
sys.stdin.readline()
sys.stdin.readline()
args = sys.argv
assert args[args.index("-workers") + 1] == "1"
output = pathlib.Path(args[args.index("-o") + 1].strip("'"))
batch = output / "Batch1"
batch.mkdir(parents=True, exist_ok=True)
(batch / "Date.txt").write_text("partial reference data")
if os.environ.get("FAKE_CRASH") == "1":
    print('Exception in thread "HouseKeeper" java.lang.IllegalArgumentException', flush=True)
    # A failed background worker can leave the main process alive.
    import time
    while True: time.sleep(0.1)
(batch / "TradeType.txt").write_text("complete reference data")
print("generation finished", flush=True)
''',
        }.items():
            path = tools / name
            path.write_text(body)
            path.chmod(0o755)
        script = (REPO / "src/containers/tpc-di-gen/entrypoint.sh").read_text()
        script = script.replace('DIGEN_DIR="/opt/digen"', f'DIGEN_DIR="{root}/digen"')
        script = script.replace('LOCAL_GEN="/tmp/digen_out"', f'LOCAL_GEN="{root}/local"')
        script = script.replace('FIFO="/tmp/digen_input"', f'FIFO="{root}/fifo"')
        (root / "digen/pdgf").mkdir(parents=True, exist_ok=True)
        env = {**os.environ, "PATH": f"{tools}:{os.environ['PATH']}",
               "DIGEN_PATH": str(root / "output"), "REPEATED_REFRESH": "1",
               "DIGEN_INCREMENTAL_BATCHES": "53", "SCALE_FACTOR": "10",
               "FAKE_CRASH": "1" if crash else "0"}
        return subprocess.run(["bash", "-c", script], env=env, text=True,
                              capture_output=True, timeout=15)

    def test_only_completed_generation_is_cached(self):
        with tempfile.TemporaryDirectory() as tmp:
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
            self.assertIn("PDGF worker crashed", result.stdout)
            output = root / "output/horizon-53"
            self.assertFalse((output / ".generation-complete").exists())
            self.assertIn("HouseKeeper", (output / "generator.log").read_text())


if __name__ == "__main__":
    unittest.main()
