"""Behavior tests for the dynamic RisingWave compaction tuner."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


TUNER = (
    Path(__file__).resolve().parents[2]
    / "risingwave"
    / "tune-compaction.sh"
)


class RisingWaveCompactionTunerTest(unittest.TestCase):
    def test_updates_only_groups_whose_first_three_levels_are_not_lz4(self):
        with tempfile.TemporaryDirectory() as directory:
            work = Path(directory)
            calls = work / "calls"
            fake_ctl = work / "risingwave"
            fake_ctl.write_text(
                '''#!/usr/bin/env python3
import os
import sys

args = sys.argv[1:]
if args == ["ctl", "hummock", "list-compaction-group"]:
    print(r"""
CompactionGroupInfo {
    id: 1,
    compression_algorithm: [
        "Lz4",
        "Lz4",
        "Lz4",
    ],
}
CompactionGroupInfo {
    id: 2,
    compression_algorithm: [
        "None",
        "None",
        "Lz4",
    ],
}
""")
elif args[:3] == ["ctl", "hummock", "update-compaction-config"]:
    with open(os.environ["CALLS_FILE"], "a") as handle:
        handle.write(" ".join(args) + "\\n")
else:
    print(f"unexpected command: {args}", file=sys.stderr)
    sys.exit(2)
'''
            )
            fake_ctl.chmod(0o755)
            env = os.environ | {
                "RW_CTL_BIN": str(fake_ctl),
                "RW_TUNER_ONCE": "1",
                "RW_TUNER_READY_FILE": str(work / "ready"),
                "CALLS_FILE": str(calls),
            }

            subprocess.run(
                ["bash", str(TUNER)],
                env=env,
                check=True,
                capture_output=True,
                timeout=5,
            )
            recorded = calls.read_text().splitlines()
            self.assertTrue((work / "ready").is_file())

        self.assertEqual(len(recorded), 3)
        for level, call in enumerate(recorded):
            self.assertIn("update-compaction-config", call)
            self.assertIn("--compaction-group-ids 2", call)
            self.assertIn(f"--compression-level {level}", call)
            self.assertIn("--compression-algorithm Lz4", call)


if __name__ == "__main__":
    unittest.main()
