"""PR experiment selection must not change manual dispatch or default TPC-DI runs."""

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SELECTOR = REPO / "tools/scripts/github/select-experiments-file.sh"
COST_MODEL = "src/containers/benchmark-server/experiments/cost-model-bench.json"


class GciExperimentSelectionTest(unittest.TestCase):
    def select(self, event, config=None, override=""):
        with tempfile.TemporaryDirectory() as directory:
            if config is not None:
                path = Path(directory) / ".github/gci.json"
                path.parent.mkdir()
                path.write_text(json.dumps(config))
            return subprocess.run(
                ["bash", str(SELECTOR)], cwd=directory, text=True, capture_output=True,
                env={**os.environ, "GITHUB_EVENT_NAME": event, "INPUT_EXPERIMENTS_FILE": override},
            )

    def test_pr_selects_cost_model_experiment(self):
        result = self.select("pull_request", {"experiments_file": COST_MODEL})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), COST_MODEL)

    def test_empty_or_absent_selector_keeps_tpcdi_defaults(self):
        for config in (None, {"experiments_file": ""}):
            with self.subTest(config=config):
                result = self.select("pull_request", config)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout.strip(), "")

    def test_manual_dispatch_file_overrides_pr_selector(self):
        result = self.select("workflow_dispatch", {"experiments_file": COST_MODEL}, "custom.json")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "custom.json")

    def test_manual_dispatch_knobs_ignore_pr_selector(self):
        result = self.select("workflow_dispatch", {"experiments_file": COST_MODEL})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "")

    def test_invalid_selector_fails_instead_of_running_default_benchmark(self):
        for config in ({}, {"experiments_file": [COST_MODEL]}, {"experiments_file": None}):
            with self.subTest(config=config):
                result = self.select("pull_request", config)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("experiments_file must be a string", result.stderr)


if __name__ == "__main__":
    unittest.main()
