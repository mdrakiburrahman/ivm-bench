"""GCI runs one fixed local-engine preset; CI runs every engine or what it is asked."""

import os
import re
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import yaml

REPO = Path(__file__).resolve().parents[4]
BENCHMARK_SERVER = REPO / "src/containers/benchmark-server"
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.config import CLOUD_ENGINES, ENGINE_PORTS  # noqa: E402
from models.experiments import parse_experiments_json  # noqa: E402

SELECTOR = REPO / "tools/scripts/github/select-experiments-file.sh"
WORKFLOWS = REPO / ".github/workflows"
GCI_PRESET = "src/containers/benchmark-server/experiments/gci-sf10-local-engines.json"
CI_PRESET = "src/containers/benchmark-server/experiments/ci-sf10-all-engines.json"
ALL_ENGINES = set(ENGINE_PORTS)
LOCAL_ENGINES = ALL_ENGINES - CLOUD_ENGINES


class ExperimentSelectionTest(unittest.TestCase):
    def select(self, event, override=""):
        with tempfile.TemporaryDirectory() as directory:
            # A leftover PR selector must not steer anything any more.
            selector = Path(directory) / ".github/gci.json"
            selector.parent.mkdir()
            selector.write_text('{"experiments_file": "cost-model-bench.json"}')
            result = subprocess.run(
                ["bash", str(SELECTOR)], cwd=directory, text=True, capture_output=True,
                env={**os.environ, "GITHUB_EVENT_NAME": event, "INPUT_EXPERIMENTS_FILE": override},
            )
        self.assertEqual(result.returncode, 0, result.stderr)
        return result.stdout.strip()

    def test_explicit_file_wins_for_every_event(self):
        for event in ("pull_request", "push", "workflow_dispatch"):
            with self.subTest(event=event):
                self.assertEqual(self.select(event, "custom.json"), "custom.json")

    def test_merge_to_main_runs_the_all_engine_preset(self):
        self.assertEqual(self.select("push"), CI_PRESET)

    def test_dispatch_without_a_file_uses_the_inline_knobs(self):
        self.assertEqual(self.select("workflow_dispatch"), "")

    def test_pull_request_ignores_the_retired_gci_json(self):
        self.assertEqual(self.select("pull_request"), "")


class PresetCoverageTest(unittest.TestCase):
    def engines(self, preset):
        experiments = parse_experiments_json((REPO / preset).read_text(encoding="utf-8"))
        self.assertTrue(experiments)
        for experiment in experiments:
            self.assertEqual(experiment.scale_factor, 10)
            self.assertFalse(experiment.repeated_refresh)
            self.assertEqual(experiment.batch_count, 3)
        return [set(experiment.engines) for experiment in experiments]

    def test_gci_preset_runs_every_local_engine_and_no_cloud_engine(self):
        for engines in self.engines(GCI_PRESET):
            self.assertEqual(engines, LOCAL_ENGINES)

    def test_ci_preset_runs_every_engine(self):
        for engines in self.engines(CI_PRESET):
            self.assertEqual(engines, ALL_ENGINES)

    def test_presets_cannot_be_switched_to_untimed_sweeps_by_environment(self):
        with patch.dict(os.environ, {"COMPILER_BENCH": "1", "COST_MODEL_BENCH": "1"}):
            for preset in (GCI_PRESET, CI_PRESET):
                for experiment in parse_experiments_json((REPO / preset).read_text()):
                    self.assertFalse(experiment.feature_flags.compiler_bench)
                    self.assertFalse(experiment.feature_flags.cost_model_bench)

    def test_gci_is_pinned_to_its_preset_with_no_knobs(self):
        gci = (WORKFLOWS / "gci.yaml").read_text(encoding="utf-8")
        self.assertIn("uses: ./.github/workflows/ci.yaml", gci)
        self.assertIn(f"experiments_file: {GCI_PRESET}", gci)
        self.assertNotIn("workflow_dispatch", gci)
        self.assertNotIn("inputs.", gci)

    def test_ci_inline_default_engines_cover_every_engine(self):
        ci = (WORKFLOWS / "ci.yaml").read_text(encoding="utf-8")
        defaults = re.findall(r"inputs\.engines \|\| '([^']+)'", ci)
        defaults += re.findall(r'(?ms)^      engines:\n.*?default: "([^"]+)"', ci)
        self.assertEqual(len(defaults), 2)
        for default in defaults:
            self.assertEqual(set(default.split(",")), ALL_ENGINES)

    def test_ci_guards_engine_coverage_before_reporting(self):
        ci = (WORKFLOWS / "ci.yaml").read_text(encoding="utf-8")
        guard = ci.index("check-engine-coverage.py")
        self.assertLess(ci.index("src/.scripts/benchmark.sh"), guard)
        self.assertLess(guard, ci.index("post-pr-comment.sh"))

    def test_workflow_triggers_keep_pr_and_post_merge_paths_separate(self):
        gci = yaml.load((WORKFLOWS / "gci.yaml").read_text(), Loader=yaml.BaseLoader)
        ci = yaml.load((WORKFLOWS / "ci.yaml").read_text(), Loader=yaml.BaseLoader)
        self.assertEqual(set(gci["on"]), {"pull_request"})
        self.assertIn("synchronize", gci["on"]["pull_request"]["types"])
        self.assertIn("ready_for_review", gci["on"]["pull_request"]["types"])
        self.assertEqual(set(ci["on"]), {"push", "workflow_dispatch", "workflow_call"})
        self.assertEqual(ci["on"]["push"]["branches"], ["main"])
        self.assertEqual(len(ci["on"]["workflow_dispatch"]["inputs"]), 25)
        self.assertEqual(set(gci["jobs"]["benchmark"]["with"]), {"experiments_file", "pr_number"})


if __name__ == "__main__":
    unittest.main()