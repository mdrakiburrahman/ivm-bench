"""Cost model sweep runner: failure propagation and per-experiment output scoping."""

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

BENCHMARK_SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BENCHMARK_SERVER))

from models.experiments import CostModelBenchOptions, ExperimentInputs  # noqa: E402
from services.orchestrator import Orchestrator  # noqa: E402


class CostModelBenchTest(unittest.TestCase):
    def _orchestrator(self, repo):
        """An Orchestrator with just enough wired up to drive _run_cost_model_bench."""
        orch = Orchestrator.__new__(Orchestrator)
        orch._config = SimpleNamespace(repo_dir=repo)
        orch._oat_run_id = "run-abc"
        orch.emit = lambda *a, **k: None
        # _heartbeat is a context manager in the real class; a no-op stands in here.
        class _NullCtx:
            def __enter__(self):
                return None

            def __exit__(self, *exc):
                return False

        orch._heartbeat = lambda *a, **k: _NullCtx()
        # The runner refuses to proceed without the exported binary.
        binary = os.path.join(repo, "mount/bin/duckdb-openivm/cost_model_benchmark")
        os.makedirs(os.path.dirname(binary), exist_ok=True)
        Path(binary).write_text("")
        return orch

    def _inputs(self, label, scales):
        inp = ExperimentInputs()
        inp.label = label
        inp.cost_model_bench = CostModelBenchOptions(scale_factors=scales, args="--reps 1")
        return inp

    def test_nonzero_exit_fails_the_experiment(self):
        """A failing case must not report a completed experiment.

        A non-zero exit means a case errored or failed its EXCEPT ALL cross-check, which is what the
        sweep exists to surface; swallowing it would report success over a correctness failure.
        """
        with tempfile.TemporaryDirectory() as repo:
            orch = self._orchestrator(repo)
            with patch("services.orchestrator.subprocess.run") as run:
                run.return_value = SimpleNamespace(returncode=3)
                with self.assertRaises(RuntimeError):
                    orch._run_cost_model_bench(0, self._inputs("smoke", [1]))

    def test_all_scales_run_before_failing(self):
        """One scale factor failing says nothing about the others, whose results are worth having."""
        with tempfile.TemporaryDirectory() as repo:
            orch = self._orchestrator(repo)
            with patch("services.orchestrator.subprocess.run") as run:
                run.side_effect = [
                    SimpleNamespace(returncode=1),
                    SimpleNamespace(returncode=0),
                    SimpleNamespace(returncode=0),
                ]
                with self.assertRaises(RuntimeError):
                    orch._run_cost_model_bench(0, self._inputs("ladder", [1, 10, 25]))
                self.assertEqual(run.call_count, 3)

    def test_experiments_sharing_a_scale_do_not_overwrite(self):
        """Naming by scale factor alone let the second experiment erase the first."""
        with tempfile.TemporaryDirectory() as repo:
            orch = self._orchestrator(repo)
            written = []

            def capture(cmd, **kwargs):
                written.append(cmd[cmd.index("--out") + 1])
                return SimpleNamespace(returncode=0)

            with patch("services.orchestrator.subprocess.run", side_effect=capture):
                orch._run_cost_model_bench(0, self._inputs("first", [1]))
                orch._run_cost_model_bench(1, self._inputs("second", [1]))

            self.assertEqual(len(written), 2)
            self.assertNotEqual(written[0], written[1])
            for path in written:
                self.assertIn("run-abc", path)

    def test_label_is_sanitised_into_the_path(self):
        with tempfile.TemporaryDirectory() as repo:
            orch = self._orchestrator(repo)
            written = []

            def capture(cmd, **kwargs):
                written.append(cmd[cmd.index("--out") + 1])
                return SimpleNamespace(returncode=0)

            with patch("services.orchestrator.subprocess.run", side_effect=capture):
                orch._run_cost_model_bench(2, self._inputs("weird/label name", [1]))

            self.assertNotIn(" ", written[0])
            self.assertNotIn("weird/label", written[0])


if __name__ == "__main__":
    unittest.main()
