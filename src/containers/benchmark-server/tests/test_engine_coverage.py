"""A benchmark fails unless every requested engine completed every batch."""

import importlib.util
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
SCRIPT = REPO / "tools/scripts/github/check-engine-coverage.py"
REPORTER = REPO / "tools/scripts/github/post-pr-comment.sh"
_spec = importlib.util.spec_from_file_location("check_engine_coverage", SCRIPT)
coverage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(coverage)

ENGINES = ["spark", "duckdb"]


def experiments_text(engines=ENGINES, **experiment):
    return json.dumps({
        "baseline": {"engines": engines},
        "experiments": [{"label": "sf10", "scale_factor": 10, **experiment}],
    })


def engine_result(status="completed", batches=3, failed_batch=None, error="boom"):
    return {
        "status": status,
        "error": None if status == "completed" else error,
        "batches": [
            {
                "batch_num": num,
                "duration_s": 61.0 * num,
                "status": "failed" if num == failed_batch else "completed",
                "error": error if num == failed_batch else None,
            }
            for num in range(1, batches + 1)
        ],
    }


def run_state(engines=None, status="completed", experiment_status="completed"):
    if engines is None:
        engines = {name: engine_result() for name in ENGINES}
    return {
        "status": status,
        "total_duration_s": 366.0,
        "experiments": [
            {"exp_idx": 0, "label": "sf10", "status": experiment_status, "engines": engines},
        ],
    }


class EngineCoverageTest(unittest.TestCase):
    def verdict(self, state, text=None):
        expected = coverage.expected_experiments(text or experiments_text())
        return coverage.check(expected, state, "experiments.json")

    def test_every_engine_and_batch_completed_passes(self):
        verdict = self.verdict(run_state())
        self.assertTrue(verdict["passed"], verdict["problems"])
        self.assertEqual(verdict["engine_summary"], {"spark": True, "duckdb": True})

    def test_engine_without_a_result_fails(self):
        verdict = self.verdict(run_state({"spark": engine_result()}))
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["engine_summary"], {"spark": True, "duckdb": False})
        self.assertIn("sf10: duckdb: no result reported", verdict["problems"])

    def test_failed_missing_or_unfinished_batch_fails(self):
        cases = {
            "failed batch": engine_result(failed_batch=2),
            "missing batch": engine_result(batches=2),
            "failed engine": engine_result(status="failed"),
            "pending engine": engine_result(status="pending"),
        }
        for name, duckdb in cases.items():
            with self.subTest(name):
                verdict = self.verdict(run_state({"spark": engine_result(), "duckdb": duckdb}))
                self.assertFalse(verdict["passed"])
                self.assertFalse(verdict["engine_summary"]["duckdb"])
                self.assertTrue(verdict["engine_summary"]["spark"])

    def test_failed_or_skipped_experiment_fails(self):
        for status in ("failed", "skipped"):
            with self.subTest(status):
                verdict = self.verdict(run_state(experiment_status=status))
                self.assertFalse(verdict["passed"])
                self.assertIn(f"sf10: experiment {status}", verdict["run_problems"])

    def test_unfinished_run_fails(self):
        verdict = self.verdict(run_state(status="running"))
        self.assertFalse(verdict["passed"])
        self.assertIn("OAT run running", verdict["run_problems"])

    def test_missing_outputs_fail_every_engine(self):
        verdict = self.verdict({"status": "missing"})
        self.assertFalse(verdict["passed"])
        self.assertEqual(verdict["engine_summary"], {"spark": False, "duckdb": False})

    def test_unrequested_experiment_fails(self):
        state = run_state()
        state["experiments"].append({"exp_idx": 1, "status": "completed", "engines": {}})
        self.assertFalse(self.verdict(state)["passed"])

    def test_empty_engine_selection_fails(self):
        with self.assertRaisesRegex(ValueError, "no engines requested"):
            coverage.expected_experiments(experiments_text(engines=[]))

    def test_duplicate_or_invalid_experiment_fails(self):
        for extra in ({"exp_idx": 0, "status": "completed"}, {"exp_idx": "bad"}, None):
            with self.subTest(extra=extra):
                state = run_state()
                state["experiments"].append(extra)
                self.assertFalse(self.verdict(state)["passed"])

    def test_duplicate_extra_or_invalid_batch_fails(self):
        for extra in ({"batch_num": 1, "status": "completed"},
                      {"batch_num": 4, "status": "completed"}, None, {"batch_num": True}):
            with self.subTest(extra=extra):
                engine = engine_result()
                engine["batches"].append(extra)
                self.assertFalse(coverage.check_engine(engine, 3)["passed"])

    def test_completed_batch_requires_a_finite_nonnegative_duration(self):
        for duration in (None, -1, "1", True, float("nan"), float("inf")):
            with self.subTest(duration=duration):
                engine = engine_result()
                engine["batches"][0]["duration_s"] = duration
                self.assertFalse(coverage.check_engine(engine, 3)["passed"])

    def test_every_experiment_uses_its_own_engine_selection(self):
        text = json.dumps({
            "baseline": {"engines": ENGINES},
            "experiments": [{"label": "first"}, {"label": "second", "engines": ["feldera"]}],
        })
        state = run_state()
        state["experiments"].append({
            "exp_idx": 1, "status": "completed", "engines": {"feldera": engine_result()},
        })
        self.assertTrue(self.verdict(state, text)["passed"])
        state["experiments"][1]["engines"] = {"spark": engine_result()}
        self.assertFalse(self.verdict(state, text)["passed"])

    def test_repeated_refresh_needs_every_refresh_batch(self):
        text = experiments_text(repeated_refresh=True, refresh_count=4)
        self.assertEqual(coverage.expected_experiments(text)[0]["batch_count"], 5)
        full = {name: engine_result(batches=5) for name in ENGINES}
        self.assertTrue(self.verdict(run_state(full), text)["passed"])
        short = {name: engine_result(batches=3) for name in ENGINES}
        self.assertFalse(self.verdict(run_state(short), text)["passed"])

    def test_untimed_experiment_only_needs_the_experiment_to_complete(self):
        text = experiments_text(feature_flags={"cost_model_bench": True})
        self.assertTrue(self.verdict(run_state({}), text)["passed"])
        self.assertFalse(self.verdict(run_state({}, experiment_status="failed"), text)["passed"])

    def test_markdown_shows_each_engine_and_escapes_errors(self):
        duckdb = engine_result(failed_batch=3, error="a | b\nc")
        verdict = self.verdict(run_state({"spark": engine_result(), "duckdb": duckdb}))
        markdown = coverage.render_markdown(verdict)
        self.assertIn("✅ `spark`, ❌ `duckdb`", markdown)
        self.assertIn("| `spark` | ✅ | 00:01:01 → 00:02:02 → 00:03:03 |", markdown)
        self.assertIn("batch 3 failed (a \\| b c)", markdown)


class CoverageCliTest(unittest.TestCase):
    def run_cli(self, directory, text, state, *extra):
        experiments = Path(directory) / "experiments.json"
        experiments.write_text(text)
        state_dir = Path(directory) / "state"
        state_dir.mkdir()
        if state is not None:
            (state_dir / "outputs.json").write_text(json.dumps(state))
        output = Path(directory) / "out/engine-coverage.json"
        env = {k: v for k, v in os.environ.items() if k not in ("BENCHMARK_EXPERIMENTS_FILE", "GITHUB_ACTIONS")}
        result = subprocess.run(
            ["python3", str(SCRIPT), "--experiments-file", str(experiments),
             "--state-dir", str(state_dir), "--output", str(output), *extra],
            cwd=directory, text=True, capture_output=True, env=env,
        )
        return result, output

    def test_cli_exit_code_and_outputs(self):
        cases = ((run_state(), 0), (run_state({"spark": engine_result()}), 1), (None, 1))
        for state, code in cases:
            with self.subTest(code=code, state=state is not None), tempfile.TemporaryDirectory() as directory:
                result, output = self.run_cli(directory, experiments_text(), state)
                self.assertEqual(result.returncode, code, result.stdout + result.stderr)
                self.assertEqual(json.loads(output.read_text())["passed"], code == 0)
                self.assertTrue(output.with_suffix(".md").is_file())

    def test_missing_latest_symlink_falls_back_to_the_only_run(self):
        with tempfile.TemporaryDirectory() as directory:
            run = Path(directory) / "oat-state/oat-1"
            run.mkdir(parents=True)
            (run / "outputs.json").write_text(json.dumps(run_state()))
            self.assertEqual(coverage.find_outputs(run.parent / "latest"), run / "outputs.json")
            self.assertEqual(coverage.find_outputs(run.parent / "other"), run.parent / "other/outputs.json")

    def test_unresolved_experiments_file_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "engine-coverage.json"
            env = {k: v for k, v in os.environ.items() if k != "BENCHMARK_EXPERIMENTS_FILE"}
            result = subprocess.run(
                ["python3", str(SCRIPT), "--output", str(output)],
                cwd=directory, text=True, capture_output=True, env=env,
            )
            self.assertEqual(result.returncode, 1)
            self.assertIn("never resolved", json.loads(output.read_text())["problems"][0])

    def test_missing_latest_with_multiple_runs_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            for name in ("run-1", "run-2"):
                run = Path(directory) / name
                run.mkdir()
                (run / "outputs.json").write_text(json.dumps(run_state()))
            with self.assertRaisesRegex(ValueError, "multiple OAT runs"):
                coverage.find_outputs(Path(directory) / "latest")


class ReporterTest(unittest.TestCase):
    def report(self, verdict, outcome="success"):
        with tempfile.TemporaryDirectory() as directory:
            summary = Path(directory) / "summary.md"
            coverage_file = Path(directory) / "engine-coverage.json"
            if verdict is not None:
                coverage_file.write_text(json.dumps(verdict))
                coverage_file.with_suffix(".md").write_text(coverage.render_markdown(verdict))
            env = {
                **{k: v for k, v in os.environ.items() if k != "PR_NUMBER"},
                "GITHUB_REPOSITORY": "o/r", "GITHUB_SERVER_URL": "https://github.com",
                "GITHUB_RUN_ID": "1", "GITHUB_STEP_SUMMARY": str(summary),
                "ENGINE_COVERAGE_FILE": str(coverage_file), "BENCHMARK_OUTCOME": outcome,
            }
            result = subprocess.run(["bash", str(REPORTER)], text=True, capture_output=True, env=env)
            self.assertEqual(result.returncode, 0, result.stderr)
            return summary.read_text()

    def passing(self):
        expected = coverage.expected_experiments(experiments_text())
        return coverage.check(expected, run_state(), "experiments.json")

    def test_green_only_when_benchmark_and_coverage_pass(self):
        self.assertIn("✅ **Every engine completed every batch**", self.report(self.passing()))

    def test_red_when_benchmark_step_failed(self):
        self.assertIn("❌ **Benchmark failed: benchmark step failure**",
                      self.report(self.passing(), outcome="failure"))

    def test_red_when_an_engine_is_missing(self):
        expected = coverage.expected_experiments(experiments_text())
        verdict = coverage.check(expected, run_state({"spark": engine_result()}), "experiments.json")
        self.assertIn("❌ **Benchmark failed: not every engine", self.report(verdict))

    def test_red_when_coverage_never_ran(self):
        self.assertIn("engine coverage was not checked", self.report(None))


if __name__ == "__main__":
    unittest.main()