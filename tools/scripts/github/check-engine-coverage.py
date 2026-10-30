#!/usr/bin/env python3
"""Fail a benchmark run unless every requested engine completed every batch.

"Requested" is the experiments file the run was given, parsed with the
benchmark server's own parser. "Completed" is the OAT master outputs.json.
Writes the verdict as JSON (for tooling) and Markdown (for the PR comment and
job summary) and exits 1 when anything is missing.

Run from the repository root:
    python3 tools/scripts/github/check-engine-coverage.py
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "src" / "containers" / "benchmark-server"))

from models.experiments import parse_experiments_json  # noqa: E402

COMPLETED = "completed"
MAX_DETAIL = 200
MAX_PROBLEMS = 20


def expected_experiments(experiments_text: str) -> List[Dict[str, Any]]:
    """What the run was asked to do: one entry per experiment, in order."""
    expected = []
    for inputs in parse_experiments_json(experiments_text):
        if not inputs.engines:
            raise ValueError(f"{inputs.label}: no engines requested")
        flags = inputs.feature_flags
        expected.append({
            "label": inputs.label,
            "engines": list(dict.fromkeys(inputs.engines)),
            "batch_count": inputs.batch_count,
            # compiler-bench and cost-model-bench replace the timed batches.
            "timed": not (flags.compiler_bench or flags.cost_model_bench),
            "params": {
                "scale_factor": inputs.scale_factor,
                "workload": inputs.workload,
                "insert_batches": " / ".join(
                    f"{pct}%" for pct in (inputs.batch_1_pct, inputs.batch_2_pct, inputs.batch_3_pct)
                ),
                "batch_2_mutations": (
                    f"update {inputs.batch_2_update_pct}%, delete {inputs.batch_2_delete_pct}%"
                ),
                "batch_3_mutations": (
                    f"update {inputs.batch_3_update_pct}%, delete {inputs.batch_3_delete_pct}%"
                ),
                "schedule": inputs.schedule,
            },
        })
    return expected


def check_engine(reported: Any, batch_count: int) -> Dict[str, Any]:
    """One engine passes only if it and each of its batches completed."""
    if not isinstance(reported, dict):
        return {"passed": False, "status": "missing", "durations_s": [],
                "problem": "no result reported"}
    issues = []
    by_num: Dict[int, Dict[str, Any]] = {}
    batches = reported.get("batches")
    if not isinstance(batches, list):
        batches = []
        issues.append("batches must be an array")
    for batch in batches:
        if not isinstance(batch, dict) or type(batch.get("batch_num")) is not int:
            issues.append("invalid batch result")
            continue
        num = batch["batch_num"]
        if num in by_num:
            issues.append(f"duplicate batch {num}")
            continue
        if num not in range(1, batch_count + 1):
            issues.append(f"unexpected batch {num}")
        by_num[num] = batch
    status = reported.get("status") or "missing"
    if status != COMPLETED:
        issues.append(_with_detail(f"engine {status}", reported.get("error")))
    durations = []
    for num in range(1, batch_count + 1):
        batch = by_num.get(num, {})
        durations.append(_number(batch.get("duration_s")))
        batch_status = batch.get("status") or "missing"
        if batch_status != COMPLETED:
            issues.append(_with_detail(f"batch {num} {batch_status}", batch.get("error")))
        elif durations[-1] is None or durations[-1] < 0:
            issues.append(f"batch {num} has no valid duration")
    return {"passed": not issues, "status": status, "durations_s": durations,
            "problem": "; ".join(issues) or None}


def check(expected: List[Dict[str, Any]], state: Dict[str, Any],
          experiments_file: str = "") -> Dict[str, Any]:
    run_problems: List[str] = []
    engine_problems: List[str] = []
    run_status = state.get("status") or "missing"
    if run_status != COMPLETED:
        run_problems.append(_with_detail(f"OAT run {run_status}", state.get("error")))

    reported_by_idx: Dict[int, Dict[str, Any]] = {}
    reported_experiments = state.get("experiments")
    if not isinstance(reported_experiments, list):
        reported_experiments = []
        run_problems.append("outputs.json experiments must be an array")
    for exp in reported_experiments:
        if not isinstance(exp, dict) or type(exp.get("exp_idx")) is not int:
            run_problems.append("outputs.json has an invalid experiment result")
            continue
        idx = exp["exp_idx"]
        if idx in reported_by_idx:
            run_problems.append(f"outputs.json has duplicate experiment {idx}")
            continue
        reported_by_idx[idx] = exp

    experiments = []
    engine_summary: Dict[str, bool] = {}
    for idx, want in enumerate(expected):
        got = reported_by_idx.get(idx, {})
        label = want["label"] or got.get("label") or f"exp-{idx}"
        status = got.get("status") or "missing"
        if status != COMPLETED:
            run_problems.append(_with_detail(
                f"{label}: experiment {status}", got.get("error") or got.get("skip_reason"),
            ))
        reported = got.get("engines") if isinstance(got.get("engines"), dict) else {}
        engines = {}
        for engine in want["engines"]:
            if want["timed"]:
                result = check_engine(reported.get(engine), want["batch_count"])
                if not result["passed"]:
                    engine_problems.append(f"{label}: {engine}: {result['problem']}")
            else:
                ok = status == COMPLETED
                result = {"passed": ok, "status": status, "durations_s": [],
                          "problem": None if ok else f"experiment {status}"}
            engines[engine] = result
            engine_summary[engine] = engine_summary.get(engine, True) and result["passed"]
        experiments.append({
            "index": idx,
            "label": label,
            "status": status,
            "timed": want["timed"],
            "batch_count": want["batch_count"],
            "params": want["params"],
            "engines": engines,
            "unexpected_engines": sorted(set(reported) - set(want["engines"])),
        })

    extra = sorted(set(reported_by_idx) - set(range(len(expected))))
    if extra:
        run_problems.append(
            f"outputs.json has {len(extra)} experiment(s) the experiments file did not ask for"
        )

    problems = run_problems + engine_problems
    return {
        "passed": not problems,
        "experiments_file": experiments_file,
        "run_status": run_status,
        "total_duration_s": _number(state.get("total_duration_s")),
        "engine_summary": engine_summary,
        "experiments": experiments,
        "run_problems": run_problems,
        "problems": problems,
    }


def failed_verdict(experiments_file: str, problem: str) -> Dict[str, Any]:
    return {
        "passed": False,
        "experiments_file": experiments_file,
        "run_status": "unknown",
        "total_duration_s": None,
        "engine_summary": {},
        "experiments": [],
        "run_problems": [problem],
        "problems": [problem],
    }


def render_markdown(verdict: Dict[str, Any]) -> str:
    experiments = verdict["experiments"]
    engines_cell = ", ".join(
        f"{'✅' if ok else '❌'} `{name}`" for name, ok in verdict["engine_summary"].items()
    ) or "none"
    file_name = Path(verdict["experiments_file"]).name if verdict["experiments_file"] else "unknown"
    count = len(experiments)
    rows = [
        ("Experiments", f"`{_cell(file_name)}` ({count} experiment{'' if count == 1 else 's'})"),
        ("Scale factor", _distinct(experiments, "scale_factor")),
        ("Workload", _distinct(experiments, "workload")),
        ("Insert batches", _distinct(experiments, "insert_batches")),
        ("Batch 2 mutations", _distinct(experiments, "batch_2_mutations")),
        ("Batch 3 mutations", _distinct(experiments, "batch_3_mutations")),
        ("Schedule", _distinct(experiments, "schedule")),
        ("Engines", engines_cell),
        ("Run status", f"`{_cell(verdict['run_status'])}`"),
        ("Total", f"`{_hms(verdict['total_duration_s'])}`"),
    ]
    lines = ["| Parameter | Value |", "|---|---|"]
    lines += [f"| {name} | {value} |" for name, value in rows]
    for exp in experiments:
        lines.append("")
        if count > 1:
            lines += [f"**{_cell(exp['label'])}**: `{_cell(exp['status'])}`", ""]
        lines += ["| Engine | Result | Batches (hh:mm:ss) |", "|---|---|---|"]
        for engine, result in exp["engines"].items():
            outcome = "✅" if result["passed"] else f"❌ {_cell(result['problem'])}"
            batches = " → ".join(_hms(d) for d in result["durations_s"]) or "—"
            lines.append(f"| `{engine}` | {outcome} | {batches} |")
    if verdict["run_problems"]:
        lines += ["", "**Why it failed**", ""]
        lines += [f"- {_cell(problem)}" for problem in verdict["run_problems"][:MAX_PROBLEMS]]
        hidden = len(verdict["run_problems"]) - MAX_PROBLEMS
        if hidden > 0:
            lines.append(f"- … and {hidden} more (see engine-coverage.json)")
    return "\n".join(lines) + "\n"


def find_outputs(state_dir: Path) -> Path:
    outputs = state_dir / "outputs.json"
    if outputs.is_file() or state_dir.name != "latest":
        return outputs
    # The symlink is best-effort. CI wipes mount/ first, so only one run is safe to infer.
    runs = list(state_dir.parent.glob("*/outputs.json"))
    if len(runs) > 1:
        raise ValueError("latest is missing and multiple OAT runs exist; cannot identify this run")
    return runs[0] if runs else outputs


def load_state(path: Path) -> Dict[str, Any]:
    if not path.is_file():
        return {"status": "missing", "error": f"{path} not found"}
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        return {"status": "unreadable", "error": f"{path}: {error}"}
    return state if isinstance(state, dict) else {"status": "unreadable", "error": f"{path} is not an object"}


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    parser.add_argument("--experiments-file", default=os.environ.get("BENCHMARK_EXPERIMENTS_FILE", ""))
    parser.add_argument("--state-dir", default="mount/oat-state/latest")
    # mount/ is created root-owned by Docker, so the verdict lives outside it.
    parser.add_argument("--output", default="ci-results/engine-coverage.json")
    args = parser.parse_args(argv)

    try:
        if not args.experiments_file:
            raise ValueError("BENCHMARK_EXPERIMENTS_FILE is not set; the experiments file was never resolved")
        expected = expected_experiments(Path(args.experiments_file).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError, AttributeError) as error:
        verdict = failed_verdict(args.experiments_file, f"cannot read the experiments file: {error}")
    else:
        try:
            state = load_state(find_outputs(Path(args.state_dir)))
        except (OSError, ValueError) as error:
            state = {"status": "unreadable", "error": str(error)}
        verdict = check(expected, state, args.experiments_file)

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(verdict, indent=2) + "\n", encoding="utf-8")
    markdown = render_markdown(verdict)
    output.with_suffix(".md").write_text(markdown, encoding="utf-8")
    print(markdown)
    if os.environ.get("GITHUB_ACTIONS") == "true":
        for problem in verdict["problems"][:MAX_PROBLEMS]:
            print(f"::error title=Engine coverage::{problem}")
    print("Engine coverage: " + ("PASSED" if verdict["passed"] else "FAILED"))
    return 0 if verdict["passed"] else 1


def _distinct(experiments: List[Dict[str, Any]], key: str) -> str:
    values = list(dict.fromkeys(str(exp["params"][key]) for exp in experiments))
    return ", ".join(f"`{_cell(value)}`" for value in values) or "—"


def _with_detail(prefix: str, detail: Any) -> str:
    if not detail:
        return prefix
    text = " ".join(str(detail).split())
    if len(text) > MAX_DETAIL:
        text = text[:MAX_DETAIL] + "…"
    return f"{prefix} ({text})"


def _number(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _hms(seconds: Optional[float]) -> str:
    if seconds is None:
        return "—"
    total = int(round(seconds))
    return f"{total // 3600:02d}:{total % 3600 // 60:02d}:{total % 60:02d}"


def _cell(text: Any) -> str:
    return " ".join(str(text).split()).replace("|", "\\|")


if __name__ == "__main__":
    sys.exit(main())
