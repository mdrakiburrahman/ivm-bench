#!/usr/bin/env bash
# Run from the repository root. Prints the experiments file CI runs:
#   1. an explicit file (workflow_dispatch input, or GCI via workflow_call);
#   2. the all-engine preset after a merge to main;
#   3. nothing, which makes CI synthesise one from its dispatch knobs.
set -euo pipefail

CI_PRESET="src/containers/benchmark-server/experiments/ci-sf10-all-engines.json"

if [[ -n "${INPUT_EXPERIMENTS_FILE:-}" ]]; then
  printf '%s\n' "$INPUT_EXPERIMENTS_FILE"
elif [[ "${GITHUB_EVENT_NAME:-}" == "push" ]]; then
  printf '%s\n' "$CI_PRESET"
fi