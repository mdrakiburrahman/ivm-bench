#!/usr/bin/env bash
# Run from the repository root. Prints the experiments file CI runs:
#   1. an explicit file (workflow_dispatch input, or GCI via workflow_call);
#   2. nothing, which makes CI synthesise one from its dispatch knobs.
set -euo pipefail

if [[ -n "${INPUT_EXPERIMENTS_FILE:-}" ]]; then
  printf '%s\n' "$INPUT_EXPERIMENTS_FILE"
fi