#!/usr/bin/env bash
# Run from the repository root. Empty output keeps GCI's inline TPC-DI inputs.
set -euo pipefail

if [[ -n "${INPUT_EXPERIMENTS_FILE:-}" ]]; then
  printf '%s\n' "$INPUT_EXPERIMENTS_FILE"
elif [[ "${GITHUB_EVENT_NAME:-}" == "pull_request" && -f .github/gci.json ]]; then
  jq -er '.experiments_file | if type == "string" then . else error("experiments_file must be a string") end' \
    .github/gci.json
fi
