#!/bin/bash
# ---------------------------------------------------------------------------
# post-pr-comment.sh — Publishes the benchmark verdict to the job summary and,
# when PR_NUMBER is set, as a PR comment.
#
# ✅ only when the benchmark step succeeded AND check-engine-coverage.py found
# every requested engine completed every batch. Anything else is ❌.
#
# Required env vars:
#   GITHUB_REPOSITORY, GITHUB_SERVER_URL, GITHUB_RUN_ID
#
# Optional env vars:
#   PR_NUMBER             — PR to comment on (no comment when empty)
#   GITHUB_TOKEN          — token with pull-requests:write (needed with PR_NUMBER)
#   BENCHMARK_OUTCOME     — outcome of the benchmark step (success/failure/...)
#   ENGINE_COVERAGE_FILE  — verdict JSON (default ci-results/engine-coverage.json);
#                           its Markdown report sits next to it with a .md suffix
# ---------------------------------------------------------------------------
set -uo pipefail

COVERAGE_FILE="${ENGINE_COVERAGE_FILE:-ci-results/engine-coverage.json}"
COVERAGE_MD="${COVERAGE_FILE%.json}.md"
MARKER="<!-- gci-benchmark-results -->"
RUN_URL="${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}"

if [[ ! -f "$COVERAGE_FILE" || ! -f "$COVERAGE_MD" ]]; then
  STATUS_LINE="❌ **Benchmark failed: engine coverage was not checked**"
  REPORT="> No engine coverage report — the run stopped before the coverage check."
else
  REPORT="$(cat "$COVERAGE_MD")"
  if [[ -n "${BENCHMARK_OUTCOME:-}" && "${BENCHMARK_OUTCOME}" != "success" ]]; then
    STATUS_LINE="❌ **Benchmark failed: benchmark step ${BENCHMARK_OUTCOME}**"
  elif jq -e '.passed == true' "$COVERAGE_FILE" > /dev/null 2>&1; then
    STATUS_LINE="✅ **Every engine completed every batch**"
  else
    STATUS_LINE="❌ **Benchmark failed: not every engine completed every batch**"
  fi
fi

BODY="${MARKER}
## 🏁 Benchmark Results

${STATUS_LINE}

${REPORT}

📦 [View workflow run & artifacts](${RUN_URL})"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  printf '%s\n' "$BODY" >> "$GITHUB_STEP_SUMMARY"
fi

if [[ -z "${PR_NUMBER:-}" ]]; then
  echo "No PR_NUMBER; results are in the job summary only."
  exit 0
fi

# Post a new comment each run
echo "Creating PR comment..."
if curl -sf \
  -X POST \
  -H "Authorization: token ${GITHUB_TOKEN}" \
  -H "Accept: application/vnd.github.v3+json" \
  "${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY}/issues/${PR_NUMBER}/comments" \
  -d "$(jq -n --arg body "$BODY" '{body: $body}')" > /dev/null; then
  echo "PR comment posted successfully."
else
  echo "WARNING: Failed to create PR comment"
fi