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
ASSET_BRANCH="ci-assets"
CHART_MARKDOWN=""

ensure_asset_branch() {
  local api_url="${GITHUB_API_URL:-https://api.github.com}"
  local ref_url="${api_url}/repos/${GITHUB_REPOSITORY}/git/ref/heads/${ASSET_BRANCH}"
  local payload
  if curl -sSf -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/vnd.github+json" "$ref_url" -o /dev/null 2>/dev/null; then
    return 0
  fi
  [[ -n "${GITHUB_SHA:-}" ]] || return 1
  payload="$(jq -n --arg ref "refs/heads/${ASSET_BRANCH}" --arg sha "$GITHUB_SHA" '{ref: $ref, sha: $sha}')"
  if ! curl -sSf -X POST -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github+json" "$api_url/repos/${GITHUB_REPOSITORY}/git/refs" \
    -d "$payload" -o /dev/null 2>/dev/null; then
    curl -sSf -H "Authorization: Bearer ${GITHUB_TOKEN}" -H "Accept: application/vnd.github+json" "$ref_url" -o /dev/null 2>/dev/null || return 1
  fi
}

upload_chart() {
  local file="$1" asset_path="$2" payload content
  [[ -n "${GITHUB_TOKEN:-}" ]] || return 1
  ensure_asset_branch || return 1
  content="$(base64 < "$file" | tr -d '\n')"
  payload="$(jq -n --arg message "Add benchmark chart for run ${GITHUB_RUN_ID}" \
    --arg branch "$ASSET_BRANCH" --arg content "$content" \
    '{message: $message, branch: $branch, content: $content}')"
  curl -sSf -X PUT -H "Authorization: Bearer ${GITHUB_TOKEN}" \
    -H "Accept: application/vnd.github+json" \
    "${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY}/contents/${asset_path}" \
    -d "$payload" -o /dev/null 2>/dev/null
}

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

# Chart generation and hosting are best-effort; benchmark reporting must remain reliable.
CHART_ALLOWED=1
if [[ -n "${BENCHMARK_OUTCOME:-}" && "${BENCHMARK_OUTCOME}" != "success" ]]; then
  CHART_ALLOWED=0
fi
if [[ -n "${PR_NUMBER:-}" && "$CHART_ALLOWED" == "1" && -f "$COVERAGE_FILE" ]] \
  && jq -e '.passed == true' "$COVERAGE_FILE" > /dev/null 2>&1; then
  CHART_PYTHON="${ENGINE_CHART_PYTHON:-}"
  if [[ -z "$CHART_PYTHON" ]]; then
    if [[ -x .runtime/ci-tests/bin/python ]]; then
      CHART_PYTHON=".runtime/ci-tests/bin/python"
    else
      CHART_PYTHON="python3"
    fi
  fi
  CHART_DIR="ci-results/engine-charts"
  CHART_MANIFEST="ci-results/engine-chart-manifest.json"
  if mkdir -p "$CHART_DIR" && "$CHART_PYTHON" tools/scripts/github/render-engine-chart.py \
    --coverage "$COVERAGE_FILE" --output-dir "$CHART_DIR" > "$CHART_MANIFEST"; then
    while IFS= read -r chart; do
      chart_file="$(jq -r '.path' <<< "$chart")"
      chart_alt="$(jq -r '.alt' <<< "$chart")"
      asset_path="pr-${PR_NUMBER}/run-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT:-1}/$(basename "$chart_file")"
      if [[ "${DRY_RUN:-}" == "1" ]]; then
        image_url="https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${ASSET_BRANCH}/${asset_path}"
      elif upload_chart "$chart_file" "$asset_path"; then
        image_url="https://raw.githubusercontent.com/${GITHUB_REPOSITORY}/${ASSET_BRANCH}/${asset_path}"
      else
        echo "WARNING: Failed to publish benchmark chart; continuing without the image." >&2
        continue
      fi
      CHART_MARKDOWN+="${CHART_MARKDOWN:+$'\n\n'}<img src=\"${image_url}\" alt=\"${chart_alt}\" width=\"760\">"
    done < <(jq -c '.charts[]?' "$CHART_MANIFEST")
  else
    echo "WARNING: Failed to render benchmark chart; continuing without the image." >&2
  fi
fi

BODY="${MARKER}
## 🏁 Benchmark Results

${STATUS_LINE}"
if [[ -n "$CHART_MARKDOWN" ]]; then
  BODY+=$'\n\n'
  BODY+="$CHART_MARKDOWN"
fi
BODY+=$'\n\n'
BODY+="$REPORT"
BODY+=$'\n\n📦 [View workflow run & artifacts]('
BODY+="${RUN_URL})"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  printf '%s\n' "$BODY" >> "$GITHUB_STEP_SUMMARY"
fi

if [[ -z "${PR_NUMBER:-}" ]]; then
  echo "No PR_NUMBER; results are in the job summary only."
  exit 0
fi
if [[ "${DRY_RUN:-}" == "1" ]]; then
  printf '%s\n' "$BODY"
  echo "Dry run; no PR comment was posted."
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