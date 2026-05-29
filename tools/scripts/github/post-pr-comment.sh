#!/bin/bash
# ---------------------------------------------------------------------------
# post-pr-comment.sh — Posts benchmark results as a PR comment via GitHub API.
#
# Required env vars:
#   GITHUB_TOKEN       — GitHub token with pull-requests:write
#   GITHUB_REPOSITORY  — owner/repo
#   PR_NUMBER          — Pull request number
#   SCALE_FACTOR       — TPC-DI scale factor
#   GITHUB_SERVER_URL  — e.g. https://github.com
#   GITHUB_RUN_ID      — Workflow run ID (for artifact link)
#
# Optional env vars:
#   BATCH_N_INSERT_PCT, BATCH_N_*_PCT, ENGINES, PARALLEL
# ---------------------------------------------------------------------------
set -uo pipefail

RESULTS_FILE="mount/results/${SCALE_FACTOR}/dbt-server/benchmark-results.json"
MARKER="<!-- gci-benchmark-results -->"

if [[ ! -f "$RESULTS_FILE" ]]; then
  echo "WARNING: $RESULTS_FILE not found — benchmark may have failed. Posting failure notice."
  BODY="${MARKER}
## 🏁 Benchmark Results

> **⚠️ No results available** — benchmark did not produce output.

📦 [View workflow run](${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID})"
else
  STATUS=$(jq -r '.status // "unknown"' "$RESULTS_FILE")
  TOTAL=$(jq -r '.total_duration_s // 0' "$RESULTS_FILE")

  fmt_duration() {
    local secs="$1"
    local int_secs
    int_secs=$(printf '%.0f' "$secs")
    local h m s
    h=$((int_secs / 3600))
    m=$(( (int_secs % 3600) / 60 ))
    s=$((int_secs % 60))
    printf '%02d:%02d:%02d' "$h" "$m" "$s"
  }

  # Build the results table
  TABLE=""
  MAX_LEN=0
  for name in $(jq -r '.engines | keys[]' "$RESULTS_FILE"); do
    (( ${#name} > MAX_LEN )) && MAX_LEN=${#name}
  done

  PAD=$((MAX_LEN + 2))
  TABLE+="$(printf "%-${PAD}s 1           2           3" "")\n"

  for name in $(jq -r '.engines | keys[]' "$RESULTS_FILE"); do
    LABEL="$(echo "${name:0:1}" | tr '[:lower:]' '[:upper:]')${name:1}:"
    B1=$(jq -r ".engines[\"$name\"].batches[0].duration_s // 0" "$RESULTS_FILE")
    B2=$(jq -r ".engines[\"$name\"].batches[1].duration_s // 0" "$RESULTS_FILE")
    B3=$(jq -r ".engines[\"$name\"].batches[2].duration_s // 0" "$RESULTS_FILE")
    TABLE+="$(printf "%-${PAD}s %s -> %s -> %s" "$LABEL" "$(fmt_duration "$B1")" "$(fmt_duration "$B2")" "$(fmt_duration "$B3")")\n"
  done

  TOTAL_FMT=$(fmt_duration "$TOTAL")

  if [[ "$STATUS" == "completed" ]]; then
    STATUS_ICON="✅"
    STATUS_TEXT="All benchmarks completed successfully"
  else
    STATUS_ICON="❌"
    STATUS_TEXT="Benchmark finished with status: ${STATUS}"
  fi

  BODY="${MARKER}
## 🏁 Benchmark Results

${STATUS_ICON} **${STATUS_TEXT}**

| Parameter | Value |
|---|---|
| Scale Factor | \`${SCALE_FACTOR}\` |
| Insert Batches | \`${BATCH_1_INSERT_PCT:-${BATCH_1_PCT:-?}}%\` / \`${BATCH_2_INSERT_PCT:-${BATCH_2_PCT:-?}}%\` / \`${BATCH_3_INSERT_PCT:-${BATCH_3_PCT:-?}}%\` |
| Batch 2 Mutations | update \`${BATCH_2_UPDATE_PCT:-0}%\`, delete \`${BATCH_2_DELETE_PCT:-0}%\` |
| Batch 3 Mutations | update \`${BATCH_3_UPDATE_PCT:-0}%\`, delete \`${BATCH_3_DELETE_PCT:-0}%\` |
| Engines | \`${ENGINES:-all}\` |
| Parallel | \`${PARALLEL:-0}\` |
| Total | \`${TOTAL_FMT}\` |

\`\`\`
$(echo -e "$TABLE")
\`\`\`

📦 [View workflow run & artifacts](${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID})"
fi

# Post a new comment each run
echo "Creating PR comment..."
curl -sf \
  -X POST \
  -H "Authorization: token ${GITHUB_TOKEN}" \
  -H "Accept: application/vnd.github.v3+json" \
  "${GITHUB_API_URL:-https://api.github.com}/repos/${GITHUB_REPOSITORY}/issues/${PR_NUMBER}/comments" \
  -d "$(jq -n --arg body "$BODY" '{body: $body}')" > /dev/null || echo "WARNING: Failed to create PR comment"

echo "PR comment posted successfully."
