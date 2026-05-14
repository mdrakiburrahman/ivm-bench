#!/bin/bash
# ---------------------------------------------------------------------------
# run-streak.sh — Fire N consecutive SF=X benchmark runs and gate each one
# on the previous run's green status.
#
# The script:
#   1. Dispatches a workflow_dispatch run via `gh workflow run`.
#   2. Polls the new run until it reaches a terminal state.
#   3. If conclusion == "success", increments the streak and dispatches
#      the next run. Otherwise, exits non-zero with the failing run URL.
#
# Designed for the "5 consecutive green SF=1000 runs" stability check.
# Runs are sequential on a single runner — the second runner stays idle
# as a hot spare.
#
# Usage:
#   tools/scripts/github/run-streak.sh                  # 5x SF=1000 default
#   STREAK_TARGET=3 SCALE_FACTOR=100 \
#     tools/scripts/github/run-streak.sh                # 3x SF=100
#
# Required: gh CLI authenticated to the repo.
# ---------------------------------------------------------------------------
set -euo pipefail

REPO="${REPO:-mdrakiburrahman/ivm-bench}"
REF="${REF:-dev/mdrrahman/sf-1000}"
STREAK_TARGET="${STREAK_TARGET:-5}"
SCALE_FACTOR="${SCALE_FACTOR:-1000}"
BATCH_1_PCT="${BATCH_1_PCT:-100}"
BATCH_2_PCT="${BATCH_2_PCT:-1}"
BATCH_3_PCT="${BATCH_3_PCT:-1}"
PARALLEL="${PARALLEL:-0}"
ENGINES="${ENGINES:-spark,duckdb,duckdb-openivm,feldera}"
PRESERVE_RAW="${PRESERVE_RAW:-0}"
OPENIVM_VALIDATE="${OPENIVM_VALIDATE:-0}"
TIMEOUT_MINUTES="${TIMEOUT_MINUTES:-960}"
POLL_INTERVAL="${POLL_INTERVAL:-600}"

STREAK=0
RUNS_FIRED=()

while [ "$STREAK" -lt "$STREAK_TARGET" ]; do
  echo "================================================================"
  echo "Streak progress: $STREAK / $STREAK_TARGET"
  echo "Firing run #$((STREAK + 1)) at $(date -u +%FT%TZ)..."
  echo "================================================================"

  gh workflow run gci.yaml --repo "$REPO" --ref "$REF" \
    -f scale_factor="$SCALE_FACTOR" \
    -f batch_1_pct="$BATCH_1_PCT" \
    -f batch_2_pct="$BATCH_2_PCT" \
    -f batch_3_pct="$BATCH_3_PCT" \
    -f parallel="$PARALLEL" \
    -f engines="$ENGINES" \
    -f preserve_raw="$PRESERVE_RAW" \
    -f openivm_validate="$OPENIVM_VALIDATE" \
    -f timeout_minutes="$TIMEOUT_MINUTES" \
    -f cancel_in_progress=false

  # gh workflow run doesn't return the run id directly. The newest run
  # for this workflow on this ref is the one we just fired (modulo race
  # conditions; we wait a few seconds for it to materialise).
  echo "Waiting for run id..."
  RUN_ID=""
  for attempt in 1 2 3 4 5 6 7 8 9 10; do
    sleep 5
    RUN_ID=$(gh run list --workflow=gci.yaml --repo "$REPO" \
      --branch "$REF" --limit 1 \
      --json databaseId,createdAt,status \
      --jq '.[] | select(.status == "in_progress" or .status == "queued") | .databaseId' \
      | head -1)
    if [ -n "$RUN_ID" ]; then break; fi
  done

  if [ -z "$RUN_ID" ]; then
    echo "ERROR: could not find newly dispatched run id" >&2
    exit 1
  fi
  echo "Run id: $RUN_ID  https://github.com/$REPO/actions/runs/$RUN_ID"
  RUNS_FIRED+=("$RUN_ID")

  # Poll until terminal.
  while :; do
    RAW=$(gh run view "$RUN_ID" --repo "$REPO" \
      --json status,conclusion,updatedAt 2>&1)
    STATUS=$(echo "$RAW" | jq -r .status 2>/dev/null)
    CONCL=$(echo "$RAW" | jq -r .conclusion 2>/dev/null)
    UPDATED=$(echo "$RAW" | jq -r .updatedAt 2>/dev/null)
    echo "$(date -u +%H:%M:%SZ) run=$RUN_ID status=$STATUS conclusion=$CONCL"
    if [ "$STATUS" = "completed" ]; then break; fi
    sleep "$POLL_INTERVAL"
  done

  if [ "$CONCL" = "success" ]; then
    STREAK=$((STREAK + 1))
    echo "✅ Run $RUN_ID green (streak now $STREAK / $STREAK_TARGET)"
  else
    echo "❌ Run $RUN_ID conclusion=$CONCL"
    echo "Streak broken. Fired so far: ${RUNS_FIRED[*]}"
    exit 1
  fi
done

echo "================================================================"
echo "🎉 Streak complete: $STREAK / $STREAK_TARGET green runs"
echo "Runs: ${RUNS_FIRED[*]}"
echo "================================================================"
