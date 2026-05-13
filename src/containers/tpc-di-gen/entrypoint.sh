#!/bin/bash
set -euo pipefail
# Ignore SIGPIPE — the PDGF timeout kill can trigger broken pipe on the FIFO
trap '' PIPE

DIGEN_PATH="${DIGEN_PATH:-/data/digen}"
SCALE_FACTOR="${SCALE_FACTOR:-3}"
DIGEN_DIR="/opt/digen"

SENTINEL="$DIGEN_PATH/Batch1/Date.txt"
if [[ -f "$SENTINEL" ]]; then
  echo "=== DIGen: Skipping — sentinel file $SENTINEL already exists ==="
  exit 0
fi

echo "=== DIGen: Generating TPC-DI data (SF=$SCALE_FACTOR) ==="
# Generate to a local temp directory first, then copy to the output path.
LOCAL_GEN="/tmp/digen_out"
rm -rf "$LOCAL_GEN"
mkdir -p "$LOCAL_GEN" "$DIGEN_PATH"

# Must run from DIGEN_DIR so DIGen can find the pdgf/ subdirectory.
# Use a named pipe to keep stdin open (PDGF reads commands from stdin;
# closing it causes "null" command errors).
cd "$DIGEN_DIR"
FIFO="/tmp/digen_input"
rm -f "$FIFO"
mkfifo "$FIFO"

setsid java \
  -cp "$DIGEN_DIR/DIGen.jar:$DIGEN_DIR/commons-cli-1.2.jar" \
  org.tpc.di.digen.DIGen \
  -sf "$SCALE_FACTOR" \
  -o "$LOCAL_GEN" < "$FIFO" &
DIGEN_PID=$!

# Open FIFO for writing (keeps it open via fd 3)
exec 3>"$FIFO"
echo >&3       # Press enter for initial EULA prompt
echo YES >&3   # Agree to EULA

# PDGF has a known BucketSort race condition that can kill a housekeeper
# thread and deadlock the process (it never exits despite all data being
# written). We monitor the sentinel file and kill PDGF if it hangs.
# DIGEN_TIMEOUT is a safety net — DIGEN_SETTLE_LIMIT below cuts off
# cleanly when files stop being written, so the timeout only fires when
# the BucketSort deadlock leaves PDGF spinning. Default scales with
# SCALE_FACTOR so SF=1000 has the ~1-2 h window it needs (SF=100 takes
# ~5 min, SF=1000 takes ~60-90 min on a 32 vCPU / 251 GB host).
if [[ -z "${DIGEN_TIMEOUT:-}" ]]; then
  if [[ "$SCALE_FACTOR" -ge 1000 ]]; then
    DIGEN_TIMEOUT=10800   # 3 h
  elif [[ "$SCALE_FACTOR" -ge 100 ]]; then
    DIGEN_TIMEOUT=3600    # 1 h
  else
    DIGEN_TIMEOUT=600     # 10 min
  fi
fi
DIGEN_SETTLE_LIMIT="${DIGEN_SETTLE_LIMIT:-60}"  # consider done after 60s of no writes
echo "=== DIGen: SF=$SCALE_FACTOR timeout=${DIGEN_TIMEOUT}s settle=${DIGEN_SETTLE_LIMIT}s ==="
ELAPSED=0
while kill -0 "$DIGEN_PID" 2>/dev/null; do
  sleep 5
  ELAPSED=$((ELAPSED + 5))
  if [[ -f "$LOCAL_GEN/Batch1/Date.txt" ]]; then
    # Sentinel exists — check if files are still being written
    LATEST_MOD=$(find "$LOCAL_GEN" -type f -printf '%T@\n' 2>/dev/null | sort -rn | head -1 | cut -d. -f1) || true
    NOW=$(date +%s)
    AGE=$((NOW - ${LATEST_MOD:-0}))
    if [[ $AGE -gt $DIGEN_SETTLE_LIMIT ]]; then
      echo "=== DIGen: No file writes for ${AGE}s — data generation complete, terminating PDGF ==="
      kill -- -"$DIGEN_PID" 2>/dev/null || kill "$DIGEN_PID" 2>/dev/null || true
      break
    fi
  fi
  if [[ $ELAPSED -ge $DIGEN_TIMEOUT ]]; then
    echo "WARNING: DIGen timeout after ${DIGEN_TIMEOUT}s — terminating"
    kill -- -"$DIGEN_PID" 2>/dev/null || kill "$DIGEN_PID" 2>/dev/null || true
    break
  fi
done
wait "$DIGEN_PID" 2>/dev/null || true
exec 3>&-
rm -f "$FIFO"

# Verify generation succeeded
if [[ ! -f "$LOCAL_GEN/Batch1/Date.txt" ]]; then
  echo "ERROR: DIGen completed but sentinel file was not created"
  exit 1
fi

# Verify the heavy transactional tables we depend on were actually
# generated. PDGF's BucketSort deadlock has been observed terminating
# the run before CashTransaction (the slowest writer at SF=1000) is
# done — the sentinel Date.txt comes early in the schedule and would
# otherwise lie about completeness, leading to a baffling downstream
# spark-batch-loader "Path does not exist: batch1/cash_transaction"
# error in run 25780611981.
REQUIRED_FILES=(
  "Batch1/CashTransaction.txt"
  "Batch1/DailyMarket.txt"
  "Batch1/HoldingHistory.txt"
  "Batch1/WatchHistory.txt"
  "Batch1/Trade.txt"
  "Batch1/TradeHistory.txt"
)
MISSING=()
for rel in "${REQUIRED_FILES[@]}"; do
  if [[ ! -s "$LOCAL_GEN/$rel" ]]; then
    MISSING+=("$rel")
  fi
done
if (( ${#MISSING[@]} > 0 )); then
  echo "ERROR: DIGen produced sentinel but missing required heavy files:"
  for f in "${MISSING[@]}"; do echo "  - $f"; done
  echo "This usually means PDGF's timeout or BucketSort deadlock fired"
  echo "before all transactional generators completed. Re-run with a"
  echo "larger DIGEN_TIMEOUT (current: ${DIGEN_TIMEOUT}s)."
  exit 1
fi

# Copy generated files to output path (host mount)
echo "=== DIGen: Copying generated data to $DIGEN_PATH ==="
cp -a "$LOCAL_GEN"/. "$DIGEN_PATH"/
rm -rf "$LOCAL_GEN"

# PDGF's BucketSort deadlock can prevent the TradeType generator (16/16)
# from running. TradeType is a fixed 5-row lookup table — create it as
# a fallback if PDGF didn't generate it.
if [[ ! -f "$DIGEN_PATH/Batch1/TradeType.txt" ]]; then
  echo "=== DIGen: Creating TradeType.txt (PDGF fallback) ==="
  printf 'TMB|Market Buy|0|1\nTMS|Market Sell|1|1\nTSL|Stop Loss|1|1\nTLS|Limit Sell|1|0\nTLB|Limit Buy|0|0\n' \
    > "$DIGEN_PATH/Batch1/TradeType.txt"
fi

echo "=== DIGen: Data generation complete ==="
