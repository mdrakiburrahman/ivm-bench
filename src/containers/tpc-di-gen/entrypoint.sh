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
# DIGEN_TIMEOUT is intentionally generous — SF=400 tpc-di-gen alone takes
# ~10-15 min, SF=1000 estimated 30-40 min. The SETTLE check above kills
# DIGen as soon as writes go quiet, so the timeout is purely a safety net
# for true hangs. Override via env for unusual scale factors.
DIGEN_TIMEOUT="${DIGEN_TIMEOUT:-7200}"  # 2 hours hard timeout
DIGEN_SETTLE_LIMIT="${DIGEN_SETTLE_LIMIT:-30}"  # consider done after 30s of no writes
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
