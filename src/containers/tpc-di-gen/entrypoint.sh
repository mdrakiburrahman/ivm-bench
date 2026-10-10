#!/bin/bash
set -euo pipefail
# Ignore SIGPIPE — the PDGF timeout kill can trigger broken pipe on the FIFO
trap '' PIPE

DIGEN_PATH="${DIGEN_PATH:-/data/digen}"
SCALE_FACTOR="${SCALE_FACTOR:-3}"
DIGEN_DIR="/opt/digen"
# Increase the native event horizon, not SF: Batch1 keeps its requested size.
HORIZON="${DIGEN_INCREMENTAL_BATCHES:-2}"
if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
  [[ "$HORIZON" =~ ^[0-9]+$ ]] && [[ "$HORIZON" -ge 2 && "$HORIZON" -le 1200 ]] || {
    echo "ERROR: DIGEN_INCREMENTAL_BATCHES must be between 2 and 1200"; exit 1;
  }
  DIGEN_PATH="$DIGEN_PATH/horizon-$HORIZON"
fi

SENTINEL="$DIGEN_PATH/Batch1/Date.txt"
if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
  SENTINEL="$DIGEN_PATH/.generation-complete"
fi
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

if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
  # PDGF loads an embedded byte configuration; editing the distributed XML
  # does not change it. Override the property before initialization instead.
  cd "$DIGEN_DIR/pdgf"
  # Concurrent producers can crash the sorted writer's housekeeper. One
  # generation worker preserves sorted output and avoids that race.
  setsid java -Xmx1g -jar "$DIGEN_DIR/pdgf/pdgf.jar" \
    -workers 1 \
    -sp NUMBER_OF_INCREMENTAL_BATCHES "$HORIZON" \
    -sf "$((SCALE_FACTOR * 1000))" -o "'$LOCAL_GEN/'" \
    -closeWhenDone -start < "$FIFO" > >(tee "$DIGEN_PATH/generator.log") 2>&1 &
else
  setsid java \
    -cp "$DIGEN_DIR/DIGen.jar:$DIGEN_DIR/commons-cli-1.2.jar" \
    org.tpc.di.digen.DIGen \
    -sf "$SCALE_FACTOR" -o "$LOCAL_GEN" < "$FIFO" &
fi
DIGEN_PID=$!

# Open FIFO for writing (keeps it open via fd 3)
exec 3>"$FIFO"
echo >&3       # Press enter for initial EULA prompt
echo YES >&3   # Agree to EULA

# Legacy generation tolerates PDGF's writer deadlock after writes settle.
# Repeated refresh requires a clean exit: worker failures and stalled writes
# fail the run instead of accepting partial data. The hard deadline remains
# generous for large scale factors.
DIGEN_TIMEOUT="${DIGEN_TIMEOUT:-7200}"  # 2 hours hard timeout
if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
  DIGEN_SETTLE_LIMIT="${DIGEN_SETTLE_LIMIT:-120}"
else
  DIGEN_SETTLE_LIMIT="${DIGEN_SETTLE_LIMIT:-30}"
fi
ELAPSED=0
while kill -0 "$DIGEN_PID" 2>/dev/null; do
  sleep 5
  ELAPSED=$((ELAPSED + 5))
  if [[ "${REPEATED_REFRESH:-0}" == "1" ]] && grep -q 'Exception in thread' "$DIGEN_PATH/generator.log"; then
    echo "ERROR: PDGF worker crashed; see $DIGEN_PATH/generator.log"
    kill -- -"$DIGEN_PID" 2>/dev/null || kill "$DIGEN_PID" 2>/dev/null || true
    exit 1
  fi
  if [[ -f "$LOCAL_GEN/Batch1/Date.txt" ]]; then
    # Sentinel exists — check if files are still being written
    LATEST_MOD=$(find "$LOCAL_GEN" -type f -printf '%T@\n' 2>/dev/null | sort -rn | head -1 | cut -d. -f1) || true
    NOW=$(date +%s)
    AGE=$((NOW - ${LATEST_MOD:-0}))
    if [[ $AGE -gt $DIGEN_SETTLE_LIMIT ]]; then
      if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
        echo "ERROR: PDGF made no file progress for ${AGE}s; refusing incomplete data"
        kill -QUIT "$DIGEN_PID" 2>/dev/null || true
        sleep 1
        kill -- -"$DIGEN_PID" 2>/dev/null || kill "$DIGEN_PID" 2>/dev/null || true
        exit 1
      fi
      echo "=== DIGen: No file writes for ${AGE}s — data generation complete, terminating PDGF ==="
      kill -- -"$DIGEN_PID" 2>/dev/null || kill "$DIGEN_PID" 2>/dev/null || true
      break
    fi
  fi
  if [[ $ELAPSED -ge $DIGEN_TIMEOUT ]]; then
    echo "WARNING: DIGen timeout after ${DIGEN_TIMEOUT}s — terminating"
    kill -- -"$DIGEN_PID" 2>/dev/null || kill "$DIGEN_PID" 2>/dev/null || true
    if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
      exit 1
    fi
    break
  fi
done
DIGEN_EXIT=0
wait "$DIGEN_PID" || DIGEN_EXIT=$?
exec 3>&-
rm -f "$FIFO"

if [[ "${REPEATED_REFRESH:-0}" == "1" ]] && { [[ "$DIGEN_EXIT" -ne 0 ]] || [[ ! -f "$LOCAL_GEN/Batch1/TradeType.txt" ]]; }; then
  echo "ERROR: PDGF did not complete successfully (exit=$DIGEN_EXIT)"
  exit 1
fi

# Verify generation succeeded
if [[ ! -f "$LOCAL_GEN/Batch1/Date.txt" ]]; then
  echo "ERROR: DIGen completed but sentinel file was not created"
  exit 1
fi

# Copy generated files to output path (host mount)
echo "=== DIGen: Copying generated data to $DIGEN_PATH ==="
cp -a "$LOCAL_GEN"/. "$DIGEN_PATH"/
rm -rf "$LOCAL_GEN"
if [[ "${REPEATED_REFRESH:-0}" == "1" ]]; then
  touch "$SENTINEL"
fi

# PDGF's BucketSort deadlock can prevent the TradeType generator (16/16)
# from running. TradeType is a fixed 5-row lookup table — create it as
# a fallback if PDGF didn't generate it.
if [[ ! -f "$DIGEN_PATH/Batch1/TradeType.txt" ]]; then
  echo "=== DIGen: Creating TradeType.txt (PDGF fallback) ==="
  printf 'TMB|Market Buy|0|1\nTMS|Market Sell|1|1\nTSL|Stop Loss|1|1\nTLS|Limit Sell|1|0\nTLB|Limit Buy|0|0\n' \
    > "$DIGEN_PATH/Batch1/TradeType.txt"
fi

echo "=== DIGen: Data generation complete ==="
