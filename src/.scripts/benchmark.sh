#!/bin/bash
# ---------------------------------------------------------------------------
# benchmark.sh — thin client that launches the benchmark-server and streams
# progress via Server-Sent Events.
#
# Environment variables:
#   SCALE_FACTOR   — TPC-DI scale factor (default: 3)
#   BATCH_1_PCT    — % of batch 1 data (required)
#   BATCH_2_PCT    — % of batch 2 data (required)
#   BATCH_3_PCT    — % of batch 3 data (required)
#   PARALLEL       — 0 = serial (default), 1 = run engines in parallel
#   ENGINES        — comma-separated engine list (default: spark,duckdb,openivm,feldera)
#   HOST_CORES     — override auto-detected CPU count
#   HOST_MEMORY    — override auto-detected memory in GB
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

export SCALE_FACTOR="${SCALE_FACTOR:-3}"

if [[ -z "${BATCH_1_PCT:-}" || -z "${BATCH_2_PCT:-}" || -z "${BATCH_3_PCT:-}" ]]; then
  echo "ERROR: BATCH_1_PCT, BATCH_2_PCT, and BATCH_3_PCT must all be set."
  echo "  Example: export BATCH_1_PCT=1 BATCH_2_PCT=0.001 BATCH_3_PCT=0.002"
  exit 1
fi
export BATCH_1_PCT BATCH_2_PCT BATCH_3_PCT
export PARALLEL="${PARALLEL:-0}"
export ENGINES="${ENGINES:-spark,duckdb,openivm,feldera}"
export HOST_CORES="${HOST_CORES:-}"
export HOST_MEMORY="${HOST_MEMORY:-}"
export REPO_HOST_PATH="$(pwd)"

COMPOSE_FILE="docker-compose.benchmark-server.yml"
HEALTH_RETRIES=60

capture_benchmark_server_logs() {
  local log_dir="mount/logs/${SCALE_FACTOR}"
  mkdir -p "$log_dir"
  docker compose -f "$COMPOSE_FILE" logs --no-color --timestamps \
    > "${log_dir}/benchmark-server.log" 2>&1 || true
}

docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

echo "=== Starting benchmark-server ==="
docker compose -f "$COMPOSE_FILE" up -d --build

HEALTH_OK=0
for i in $(seq 1 $HEALTH_RETRIES); do
  if curl -sf http://localhost:9000/health >/dev/null 2>&1; then
    HEALTH_OK=1
    break
  fi
  echo "  waiting for benchmark-server... ($i/$HEALTH_RETRIES)"
  sleep 5
done

if [[ "$HEALTH_OK" != "1" ]]; then
  echo "=== FAILURE — benchmark-server did not become healthy ==="
  capture_benchmark_server_logs
  docker compose -f "$COMPOSE_FILE" logs
  docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== benchmark-server is healthy — streaming progress ==="
echo ""

FINAL_STATUS="unknown"
while IFS= read -r line; do
  case "$line" in
    "event: progress") ;;
    "event: done")     ;;
    data:\ *)
      payload="${line#data: }"
      if [[ "$payload" == "completed" || "$payload" == "failed" ]]; then
        FINAL_STATUS="$payload"
      else
        echo "$payload"
      fi
      ;;
  esac
done < <(curl -sfN "http://localhost:9000/benchmark/stream" 2>/dev/null)

echo ""

echo "=== Final results ==="
curl -sf "http://localhost:9000/benchmark/status" | jq .

echo "=== Capturing benchmark-server logs ==="
capture_benchmark_server_logs

docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

if [[ "$FINAL_STATUS" != "completed" ]]; then
  echo "=== BENCHMARK FAILED ==="
  exit 1
fi

echo "=== BENCHMARK COMPLETED ==="
