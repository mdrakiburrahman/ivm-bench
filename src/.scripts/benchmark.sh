#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

export SCALE_FACTOR="${SCALE_FACTOR:-3}"
LOGS_DIR=".logs"

mkdir -p "$LOGS_DIR"

# Pre-create mount directories so Docker doesn't create them as root
for d in "mount/results/${SCALE_FACTOR}/spark" "mount/results/${SCALE_FACTOR}/duckdb" "mount/results/${SCALE_FACTOR}/feldera" "mount/results/${SCALE_FACTOR}/dbt-server"; do
  mkdir -p "$d" 2>/dev/null || {
    docker run --rm -v "$(pwd)/mount:/mount" alpine mkdir -p "/${d}" 2>/dev/null
    docker run --rm -v "$(pwd)/mount:/mount" alpine chown -R "$(id -u):$(id -g)" /mount 2>/dev/null
    mkdir -p "$d" 2>/dev/null || true
  }
done

echo "=== Tearing down any running containers ==="
docker compose -f docker-compose.datagen.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.batch-loader.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.benchmark.spark.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.benchmark.duckdb.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.benchmark.feldera.yml down --remove-orphans 2>/dev/null || true

DATAGEN_COMPOSE="docker-compose.datagen.yml"
BATCH_LOADER_COMPOSE="docker-compose.batch-loader.yml"
BENCHMARK_COMPOSE="docker-compose.benchmark.spark.yml"
DUCKDB_COMPOSE="docker-compose.benchmark.duckdb.yml"
FELDERA_COMPOSE="docker-compose.benchmark.feldera.yml"

RESULTS_DIR="mount/results/${SCALE_FACTOR}/dbt-server"
HEALTH_RETRIES=60

# ---------------------------------------------------------------------------
# Helper: stream dbt progress from the SSE endpoint and set RUN_STATUS
# Usage:  stream_progress <run_id>
#         (sets global RUN_STATUS to "completed", "failed", or "not found")
# ---------------------------------------------------------------------------
stream_progress() {
  local run_id="$1"
  RUN_STATUS="unknown"
  while IFS= read -r line; do
    case "$line" in
      "event: progress") ;;
      "event: done")     ;;
      "event: error")    ;;
      data:\ *)
        local payload="${line#data: }"
        if [[ "$payload" == "completed" || "$payload" == "failed" || "$payload" == "not found" ]]; then
          RUN_STATUS="$payload"
        else
          echo "$payload"
        fi
        ;;
    esac
  done < <(curl -sfN "http://localhost:5000/runs/$run_id/progress/stream" 2>/dev/null)
}

# ---------------------------------------------------------------------------
# Helper: wait for dbt-server to be healthy
# Usage:  wait_for_health
# ---------------------------------------------------------------------------
wait_for_health() {
  local HEALTH_OK=0
  for i in $(seq 1 $HEALTH_RETRIES); do
    if curl -sf http://localhost:5000/health >/dev/null 2>&1; then
      HEALTH_OK=1
      break
    fi
    echo "  waiting for dbt-server... ($i/$HEALTH_RETRIES)"
    sleep 5
  done
  if [[ "$HEALTH_OK" != "1" ]]; then
    echo "=== FAILURE — dbt-server did not become healthy ==="
    return 1
  fi
  echo "  dbt-server is healthy"
}

# ---------------------------------------------------------------------------
# Helper: trigger a dbt run, stream progress, save results
# Usage:  run_dbt <engine> <batch_num>
#         (sets global RUN_STATUS)
# ---------------------------------------------------------------------------
run_dbt() {
  local engine="$1"
  local batch_num="$2"

  echo "=== Triggering dbt run (engine=$engine, batch=$batch_num, SF=$SCALE_FACTOR) ==="
  local RUN_RESPONSE
  RUN_RESPONSE=$(curl -sf -X POST "http://localhost:5000/run/$engine" \
    -H 'Content-Type: application/json' \
    -d "{\"scale_factor\": $SCALE_FACTOR, \"full_refresh\": true}")
  local RUN_ID
  RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
  echo "  run_id=$RUN_ID"

  echo ""
  echo "=== dbt build progress ($engine, batch $batch_num) ==="
  echo ""

  stream_progress "$RUN_ID"

  echo ""

  mkdir -p "$RESULTS_DIR" 2>/dev/null || true
  curl -sf "http://localhost:5000/runs/$RUN_ID" | jq . > "$RESULTS_DIR/run-${engine}-batch${batch_num}.json"
  echo "  results saved to $RESULTS_DIR/run-${engine}-batch${batch_num}.json"
}

# ---------------------------------------------------------------------------
# Helper: run spark-batch-loader (init or append)
# Usage:  batch_loader init
#         batch_loader append <batch_num>
# ---------------------------------------------------------------------------
batch_loader() {
  local mode="$1"
  shift
  echo "=== Batch Loader: $mode $* ==="
  docker compose -f "$BATCH_LOADER_COMPOSE" run --rm spark-batch-loader "$mode" "$@" 2>&1 \
    | tee "$LOGS_DIR/batch-loader-${mode}${1:+-$1}.log"
  echo "=== Batch Loader: $mode $* complete ==="
  # Fix permissions after Spark writes
  docker run --rm -v "$(pwd)/mount/raw/${SCALE_FACTOR}/delta:/data" alpine chmod -R 777 /data/staging 2>/dev/null || true
}

# ---------------------------------------------------------------------------
# Phase 1 — Data generation (idempotent)
# ---------------------------------------------------------------------------
echo "=== Phase 1: Building datagen images ==="
docker compose -f "$DATAGEN_COMPOSE" build tpc-di-gen spark-digen-delta

echo "=== Phase 1: Running tpc-di-gen → spark-digen-delta (SF=$SCALE_FACTOR) ==="
COMPOSE_RC=0
docker compose -f "$DATAGEN_COMPOSE" up spark-digen-delta 2>&1 | tee "$LOGS_DIR/spark-digen-delta.log" || COMPOSE_RC=$?

DIGEN_EXIT=$(docker compose -f "$DATAGEN_COMPOSE" ps -a tpc-di-gen --format '{{.ExitCode}}' 2>/dev/null || echo "unknown")
DELTA_EXIT=$(docker compose -f "$DATAGEN_COMPOSE" ps -a spark-digen-delta --format '{{.ExitCode}}' 2>/dev/null || echo "unknown")

if [[ "$DIGEN_EXIT" != "0" || "$DELTA_EXIT" != "0" || "$COMPOSE_RC" != "0" ]]; then
  echo ""
  echo "=== FAILURE — dumping datagen container logs ==="
  echo ""
  echo "--- tpc-di-gen (exit $DIGEN_EXIT) ---"
  docker compose -f "$DATAGEN_COMPOSE" logs tpc-di-gen 2>/dev/null || true
  echo ""
  echo "--- spark-digen-delta (exit $DELTA_EXIT) ---"
  docker compose -f "$DATAGEN_COMPOSE" logs spark-digen-delta 2>/dev/null || true
  echo ""
  docker compose -f "$DATAGEN_COMPOSE" down --remove-orphans 2>/dev/null || true
  echo "=== FAILED (tpc-di-gen=$DIGEN_EXIT, spark-digen-delta=$DELTA_EXIT) ==="
  exit 1
fi

docker compose -f "$DATAGEN_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 1: Completed (tpc-di-gen=$DIGEN_EXIT, spark-digen-delta=$DELTA_EXIT) ==="

echo "=== Phase 1: Fixing Delta directory permissions ==="
docker run --rm -v "$(pwd)/mount/raw/${SCALE_FACTOR}/delta:/data" alpine chmod -R 777 /data

# ---------------------------------------------------------------------------
# Build batch-loader image (once)
# ---------------------------------------------------------------------------
echo ""
echo "=== Building batch-loader image ==="
docker compose -f "$BATCH_LOADER_COMPOSE" build

# ---------------------------------------------------------------------------
# Phase 2 — dbt benchmark (multi-batch)
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2: Building benchmark images ==="
docker compose -f "$BENCHMARK_COMPOSE" build
docker compose -f "$DUCKDB_COMPOSE" build
docker compose -f "$FELDERA_COMPOSE" build

# ---------------------------------------------------------------------------
# Phase 2a — Spark benchmark (3 batches, full refresh each time)
# ---------------------------------------------------------------------------
echo ""
echo "========================================"
echo "=== Phase 2a: Spark benchmark start ==="
echo "========================================"

# Init staging from batch1
batch_loader init

echo "=== Phase 2a: Starting benchmark stack (mssql → spark → dbt-server) ==="
docker compose -f "$BENCHMARK_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/benchmark-up.log"

echo "=== Phase 2a: Waiting for dbt-server health ==="
if ! wait_for_health; then
  docker compose -f "$BENCHMARK_COMPOSE" logs 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 1 run
run_dbt spark 1
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed (batch 1) ==="
  docker compose -f "$BENCHMARK_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" logs spark 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: append + run
batch_loader append 2
run_dbt spark 2
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed (batch 2) ==="
  docker compose -f "$BENCHMARK_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 3: append + run
batch_loader append 3
run_dbt spark 3
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed (batch 3) ==="
  docker compose -f "$BENCHMARK_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2a: Tearing down Spark benchmark stack ==="
docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2a: Spark completed successfully ==="

# ---------------------------------------------------------------------------
# Phase 2b — DuckDB benchmark (3 batches, full refresh each time)
# ---------------------------------------------------------------------------
echo ""
echo "========================================="
echo "=== Phase 2b: DuckDB benchmark start ==="
echo "========================================="

# Re-init staging from batch1 (fresh start for DuckDB)
batch_loader init

echo "=== Phase 2b: Starting DuckDB benchmark stack (dbt-server only) ==="
docker compose -f "$DUCKDB_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/duckdb-up.log"

echo "=== Phase 2b: Waiting for dbt-server health ==="
if ! wait_for_health; then
  docker compose -f "$DUCKDB_COMPOSE" logs 2>/dev/null || true
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 1 run
run_dbt duckdb 1
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed (batch 1) ==="
  docker compose -f "$DUCKDB_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: append + run
batch_loader append 2
run_dbt duckdb 2
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed (batch 2) ==="
  docker compose -f "$DUCKDB_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 3: append + run
batch_loader append 3
run_dbt duckdb 3
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed (batch 3) ==="
  docker compose -f "$DUCKDB_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2b: Tearing down DuckDB benchmark stack ==="
docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2b: DuckDB completed successfully ==="

# ---------------------------------------------------------------------------
# Phase 2c — Feldera benchmark (3 batches, pipeline stays running)
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "=== Phase 2c: Feldera benchmark start ==="
echo "=========================================="

# Re-init staging from batch1 (fresh start for Feldera)
batch_loader init

echo "=== Phase 2c: Starting Feldera benchmark stack (pipeline-manager → dbt-server) ==="
docker compose -f "$FELDERA_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/feldera-up.log"

echo "=== Phase 2c: Waiting for dbt-server health ==="
if ! wait_for_health; then
  docker compose -f "$FELDERA_COMPOSE" logs 2>/dev/null || true
  docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 1: dbt build creates the pipeline, dbt-server polls until pipeline finishes + Delta flush
run_dbt feldera 1
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Feldera dbt run failed (batch 1) ==="
  docker compose -f "$FELDERA_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$FELDERA_COMPOSE" logs pipeline-manager 2>/dev/null || true
  docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: capture baseline BEFORE append, then append, then wait
BASELINE_INPUT=$(curl -sf "http://localhost:5000/stats/feldera" | jq -r '.total_input_records // 0')
START_EPOCH=$(date +%s.%N)
batch_loader append 2
echo "=== Phase 2c: Waiting for Feldera pipeline to process batch 2 ==="
WAIT_RESPONSE=$(curl -sf -X POST "http://localhost:5000/wait/feldera" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"batch_num\": 2, \"baseline_input\": $BASELINE_INPUT, \"start_epoch_s\": $START_EPOCH}")
FELDERA_B2_DURATION=$(echo "$WAIT_RESPONSE" | jq -r '.duration_s // "?"')
echo "  Feldera batch 2 processing time: ${FELDERA_B2_DURATION}s"
echo "  results saved to $RESULTS_DIR/run-feldera-batch2.json"

# Batch 3: capture baseline BEFORE append, then append, then wait
BASELINE_INPUT=$(curl -sf "http://localhost:5000/stats/feldera" | jq -r '.total_input_records // 0')
START_EPOCH=$(date +%s.%N)
batch_loader append 3
echo "=== Phase 2c: Waiting for Feldera pipeline to process batch 3 ==="
WAIT_RESPONSE=$(curl -sf -X POST "http://localhost:5000/wait/feldera" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"batch_num\": 3, \"baseline_input\": $BASELINE_INPUT, \"start_epoch_s\": $START_EPOCH}")
FELDERA_B3_DURATION=$(echo "$WAIT_RESPONSE" | jq -r '.duration_s // "?"')
echo "  Feldera batch 3 processing time: ${FELDERA_B3_DURATION}s"
echo "  results saved to $RESULTS_DIR/run-feldera-batch3.json"

echo "=== Phase 2c: Tearing down Feldera benchmark stack ==="
docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2c: Feldera completed successfully ==="

echo ""
echo "=== Phase 3: Generating results chart ==="
docker compose -f "$DUCKDB_COMPOSE" up -d 2>&1 | tail -3
wait_for_health 5000
curl -sf -o results.png http://localhost:5000/chart
docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "  Chart saved to results.png"

echo ""
echo "=== All benchmarks completed successfully ==="
