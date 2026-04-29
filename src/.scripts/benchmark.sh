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
docker compose -f docker-compose.benchmark.spark.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.benchmark.duckdb.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.benchmark.feldera.yml down --remove-orphans 2>/dev/null || true

DATAGEN_COMPOSE="docker-compose.datagen.yml"
BENCHMARK_COMPOSE="docker-compose.benchmark.spark.yml"
DUCKDB_COMPOSE="docker-compose.benchmark.duckdb.yml"
FELDERA_COMPOSE="docker-compose.benchmark.feldera.yml"

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
# Phase 2 — dbt benchmark
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2: Building benchmark images ==="
docker compose -f "$BENCHMARK_COMPOSE" build
docker compose -f "$DUCKDB_COMPOSE" build
docker compose -f "$FELDERA_COMPOSE" build

echo "=== Phase 2a: Starting benchmark stack (mssql → spark → dbt-server) ==="
docker compose -f "$BENCHMARK_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/benchmark-up.log"

echo "=== Phase 2a: Waiting for dbt-server health ==="
HEALTH_RETRIES=60
HEALTH_OK=0
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
  docker compose -f "$BENCHMARK_COMPOSE" logs 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi
echo "  dbt-server is healthy"

echo "=== Phase 2a: Triggering dbt run (engine=spark, SF=$SCALE_FACTOR) ==="
RUN_RESPONSE=$(curl -sf -X POST "http://localhost:5000/run/spark" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"full_refresh\": true}")
RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
echo "  run_id=$RUN_ID"

# ---------------------------------------------------------------------------
# Stream /runs/<id>/progress/stream — server sends pre-formatted lines
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2a: dbt build progress ==="
echo ""

stream_progress "$RUN_ID"

echo ""

RESULTS_DIR="mount/results/${SCALE_FACTOR}/dbt-server"
mkdir -p "$RESULTS_DIR" 2>/dev/null || true
curl -sf "http://localhost:5000/runs/$RUN_ID" | jq . > "$RESULTS_DIR/run-spark.json"
echo "  results saved to $RESULTS_DIR/run-spark.json"

if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed ==="
  echo "--- dbt-server logs ---"
  docker compose -f "$BENCHMARK_COMPOSE" logs dbt-server 2>/dev/null || true
  echo "--- spark logs ---"
  docker compose -f "$BENCHMARK_COMPOSE" logs spark 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2a: Tearing down Spark benchmark stack ==="
docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2a: Spark completed successfully ==="

# ---------------------------------------------------------------------------
# Phase 2b — DuckDB benchmark (dbt-server only, full host resources)
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2b: Starting DuckDB benchmark stack (dbt-server only) ==="
docker compose -f "$DUCKDB_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/duckdb-up.log"

echo "=== Phase 2b: Waiting for dbt-server health ==="
HEALTH_OK=0
for i in $(seq 1 $HEALTH_RETRIES); do
  if curl -sf http://localhost:5000/health >/dev/null 2>&1; then
    HEALTH_OK=1
    break
  fi
  echo "  waiting for dbt-server... ($i/$HEALTH_RETRIES)"
  sleep 5
done

if [[ "$HEALTH_OK" != "1" ]]; then
  echo "=== FAILURE — dbt-server did not become healthy (DuckDB phase) ==="
  docker compose -f "$DUCKDB_COMPOSE" logs 2>/dev/null || true
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi
echo "  dbt-server is healthy"

echo "=== Phase 2b: Triggering dbt run (engine=duckdb, SF=$SCALE_FACTOR) ==="
RUN_RESPONSE=$(curl -sf -X POST "http://localhost:5000/run/duckdb" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"full_refresh\": true}")
RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
echo "  run_id=$RUN_ID"

echo ""
echo "=== Phase 2b: dbt build progress (DuckDB) ==="
echo ""

stream_progress "$RUN_ID"

echo ""

curl -sf "http://localhost:5000/runs/$RUN_ID" | jq . > "$RESULTS_DIR/run-duckdb.json"
echo "  results saved to $RESULTS_DIR/run-duckdb.json"

if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed ==="
  echo "--- dbt-server logs ---"
  docker compose -f "$DUCKDB_COMPOSE" logs dbt-server 2>/dev/null || true
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2b: Tearing down DuckDB benchmark stack ==="
docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2b: DuckDB completed successfully ==="

# ---------------------------------------------------------------------------
# Phase 2c — Feldera benchmark (pipeline-manager + dbt-server)
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2c: Starting Feldera benchmark stack (pipeline-manager → dbt-server) ==="
docker compose -f "$FELDERA_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/feldera-up.log"

echo "=== Phase 2c: Waiting for dbt-server health ==="
HEALTH_OK=0
for i in $(seq 1 $HEALTH_RETRIES); do
  if curl -sf http://localhost:5000/health >/dev/null 2>&1; then
    HEALTH_OK=1
    break
  fi
  echo "  waiting for dbt-server... ($i/$HEALTH_RETRIES)"
  sleep 5
done

if [[ "$HEALTH_OK" != "1" ]]; then
  echo "=== FAILURE — dbt-server did not become healthy (Feldera phase) ==="
  docker compose -f "$FELDERA_COMPOSE" logs 2>/dev/null || true
  docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi
echo "  dbt-server is healthy"

echo "=== Phase 2c: Triggering dbt run (engine=feldera, SF=$SCALE_FACTOR) ==="
RUN_RESPONSE=$(curl -sf -X POST "http://localhost:5000/run/feldera" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"full_refresh\": true}")
RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
echo "  run_id=$RUN_ID"

echo ""
echo "=== Phase 2c: dbt build progress (Feldera) ==="
echo ""

stream_progress "$RUN_ID"

echo ""

curl -sf "http://localhost:5000/runs/$RUN_ID" | jq . > "$RESULTS_DIR/run-feldera.json"
echo "  results saved to $RESULTS_DIR/run-feldera.json"

if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Feldera dbt run failed ==="
  echo "--- dbt-server logs ---"
  docker compose -f "$FELDERA_COMPOSE" logs dbt-server 2>/dev/null || true
  echo "--- pipeline-manager logs ---"
  docker compose -f "$FELDERA_COMPOSE" logs pipeline-manager 2>/dev/null || true
  docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Wait for Feldera pipeline to finish processing all input data.
# dbt build only waits for compilation+startup, not snapshot ingestion.
FELDERA_URL="http://localhost:8080"
PIPELINE_NAME="tpcdi"
PIPELINE_WAIT_RETRIES=1200
echo "=== Phase 2c: Waiting for Feldera pipeline to finish processing snapshots ==="
PIPELINE_IDLE=0
for i in $(seq 1 $PIPELINE_WAIT_RETRIES); do
  PIPELINE_STATUS=$(curl -sf "${FELDERA_URL}/v0/pipelines/${PIPELINE_NAME}/stats" 2>/dev/null || echo "{}")
  STATE=$(echo "$PIPELINE_STATUS" | jq -r '.global_metrics.state // "unknown"')
  TOTAL_IN=$(echo "$PIPELINE_STATUS" | jq -r '.global_metrics.total_input_records // 0')
  TOTAL_PROC=$(echo "$PIPELINE_STATUS" | jq -r '.global_metrics.total_processed_records // 0')
  COMPLETE=$(echo "$PIPELINE_STATUS" | jq -r '.global_metrics.pipeline_complete // false')

  if [[ "$COMPLETE" == "True" ]] || [[ "$COMPLETE" == "true" ]]; then
    PIPELINE_IDLE=1
    echo "  Pipeline complete: processed=$TOTAL_PROC input=$TOTAL_IN"
    break
  fi
  # Also check if all inputs consumed and pipeline is idle
  if [[ "$TOTAL_IN" -gt 0 ]] && [[ "$TOTAL_PROC" -ge "$TOTAL_IN" ]]; then
    PIPELINE_IDLE=1
    echo "  Pipeline idle: processed=$TOTAL_PROC >= input=$TOTAL_IN"
    break
  fi

  echo "  waiting for pipeline... state=$STATE processed=$TOTAL_PROC/$TOTAL_IN ($i/$PIPELINE_WAIT_RETRIES)"
  sleep 5
done

if [[ "$PIPELINE_IDLE" != "1" ]]; then
  echo "=== WARNING — Feldera pipeline did not reach idle state within timeout ==="
fi

echo "=== Phase 2c: Tearing down Feldera benchmark stack ==="
docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2c: Feldera completed successfully ==="
echo ""
echo "=== All benchmarks completed successfully ==="
