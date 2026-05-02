#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

export SCALE_FACTOR="${SCALE_FACTOR:-3}"
LOGS_DIR=".logs"

mkdir -p "$LOGS_DIR"

# Pre-create mount directories so Docker doesn't create them as root
for d in "mount/results/${SCALE_FACTOR}/spark" "mount/results/${SCALE_FACTOR}/duckdb" "mount/results/${SCALE_FACTOR}/feldera" "mount/results/${SCALE_FACTOR}/openivm" "mount/results/${SCALE_FACTOR}/dbt-server" "mount/bin/openivm" "mount/logs/${SCALE_FACTOR}/spark" "mount/logs/${SCALE_FACTOR}/duckdb" "mount/logs/${SCALE_FACTOR}/feldera" "mount/logs/${SCALE_FACTOR}/openivm" "mount/stats/${SCALE_FACTOR}/spark" "mount/stats/${SCALE_FACTOR}/duckdb" "mount/stats/${SCALE_FACTOR}/feldera" "mount/stats/${SCALE_FACTOR}/openivm"; do
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
docker compose -f docker-compose.benchmark.openivm.yml down --remove-orphans 2>/dev/null || true

DATAGEN_COMPOSE="docker-compose.datagen.yml"
OPENIVM_BUILD_COMPOSE="docker-compose.openivm-build.yml"
BATCH_LOADER_COMPOSE="docker-compose.batch-loader.yml"
BENCHMARK_COMPOSE="docker-compose.benchmark.spark.yml"
DUCKDB_COMPOSE="docker-compose.benchmark.duckdb.yml"
OPENIVM_COMPOSE="docker-compose.benchmark.openivm.yml"
FELDERA_COMPOSE="docker-compose.benchmark.feldera.yml"

RESULTS_DIR="mount/results/${SCALE_FACTOR}/dbt-server"
HEALTH_RETRIES=60

# ---------------------------------------------------------------------------
# Timing variables (seconds per engine per batch)
# ---------------------------------------------------------------------------
SPARK_B1=0; SPARK_B2=0; SPARK_B3=0
DUCKDB_B1=0; DUCKDB_B2=0; DUCKDB_B3=0
FELDERA_B1=0; FELDERA_B2=0; FELDERA_B3=0
OPENIVM_B1=0; OPENIVM_B2=0; OPENIVM_B3=0

fmt_duration() {
  local secs=$1
  printf "%02d:%02d:%02d" $((secs/3600)) $(((secs%3600)/60)) $((secs%60))
}

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
# Helper: trigger an OpenIVM run, stream progress, save results
# Usage:  run_openivm <batch_num>
#         (sets global RUN_STATUS)
# ---------------------------------------------------------------------------
run_openivm() {
  local batch_num="$1"
  local full_refresh="true"
  if [[ "$batch_num" != "1" ]]; then
    full_refresh="false"
  fi

  echo "=== Triggering OpenIVM run (batch=$batch_num, SF=$SCALE_FACTOR, full_refresh=$full_refresh) ==="
  local RUN_RESPONSE
  RUN_RESPONSE=$(curl -sf -X POST "http://localhost:5000/run/openivm" \
    -H 'Content-Type: application/json' \
    -d "{\"scale_factor\": $SCALE_FACTOR, \"full_refresh\": $full_refresh, \"batch_num\": $batch_num}")
  local RUN_ID
  RUN_ID=$(echo "$RUN_RESPONSE" | jq -r '.run_id')
  echo "  run_id=$RUN_ID"

  echo ""
  echo "=== OpenIVM progress (batch $batch_num) ==="
  echo ""

  stream_progress "$RUN_ID"

  echo ""

  mkdir -p "$RESULTS_DIR" 2>/dev/null || true
  curl -sf "http://localhost:5000/runs/$RUN_ID" | jq . > "$RESULTS_DIR/run-openivm-batch${batch_num}.json"
  echo "  results saved to $RESULTS_DIR/run-openivm-batch${batch_num}.json"
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
# Helper: capture all container logs before teardown
# Usage:  capture_logs <compose_file> <engine>
# ---------------------------------------------------------------------------
capture_logs() {
  local compose_file="$1"
  local engine="$2"
  local logs_dest="mount/logs/${SCALE_FACTOR}/${engine}"
  mkdir -p "$logs_dest" 2>/dev/null || true

  echo "=== Capturing container logs for $engine ==="
  local services
  services=$(docker compose -f "$compose_file" ps --services 2>/dev/null || true)
  for svc in $services; do
    docker compose -f "$compose_file" logs --no-color --timestamps "$svc" \
      > "$logs_dest/${svc}.log" 2>/dev/null || true
  done
  echo "  logs saved to $logs_dest/"
}

# ---------------------------------------------------------------------------
# Helper: fetch dbt lineage for an engine
# Usage:  fetch_lineage <engine>
# ---------------------------------------------------------------------------
fetch_lineage() {
  local engine="$1"
  local output_name="${2:-$engine}"
  echo "=== Fetching dbt lineage for $output_name ==="
  local lineage_file="$RESULTS_DIR/lineage-${output_name}.json"
  if curl -sf "http://localhost:5000/lineage/$engine" | jq . > "$lineage_file" 2>/dev/null; then
    echo "  lineage saved to $lineage_file"
  else
    echo "  WARNING: failed to fetch lineage for $output_name (non-fatal)"
  fi
}

# ---------------------------------------------------------------------------
# Helper: fetch SQL analysis (AST + operators) for an engine
# Usage:  fetch_sql_analysis <engine>
# ---------------------------------------------------------------------------
fetch_sql_analysis() {
  local engine="$1"
  local output_name="${2:-$engine}"
  echo "=== Fetching SQL analysis for $output_name ==="
  local analysis_file="$RESULTS_DIR/sql-analysis-${output_name}.json"
  if curl -sf "http://localhost:5000/sql/$engine" | jq . > "$analysis_file" 2>/dev/null; then
    echo "  sql analysis saved to $analysis_file"
  else
    echo "  WARNING: failed to fetch sql analysis for $output_name (non-fatal)"
  fi
}

# ---------------------------------------------------------------------------
# Helper: start container stats collection
# Usage:  start_stats <engine>
# ---------------------------------------------------------------------------
start_stats() {
  local engine="$1"
  echo "=== Starting container stats collection for $engine ==="
  if curl -sf -X POST "http://localhost:5000/stats/containers/start" \
    -H 'Content-Type: application/json' \
    -d "{\"engine\": \"$engine\", \"scale_factor\": $SCALE_FACTOR}" >/dev/null 2>&1; then
    echo "  stats collection started"
  else
    echo "  WARNING: failed to start stats collection (non-fatal)"
  fi
}

# ---------------------------------------------------------------------------
# Helper: stop container stats collection
# Usage:  stop_stats
# ---------------------------------------------------------------------------
stop_stats() {
  echo "=== Stopping container stats collection ==="
  local resp
  resp=$(curl -sf -X POST "http://localhost:5000/stats/containers/stop" 2>/dev/null || echo "{}")
  local count
  count=$(echo "$resp" | jq -r '.sample_count // 0')
  echo "  stats collection stopped ($count samples)"
}

# ---------------------------------------------------------------------------
# Helper: capture delta stats for staging tables
# Usage:  capture_delta_stats <batch_num>
# ---------------------------------------------------------------------------
capture_delta_stats() {
  local batch_num="$1"
  echo "=== Capturing delta stats for batch $batch_num ==="
  local stats_file="$RESULTS_DIR/delta-stats-batch${batch_num}.json"
  if curl -sf "http://localhost:5000/delta-stats" | jq . > "$stats_file" 2>/dev/null; then
    echo "  delta stats saved to $stats_file"
  else
    echo "  WARNING: failed to capture delta stats for batch $batch_num (non-fatal)"
  fi
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
# Build OpenIVM binary (once, idempotent)
# ---------------------------------------------------------------------------
echo ""
echo "=== Building OpenIVM binary ==="
docker compose -f "$OPENIVM_BUILD_COMPOSE" build 2>&1 | tee "$LOGS_DIR/openivm-build.log"
docker compose -f "$OPENIVM_BUILD_COMPOSE" up openivm-builder 2>&1 | tee -a "$LOGS_DIR/openivm-build.log"
docker compose -f "$OPENIVM_BUILD_COMPOSE" down --remove-orphans 2>/dev/null || true

if [[ ! -f "mount/bin/openivm/duckdb" ]]; then
  echo "=== FAILURE — OpenIVM binary not found at mount/bin/openivm/duckdb ==="
  exit 1
fi
echo "=== OpenIVM binary ready ==="

# ---------------------------------------------------------------------------
# Phase 2 — dbt benchmark (multi-batch)
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2: Building benchmark images ==="
docker compose -f "$BENCHMARK_COMPOSE" build
docker compose -f "$DUCKDB_COMPOSE" build
docker compose -f "$OPENIVM_COMPOSE" build
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
  capture_logs "$BENCHMARK_COMPOSE" spark
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

start_stats spark
capture_delta_stats 1
_t0=$(date +%s)
run_dbt spark 1
SPARK_B1=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed (batch 1) ==="
  capture_logs "$BENCHMARK_COMPOSE" spark
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: append + run
batch_loader append 2
capture_delta_stats 2
_t0=$(date +%s)
run_dbt spark 2
SPARK_B2=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed (batch 2) ==="
  capture_logs "$BENCHMARK_COMPOSE" spark
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 3: append + run
batch_loader append 3
capture_delta_stats 3
_t0=$(date +%s)
run_dbt spark 3
SPARK_B3=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Spark dbt run failed (batch 3) ==="
  capture_logs "$BENCHMARK_COMPOSE" spark
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2a: Tearing down Spark benchmark stack ==="
stop_stats
fetch_sql_analysis spark
fetch_lineage spark
capture_logs "$BENCHMARK_COMPOSE" spark
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
  capture_logs "$DUCKDB_COMPOSE" duckdb
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

start_stats duckdb
_t0=$(date +%s)
run_dbt duckdb 1
DUCKDB_B1=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed (batch 1) ==="
  capture_logs "$DUCKDB_COMPOSE" duckdb
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: append + run
batch_loader append 2
_t0=$(date +%s)
run_dbt duckdb 2
DUCKDB_B2=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed (batch 2) ==="
  capture_logs "$DUCKDB_COMPOSE" duckdb
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 3: append + run
batch_loader append 3
_t0=$(date +%s)
run_dbt duckdb 3
DUCKDB_B3=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — DuckDB dbt run failed (batch 3) ==="
  capture_logs "$DUCKDB_COMPOSE" duckdb
  docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2b: Tearing down DuckDB benchmark stack ==="
stop_stats
fetch_sql_analysis duckdb
fetch_lineage duckdb

# Persist DuckDB manifest for OpenIVM to reuse (it needs compiled SQL + DAG)
echo "=== Phase 2b: Persisting DuckDB manifest for OpenIVM ==="
DBT_SERVER_CONTAINER=$(docker compose -f "$DUCKDB_COMPOSE" ps -q dbt-server 2>/dev/null | head -1)
if [[ -n "$DBT_SERVER_CONTAINER" ]]; then
  docker cp "${DBT_SERVER_CONTAINER}:/app/dbt-projects/duckdb/target/manifest.json" \
    "$RESULTS_DIR/manifest-duckdb.json" 2>/dev/null || \
    echo "  WARNING: Could not copy DuckDB manifest (non-fatal)"
fi

capture_logs "$DUCKDB_COMPOSE" duckdb
docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2b: DuckDB completed successfully ==="

# ---------------------------------------------------------------------------
# Phase 2c — OpenIVM benchmark (3 batches, DuckLake + OpenIVM executable)
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "=== Phase 2c: OpenIVM benchmark start ==="
echo "=========================================="

echo "=== Phase 2c: Starting OpenIVM benchmark stack (dbt-server only) ==="
docker compose -f "$OPENIVM_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/openivm-up.log"

echo "=== Phase 2c: Waiting for dbt-server health ==="
if ! wait_for_health; then
  capture_logs "$OPENIVM_COMPOSE" openivm
  docker compose -f "$OPENIVM_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

start_stats openivm
_t0=$(date +%s)
run_openivm 1
OPENIVM_B1=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — OpenIVM run failed (batch 1) ==="
  capture_logs "$OPENIVM_COMPOSE" openivm
  docker compose -f "$OPENIVM_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: append + refresh (source loading handled inside dbt-server)
_t0=$(date +%s)
run_openivm 2
OPENIVM_B2=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — OpenIVM run failed (batch 2) ==="
  capture_logs "$OPENIVM_COMPOSE" openivm
  docker compose -f "$OPENIVM_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 3: append + refresh
_t0=$(date +%s)
run_openivm 3
OPENIVM_B3=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — OpenIVM run failed (batch 3) ==="
  capture_logs "$OPENIVM_COMPOSE" openivm
  docker compose -f "$OPENIVM_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2c: Tearing down OpenIVM benchmark stack ==="
stop_stats
# OpenIVM uses DuckDB's compiled SQL, so sql/lineage analysis is identical
fetch_sql_analysis duckdb openivm
fetch_lineage duckdb openivm
capture_logs "$OPENIVM_COMPOSE" openivm
docker compose -f "$OPENIVM_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2c: OpenIVM completed successfully ==="

# ---------------------------------------------------------------------------
# Phase 2d — Feldera benchmark (3 batches, pipeline stays running)
# ---------------------------------------------------------------------------
echo ""
echo "=========================================="
echo "=== Phase 2d: Feldera benchmark start ==="
echo "=========================================="

# Re-init staging from batch1 (fresh start for Feldera)
batch_loader init

echo "=== Phase 2d: Starting Feldera benchmark stack (pipeline-manager → dbt-server) ==="
docker compose -f "$FELDERA_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/feldera-up.log"

echo "=== Phase 2d: Waiting for dbt-server health ==="
if ! wait_for_health; then
  capture_logs "$FELDERA_COMPOSE" feldera
  docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

start_stats feldera
_t0=$(date +%s)
run_dbt feldera 1
FELDERA_B1=$(( $(date +%s) - _t0 ))
if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — Feldera dbt run failed (batch 1) ==="
  capture_logs "$FELDERA_COMPOSE" feldera
  docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

# Batch 2: capture baseline BEFORE append, then append, then wait
_t0=$(date +%s)
BASELINE_INPUT=$(curl -sf "http://localhost:5000/stats/feldera" | jq -r '.total_input_records // 0')
START_EPOCH=$(date +%s.%N)
batch_loader append 2
echo "=== Phase 2d: Waiting for Feldera pipeline to process batch 2 ==="
WAIT_RESPONSE=$(curl -sf -X POST "http://localhost:5000/wait/feldera" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"batch_num\": 2, \"baseline_input\": $BASELINE_INPUT, \"start_epoch_s\": $START_EPOCH}")
FELDERA_B2=$(( $(date +%s) - _t0 ))
FELDERA_B2_DURATION=$(echo "$WAIT_RESPONSE" | jq -r '.duration_s // "?"')
echo "  Feldera batch 2 processing time: ${FELDERA_B2_DURATION}s"
echo "  results saved to $RESULTS_DIR/run-feldera-batch2.json"

# Batch 3: capture baseline BEFORE append, then append, then wait
_t0=$(date +%s)
BASELINE_INPUT=$(curl -sf "http://localhost:5000/stats/feldera" | jq -r '.total_input_records // 0')
START_EPOCH=$(date +%s.%N)
batch_loader append 3
echo "=== Phase 2d: Waiting for Feldera pipeline to process batch 3 ==="
WAIT_RESPONSE=$(curl -sf -X POST "http://localhost:5000/wait/feldera" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"batch_num\": 3, \"baseline_input\": $BASELINE_INPUT, \"start_epoch_s\": $START_EPOCH}")
FELDERA_B3=$(( $(date +%s) - _t0 ))
FELDERA_B3_DURATION=$(echo "$WAIT_RESPONSE" | jq -r '.duration_s // "?"')
echo "  Feldera batch 3 processing time: ${FELDERA_B3_DURATION}s"
echo "  results saved to $RESULTS_DIR/run-feldera-batch3.json"

echo "=== Phase 2d: Tearing down Feldera benchmark stack ==="
stop_stats
fetch_sql_analysis feldera
fetch_lineage feldera
capture_logs "$FELDERA_COMPOSE" feldera
docker compose -f "$FELDERA_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2d: Feldera completed successfully ==="

echo ""
echo "=== Phase 3: Generating results chart ==="
docker compose -f "$DUCKDB_COMPOSE" up -d 2>&1 | tail -3
wait_for_health 5000
curl -sf -o "scale-factor-${SCALE_FACTOR}.png" "http://localhost:5000/chart?sf=${SCALE_FACTOR}"
docker compose -f "$DUCKDB_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "  Chart saved to scale-factor-${SCALE_FACTOR}.png"

echo ""
echo "=== All benchmarks completed successfully ==="
echo ""
printf "            %-12s%-12s%s\n" "1" "2" "3"
printf "Spark:      %s -> %s -> %s\n" "$(fmt_duration $SPARK_B1)" "$(fmt_duration $SPARK_B2)" "$(fmt_duration $SPARK_B3)"
printf "DuckDB:     %s -> %s -> %s\n" "$(fmt_duration $DUCKDB_B1)" "$(fmt_duration $DUCKDB_B2)" "$(fmt_duration $DUCKDB_B3)"
printf "OpenIVM:    %s -> %s -> %s\n" "$(fmt_duration $OPENIVM_B1)" "$(fmt_duration $OPENIVM_B2)" "$(fmt_duration $OPENIVM_B3)"
printf "Feldera:    %s -> %s -> %s\n" "$(fmt_duration $FELDERA_B1)" "$(fmt_duration $FELDERA_B2)" "$(fmt_duration $FELDERA_B3)"
echo ""
TOTAL_SECS=$(( SPARK_B1 + SPARK_B2 + SPARK_B3 + DUCKDB_B1 + DUCKDB_B2 + DUCKDB_B3 + OPENIVM_B1 + OPENIVM_B2 + OPENIVM_B3 + FELDERA_B1 + FELDERA_B2 + FELDERA_B3 ))
printf "================= %s ==================\n" "$(fmt_duration $TOTAL_SECS)"
