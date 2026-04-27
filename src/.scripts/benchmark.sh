#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

export SCALE_FACTOR="${SCALE_FACTOR:-3}"
LOGS_DIR=".logs"

mkdir -p "$LOGS_DIR"

# Pre-create mount directories so Docker doesn't create them as root
for d in "mount/results/${SCALE_FACTOR}/spark" "mount/results/${SCALE_FACTOR}/dbt-server"; do
  mkdir -p "$d" 2>/dev/null || {
    docker run --rm -v "$(pwd)/mount:/mount" alpine mkdir -p "/${d}" 2>/dev/null
    docker run --rm -v "$(pwd)/mount:/mount" alpine chown -R "$(id -u):$(id -g)" /mount 2>/dev/null
    mkdir -p "$d" 2>/dev/null || true
  }
done

echo "=== Tearing down any running containers ==="
docker compose -f docker-compose.datagen.yml down --remove-orphans 2>/dev/null || true
docker compose -f docker-compose.benchmark.yml down --remove-orphans 2>/dev/null || true

DATAGEN_COMPOSE="docker-compose.datagen.yml"
BENCHMARK_COMPOSE="docker-compose.benchmark.yml"

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

# ---------------------------------------------------------------------------
# Phase 2 — dbt benchmark
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2: Building benchmark images ==="
docker compose -f "$BENCHMARK_COMPOSE" build

echo "=== Phase 2: Starting benchmark stack (mssql → spark → dbt-server) ==="
docker compose -f "$BENCHMARK_COMPOSE" up -d 2>&1 | tee "$LOGS_DIR/benchmark-up.log"

echo "=== Phase 2: Waiting for dbt-server health ==="
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

echo "=== Phase 2: Triggering dbt run (engine=spark, SF=$SCALE_FACTOR) ==="
RUN_RESPONSE=$(curl -sf -X POST "http://localhost:5000/run/spark" \
  -H 'Content-Type: application/json' \
  -d "{\"scale_factor\": $SCALE_FACTOR, \"full_refresh\": true}")
RUN_ID=$(echo "$RUN_RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])")
echo "  run_id=$RUN_ID"

# ---------------------------------------------------------------------------
# Poll /runs/<id>/progress with Retry-After, print dbt-CLI-style progress
# ---------------------------------------------------------------------------
echo ""
echo "=== Phase 2: dbt build progress ==="
echo ""

CURSOR=0
RUN_STATUS="running"

while true; do
  PROGRESS=$(curl -sf -D /tmp/dbt-progress-headers \
    "http://localhost:5000/runs/$RUN_ID/progress?since=$CURSOR" 2>/dev/null) || {
    sleep 3
    continue
  }

  # Parse and pretty-print new events
  read -r RUN_STATUS NEW_CURSOR < <(echo "$PROGRESS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
rs = d.get('run_status', 'unknown')
nc = d.get('next_cursor', 0)
total = d.get('total', 0)
completed = d.get('completed', 0)
running = d.get('running', 0)

# Print new events (dbt-CLI style) — number events sequentially
events = d.get('events', [])
since = int(sys.argv[1]) if len(sys.argv) > 1 else 0
done_count = 0
for i, e in enumerate(events):
    st = e.get('status', '')
    name = e.get('name', '')
    rtype = e.get('resource_type', 'model')
    t = e.get('execution_time_s')
    rows = e.get('rows_affected')
    idx = since + i + 1  # 1-based sequential

    if st == 'running':
        print(f'  {idx:>3} of {total}  START {rtype} {name}', file=sys.stderr)
    elif st in ('success', 'pass'):
        done_count += 1
        ts = f'{t:.2f}s' if t else '?'
        row_str = f' [{rows} rows]' if rows else ''
        print(f'  {idx:>3} of {total}  OK    {rtype} {name}{row_str} [{ts}]', file=sys.stderr)
    elif st == 'error':
        ts = f'{t:.2f}s' if t else '?'
        print(f'  {idx:>3} of {total}  ERROR {rtype} {name} [{ts}]', file=sys.stderr)
    else:
        print(f'  {idx:>3} of {total}  {st:5s} {rtype} {name}', file=sys.stderr)

# Print summary line (running nodes)
running_nodes = d.get('running_nodes', [])
if running_nodes and rs == 'running':
    rn_str = ', '.join(running_nodes[:4])
    if len(running_nodes) > 4:
        rn_str += f' (+{len(running_nodes)-4} more)'
    print(f'  ... {completed}/{total} done, {running} running: {rn_str}', file=sys.stderr)

print(f'{rs} {nc}')
" "$CURSOR")

  CURSOR=$NEW_CURSOR

  if [[ "$RUN_STATUS" == "completed" || "$RUN_STATUS" == "failed" ]]; then
    break
  fi

  # Respect Retry-After header, fall back to 3s
  RETRY_AFTER=$(grep -i 'retry-after' /tmp/dbt-progress-headers 2>/dev/null | awk '{print $2}' | tr -d '\r' || echo "3")
  RETRY_AFTER=${RETRY_AFTER:-3}
  sleep "$RETRY_AFTER"
done

echo ""

RESULTS_DIR="mount/results/${SCALE_FACTOR}/dbt-server"
mkdir -p "$RESULTS_DIR" 2>/dev/null || true
curl -sf "http://localhost:5000/runs/$RUN_ID" | python3 -m json.tool > "$RESULTS_DIR/run-spark.json"
echo "  results saved to $RESULTS_DIR/run-spark.json"

if [[ "$RUN_STATUS" == "failed" ]]; then
  echo "=== FAILURE — dbt run failed ==="
  echo "--- dbt-server logs ---"
  docker compose -f "$BENCHMARK_COMPOSE" logs dbt-server 2>/dev/null || true
  echo "--- spark logs ---"
  docker compose -f "$BENCHMARK_COMPOSE" logs spark 2>/dev/null || true
  docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
  exit 1
fi

echo "=== Phase 2: Tearing down benchmark stack ==="
docker compose -f "$BENCHMARK_COMPOSE" down --remove-orphans 2>/dev/null || true
echo "=== Phase 2: Completed successfully ==="
