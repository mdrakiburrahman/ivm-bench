#!/bin/bash
# ---------------------------------------------------------------------------
# benchmark.sh — thin client that launches the benchmark-server and streams
# progress via Server-Sent Events.
#
# Two modes:
#
#   1. SINGLE EXPERIMENT (back-compat, default)
#      Drive one benchmark via the classic env vars below. Result PNG lands at
#      imgs/scale-factor-<SF>-<B1>-<B2>-<B3>.png and benchmark-results.json at
#      mount/results/<SF>/dbt-server/.
#
#   2. OAT SWEEP (new)
#      Set BENCHMARK_EXPERIMENTS_FILE=<absolute path under repo root> to an
#      experiments JSON file. The server runs the experiments serially with
#      disk-aware cleanup; aggregate artifacts (chart-oat.png,
#      chart-per-model.png, RESULTS.md, outputs.json) land at
#      mount/oat-state/<oat_run_id>/ (and mount/oat-state/latest →).
#
#      Built-in example sweeps:
#        src/containers/benchmark-server/experiments/sf-sweep.json   # full SF sweep
#        src/containers/benchmark-server/experiments/smoke.json      # SF=3 smoke
#
# Environment variables:
#   BENCHMARK_EXPERIMENTS_FILE — absolute path of OAT experiments JSON.
#                    MUST live under repo root so the existing bind-mount
#                    makes it visible inside the benchmark-server container
#                    at the same path. When set, batch/engine env vars are
#                    ignored (each experiment owns its own knobs).
#   OAT_MIN_FREE_PCT — minimum % free disk under repo root to keep running.
#                    Default 10. The sweep SKIPS remaining experiments below
#                    this threshold (no host crash on WSL).
#   SCALE_FACTOR   — TPC-DI scale factor (default: 3) [single mode]
#   BATCH_1_PCT    — % of batch 1 data (required in single mode)
#   BATCH_2_PCT    — % of batch 2 data (required in single mode)
#   BATCH_3_PCT    — % of batch 3 data (required in single mode)
#   PARALLEL       — 0 = serial (default), 1 = run engines in parallel
#   ENGINES        — comma-separated engine list (default: spark,spark-openivm,duckdb,duckdb-openivm,feldera,spark-openivm)
#   HOST_CORES     — override auto-detected CPU count
#   HOST_MEMORY    — override auto-detected memory in GB
#   PRESERVE_RAW   — 0 = clean mount/ at start (default), 1 = keep mount/raw/
#                    and mount/bin/ across runs so multi-hour Phase-1
#                    datagen + duckdb-openivm builds are reused
#   OPENIVM_VALIDATE — 1 = validate OpenIVM views with EXCEPT ALL after each
#                    timed batch (default), 0 = skip post-timer validation
#   OPENIVM_PROFILE_REFRESH — 1 = export OpenIVM refresh-profile CSVs after
#                    each timed batch (both duckdb-openivm and spark-openivm).
#                    Default: 0. When enabled, both engines emit a per-step
#                    profile under mount/results/<sf>/dbt-server/.
#   OPENIVM_QUERY_LOG — 1 = export the full SQL trace OpenIVM ran for every
#                    CREATE/REFRESH MV (spark-openivm only). Default: 1.
#                    Writes a per-MV per-refresh directory tree of `.sql`
#                    files (sqlglot-formatted) under
#                    mount/results/<sf>/spark-openivm/query-log/.
#   OPENIVM_UBUNTU_MIRROR — override OpenIVM Docker build apt mirror
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
REPO_ROOT="$(pwd)"

export BENCHMARK_EXPERIMENTS_FILE="${BENCHMARK_EXPERIMENTS_FILE:-}"
export OAT_MIN_FREE_PCT="${OAT_MIN_FREE_PCT:-10}"

if [[ -n "$BENCHMARK_EXPERIMENTS_FILE" ]]; then
  # OAT mode: resolve to absolute, validate under repo root.
  if [[ "$BENCHMARK_EXPERIMENTS_FILE" != /* ]]; then
    BENCHMARK_EXPERIMENTS_FILE="$REPO_ROOT/$BENCHMARK_EXPERIMENTS_FILE"
  fi
  if [[ ! -f "$BENCHMARK_EXPERIMENTS_FILE" ]]; then
    echo "ERROR: BENCHMARK_EXPERIMENTS_FILE=$BENCHMARK_EXPERIMENTS_FILE does not exist"
    exit 1
  fi
  case "$BENCHMARK_EXPERIMENTS_FILE" in
    "$REPO_ROOT"/*) ;;
    *)
      echo "ERROR: BENCHMARK_EXPERIMENTS_FILE must live under the repo root ($REPO_ROOT)"
      echo "       so the benchmark-server container can see it via the bind-mount."
      echo "       Move the file under the repo or symlink it in."
      exit 1
      ;;
  esac
  echo "=== OAT mode — experiments file: $BENCHMARK_EXPERIMENTS_FILE ==="
  # Provide harmless defaults so docker-compose interpolation never fails.
  export SCALE_FACTOR="${SCALE_FACTOR:-3}"
  export BATCH_1_PCT="${BATCH_1_PCT:-1}"
  export BATCH_2_PCT="${BATCH_2_PCT:-0.001}"
  export BATCH_3_PCT="${BATCH_3_PCT:-0.002}"
else
  export SCALE_FACTOR="${SCALE_FACTOR:-3}"
  if [[ -z "${BATCH_1_PCT:-}" || -z "${BATCH_2_PCT:-}" || -z "${BATCH_3_PCT:-}" ]]; then
    echo "ERROR: BATCH_1_PCT, BATCH_2_PCT, and BATCH_3_PCT must all be set."
    echo "  Example: export BATCH_1_PCT=1 BATCH_2_PCT=0.001 BATCH_3_PCT=0.002"
    exit 1
  fi
  export BATCH_1_PCT BATCH_2_PCT BATCH_3_PCT
fi

export PARALLEL="${PARALLEL:-0}"
export ENGINES="${ENGINES:-spark,spark-openivm,duckdb,duckdb-openivm,feldera,spark-openivm}"
export HOST_CORES="${HOST_CORES:-}"
export HOST_MEMORY="${HOST_MEMORY:-}"
export PRESERVE_RAW="${PRESERVE_RAW:-0}"
export OPENIVM_VALIDATE="${OPENIVM_VALIDATE:-1}"
export OPENIVM_PROFILE_REFRESH="${OPENIVM_PROFILE_REFRESH:-0}"
export OPENIVM_QUERY_LOG="${OPENIVM_QUERY_LOG:-1}"
export REPO_HOST_PATH="$(pwd)"

detect_ec2_ubuntu_mirror() {
  local token region
  token="$(curl -fsS -m 1 -X PUT \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60" \
    "http://169.254.169.254/latest/api/token" 2>/dev/null || true)"
  [[ -n "$token" ]] || return 1

  region="$(curl -fsS -m 1 \
    -H "X-aws-ec2-metadata-token: $token" \
    "http://169.254.169.254/latest/meta-data/placement/region" 2>/dev/null || true)"
  [[ -n "$region" ]] || return 1

  printf 'http://%s.ec2.archive.ubuntu.com/ubuntu\n' "$region"
}

if [[ -z "${OPENIVM_UBUNTU_MIRROR:-}" ]]; then
  OPENIVM_UBUNTU_MIRROR="$(detect_ec2_ubuntu_mirror || true)"
fi
export OPENIVM_UBUNTU_MIRROR

COMPOSE_FILE="docker/docker-compose.benchmark-server.yml"
HEALTH_RETRIES=60

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

print_results() {
  local json="$1"
  local status
  status=$(echo "$json" | jq -r '.status')

  if [[ "$status" == "completed" ]]; then
    echo "=== All benchmarks completed successfully ==="
  else
    echo "=== Benchmark finished with status: ${status} ==="
  fi
  echo ""

  # Find the longest engine name for alignment
  local max_len=0
  for name in $(echo "$json" | jq -r '.engines | keys[]'); do
    (( ${#name} > max_len )) && max_len=${#name}
  done

  # Header
  local pad=$((max_len + 2))
  printf "%-${pad}s 1           2           3\n" ""

  # Per-engine rows
  for name in $(echo "$json" | jq -r '.engines | keys[]'); do
    local label
    label="$(echo "${name:0:1}" | tr '[:lower:]' '[:upper:]')${name:1}:"
    local b1 b2 b3
    b1=$(echo "$json" | jq -r ".engines[\"$name\"].batches[0].duration_s")
    b2=$(echo "$json" | jq -r ".engines[\"$name\"].batches[1].duration_s")
    b3=$(echo "$json" | jq -r ".engines[\"$name\"].batches[2].duration_s")
    printf "%-${pad}s %s -> %s -> %s\n" "$label" "$(fmt_duration "$b1")" "$(fmt_duration "$b2")" "$(fmt_duration "$b3")"
  done

  echo ""
  local total
  total=$(echo "$json" | jq -r '.total_duration_s')
  local total_fmt
  total_fmt=$(fmt_duration "$total")
  local ruler_len=$(( ${#total_fmt} + pad + 30 ))
  local side=$(( (ruler_len - ${#total_fmt} - 2) / 2 ))
  printf "%${side}s %s %s\n" "$(printf '=%.0s' $(seq 1 $side))" "$total_fmt" "$(printf '=%.0s' $(seq 1 $side))"
}

cleanup() {
  docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

cleanup

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

RESULTS_JSON=$(curl -sf "http://localhost:9000/benchmark/status")

if [[ -n "$BENCHMARK_EXPERIMENTS_FILE" ]]; then
  # OAT mode — single-engine print_results is misleading because the last
  # experiment is just one snapshot. Dump the per-sweep RESULTS.md instead.
  echo "=== OAT sweep finished — status: ${FINAL_STATUS} ==="
  echo ""
  OAT_RESULTS_MD="mount/oat-state/latest/RESULTS.md"
  if [[ -f "$OAT_RESULTS_MD" ]]; then
    cat "$OAT_RESULTS_MD"
    echo ""
    echo "Artifacts: mount/oat-state/latest/"
  else
    echo "(no RESULTS.md found at $OAT_RESULTS_MD — check mount/oat-state/ for partial output)"
  fi
else
  print_results "$RESULTS_JSON"
fi

docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

if [[ "$FINAL_STATUS" != "completed" ]]; then
  echo "=== BENCHMARK FAILED ==="
  exit 1
fi
