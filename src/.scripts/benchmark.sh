#!/bin/bash
# ---------------------------------------------------------------------------
# benchmark.sh — thin client that drives an OAT (one-at-a-time) sweep against
# the benchmark-server and streams progress via Server-Sent Events.
#
# REQUIRED:
#   BENCHMARK_EXPERIMENTS_FILE — absolute path of an experiments JSON file
#                    that MUST live under the repo root so the existing
#                    bind-mount makes it visible inside the benchmark-server
#                    container at the same path. Each experiment owns its
#                    own scale factor, batch percentages, engines, Spark
#                    tunables, and OpenIVM feature flags.
#
#                    Built-in sweeps:
#                      src/containers/benchmark-server/experiments/sf-sweep.json
#                      src/containers/benchmark-server/experiments/smoke.json
#
# Artifacts (per-sweep, refreshed after every experiment):
#   mount/oat-state/<oat_run_id>/{chart-oat,chart-per-model}.png
#   mount/oat-state/<oat_run_id>/RESULTS.md
#   mount/oat-state/<oat_run_id>/outputs.json
#   mount/oat-state/latest → <oat_run_id>
#
# Environment variables:
#   BENCHMARK_RUNS — repeat the full 3-batch benchmark N times and average
#                    the per-engine batch timings (default: 1)
#   OAT_MIN_FREE_PCT — minimum % free disk under repo root to keep running.
#                    Default 10. The sweep SKIPS remaining experiments below
#                    this threshold (no host crash on WSL).
#   PARALLEL       — 0 = serial (default), 1 = run engines in parallel.
#                    Can also be set per-experiment inside the JSON.
#   ENGINES        — comma-separated engine list. Default lets each
#                    experiment in the JSON pick. The harness rebuilds the
#                    union once during Phase 0 so no rebuilds mid-sweep.
#   BATCH_N_INSERT_PCT / UPDATE_PCT / DELETE_PCT — per-experiment defaults
#                    forwarded to the benchmark-server container. The OAT
#                    JSON overrides these per experiment. BATCH_N_PCT is a
#                    backwards-compatible alias for BATCH_N_INSERT_PCT.
#   HOST_CORES     — override auto-detected CPU count
#   HOST_MEMORY    — override auto-detected memory in GB
#   PRESERVE_RAW   — 1 = keep mount/bin/ across sweeps. The OAT runner ALWAYS
#                    wipes mount/raw/ in Phase 0 regardless (stale per-SF
#                    Delta tables would silently feed wrong batch percentages
#                    into the first experiment).
#   OPENIVM_VALIDATE — 1 = validate OpenIVM views with EXCEPT ALL after each
#                    timed batch (default), 0 = skip post-timer validation
#   OPENIVM_PROFILE_REFRESH — 1 = export OpenIVM profiling CSVs into
#                    mount/results/<sf>/dbt-server/ after each timed batch
#   OPENIVM_QUERY_LOG — 1 = export the full SQL trace OpenIVM ran for every
#                    CREATE/REFRESH MV (spark-openivm only). Writes a per-MV
#                    per-refresh directory tree of `.sql` files under
#                    mount/results/<sf>/spark-openivm/query-log/.
#   OPENIVM_UBUNTU_MIRROR — override OpenIVM Docker build apt mirror
# ---------------------------------------------------------------------------
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
REPO_ROOT="$(pwd)"

if [[ -f "$REPO_ROOT/.env" ]]; then
  set -o allexport
  source "$REPO_ROOT/.env"
  set +o allexport
fi

export BENCHMARK_RUNS="${BENCHMARK_RUNS:-1}"

if [[ -z "${BENCHMARK_EXPERIMENTS_FILE:-}" ]]; then
  echo "ERROR: BENCHMARK_EXPERIMENTS_FILE must be set."
  echo "  Example:"
  echo "    export BENCHMARK_EXPERIMENTS_FILE=\"\$(git rev-parse --show-toplevel)/src/containers/benchmark-server/experiments/sf-sweep.json\""
  echo "    bash src/.scripts/benchmark.sh"
  exit 1
fi

# Resolve to absolute path; require it live under the repo root so the
# benchmark-server bind-mount makes it visible at the same in-container path.
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
export BENCHMARK_EXPERIMENTS_FILE

echo "=== OAT sweep — experiments file: $BENCHMARK_EXPERIMENTS_FILE ==="

export OAT_MIN_FREE_PCT="${OAT_MIN_FREE_PCT:-10}"

# Forward per-batch DML mix env vars to the benchmark-server container.
# These act as defaults only — the OAT JSON's per-experiment block overrides.
export BATCH_1_INSERT_PCT="${BATCH_1_INSERT_PCT:-${BATCH_1_PCT:-}}"
export BATCH_2_INSERT_PCT="${BATCH_2_INSERT_PCT:-${BATCH_2_PCT:-}}"
export BATCH_3_INSERT_PCT="${BATCH_3_INSERT_PCT:-${BATCH_3_PCT:-}}"
export BATCH_1_PCT="${BATCH_1_PCT:-${BATCH_1_INSERT_PCT:-1}}"
export BATCH_2_PCT="${BATCH_2_PCT:-${BATCH_2_INSERT_PCT:-0.001}}"
export BATCH_3_PCT="${BATCH_3_PCT:-${BATCH_3_INSERT_PCT:-0.002}}"
export BATCH_2_UPDATE_PCT="${BATCH_2_UPDATE_PCT:-0}"
export BATCH_2_DELETE_PCT="${BATCH_2_DELETE_PCT:-0}"
export BATCH_3_UPDATE_PCT="${BATCH_3_UPDATE_PCT:-0}"
export BATCH_3_DELETE_PCT="${BATCH_3_DELETE_PCT:-0}"
export PARALLEL="${PARALLEL:-0}"
# Scheduling mode: serial | parallel | serial-host-parallel-cloud. The OAT JSON's
# per-experiment `schedule` (or legacy `parallel`) overrides this default.
export SCHEDULE="${SCHEDULE:-serial}"
export ENGINES="${ENGINES:-spark,spark-openivm,duckdb,duckdb-openivm,feldera}"
export HOST_CORES="${HOST_CORES:-}"
export HOST_MEMORY="${HOST_MEMORY:-}"
export PRESERVE_RAW="${PRESERVE_RAW:-0}"
export OPENIVM_VALIDATE="${OPENIVM_VALIDATE:-1}"
export OPENIVM_PROFILE_REFRESH="${OPENIVM_PROFILE_REFRESH:-0}"
export OPENIVM_QUERY_LOG="${OPENIVM_QUERY_LOG:-1}"
export REPO_HOST_PATH="$REPO_ROOT"

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

docker compose -f "$COMPOSE_FILE" down --remove-orphans 2>/dev/null || true

if [[ "$FINAL_STATUS" != "completed" ]]; then
  echo "=== BENCHMARK FAILED ==="
  exit 1
fi
