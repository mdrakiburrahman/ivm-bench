#!/bin/bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

export SCALE_FACTOR="${SCALE_FACTOR:-3}"
LOGS_DIR=".logs"

mkdir -p "$LOGS_DIR"

echo "=== Tearing down any running containers ==="
docker compose down --remove-orphans 2>/dev/null || true

echo "=== Building images ==="
docker compose build tpc-di-gen spark-digen-delta

echo "=== Running tpc-di-gen → spark-digen-delta (SF=$SCALE_FACTOR) ==="
COMPOSE_RC=0
docker compose up spark-digen-delta 2>&1 | tee "$LOGS_DIR/spark-digen-delta.log" || COMPOSE_RC=$?

DIGEN_EXIT=$(docker compose ps -a tpc-di-gen --format '{{.ExitCode}}' 2>/dev/null || echo "unknown")
DELTA_EXIT=$(docker compose ps -a spark-digen-delta --format '{{.ExitCode}}' 2>/dev/null || echo "unknown")

if [[ "$DIGEN_EXIT" != "0" || "$DELTA_EXIT" != "0" || "$COMPOSE_RC" != "0" ]]; then
  echo ""
  echo "=== FAILURE — dumping container logs ==="
  echo ""
  echo "--- tpc-di-gen (exit $DIGEN_EXIT) ---"
  docker compose logs tpc-di-gen 2>/dev/null || true
  echo ""
  echo "--- spark-digen-delta (exit $DELTA_EXIT) ---"
  docker compose logs spark-digen-delta 2>/dev/null || true
  echo ""
  docker compose down --remove-orphans 2>/dev/null || true
  echo "=== FAILED (tpc-di-gen=$DIGEN_EXIT, spark-digen-delta=$DELTA_EXIT) ==="
  exit 1
fi

docker compose down --remove-orphans 2>/dev/null || true
echo "=== Completed successfully (tpc-di-gen=$DIGEN_EXIT, spark-digen-delta=$DELTA_EXIT) ==="
