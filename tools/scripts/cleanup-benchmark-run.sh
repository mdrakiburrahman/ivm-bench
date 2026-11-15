#!/bin/bash
set -uo pipefail

run_id="${1:-}"
if [[ ! "$run_id" =~ ^[0-9a-f]{32}$ ]]; then
  echo "ERROR: A valid benchmark run ID is required for cleanup." >&2
  exit 1
fi

status=0
if ! docker compose -f docker/docker-compose.benchmark-server.yml -p "ivm-$run_id" down --remove-orphans; then
  echo "ERROR: Failed to stop this run's benchmark-server stack." >&2
  status=1
fi
if ids="$(docker ps -aq --filter "label=org.ivm-bench.run-id=$run_id")"; then
  while IFS= read -r id; do
    [[ -z "$id" ]] && continue
    if ! docker rm -f "$id"; then
      echo "ERROR: Failed to remove this run's container $id." >&2
      status=1
    fi
  done <<< "$ids"
else
  echo "ERROR: Failed to enumerate this run's containers." >&2
  status=1
fi
if ids="$(docker network ls -q --filter "label=org.ivm-bench.run-id=$run_id")"; then
  while IFS= read -r id; do
    [[ -z "$id" ]] && continue
    if ! docker network rm "$id"; then
      echo "ERROR: Failed to remove this run's network $id." >&2
      status=1
    fi
  done <<< "$ids"
else
  echo "ERROR: Failed to enumerate this run's networks." >&2
  status=1
fi
exit "$status"
