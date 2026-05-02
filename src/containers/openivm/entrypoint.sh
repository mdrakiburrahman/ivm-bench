#!/bin/bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/output}"
BINARY="/opt/openivm/duckdb"

if [ -f "${OUTPUT_DIR}/duckdb" ]; then
  echo "OpenIVM binary already exists at ${OUTPUT_DIR}/duckdb — skipping copy"
  exit 0
fi

mkdir -p "${OUTPUT_DIR}"
cp "${BINARY}" "${OUTPUT_DIR}/duckdb"
chmod +x "${OUTPUT_DIR}/duckdb"
echo "OpenIVM binary copied to ${OUTPUT_DIR}/duckdb"
