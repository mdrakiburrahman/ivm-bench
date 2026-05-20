#!/bin/bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/output}"
BINARY="/opt/duckdb-openivm/duckdb"

mkdir -p "${OUTPUT_DIR}"
cp "${BINARY}" "${OUTPUT_DIR}/duckdb"
chmod +x "${OUTPUT_DIR}/duckdb"
echo "DuckDB-OpenIVM binary copied to ${OUTPUT_DIR}/duckdb"
