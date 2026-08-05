#!/bin/bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/output}"
BINARY="/opt/duckdb-openivm/duckdb"
QUERIES="/opt/duckdb-openivm/queries"

mkdir -p "${OUTPUT_DIR}"
cp "${BINARY}" "${OUTPUT_DIR}/duckdb"
chmod +x "${OUTPUT_DIR}/duckdb"
echo "DuckDB-OpenIVM binary copied to ${OUTPUT_DIR}/duckdb"

# compiler-bench query corpus, pinned to the same OPENIVM_COMMIT as the binary.
# Replaced wholesale so a pin bump never leaves stale queries behind.
if [ -d "${QUERIES}" ]; then
    rm -rf "${OUTPUT_DIR}/queries"
    cp -r "${QUERIES}" "${OUTPUT_DIR}/queries"
    echo "OpenIVM query corpus copied to ${OUTPUT_DIR}/queries ($(find "${OUTPUT_DIR}/queries" -name '*.sql' | wc -l) files)"
fi
