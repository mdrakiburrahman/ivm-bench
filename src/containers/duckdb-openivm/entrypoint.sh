#!/bin/bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/output}"
BINARY="/opt/duckdb-openivm/duckdb"
ICU_EXTENSION="/opt/duckdb-openivm/icu.duckdb_extension"
QUERIES="/opt/duckdb-openivm/queries"

mkdir -p "${OUTPUT_DIR}"
cp "${BINARY}" "${OUTPUT_DIR}/duckdb"
chmod +x "${OUTPUT_DIR}/duckdb"
echo "DuckDB-OpenIVM binary copied to ${OUTPUT_DIR}/duckdb"

# The native OpenIVM rewriter benchmark installs and loads ICU before running
# the corpus. Export the extension built against this exact DuckDB revision so
# the offline dbt-server container can do the same without autoinstalling it.
cp "${ICU_EXTENSION}" "${OUTPUT_DIR}/icu.duckdb_extension"
echo "ICU extension copied to ${OUTPUT_DIR}/icu.duckdb_extension"

# compiler-bench query corpus, pinned to the same OPENIVM_COMMIT as the binary.
# Replaced wholesale so a pin bump never leaves stale queries behind.
if [ -d "${QUERIES}" ]; then
    rm -rf "${OUTPUT_DIR}/queries"
    cp -r "${QUERIES}" "${OUTPUT_DIR}/queries"
    echo "OpenIVM query corpus copied to ${OUTPUT_DIR}/queries ($(find "${OUTPUT_DIR}/queries" -name '*.sql' | wc -l) files)"
fi
