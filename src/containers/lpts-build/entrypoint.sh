#!/bin/bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/output}"

mkdir -p "${OUTPUT_DIR}"
cp /opt/lpts/lpts.duckdb_extension "${OUTPUT_DIR}/lpts.duckdb_extension"
cp /opt/lpts/SHA256SUMS "${OUTPUT_DIR}/SHA256SUMS"
echo "LPTS extension copied to ${OUTPUT_DIR}/lpts.duckdb_extension"
