#!/bin/bash
# spark-openivm-build entrypoint — idempotently copy the three runtime
# artifacts to the bind-mounted output directory.
#
# Artifacts:
#   * openivm-extension.jar      — Spark SQL extension fat jar
#   * openivm.duckdb_extension   — DuckDB extension loaded by OpenIvmCompiler
#   * duckdb                     — pinned DuckDB CLI

set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-/output}"
SOURCE_DIR="/opt/spark-openivm"

mkdir -p "${OUTPUT_DIR}"

copy_if_missing() {
  local name="$1"
  local src="${SOURCE_DIR}/${name}"
  local dst="${OUTPUT_DIR}/${name}"

  if [[ ! -f "${src}" ]]; then
    echo "ERROR: source artifact missing: ${src}" >&2
    exit 1
  fi

  if [[ -f "${dst}" ]]; then
    if [[ "$(sha256sum < "${src}" | awk '{print $1}')" == \
          "$(sha256sum < "${dst}" | awk '{print $1}')" ]]; then
      echo "spark-openivm: ${name} already up to date — skipping"
      return 0
    fi
    rm -f "${dst}"
  fi

  cp "${src}" "${dst}"
  echo "spark-openivm: copied ${name} → ${dst}"
}

copy_if_missing openivm-extension.jar
copy_if_missing openivm.duckdb_extension
copy_if_missing duckdb
chmod +x "${OUTPUT_DIR}/duckdb"

# Drop the SHA256SUMS too, for traceability.
if [[ -f "${SOURCE_DIR}/SHA256SUMS" ]]; then
  cp "${SOURCE_DIR}/SHA256SUMS" "${OUTPUT_DIR}/SHA256SUMS"
fi

echo "spark-openivm: all artifacts ready at ${OUTPUT_DIR}"
