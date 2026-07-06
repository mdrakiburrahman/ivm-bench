# Build the openivm DuckDB binary on debian:bookworm (glibc 2.36) so it runs on
# the dbt-server runtime (python:3.11-slim-bookworm, also glibc 2.36). Building on
# a newer base (e.g. ubuntu:24.04 / glibc 2.39) produces a binary that fails to
# load on bookworm at runtime with "GLIBC_2.38 not found (required by duckdb)".
FROM debian:bookworm AS builder

# Kept for build-compose compatibility (OPENIVM_UBUNTU_MIRROR); unused on debian,
# which uses deb.debian.org by default.
ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates python3 \
        libssl-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /src
ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_COMMIT=cec35ae41e930a607db774673310e6cc263775bd
RUN git clone "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git submodule update --init --recursive

RUN GEN=ninja make -j$(nproc)

FROM debian:bookworm-slim

ARG OPENIVM_COMMIT=cec35ae41e930a607db774673310e6cc263775bd
LABEL org.openivm.commit="${OPENIVM_COMMIT}"

# Hash of THIS Dockerfile, injected by the orchestrator. The Phase-1 build reuses
# the cached builder image only when this label matches, so any Dockerfile change
# (e.g. the base image / glibc) forces a rebuild even when OPENIVM_COMMIT is
# unchanged.
ARG DUCKDB_OPENIVM_DOCKERFILE_HASH=""
LABEL org.openivm.duckdb.dockerfile_hash="${DUCKDB_OPENIVM_DOCKERFILE_HASH}"

RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/release/duckdb /opt/duckdb-openivm/duckdb
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/duckdb-openivm/duckdb

ENTRYPOINT ["/entrypoint.sh"]
