FROM debian:bookworm AS builder

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
