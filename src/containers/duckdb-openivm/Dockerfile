FROM ubuntu:24.04 AS builder

RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates python3 \
        libssl-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /src
ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_COMMIT=fa500813cc774375c1cf49f0b6c683cbaafd1105
RUN git clone "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git submodule update --init --recursive && \
    (cd duckdb && git fetch --depth 1 origin 08e34c447bae34eaee3723cac61f2878b6bdf787 && git checkout FETCH_HEAD) && \
    (cd extension-ci-tools && git fetch --depth 1 origin 72e76e99cd7fee45a99739cd118ec2db64e034ec && git checkout FETCH_HEAD)

RUN GEN=ninja make -j$(nproc)

FROM debian:bookworm-slim

ARG OPENIVM_COMMIT=fa500813cc774375c1cf49f0b6c683cbaafd1105
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
