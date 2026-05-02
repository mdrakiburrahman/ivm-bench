FROM ubuntu:24.04 AS builder

ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_COMMIT=110025c572dcc51752ee8bd0d214249ab08b4ecb

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates python3 \
        libssl-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /src
RUN git clone "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git submodule update --init --recursive

RUN GEN=ninja make -j$(nproc)

# Minimal output stage
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/release/duckdb /opt/openivm/duckdb
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/openivm/duckdb

ENTRYPOINT ["/entrypoint.sh"]
