FROM ubuntu:24.04 AS builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list.d/ubuntu.sources; \
    fi
RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates python3 \
        libssl-dev && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /src
ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_COMMIT=b1b2113ad49f11b70443436ae809b47246aa3513
RUN git clone "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git submodule update --init --recursive

RUN GEN=ninja make -j$(nproc)

FROM ubuntu:24.04

ARG OPENIVM_COMMIT=b1b2113ad49f11b70443436ae809b47246aa3513
LABEL org.openivm.commit="${OPENIVM_COMMIT}"

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list.d/ubuntu.sources; \
    fi
RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/release/duckdb /opt/duckdb-openivm/duckdb
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/duckdb-openivm/duckdb

ENTRYPOINT ["/entrypoint.sh"]
