# Build on ubuntu:22.04 (GLIBC 2.35) — NOT 24.04 (GLIBC 2.39). The compiled
# `duckdb` binary is executed by the dbt-server container (python:3.11-slim =
# debian bookworm, GLIBC 2.36), so it must link against GLIBC <= 2.36 or it
# fails at load with `GLIBC_2.38 not found`. 22.04's 2.35 is compatible.
# lpts 13786cbd needs GCC 13 (GCC 11 fails to compile it), so pull gcc-13 from
# the toolchain PPA and make it the default cc/c++.
FROM ubuntu:22.04 AS builder

RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates python3 \
        libssl-dev software-properties-common gpg-agent && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends gcc-13 g++-13 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /src
ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_COMMIT=154323e8ec8b53d26210660c0a6f06eaf1217bd7
RUN git clone "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git submodule update --init --recursive && \
    (cd duckdb && git fetch --depth 1 origin 08e34c447bae34eaee3723cac61f2878b6bdf787 && git checkout FETCH_HEAD) && \
    (cd extension-ci-tools && git fetch --depth 1 origin 72e76e99cd7fee45a99739cd118ec2db64e034ec && git checkout FETCH_HEAD)

# Statically link libstdc++/libgcc so the binary carries GCC 13's GLIBCXX_3.4.32
# rather than requiring it at runtime (debian bookworm ships GLIBCXX_3.4.30).
# CMake seeds CMAKE_*_LINKER_FLAGS from LDFLAGS at first configure.
ENV CC=gcc-13 \
    CXX=g++-13 \
    LDFLAGS="-static-libstdc++ -static-libgcc"
COPY icu_extension_config.cmake /tmp/icu_extension_config.cmake
# Build ICU as a loadable-only extension. Linking it statically through
# CORE_EXTENSIONS changes generated loader symbols for every OpenIVM benchmark
# helper, whose link lines intentionally contain only OpenIVM's normal static
# dependencies.
RUN GEN=ninja EXTRA_EXTENSION_CONFIGS=/tmp/icu_extension_config.cmake make -j$(nproc)

FROM debian:bookworm-slim

ARG OPENIVM_COMMIT=154323e8ec8b53d26210660c0a6f06eaf1217bd7
LABEL org.openivm.commit="${OPENIVM_COMMIT}"

ARG DUCKDB_OPENIVM_DOCKERFILE_HASH=""
LABEL org.openivm.duckdb.dockerfile_hash="${DUCKDB_OPENIVM_DOCKERFILE_HASH}"

RUN apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=30 && \
    apt-get install -y --no-install-recommends \
        ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/release/duckdb /opt/duckdb-openivm/duckdb
COPY --from=builder /src/build/release/extension/icu/icu.duckdb_extension /opt/duckdb-openivm/icu.duckdb_extension

# The compiler-bench query corpus travels with the binary so the queries are
# always the ones OPENIVM_COMMIT ships — no submodule in this repo to drift out
# of sync with the pin, and no recursive clone of openivm's own submodules
# (duckdb + lpts + benchbase) just to read ~2.5k small .sql files.
COPY --from=builder /src/benchmark/queries /opt/duckdb-openivm/queries

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/duckdb-openivm/duckdb

ENTRYPOINT ["/entrypoint.sh"]
