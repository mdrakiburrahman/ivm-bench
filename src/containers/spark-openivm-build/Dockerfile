# Pinned to docker-compose v1 / classic builder semantics — no BuildKit-only
# features (no `--mount=type=cache`, no `# syntax=...` directive) so the
# orchestrator's plain `docker compose build` works without DOCKER_BUILDKIT=1.

# =============================================================================
#  spark-openivm-build — builder for the spark-openivm engine.
#
#  Produces three runtime artifacts that the benchmark's `spark-openivm`
#  service binds at /opt/spark-openivm/:
#    1. openivm-extension.jar      — the Spark SQL extension fat jar
#    2. openivm.duckdb_extension   — DuckDB extension loaded by the spark
#                                    driver's OpenIvmCompiler subprocess
#    3. duckdb                     — pinned DuckDB CLI matching the
#                                    extension ABI
#
#  Stages:
#    1. openivm-builder    — builds the pinned DuckDB 2.0 CLI and extension.
#    2. spark-ext-builder  — JDK 17 + sbt; clones openivm-spark @ pinned
#                            SHA and runs `sbt ivmExtension/assembly`.
#    3. final              — Ubuntu 24.04 runtime; aggregates all three
#                            artifacts at /opt/spark-openivm/. entrypoint
#                            copies them to ${OUTPUT_DIR:-/output}/.
#
#  This benchmark holds OPENIVM_SPARK_COMMIT fixed while testing the native
#  DuckDB 2.0 compiler from the same OpenIVM revision as the DuckDB engine.
#  The native revision pins DuckDB, LPTS, and CI tools through its submodules.
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: openivm-builder
# -----------------------------------------------------------------------------
# Build the openivm duckdb CLI + extension on ubuntu:22.04 (GLIBC 2.35), NOT
# 24.04 (GLIBC 2.39). These artifacts are copied into the ubuntu:22.04 final
# stage and executed there (the spark driver's OpenIvmCompiler shells out to
# the `duckdb` CLI and dlopens `openivm.duckdb_extension`); a 24.04-built binary
# fails on 22.04 with `GLIBC_2.38 not found` / `GLIBCXX_3.4.32 not found`, which
# crashes every per-view compile -> COMPILE_FAILED -> FULL_REFRESH for all views.
# The pinned lpts revision needs GCC 13, so pull gcc-13 from the toolchain PPA. Keep libstdc++
# DYNAMIC (no -static-libstdc++): the CLI and the loadable extension must share a
# single libstdc++, so the final stage installs the matching gcc-13 libstdc++6.
FROM ubuntu:22.04 AS openivm-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_COMMIT=f37e1ad212cadef1128532d7e4b0c0e0b2248a3c

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        if [ -f /etc/apt/sources.list ]; then \
            sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list; \
        fi; \
        if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
            sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list.d/ubuntu.sources; \
        fi; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build git ca-certificates \
        python3 python3-pip libssl-dev pkg-config zip unzip curl \
        software-properties-common gpg-agent && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends gcc-13 g++-13 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# The OpenIVM revision pins matching DuckDB 2.0 and LPTS submodules.
RUN git init -q && \
    git remote add origin "${OPENIVM_REPO}" && \
    git fetch --depth 1 origin "${OPENIVM_COMMIT}" && \
    git checkout -q FETCH_HEAD && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools third_party/lpts

# Build (release).
RUN GEN=ninja make -j"$(nproc)"

# Collect deliverables.
RUN mkdir -p /artifacts && \
    cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ && \
    cp build/release/duckdb /artifacts/duckdb && \
    cp -L "$(g++ -print-file-name=libstdc++.so.6)" /artifacts/libstdc++.so.6 && \
    cp -L "$(gcc -print-file-name=libgcc_s.so.1)" /artifacts/libgcc_s.so.1 && \
    chmod +x /artifacts/duckdb && \
    sha256sum /artifacts/openivm.duckdb_extension /artifacts/duckdb > /artifacts/SHA256SUMS

# -----------------------------------------------------------------------------
# Stage 2: spark-ext-builder — sbt + JDK 17, builds ivmExtension/assembly.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk AS spark-ext-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_SPARK_REPO=https://github.com/mdrakiburrahman/openivm-spark.git
ARG OPENIVM_SPARK_COMMIT=243d13f5cbd9eee4f4c8975cea94d5d40f3fe6a3
ARG SBT_VERSION=1.9.7

ENV DEBIAN_FRONTEND=noninteractive

RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" \
            /etc/apt/sources.list.d/ubuntu.sources 2>/dev/null || true; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl git && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/sbt && \
    curl -fsSL "https://github.com/sbt/sbt/releases/download/v${SBT_VERSION}/sbt-${SBT_VERSION}.tgz" \
        -o /tmp/sbt.tgz && \
    tar -xzf /tmp/sbt.tgz -C /opt && \
    rm /tmp/sbt.tgz && \
    ln -sf /opt/sbt/bin/sbt /usr/local/bin/sbt

WORKDIR /src

RUN git init . && \
    git remote add origin "${OPENIVM_SPARK_REPO}" && \
    git fetch --depth 1 origin "${OPENIVM_SPARK_COMMIT}" && \
    git checkout -q FETCH_HEAD

# Keep this DuckDB 2.0 compatibility fix with the benchmark pin. The Spark
# translator must lower qualified interval calls, and Fabric needs the native
# compiler's C++ runtime alongside the bundled CLI and extension.
COPY patches/duckdb2-spark-compiler.patch /tmp/duckdb2-spark-compiler.patch
# The patch targets the exact OPENIVM_SPARK_COMMIT above; zero-context format
# keeps the vendored diff free of whitespace-only context lines.
RUN git apply --unidiff-zero --check /tmp/duckdb2-spark-compiler.patch && \
    git apply --unidiff-zero /tmp/duckdb2-spark-compiler.patch

WORKDIR /src/spark-ext

ENV SBT_OPTS="-Xmx4g -XX:+UseG1GC"

COPY --from=openivm-builder /artifacts/duckdb                 /native/duckdb
COPY --from=openivm-builder /artifacts/openivm.duckdb_extension /native/openivm.duckdb_extension
COPY --from=openivm-builder /artifacts/libstdc++.so.6 /native/libstdc++.so.6
COPY --from=openivm-builder /artifacts/libgcc_s.so.1 /native/libgcc_s.so.1
ENV OPENIVM_NATIVE_DIR=/native
ENV OPENIVM_SPARK_TARGET=spark-3.5

# Build the assembly fat jar. BuildKit cache mounts are unavailable in the
# orchestrator's plain `docker compose build` invocation, so we accept a
# slower cold cache here — sbt will re-fetch deps when the builder image changes.
RUN sbt -batch 'ivmCompiler/testOnly org.openivm.spark.compiler.LptsSparkDialectSpec' ivmExtension/assembly && \
    find /src/spark-ext -name 'ivmExtension-*-assembly.jar' -path '*/scala-2.12/*' -exec cp {} /tmp/openivm-extension.jar \;

# Sanity check.
RUN test -s /tmp/openivm-extension.jar && \
    jar tf /tmp/openivm-extension.jar | grep -Fq openivm-native/libstdc++.so.6 && \
    jar tf /tmp/openivm-extension.jar | grep -Fq openivm-native/libgcc_s.so.1

# -----------------------------------------------------------------------------
# Stage 3: final runtime image.
# -----------------------------------------------------------------------------
FROM ubuntu:22.04

ARG OPENIVM_SPARK_COMMIT=243d13f5cbd9eee4f4c8975cea94d5d40f3fe6a3
LABEL org.openivm.spark.commit="${OPENIVM_SPARK_COMMIT}"

# Cache-bust key: hash of the WHOLE Dockerfile. The orchestrator reuses this
# builder image only when the label still matches, so a base-image / glibc /
# toolchain change (not just an OPENIVM_SPARK_COMMIT bump) forces a rebuild.
ARG SPARK_OPENIVM_DOCKERFILE_HASH=""
LABEL org.openivm.spark.dockerfile_hash="${SPARK_OPENIVM_DOCKERFILE_HASH}"

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
RUN if [ "${UBUNTU_MIRROR}" != "http://archive.ubuntu.com/ubuntu" ]; then \
        if [ -f /etc/apt/sources.list ]; then \
            sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list; \
        fi; \
        if [ -f /etc/apt/sources.list.d/ubuntu.sources ]; then \
            sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list.d/ubuntu.sources; \
        fi; \
    fi && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends ca-certificates software-properties-common gpg-agent && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --only-upgrade libstdc++6 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=openivm-builder /artifacts/openivm.duckdb_extension /opt/spark-openivm/openivm.duckdb_extension
COPY --from=openivm-builder /artifacts/duckdb                   /opt/spark-openivm/duckdb
COPY --from=openivm-builder /artifacts/SHA256SUMS               /opt/spark-openivm/SHA256SUMS
COPY --from=spark-ext-builder /tmp/openivm-extension.jar        /opt/spark-openivm/openivm-extension.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/spark-openivm/duckdb

ENTRYPOINT ["/entrypoint.sh"]
