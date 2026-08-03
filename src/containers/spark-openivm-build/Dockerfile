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
#    1. openivm-builder    — verbatim from spark-ext/dev/docker/Dockerfile.
#                            Builds the two duckdb-side binaries.
#    2. spark-ext-builder  — JDK 17 + sbt; clones openivm-spark @ pinned
#                            SHA and runs `sbt ivmExtension/assembly`.
#    3. final              — Ubuntu 24.04 runtime; aggregates all three
#                            artifacts at /opt/spark-openivm/. entrypoint
#                            copies them to ${OUTPUT_DIR:-/output}/.
#
#  OPENIVM_SPARK_COMMIT pins the openivm-spark repo. OPENIVM_COMMIT and
#  LPTS_COMMIT MUST match the values in spark-ext/dev/pins.env at that SHA
#  (the spark-side compiler loads only those exact builds — ABI sensitive).
#  Whenever OPENIVM_SPARK_COMMIT is bumped, re-sync the other two pins from
#  the new pins.env.
# =============================================================================

# -----------------------------------------------------------------------------
# Stage 1: openivm-builder (verbatim from spark-ext/dev/docker/Dockerfile)
# -----------------------------------------------------------------------------
# Build the openivm duckdb CLI + extension on ubuntu:22.04 (GLIBC 2.35), NOT
# 24.04 (GLIBC 2.39). These artifacts are copied into the ubuntu:22.04 final
# stage and executed there (the spark driver's OpenIvmCompiler shells out to
# the `duckdb` CLI and dlopens `openivm.duckdb_extension`); a 24.04-built binary
# fails on 22.04 with `GLIBC_2.38 not found` / `GLIBCXX_3.4.32 not found`, which
# crashes every per-view compile -> COMPILE_FAILED -> FULL_REFRESH for all views.
# lpts 13786cbd needs GCC 13, so pull gcc-13 from the toolchain PPA. Keep libstdc++
# DYNAMIC (no -static-libstdc++): the CLI and the loadable extension must share a
# single libstdc++, so the final stage installs the matching gcc-13 libstdc++6.
FROM ubuntu:22.04 AS openivm-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_REPO=https://github.com/ila/openivm.git
ARG OPENIVM_BRANCH=main
ARG OPENIVM_COMMIT=051dcc3c0e82884ff2327108927e2197a34c738c
ARG LPTS_REPO=https://github.com/cwida/lpts.git
ARG LPTS_BRANCH=main
ARG LPTS_COMMIT=13786cbd8f32216aa5d92c99f8bf53c7cc52c9fb

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

# Clone openivm at the pinned commit on the openivm-spark branch, then
# override the lpts submodule to point at OUR fork so we never need to
# commit a submodule-URL bump into openivm to pick up new lpts work.
RUN git init -q && \
    git remote add origin "${OPENIVM_REPO}" && \
    git fetch --depth 1 origin "${OPENIVM_COMMIT}" && \
    git checkout -q FETCH_HEAD && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git config -f .gitmodules submodule.third_party/lpts.url "${LPTS_REPO}" && \
    git submodule sync --recursive && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools && \
    (cd duckdb && git fetch --depth 1 origin 08e34c447bae34eaee3723cac61f2878b6bdf787 && git checkout FETCH_HEAD) && \
    (cd extension-ci-tools && git fetch --depth 1 origin 72e76e99cd7fee45a99739cd118ec2db64e034ec && git checkout FETCH_HEAD) && \
    (cd third_party/lpts && \
        git init -q && \
        (git remote add origin "${LPTS_REPO}" 2>/dev/null || git remote set-url origin "${LPTS_REPO}") && \
        git fetch --depth 1 origin "${LPTS_COMMIT}" && \
        git checkout -q FETCH_HEAD && \
        git submodule update --init --recursive --depth 1)

# Build (release).
RUN GEN=ninja make -j"$(nproc)"

# Collect deliverables.
RUN mkdir -p /artifacts && \
    cp build/release/extension/openivm/openivm.duckdb_extension /artifacts/ && \
    cp build/release/duckdb /artifacts/duckdb && \
    chmod +x /artifacts/duckdb && \
    sha256sum /artifacts/openivm.duckdb_extension /artifacts/duckdb > /artifacts/SHA256SUMS

# -----------------------------------------------------------------------------
# Stage 2: spark-ext-builder — sbt + JDK 17, builds ivmExtension/assembly.
# -----------------------------------------------------------------------------
FROM eclipse-temurin:17-jdk AS spark-ext-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_SPARK_REPO=https://github.com/ila/openivm-spark.git
ARG OPENIVM_SPARK_BRANCH=fix/mv-view-delta-insert-only
ARG OPENIVM_SPARK_COMMIT=f8fadef74caf841c1fb8eba70f40f2e44fda6c17
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

RUN git clone --branch "${OPENIVM_SPARK_BRANCH}" "${OPENIVM_SPARK_REPO}" . && \
    git checkout "${OPENIVM_SPARK_COMMIT}"

WORKDIR /src/spark-ext

ENV SBT_OPTS="-Xmx4g -XX:+UseG1GC"

COPY --from=openivm-builder /artifacts/duckdb                 /native/duckdb
COPY --from=openivm-builder /artifacts/openivm.duckdb_extension /native/openivm.duckdb_extension
ENV OPENIVM_NATIVE_DIR=/native

# Build the assembly fat jar. BuildKit cache mounts are unavailable in the
# orchestrator's plain `docker compose build` invocation, so we accept a
# slower cold cache here — sbt will re-fetch deps each rebuild. Acceptable
# since rebuilds only happen on OPENIVM_SPARK_COMMIT bumps.
RUN sbt -batch ivmExtension/assembly && \
    find /src/spark-ext -name 'ivmExtension-*-assembly.jar' -path '*/scala-2.12/*' -exec cp {} /tmp/openivm-extension.jar \;

# Sanity check.
RUN test -s /tmp/openivm-extension.jar

# -----------------------------------------------------------------------------
# Stage 3: final runtime image.
# -----------------------------------------------------------------------------
FROM ubuntu:22.04

ARG OPENIVM_SPARK_COMMIT=f8fadef74caf841c1fb8eba70f40f2e44fda6c17
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
