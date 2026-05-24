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
FROM ubuntu:22.04 AS openivm-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG OPENIVM_REPO=https://github.com/mdrakiburrahman/openivm.git
ARG OPENIVM_BRANCH=openivm-spark
ARG OPENIVM_COMMIT=aab817fcfbfec3545c716df4779f4e6be3cbcb19
ARG LPTS_REPO=https://github.com/mdrakiburrahman/lpts.git
ARG LPTS_BRANCH=openivm-spark
ARG LPTS_COMMIT=21e7e313e52f0e98fe961533e06bffc368878ec6

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
        python3 python3-pip libssl-dev pkg-config zip unzip curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Clone openivm at the pinned commit on the openivm-spark branch, then
# override the lpts submodule to point at OUR fork so we never need to
# commit a submodule-URL bump into openivm to pick up new lpts work.
RUN git clone --branch "${OPENIVM_BRANCH}" --depth 50 "${OPENIVM_REPO}" . && \
    git checkout "${OPENIVM_COMMIT}" && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git config -f .gitmodules submodule.third_party/lpts.url "${LPTS_REPO}" && \
    git submodule sync --recursive && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools && \
    (cd third_party/lpts && \
        git init -q && \
        (git remote add origin "${LPTS_REPO}" 2>/dev/null || git remote set-url origin "${LPTS_REPO}") && \
        git fetch --depth 50 origin "${LPTS_BRANCH}" && \
        git checkout "${LPTS_COMMIT}" && \
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
ARG OPENIVM_SPARK_REPO=https://github.com/mdrakiburrahman/openivm-spark.git
ARG OPENIVM_SPARK_BRANCH=dev/mdrrahman/implementations
ARG OPENIVM_SPARK_COMMIT=6dfe127dc03195bdcf4b1286ee612029ba10b23f
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

ARG OPENIVM_SPARK_COMMIT=6dfe127dc03195bdcf4b1286ee612029ba10b23f
LABEL org.openivm.spark.commit="${OPENIVM_SPARK_COMMIT}"

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
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=openivm-builder /artifacts/openivm.duckdb_extension /opt/spark-openivm/openivm.duckdb_extension
COPY --from=openivm-builder /artifacts/duckdb                   /opt/spark-openivm/duckdb
COPY --from=openivm-builder /artifacts/SHA256SUMS               /opt/spark-openivm/SHA256SUMS
COPY --from=spark-ext-builder /tmp/openivm-extension.jar        /opt/spark-openivm/openivm-extension.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /opt/spark-openivm/duckdb

ENTRYPOINT ["/entrypoint.sh"]
