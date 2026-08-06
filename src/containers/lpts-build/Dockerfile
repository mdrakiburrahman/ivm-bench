# Pinned to docker-compose v1 / classic builder semantics — no BuildKit-only
# features (no `--mount=type=cache`, no `# syntax=...` directive) so the
# orchestrator's plain `docker compose build` works without DOCKER_BUILDKIT=1.

# =============================================================================
#  lpts-build — builder for the standalone LPTS DuckDB extension.
#
#  Produces one artifact that the compiler-bench corpus translation step loads
#  into the duckdb-openivm CLI:
#    1. lpts.duckdb_extension  — loadable extension exposing `PRAGMA lpts(...)`
#                                and the `lpts_dialect` / `lpts_input_dialect`
#                                session settings.
#
#  WHY A SEPARATE BUILD: openivm compiles the LPTS *sources* into its own
#  extension (see openivm/CMakeLists.txt) and uses them internally for
#  refresh-SQL generation, but it does NOT compile `lpts_extension.cpp` — the
#  file that registers the `lpts` pragma and the dialect settings. So the
#  shipped duckdb-openivm binary can rewrite plans internally but exposes no
#  transpilation entry point. This image builds LPTS as a normal loadable
#  extension so the CLI can `LOAD` it and transpile corpus queries to each
#  engine's dialect.
#
#  ABI: the extension is dlopen'd by the duckdb-openivm CLI, so DUCKDB_COMMIT
#  and EXTENSION_CI_TOOLS_COMMIT MUST match the submodule pins used in
#  src/containers/duckdb-openivm/Dockerfile (v1.5.4 / v1.5-variegata). A
#  mismatch fails at LOAD with an extension-version error.
#
#  LPTS_COMMIT MUST match the pin in src/containers/spark-openivm-build/Dockerfile
#  so the transpiler used for the corpus is the same LPTS revision the openivm
#  engines were compiled against.
# =============================================================================

# Build on ubuntu:22.04 (GLIBC 2.35) — NOT 24.04 (GLIBC 2.39). The extension is
# dlopen'd by the duckdb-openivm CLI, which itself runs in the dbt-server
# (debian bookworm, GLIBC 2.36) and the benchmark-server, so it must link
# against GLIBC <= 2.36. lpts 13786cbd needs GCC 13 (GCC 11 fails to compile
# it), so pull gcc-13 from the toolchain PPA and make it the default cc/c++.
FROM ubuntu:22.04 AS lpts-builder

ARG UBUNTU_MIRROR=http://archive.ubuntu.com/ubuntu
ARG LPTS_REPO=https://github.com/cwida/lpts.git
# cwida/lpts main. Carries two things the corpus translation needs:
#   - lpts_output_catalog / _schema / _unqualified, without which LPTS bakes the
#     local planning catalog into every emitted table reference
#   - dialect-valid COUNT(*) and datediff units; DuckDB's count_star() and
#     datediff('day', ...) are rejected by Databricks/Spark
# Descends from 13786cbd (the LPTS revision the openivm engines are compiled
# against), so the transpiler and the engines stay in sync.
ARG LPTS_COMMIT=79ea9b2e2281318232d27f038e0347831847a645
ARG DUCKDB_COMMIT=08e34c447bae34eaee3723cac61f2878b6bdf787
ARG EXTENSION_CI_TOOLS_COMMIT=72e76e99cd7fee45a99739cd118ec2db64e034ec
# Must be the tag DUCKDB_COMMIT corresponds to. A loadable extension records the
# DuckDB version it was built for and refuses to load into any other, and that
# version comes from `git describe` on the duckdb submodule. The shallow
# fetch-by-SHA below carries no tags, so git describe fails and the version
# silently becomes v0.0.1 — the extension then fails to load with "built
# specifically for DuckDB version 'v0.0.1'" and every corpus query is reported
# untranslatable. OVERRIDE_GIT_DESCRIBE is DuckDB's supported way to supply it.
ARG DUCKDB_VERSION=v1.5.4

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
        build-essential cmake ninja-build git ca-certificates python3 \
        libssl-dev pkg-config software-properties-common gpg-agent && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update -o Acquire::ForceIPv4=true -o Acquire::Retries=3 -o Acquire::http::Timeout=60 && \
    apt-get install -y --no-install-recommends gcc-13 g++-13 && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-13 100 && \
    update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-13 100 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Fetch the pinned SHA directly rather than clone-then-checkout: LPTS_COMMIT may
# not be on any branch tip, and a shallow clone of main would not contain it.
RUN git init -q && \
    git remote add origin "${LPTS_REPO}" && \
    git fetch --depth 1 origin "${LPTS_COMMIT}" && \
    git checkout -q FETCH_HEAD && \
    git config --global url."https://github.com/".insteadOf "git@github.com:" && \
    git submodule update --init --recursive --depth 1 duckdb extension-ci-tools third_party/ducklake && \
    (cd duckdb && git fetch --depth 1 origin "${DUCKDB_COMMIT}" && git checkout -q FETCH_HEAD) && \
    (cd extension-ci-tools && git fetch --depth 1 origin "${EXTENSION_CI_TOOLS_COMMIT}" && git checkout -q FETCH_HEAD)

# Keep libstdc++ DYNAMIC (no -static-libstdc++): the extension is dlopen'd into
# the duckdb-openivm CLI, and a loadable extension must share one libstdc++ with
# its host process. Both are built with gcc-13, so the runtime that executes the
# CLI provides the matching GLIBCXX (the dbt-server / benchmark-server images
# already do — see the duckdb-openivm and spark-openivm Dockerfiles).
ENV CC=gcc-13 \
    CXX=g++-13 \
    OVERRIDE_GIT_DESCRIBE=${DUCKDB_VERSION}

RUN GEN=ninja make release -j"$(nproc)"

# Fail the build here rather than at LOAD time: an extension stamped with the
# wrong DuckDB version is useless, and the failure would otherwise surface as
# "every query is untranslatable" much later.
RUN duckdb_ver=$(build/release/duckdb -noheader -list -c "SELECT version()" 2>/dev/null) && \
    echo "built against DuckDB ${duckdb_ver} (expected ${DUCKDB_VERSION})" && \
    [ "${duckdb_ver}" = "${DUCKDB_VERSION}" ]

RUN mkdir -p /artifacts && \
    cp build/release/extension/lpts/lpts.duckdb_extension /artifacts/ && \
    sha256sum /artifacts/lpts.duckdb_extension > /artifacts/SHA256SUMS

FROM debian:bookworm-slim

# Keep in step with the builder-stage pin above; this only feeds the label.
ARG LPTS_COMMIT=79ea9b2e2281318232d27f038e0347831847a645
LABEL org.lpts.commit="${LPTS_COMMIT}"

# Cache-bust key: hash of the WHOLE Dockerfile, mirroring the duckdb-openivm and
# spark-openivm-build mechanism. The orchestrator reuses this builder image only
# while the label matches, so a base-image / toolchain / pin change forces a
# rebuild instead of silently shipping a stale extension.
ARG LPTS_BUILD_DOCKERFILE_HASH=""
LABEL org.lpts.dockerfile_hash="${LPTS_BUILD_DOCKERFILE_HASH}"

COPY --from=lpts-builder /artifacts/lpts.duckdb_extension /opt/lpts/lpts.duckdb_extension
COPY --from=lpts-builder /artifacts/SHA256SUMS            /opt/lpts/SHA256SUMS
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
