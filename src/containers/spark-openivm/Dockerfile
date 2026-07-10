FROM rakirahman.azurecr.io/devcontainer/spark:2d6281b13738c8576a232df9a708f5c083820105d8d3f6d4696c28aa99bb091e

USER root

# The bundled openivm `duckdb` CLI (mounted from /opt/spark-openivm, built with
# gcc-13) is executed HERE by the spark driver's OpenIvmCompiler to classify each
# MV. This base is Ubuntu 22.04 (GLIBC 2.35, libstdc++ GLIBCXX 3.4.30). The CLI
# needs GLIBCXX 3.4.32 (gcc-13 runtime), so upgrade libstdc++6 from the toolchain
# PPA — otherwise the CLI fails to load ("GLIBCXX_3.4.32 not found"), every view
# hits COMPILE_FAILED, and IVM silently degrades to FULL_REFRESH for all views.
# (GLIBC itself is fine: the CLI is built on 22.04, so it needs only <= 2.35.)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl software-properties-common gpg-agent && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
    apt-get update && \
    apt-get install -y --only-upgrade libstdc++6 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY config/ /opt/spark-benchmark/config/
COPY entrypoint.sh /opt/spark-benchmark/entrypoint.sh
RUN chmod +x /opt/spark-benchmark/entrypoint.sh

EXPOSE 8998

ENTRYPOINT ["/opt/spark-benchmark/entrypoint.sh"]
