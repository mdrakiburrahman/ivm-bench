FROM rakirahman.azurecr.io/devcontainer/spark:2d6281b13738c8576a232df9a708f5c083820105d8d3f6d4696c28aa99bb091e

USER root

# The bundled openivm `duckdb` CLI (mounted from /opt/spark-openivm, built with
# GCC 12) is executed HERE by the spark driver's OpenIvmCompiler to classify each
# MV. Both the CLI and this base use Ubuntu 22.04 and its GCC 12 libstdc++.
# Verify the runtime package is present without relying on the external
# toolchain PPA; the jar also bundles the builder's matching libstdc++.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl libstdc++6 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY config/ /opt/spark-benchmark/config/
COPY entrypoint.sh /opt/spark-benchmark/entrypoint.sh
RUN chmod +x /opt/spark-benchmark/entrypoint.sh

EXPOSE 8998

ENTRYPOINT ["/opt/spark-benchmark/entrypoint.sh"]
