FROM rakirahman.azurecr.io/devcontainer/spark:2d6281b13738c8576a232df9a708f5c083820105d8d3f6d4696c28aa99bb091e

USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY config/ /opt/spark-benchmark/config/
COPY entrypoint.sh /opt/spark-benchmark/entrypoint.sh
RUN chmod +x /opt/spark-benchmark/entrypoint.sh

EXPOSE 8998

ENTRYPOINT ["/opt/spark-benchmark/entrypoint.sh"]
