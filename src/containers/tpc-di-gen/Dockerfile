FROM eclipse-temurin:8-jre

RUN apt-get update && apt-get install -y --no-install-recommends git unzip curl && \
    git clone --depth 1 --filter=blob:none --sparse \
      https://github.com/shannon-barrow/databricks-tpc-di.git /tmp/databricks-tpc-di && \
    cd /tmp/databricks-tpc-di && \
    git sparse-checkout set src/tools/datagen && \
    mkdir -p /opt/digen && \
    cp /tmp/databricks-tpc-di/src/tools/datagen/DIGen.jar /opt/digen/ && \
    cp -r /tmp/databricks-tpc-di/src/tools/datagen/pdgf /opt/digen/pdgf && \
    cd /opt/digen && \
    unzip -j DIGen.jar commons-cli-1.2.jar && \
    rm -rf /tmp/databricks-tpc-di && \
    apt-get purge -y git unzip && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /opt/entrypoint.sh
RUN chmod +x /opt/entrypoint.sh

ENTRYPOINT ["/opt/entrypoint.sh"]
