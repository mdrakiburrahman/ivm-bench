FROM rakirahman.azurecr.io/devcontainer/spark:2d6281b13738c8576a232df9a708f5c083820105d8d3f6d4696c28aa99bb091e
USER root
RUN apt-get update && apt-get install -y --no-install-recommends software-properties-common gpg-agent && \
    add-apt-repository -y ppa:ubuntu-toolchain-r/test && apt-get update && \
    apt-get install -y --only-upgrade libstdc++6 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir flask==3.1.2
COPY Compiler.scala server.py entrypoint.sh /app/
ENTRYPOINT ["bash", "/app/entrypoint.sh"]
