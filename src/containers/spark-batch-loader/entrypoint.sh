#!/bin/bash
set -euo pipefail

echo "=== Spark Batch Loader: mode=$1 ==="
# Preserve real stderr (Spark/Delta exceptions + log4j ERROR, which
# log4j2.properties routes to SYSTEM_ERR) so failures are diagnosable; filter
# only the harmless JDK launcher "WARNING:" lines. `exec` makes java's real exit
# code the container's. Do NOT re-add `2>/dev/null` — it silently hid the cause
# of batch-loader failures.
exec java \
  -Dlog4j2.configurationFile=file:/opt/log4j2.properties \
  -Dio.netty.tryReflectionSetAccessible=true \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED \
  -cp /opt/batch-loader.jar batchloader.BatchLoader "$@" \
  2> >(grep --line-buffered -v '^WARNING: ' >&2)
