#!/usr/bin/env bash
set -euo pipefail
export SPARK_HOME=/opt/spark
export OPENIVM_EXTENSION_PATH=/opt/spark-openivm/openivm.duckdb_extension
export OPENIVM_CLI_PATH=/opt/spark-openivm/duckdb
mkdir -p /tmp/compiler-classes
java -cp '/opt/spark/jars/*' scala.tools.nsc.Main \
  -classpath '/opt/spark/jars/*:/opt/spark-openivm/openivm-extension.jar' \
  -d /tmp/compiler-classes /app/Compiler.scala
jar cf /tmp/compiler.jar -C /tmp/compiler-classes .
exec /opt/spark/bin/spark-submit --master 'local[1]' --driver-memory 2g \
  --jars /tmp/compiler.jar,/opt/spark-openivm/openivm-extension.jar \
  --conf spark.ui.enabled=false --conf spark.sql.warehouse.dir=/tmp/openivm-compiler-warehouse \
  /app/server.py
