#!/bin/bash
set -euo pipefail

export SPARK_HOME=/opt/spark
export LIVY_HOME=/opt/livy

CONFIG_DIR="/opt/spark-benchmark/config"
LOG_DIR="/tmp/livy-logs"

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Resource detection & template processing (mirrors dbt-fabricspark pattern)
# ---------------------------------------------------------------------------
calc_ram() {
  local pct=$1 total=$2
  echo $(( (total * pct) / 100 ))
}

calc_cores_clamped() {
  local pct=$1 total=$2 min=$3
  local result=$(( (total * pct) / 100 ))
  echo $(( result < min ? min : result ))
}

process_template() {
  local src=$1 dst=$2
  envsubst < "$src" > "$dst"
}

TOTAL_RAM_GB=$(free -g | awk '/^Mem:/{print $2}')
TOTAL_CORES=$(nproc)

BREAKDOWN="$CONFIG_DIR/spark-defaults-breakdown.yaml"
DRIVER_PCT_RAM=$(yq eval '.driver.pct_ram' "$BREAKDOWN")
DRIVER_PCT_CORES=$(yq eval '.driver.pct_cores' "$BREAKDOWN")
EXECUTOR_PCT_RAM=$(yq eval '.executor.pct_ram' "$BREAKDOWN")
EXECUTOR_PCT_CORES=$(yq eval '.executor.pct_cores' "$BREAKDOWN")
MIN_CORES=$(yq eval '.resource_allocation.min_cores' "$BREAKDOWN")
SHUFFLE_PARTITIONS=$(yq eval '.parallelism.shuffle_partitions' "$BREAKDOWN")
DEFAULT_PARALLELISM=$(yq eval '.parallelism.default_parallelism' "$BREAKDOWN")

export SPARK_DRIVER_MEMORY="$(calc_ram "$DRIVER_PCT_RAM" "$TOTAL_RAM_GB")g"
export SPARK_DRIVER_CORES=$(calc_cores_clamped "$DRIVER_PCT_CORES" "$TOTAL_CORES" "$MIN_CORES")
export SPARK_EXECUTOR_MEMORY="$(calc_ram "$EXECUTOR_PCT_RAM" "$TOTAL_RAM_GB")g"
export SPARK_EXECUTOR_CORES=$(calc_cores_clamped "$EXECUTOR_PCT_CORES" "$TOTAL_CORES" "$MIN_CORES")
export SPARK_SUBMIT_SHUFFLE_PARTITIONS=$SHUFFLE_PARTITIONS
export SPARK_SUBMIT_DEFAULT_PARALLELISM=$DEFAULT_PARALLELISM

mkdir -p /opt/spark/conf
process_template "$CONFIG_DIR/spark-defaults.conf.tmpl" "/opt/spark/conf/spark-defaults.conf"
process_template "$CONFIG_DIR/hive-site.xml.tmpl" "/opt/spark/conf/hive-site.xml"

mkdir -p /opt/livy/conf
process_template "$CONFIG_DIR/livy.conf.tmpl" "/opt/livy/conf/livy.conf"
process_template "$CONFIG_DIR/livy-server-log4j.properties.tmpl" "/opt/livy/conf/livy-server-log4j.properties"
process_template "$CONFIG_DIR/livy-spark-log4j.properties.tmpl" "/opt/livy/conf/livy-spark-log4j.properties"

cat > /opt/livy/conf/livy-env.sh << EOF
export SPARK_HOME=/opt/spark
export SPARK_CONF_DIR=/opt/spark/conf
export LIVY_SERVER_JAVA_OPTS="-Dlog4j.configuration=file:/opt/livy/conf/livy-server-log4j.properties"
EOF

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SPARK BENCHMARK CONTAINER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Host:     ${TOTAL_RAM_GB}GB RAM, ${TOTAL_CORES} cores"
echo "  Driver:   ${SPARK_DRIVER_MEMORY} RAM, ${SPARK_DRIVER_CORES} cores"
echo "  Executor: ${SPARK_EXECUTOR_MEMORY} RAM, ${SPARK_EXECUTOR_CORES} cores"
echo "  Shuffle:  ${SHUFFLE_PARTITIONS}, Parallelism: ${DEFAULT_PARALLELISM}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ---------------------------------------------------------------------------
# Wait for MSSQL metastore
# ---------------------------------------------------------------------------
echo "Waiting for mssql-metastore..."
METASTORE_HOST="${METASTORE_HOST:-mssql-metastore}"
METASTORE_PORT="${METASTORE_PORT:-1433}"
MAX_WAIT=120
elapsed=0
until curl -sf "http://${METASTORE_HOST}:${METASTORE_PORT}" >/dev/null 2>&1 || \
      timeout 2 bash -c "echo >/dev/tcp/${METASTORE_HOST}/${METASTORE_PORT}" 2>/dev/null; do
  sleep 2
  elapsed=$((elapsed + 2))
  if [[ $elapsed -ge $MAX_WAIT ]]; then
    echo "ERROR: mssql-metastore not reachable after ${MAX_WAIT}s"
    exit 1
  fi
done
echo "  mssql-metastore is reachable"

# ---------------------------------------------------------------------------
# Initialize Hive metastore schema (idempotent)
# ---------------------------------------------------------------------------
echo "Creating metastore database if needed..."

# Find the MSSQL JDBC jar bundled with Spark
JDBC_JAR=$(find "$SPARK_HOME/jars" -name 'mssql-jdbc-*.jar' 2>/dev/null | head -1)

if [[ -n "$JDBC_JAR" ]]; then
  # Create the metastore database + initialize schema via JDBC
  cat > /tmp/InitMetastore.java << 'JEOF'
import java.sql.*;
import java.io.*;
import java.nio.file.*;

public class InitMetastore {
    public static void main(String[] args) throws Exception {
        String host = System.getenv("METASTORE_HOST");
        String port = System.getenv("METASTORE_PORT");
        if (host == null) host = "mssql-metastore";
        if (port == null) port = "1433";
        String baseUrl = "jdbc:sqlserver://" + host + ":" + port + ";encrypt=false;trustServerCertificate=true";

        // 1. Create database if needed
        Connection masterConn = DriverManager.getConnection(baseUrl, "sa", "Hive@Pass123");
        Statement masterStmt = masterConn.createStatement();
        ResultSet rs = masterStmt.executeQuery("SELECT COUNT(*) FROM sys.databases WHERE name='metastore'");
        rs.next();
        if (rs.getInt(1) == 0) {
            masterStmt.executeUpdate("CREATE DATABASE metastore");
            System.out.println("  created metastore database");
        } else {
            System.out.println("  metastore database already exists");
        }
        rs.close(); masterStmt.close(); masterConn.close();

        // 2. Check if schema is already initialized (look for VERSION table)
        String metaUrl = baseUrl + ";databaseName=metastore";
        Connection conn = DriverManager.getConnection(metaUrl, "sa", "Hive@Pass123");
        Statement stmt = conn.createStatement();
        rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'");
        rs.next();
        int tableCount = rs.getInt(1);
        rs.close();

        if (tableCount >= 50) {
            System.out.println("  schema already initialized (" + tableCount + " tables)");
            stmt.close(); conn.close();
            return;
        }

        // 3. Initialize schema from SQL file
        String schemaFile = args.length > 0 ? args[0] : "/opt/spark-benchmark/config/hive-schema-4.0.0.mssql.sql";
        System.out.println("  initializing schema from " + schemaFile);
        String sql = new String(Files.readAllBytes(Paths.get(schemaFile)));

        // Split on GO statements (MSSQL batch separator)
        String[] batches = sql.split("(?mi)^\\s*GO\\s*$");
        int errors = 0;
        for (String batch : batches) {
            batch = batch.trim();
            if (batch.isEmpty()) continue;
            try {
                stmt.execute(batch);
            } catch (SQLException e) {
                // Ignore "already exists" errors for idempotency
                if (!e.getMessage().contains("already an object named")) {
                    errors++;
                    if (errors <= 3) {
                        System.err.println("  WARN: " + e.getMessage().substring(0, Math.min(200, e.getMessage().length())));
                    }
                }
            }
        }

        rs = stmt.executeQuery(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'");
        rs.next();
        System.out.println("  schema initialized (" + rs.getInt(1) + " tables)");
        rs.close(); stmt.close(); conn.close();
    }
}
JEOF
  javac -cp "$JDBC_JAR" /tmp/InitMetastore.java -d /tmp
  java -cp "$JDBC_JAR:/tmp" InitMetastore "$CONFIG_DIR/hive-schema-4.0.0.mssql.sql"
else
  echo "  WARNING: no MSSQL JDBC jar found, skipping metastore init"
fi

# ---------------------------------------------------------------------------
# Start Livy
# ---------------------------------------------------------------------------
echo "Starting Livy server..."
$LIVY_HOME/bin/livy-server start

retries=0
max_retries=60
until curl -s http://localhost:8998/sessions >/dev/null 2>&1; do
  sleep 2
  retries=$((retries + 1))
  if [[ $retries -ge $max_retries ]]; then
    echo "ERROR: Livy failed to start within $((max_retries * 2))s"
    cat "$LOG_DIR/livy-server.log" 2>/dev/null || true
    exit 1
  fi
done

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Livy Server: http://localhost:8998 (READY)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Keep container alive
exec tail -f /dev/null
