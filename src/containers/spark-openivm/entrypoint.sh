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

# Like calc_ram but enforces a minimum (in GB). openivm-spark's compiler
# subprocess and Catalyst MV planning both live on the driver — we floor
# the driver heap so SF=3 doesn't hit OOM under aggressive parallel mode.
calc_ram_min() {
  local pct=$1 total=$2 min=$3
  local result=$(( (total * pct) / 100 ))
  echo $(( result < min ? min : result ))
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
# Tunables (driver.pct_ram, executor.pct_ram, shuffle_partitions,
# default_parallelism) can be overridden by env vars from the OAT harness.
# When a SPARK_*_PCT_RAM / SPARK_SUBMIT_* env var is set we honour it;
# otherwise we fall back to the per-engine YAML baseline. This is what
# lets ExperimentInputs.spark_tunables.to_compose_env() flow through.
DRIVER_PCT_RAM="${SPARK_DRIVER_PCT_RAM:-$(yq eval '.driver.pct_ram' "$BREAKDOWN")}"
DRIVER_PCT_CORES=$(yq eval '.driver.pct_cores' "$BREAKDOWN")
EXECUTOR_PCT_RAM="${SPARK_EXECUTOR_PCT_RAM:-$(yq eval '.executor.pct_ram' "$BREAKDOWN")}"
EXECUTOR_PCT_CORES=$(yq eval '.executor.pct_cores' "$BREAKDOWN")
MIN_CORES=$(yq eval '.resource_allocation.min_cores' "$BREAKDOWN")
SHUFFLE_PARTITIONS="${SPARK_SUBMIT_SHUFFLE_PARTITIONS:-$(yq eval '.parallelism.shuffle_partitions' "$BREAKDOWN")}"
DEFAULT_PARALLELISM="${SPARK_SUBMIT_DEFAULT_PARALLELISM:-$(yq eval '.parallelism.default_parallelism' "$BREAKDOWN")}"

export SPARK_DRIVER_MEMORY="$(calc_ram_min "$DRIVER_PCT_RAM" "$TOTAL_RAM_GB" 10)g"
export SPARK_DRIVER_CORES=$(calc_cores_clamped "$DRIVER_PCT_CORES" "$TOTAL_CORES" "$MIN_CORES")
# Preserve local[*] semantics by default while allowing multi-driver benchmarks
# to bound each JVM explicitly (for example, 10 drivers x 3 threads on 32 cores).
export SPARK_LOCAL_THREADS="${SPARK_LOCAL_THREADS:-$TOTAL_CORES}"
export SPARK_EXECUTOR_MEMORY="$(calc_ram "$EXECUTOR_PCT_RAM" "$TOTAL_RAM_GB")g"
export SPARK_EXECUTOR_CORES=$(calc_cores_clamped "$EXECUTOR_PCT_CORES" "$TOTAL_CORES" "$MIN_CORES")
export SPARK_SUBMIT_SHUFFLE_PARTITIONS=$SHUFFLE_PARTITIONS
export SPARK_SUBMIT_DEFAULT_PARALLELISM=$DEFAULT_PARALLELISM

# Translate OPENIVM_PROFILE_REFRESH (`0`/`1` or unset) into the boolean
# token the spark-defaults template expects. Default OFF when unset.
case "${OPENIVM_PROFILE_REFRESH:-0}" in
  1|true|TRUE|True) export OPENIVM_PROFILE_REFRESH_BOOL=true ;;
  *) export OPENIVM_PROFILE_REFRESH_BOOL=false ;;
esac

# Translate OPENIVM_QUERY_LOG (`0`/`1` or unset) the same way. Default OFF
# when unset; the bench script flips this ON by default.
case "${OPENIVM_QUERY_LOG:-0}" in
  1|true|TRUE|True) export OPENIVM_QUERY_LOG_BOOL=true ;;
  *) export OPENIVM_QUERY_LOG_BOOL=false ;;
esac

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
echo "  SPARK-OPENIVM BENCHMARK CONTAINER"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Host:     ${TOTAL_RAM_GB}GB RAM, ${TOTAL_CORES} cores"
echo "  Driver:   ${SPARK_DRIVER_MEMORY} RAM, local[${SPARK_LOCAL_THREADS}] worker threads"
echo "  Executor: ${SPARK_EXECUTOR_MEMORY} RAM, ${SPARK_EXECUTOR_CORES} cores (decorative in local mode)"
echo "  Shuffle:  ${SHUFFLE_PARTITIONS}, Parallelism: ${DEFAULT_PARALLELISM}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# ---------------------------------------------------------------------------
# Validate openivm-spark runtime artifacts are bind-mounted at /opt/spark-openivm
# ---------------------------------------------------------------------------
SPARK_OPENIVM_DIR=/opt/spark-openivm
for artifact in openivm-extension.jar openivm.duckdb_extension duckdb; do
  if [[ ! -f "${SPARK_OPENIVM_DIR}/${artifact}" ]]; then
    echo "FATAL: missing required artifact ${SPARK_OPENIVM_DIR}/${artifact}" >&2
    echo "       (built by spark-openivm-build; bind-mount mount/bin/spark-openivm/)" >&2
    exit 1
  fi
done
chmod +x "${SPARK_OPENIVM_DIR}/duckdb" 2>/dev/null || true
echo "  openivm artifacts: $(ls -1 ${SPARK_OPENIVM_DIR} | tr '\n' ' ')"

# Place the openivm-spark assembly jar on Spark's default classpath.
# We cannot rely on `spark.jars=…` from spark-defaults.conf because Livy's
# RSC overrides --jars at spark-submit time with only its own RSC + repl
# jars (verified empirically by inspecting `SET spark.jars` from a session).
# Symlinking into /opt/spark/jars/ is how Delta gets onto the classpath
# already in this image, so we follow the same pattern.
ln -sf "${SPARK_OPENIVM_DIR}/openivm-extension.jar" /opt/spark/jars/openivm-extension.jar
echo "  linked /opt/spark/jars/openivm-extension.jar -> ${SPARK_OPENIVM_DIR}/openivm-extension.jar"

# OpenIvmCompiler defaults to /opt/openivm/{openivm.duckdb_extension,duckdb}.
# Mirror that path via a symlink to /opt/spark-openivm/ so we don't depend
# on spark.driverEnv.* (which is silently ignored in local[*] deploy mode).
mkdir -p /opt/openivm
ln -sfn "${SPARK_OPENIVM_DIR}/openivm.duckdb_extension" /opt/openivm/openivm.duckdb_extension
ln -sfn "${SPARK_OPENIVM_DIR}/duckdb" /opt/openivm/duckdb
echo "  linked /opt/openivm/ -> ${SPARK_OPENIVM_DIR}/ (compiler defaults)"

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
