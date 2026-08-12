"""Configuration models for benchmark orchestration."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class ResourceAllocation:
    """CPU and memory allocation for a Docker service."""
    cpus: int
    memory_gb: int

    @property
    def mem_str(self) -> str:
        return f"{self.memory_gb}g"


@dataclass
class EngineConfig:
    """Configuration for a single engine benchmark run."""
    name: str
    compose_file: str
    port: int
    main_service: Optional[str]  # None for DuckDB/DuckDB-OpenIVM (dbt-server only)
    main_resources: ResourceAllocation
    dbt_server_resources: ResourceAllocation
    project_name: str = ""
    staging_dir: str = ""

    def __post_init__(self):
        if not self.project_name:
            self.project_name = f"bench-{self.name}"

    def env_dict(self) -> Dict[str, str]:
        """Return env vars to inject into docker compose."""
        prefix = self.name.upper().replace("-", "_")
        env = {
            f"{prefix}_CPUS": str(self.main_resources.cpus),
            f"{prefix}_MEM": self.main_resources.mem_str,
            "DBT_SERVER_CPUS": str(self.dbt_server_resources.cpus),
            "DBT_SERVER_MEM": self.dbt_server_resources.mem_str,
            "DBT_SERVER_PORT": str(self.port),
        }
        # MSSQL for Spark — must match defaults in
        # docker/docker-compose.benchmark.spark{,-openivm}.yml. At SF=400+
        # cpus=4/mem=8g triggered SQL Server's SOS scheduler ASSERT
        # (((seenByMonitor)<(NonYieldThreshold)), sosschedmon.cpp:202)
        # under validation's 30 concurrent Livy threads → ~150 concurrent
        # metastore RPCs. Bumped to 8 cpus / 16g (internal cap 12000MB).
        if self.name in ("spark", "spark-openivm"):
            env["MSSQL_CPUS"] = "8"
            env["MSSQL_MEM"] = "16g"
        # Feldera: cap pipeline RSS at 95 % of its container memory.
        # The dbt-feldera adapter reads FELDERA_MAX_RSS_MB via env_var()
        # in profiles.yml and forwards it to runtime_config.max_rss_mb.
        if self.name == "feldera":
            env["FELDERA_MAX_RSS_MB"] = str(int(self.main_resources.memory_gb * 1024 * 0.95))
        # DuckDB / DuckDB-OpenIVM run in-process inside the dbt-server
        # container, so size their memory_limit and threads from the
        # engine's resource budget. Leave 20 % headroom for the Python
        # process, dbt, and connector overhead so the container itself
        # doesn't OOM. In parallel mode this scales the in-process
        # engines down to their per-engine slice instead of the host's
        # full memory.
        if self.name == "duckdb":
            mem_gb = max(1, int(self.main_resources.memory_gb * 0.8))
            env["DUCKDB_MEM_LIMIT"] = f"{mem_gb}GB"
            env["DUCKDB_THREADS"] = str(self.main_resources.cpus)
        if self.name == "duckdb-openivm":
            mem_gb = max(1, int(self.main_resources.memory_gb * 0.8))
            env["DUCKDB_OPENIVM_MEM_LIMIT"] = f"{mem_gb}GB"
            # Validation runs outside the timed window and materializes a full
            # expected result.  Leave extra cgroup headroom for its spill-file
            # page cache instead of letting the host OOM-kill the validator.
            validate_mem_gb = max(1, int(self.main_resources.memory_gb * 0.5))
            env["DUCKDB_OPENIVM_VALIDATE_MEM_LIMIT"] = f"{validate_mem_gb}GB"
            env["DUCKDB_OPENIVM_THREADS"] = str(self.main_resources.cpus)
        return env


ENGINE_PORTS = {
    "spark": 5001,
    "duckdb": 5002,
    "duckdb-openivm": 5003,
    "feldera": 5004,
    "spark-openivm": 5005,
    "databricks-enzyme": 5006,
    "fabric-openivm-jvm-35": 5007,
    "fabric-jvm-35": 5008,
}

ENGINE_COMPOSE_FILES = {
    "spark": "docker/docker-compose.benchmark.spark.yml",
    "duckdb": "docker/docker-compose.benchmark.duckdb.yml",
    "duckdb-openivm": "docker/docker-compose.benchmark.duckdb-openivm.yml",
    "feldera": "docker/docker-compose.benchmark.feldera.yml",
    "spark-openivm": "docker/docker-compose.benchmark.spark-openivm.yml",
    "databricks-enzyme": "docker/docker-compose.benchmark.databricks-enzyme.yml",
    "fabric-openivm-jvm-35": "docker/docker-compose.benchmark.fabric-openivm-jvm-35.yml",
    "fabric-jvm-35": "docker/docker-compose.benchmark.fabric-jvm-35.yml",
}

# Engine's primary service (None if dbt-server IS the engine).
# Fabric engines run dbt against remote Fabric Spark (Livy) — like
# databricks-enzyme, dbt-server IS the engine locally (plus the imds-router
# sidecar that brokers `az login --identity`).
ENGINE_MAIN_SERVICES = {
    "spark": "spark",
    "duckdb": None,
    "duckdb-openivm": None,
    "feldera": "pipeline-manager",
    "spark-openivm": "spark-openivm",
    "databricks-enzyme": None,
    "fabric-openivm-jvm-35": None,
    "fabric-jvm-35": None,
}

# Containers whose CPU contributes to the controlled local-system metric.
# Spark's metastore is part of query execution and is therefore included;
# dbt-server is excluded for engines where it only orchestrates remote calls.
ENGINE_COMPUTE_SERVICES = {
    "spark": ("spark", "mssql-metastore"),
    "duckdb": ("dbt-server",),
    "duckdb-openivm": ("dbt-server",),
    "feldera": ("pipeline-manager",),
    "spark-openivm": ("spark-openivm", "mssql-metastore"),
}

# Cloud-consuming engines: the heavy query compute is offloaded to a remote
# service (Databricks Serverless SQL / Fabric Spark Livy), so their host
# footprint is just the light dbt-server (+ imds-router) orchestration
# container. Everything else is host-consuming (local Spark/DuckDB/Feldera).
# The ``serial-host-parallel-cloud`` schedule serialises host engines (they
# contend for host CPU/RAM/disk) and runs cloud engines in one parallel wave.
CLOUD_ENGINES = {
    "databricks-enzyme",
    "fabric-openivm-jvm-35",
    "fabric-jvm-35",
}


def is_cloud_engine(name: str) -> bool:
    """True if the engine offloads its compute to a remote cloud service."""
    return name in CLOUD_ENGINES


@dataclass
class BenchmarkConfig:
    """Top-level benchmark configuration."""
    scale_factor: int = 3
    benchmark_runs: int = 1
    batch_1_pct: str = "1"
    batch_2_pct: str = "0.001"
    batch_3_pct: str = "0.002"
    batch_2_update_pct: str = "0"
    batch_2_delete_pct: str = "0"
    batch_3_update_pct: str = "0"
    batch_3_delete_pct: str = "0"
    parallel: bool = False
    # Scheduling mode: "serial" (all engines one-at-a-time), "parallel" (all in
    # one wave), or "serial-host-parallel-cloud" (host engines serial, cloud
    # engines in one parallel wave). ``parallel=True`` maps to "parallel" for
    # back-compat when ``schedule`` is left at its default.
    schedule: str = "serial"
    engines: List[str] = field(
        default_factory=lambda: [
            "spark",
            "duckdb",
            "duckdb-openivm",
            "feldera",
            "spark-openivm",
        ]
    )
    host_cores: Optional[int] = None
    host_memory_gb: Optional[int] = None
    repo_dir: str = "/repo"

    def __post_init__(self):
        self.benchmark_runs = max(1, int(self.benchmark_runs))

    def base_env(self) -> Dict[str, str]:
        """Environment variables shared across all compose invocations."""
        return {
            "SCALE_FACTOR": str(self.scale_factor),
            "BENCHMARK_RUNS": str(self.benchmark_runs),
            "BATCH_1_INSERT_PCT": self.batch_1_pct,
            "BATCH_2_INSERT_PCT": self.batch_2_pct,
            "BATCH_3_INSERT_PCT": self.batch_3_pct,
            "BATCH_1_PCT": self.batch_1_pct,
            "BATCH_2_PCT": self.batch_2_pct,
            "BATCH_3_PCT": self.batch_3_pct,
            "BATCH_2_UPDATE_PCT": self.batch_2_update_pct,
            "BATCH_2_DELETE_PCT": self.batch_2_delete_pct,
            "BATCH_3_UPDATE_PCT": self.batch_3_update_pct,
            "BATCH_3_DELETE_PCT": self.batch_3_delete_pct,
        }
