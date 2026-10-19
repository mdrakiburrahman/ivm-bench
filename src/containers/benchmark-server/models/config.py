"""Configuration models for benchmark orchestration."""

import hashlib
import json
import math
from decimal import Decimal, InvalidOperation
from datetime import date
from dataclasses import asdict, dataclass, field
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
class ExtendedTPCDSOptions:
    profile: str = "native"
    mode: str = "mixed"
    seed: int = 17
    report_date: str = "2002-12-31"
    history_months: int = 12
    batches: int = 3
    days: int = 3
    query_cohort: str = "all"
    mutation_cohort: str = "all"
    fallback_policy: str = "table"  # rejected CREATE MV may use an explicit table fallback
    statement_timeout_s: float = 300.0

    def __post_init__(self):
        if self.profile not in ("native", "calibrated", "jcc_stress"):
            raise ValueError("unknown Extended TPC-DS profile")
        if self.mode not in ("insertion", "updates", "deletes", "days", "repeated_insertions", "mixed"):
            raise ValueError("unknown Extended TPC-DS mode")
        if any(type(value) is not int or value < 1 for value in (self.history_months, self.batches, self.days)):
            raise ValueError("history, batches and days must be positive integers")
        if type(self.seed) is not int:
            raise ValueError("extension seed must be an integer")
        date.fromisoformat(self.report_date)
        if self.query_cohort not in ("all", "hot", "cold") or self.mutation_cohort not in ("all", "hot", "cold"):
            raise ValueError("cohort must be all, hot or cold")
        if self.fallback_policy not in ("table", "strict"):
            raise ValueError("fallback policy must be table or strict")
        if not math.isfinite(self.statement_timeout_s) or self.statement_timeout_s <= 0:
            raise ValueError("statement timeout must be positive and finite")

    @property
    def batch_count(self):
        return self.days + 1 if self.mode == "days" else self.batches

    def steps(self, config):
        return [{"insert_pct": getattr(config, f"batch_{min(number, 3)}_pct"),
                 "update_pct": getattr(config, f"batch_{min(number, 3)}_update_pct"),
                 "delete_pct": getattr(config, f"batch_{min(number, 3)}_delete_pct")}
                for number in range(2, self.batch_count + 1)]


def workload_cache_key(config):
    payload = {"workload": config.workload, "scale_factor": config.scale_factor,
               "options": asdict(config.extended_tpcds), "initial_pct": config.batch_1_pct,
               "steps": config.extended_tpcds.steps(config)}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]


@dataclass
class BenchmarkConfig:
    """Top-level benchmark configuration."""
    scale_factor: float = 3
    workload: str = "tpc-di"
    extended_tpcds: ExtendedTPCDSOptions = field(default_factory=ExtendedTPCDSOptions)
    benchmark_runs: int = 1
    batch_2_days: int = 0
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
        self.batch_2_days = int(self.batch_2_days)
        if self.batch_2_days < 0 or self.batch_2_days > 364:
            raise ValueError("batch_2_days must be between 0 and 364")
        if self.workload not in ("tpc-di", "extended-tpcds"):
            raise ValueError("workload must be tpc-di or extended-tpcds")
        sf = float(self.scale_factor)
        if not math.isfinite(sf) or sf <= 0 or (self.workload == "tpc-di" and not sf.is_integer()):
            raise ValueError("scale factor must be positive; TPC-DI requires an integer")
        self.scale_factor = int(sf) if self.workload == "tpc-di" else sf
        if self.workload == "extended-tpcds" and set(self.engines) - {"duckdb", "duckdb-openivm"}:
            raise ValueError("Extended TPC-DS currently supports duckdb and duckdb-openivm")

        if self.workload == "extended-tpcds":
            try:
                initial = Decimal(str(self.batch_1_pct))
                if not initial.is_finite() or not 0 <= initial <= 100:
                    raise ValueError("initial percentage must be finite and between 0 and 100")
                for step in self.extended_tpcds.steps(self):
                    insert, update, delete = [Decimal(str(step[key])) for key in ("insert_pct", "update_pct", "delete_pct")]
                    if any(not value.is_finite() or not 0 <= value <= 100 for value in (insert,update,delete)) or update+delete > 100:
                        raise ValueError("invalid mutation percentages")
                    mode = self.extended_tpcds.mode
                    if (mode in ("insertion","repeated_insertions") and (update or delete)) or (mode == "updates" and (insert or delete)) or (mode == "deletes" and (insert or update)) or (mode == "days" and insert):
                        raise ValueError("batch percentages conflict with the selected mutation mode")
            except InvalidOperation as error:
                raise ValueError("mutation percentages must be numbers") from error

    @property
    def batch_count(self):
        return self.extended_tpcds.batch_count if self.workload == "extended-tpcds" else 3

    def base_env(self) -> Dict[str, str]:
        """Environment variables shared across all compose invocations."""
        return {
            "SCALE_FACTOR": str(self.scale_factor),
            "WORKLOAD": self.workload,
            "EXTENDED_TPCDS_OPTIONS": json.dumps(asdict(self.extended_tpcds)),
            "EXTENDED_TPCDS_CACHE_KEY": workload_cache_key(self),
            "EXTENDED_TPCDS_STEPS": json.dumps(self.extended_tpcds.steps(self)),
            "BENCHMARK_RUNS": str(self.benchmark_runs),
            "TPCDI_BATCH_2_DAYS": str(self.batch_2_days),
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
