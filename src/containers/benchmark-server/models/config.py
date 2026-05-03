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
        # MSSQL for Spark
        if self.name == "spark":
            env["MSSQL_CPUS"] = "2"
            env["MSSQL_MEM"] = "4g"
        return env


ENGINE_PORTS = {
    "spark": 5001,
    "duckdb": 5002,
    "duckdb-openivm": 5003,
    "feldera": 5004,
}

ENGINE_COMPOSE_FILES = {
    "spark": "docker/docker-compose.benchmark.spark.yml",
    "duckdb": "docker/docker-compose.benchmark.duckdb.yml",
    "duckdb-openivm": "docker/docker-compose.benchmark.duckdb-openivm.yml",
    "feldera": "docker/docker-compose.benchmark.feldera.yml",
}

# Engine's primary service (None if dbt-server IS the engine)
ENGINE_MAIN_SERVICES = {
    "spark": "spark",
    "duckdb": None,
    "duckdb-openivm": None,
    "feldera": "pipeline-manager",
}


@dataclass
class BenchmarkConfig:
    """Top-level benchmark configuration."""
    scale_factor: int = 3
    batch_1_pct: str = "1"
    batch_2_pct: str = "0.001"
    batch_3_pct: str = "0.002"
    parallel: bool = False
    engines: List[str] = field(default_factory=lambda: ["spark", "duckdb", "duckdb-openivm", "feldera"])
    host_cores: Optional[int] = None
    host_memory_gb: Optional[int] = None
    repo_dir: str = "/repo"

    def base_env(self) -> Dict[str, str]:
        """Environment variables shared across all compose invocations."""
        return {
            "SCALE_FACTOR": str(self.scale_factor),
            "BATCH_1_PCT": self.batch_1_pct,
            "BATCH_2_PCT": self.batch_2_pct,
            "BATCH_3_PCT": self.batch_3_pct,
        }
