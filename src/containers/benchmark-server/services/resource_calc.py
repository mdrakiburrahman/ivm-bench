"""Host resource detection and subdivision for parallel/serial benchmark modes."""

import logging
import os
from typing import Dict, List

from models.config import (
    ENGINE_COMPOSE_FILES,
    ENGINE_MAIN_SERVICES,
    ENGINE_PORTS,
    BenchmarkConfig,
    EngineConfig,
    ResourceAllocation,
)

logger = logging.getLogger(__name__)

MIN_DBT_SERVER_CPUS = 2
MIN_DBT_SERVER_MEM_GB = 4

# Resources to reserve for the host OS, Docker daemon, GitHub Actions
# runner agent (azure-pipelines-agent), and any other co-tenant on the
# machine. Without this reservation the SF=1000 run starves the runner
# agent and the workflow fails with "self-hosted runner lost
# communication" after ~30 min (run 25650103013).
HOST_RESERVED_CORES = 4
HOST_RESERVED_MEM_GB = 24

# Sidecar containers brought up alongside the primary engine but not
# counted in main/dbt-server splits — must be subtracted before sizing
# the engine. The Spark stack ships an MSSQL-backed Hive metastore at
# 4 cores / 8 GB (see EngineConfig.env_dict).
ENGINE_SIDECAR_CORES: Dict[str, int] = {"spark": 4}
ENGINE_SIDECAR_MEM_GB: Dict[str, int] = {"spark": 8}


def detect_host_cores() -> int:
    """Detect available CPU cores on the host."""
    count = os.cpu_count()
    if count is None:
        logger.warning("Could not detect CPU count, defaulting to 8")
        return 8
    return count


def detect_host_memory_gb() -> int:
    """Detect available memory in GB from /proc/meminfo."""
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    gb = kb // (1024 * 1024)
                    return max(gb, 1)
    except (OSError, ValueError, IndexError):
        logger.warning("Could not read /proc/meminfo, defaulting to 16 GB")
    return 16


def compute_engine_configs(config: BenchmarkConfig) -> Dict[str, EngineConfig]:
    """
    Compute resource allocations for each engine based on mode and host resources.

    In serial mode: each engine gets the full host allocation.
    In parallel mode: host resources are divided equally among engines.

    Headroom is always reserved for the host OS, Docker daemon, and the
    self-hosted GitHub Actions runner agent. Sidecar containers (e.g.
    spark's mssql-metastore) are subtracted from the per-engine budget
    before splitting between the main service and dbt-server so we don't
    overcommit the cgroup and trip the kernel OOM-killer (run
    25650103013: runner agent lost heartbeat after main+dbt+mssql
    overshot the 251 GB host by ~8 GB).
    """
    raw_cores = config.host_cores or detect_host_cores()
    raw_memory_gb = config.host_memory_gb or detect_host_memory_gb()

    # Reserve OS / Docker / runner-agent headroom only when auto-detected.
    # If the user pinned HOST_CORES / HOST_MEMORY explicitly via env var,
    # respect those exactly so they can override for debugging.
    if config.host_cores is None:
        cores = max(MIN_DBT_SERVER_CPUS + 1, raw_cores - HOST_RESERVED_CORES)
    else:
        cores = raw_cores
    if config.host_memory_gb is None:
        memory_gb = max(MIN_DBT_SERVER_MEM_GB + 1, raw_memory_gb - HOST_RESERVED_MEM_GB)
    else:
        memory_gb = raw_memory_gb

    engines = config.engines
    n_engines = len(engines)

    logger.info(
        "Host resources: detected %d cores / %d GB, reserving %d cores / %d GB "
        "for host+runner, allocating %d cores / %d GB to engines",
        raw_cores, raw_memory_gb,
        max(0, raw_cores - cores), max(0, raw_memory_gb - memory_gb),
        cores, memory_gb,
    )

    if config.parallel and n_engines > 1:
        per_engine_cores = max(cores // n_engines, MIN_DBT_SERVER_CPUS + 1)
        per_engine_mem = max(memory_gb // n_engines, MIN_DBT_SERVER_MEM_GB + 1)
        logger.info(
            "Parallel mode: %d engines, %d cores / %d GB each",
            n_engines, per_engine_cores, per_engine_mem,
        )
    else:
        per_engine_cores = cores
        per_engine_mem = memory_gb

    result: Dict[str, EngineConfig] = {}
    for engine in engines:
        # Subtract sidecar (e.g. mssql-metastore for spark) from the
        # per-engine budget before splitting between main and dbt-server.
        sidecar_cores = ENGINE_SIDECAR_CORES.get(engine, 0)
        sidecar_mem = ENGINE_SIDECAR_MEM_GB.get(engine, 0)
        budget_cores = max(MIN_DBT_SERVER_CPUS + 1, per_engine_cores - sidecar_cores)
        budget_mem = max(MIN_DBT_SERVER_MEM_GB + 1, per_engine_mem - sidecar_mem)

        has_main = ENGINE_MAIN_SERVICES.get(engine) is not None
        if has_main:
            dbt_cpus = max(MIN_DBT_SERVER_CPUS, budget_cores // 6)
            dbt_mem = max(MIN_DBT_SERVER_MEM_GB, budget_mem // 6)
            main_cpus = budget_cores - dbt_cpus
            main_mem = budget_mem - dbt_mem
        else:
            # DuckDB / DuckDB-OpenIVM: dbt-server IS the engine
            main_cpus = budget_cores
            main_mem = budget_mem
            dbt_cpus = budget_cores
            dbt_mem = budget_mem

        staging = "staging"
        if config.parallel and engine not in ("duckdb", "duckdb-openivm"):
            staging = f"staging-{engine}"

        result[engine] = EngineConfig(
            name=engine,
            compose_file=ENGINE_COMPOSE_FILES[engine],
            port=ENGINE_PORTS[engine] if config.parallel else 5000,
            main_service=ENGINE_MAIN_SERVICES.get(engine),
            main_resources=ResourceAllocation(cpus=main_cpus, memory_gb=main_mem),
            dbt_server_resources=ResourceAllocation(cpus=dbt_cpus, memory_gb=dbt_mem),
            staging_dir=staging,
        )
        logger.info(
            "Engine %s: main=%d cores/%dg, dbt-server=%d cores/%dg, "
            "sidecar=%d cores/%dg, port=%d, staging=%s",
            engine, main_cpus, main_mem, dbt_cpus, dbt_mem,
            sidecar_cores, sidecar_mem,
            result[engine].port, staging,
        )

    return result
