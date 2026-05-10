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
    """
    cores = config.host_cores or detect_host_cores()
    memory_gb = config.host_memory_gb or detect_host_memory_gb()
    engines = config.engines
    n_engines = len(engines)

    logger.info("Host resources: %d cores, %d GB memory", cores, memory_gb)

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
        has_main = ENGINE_MAIN_SERVICES.get(engine) is not None
        if has_main:
            dbt_cpus = max(MIN_DBT_SERVER_CPUS, per_engine_cores // 6)
            dbt_mem = max(MIN_DBT_SERVER_MEM_GB, per_engine_mem // 6)
            main_cpus = per_engine_cores - dbt_cpus
            main_mem = per_engine_mem - dbt_mem
        else:
            # DuckDB / DuckDB-OpenIVM: dbt-server IS the engine
            main_cpus = per_engine_cores
            main_mem = per_engine_mem
            dbt_cpus = per_engine_cores
            dbt_mem = per_engine_mem

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
            "Engine %s: main=%d cores/%dg, dbt-server=%d cores/%dg, port=%d, staging=%s",
            engine, main_cpus, main_mem, dbt_cpus, dbt_mem,
            result[engine].port, staging,
        )

    return result
