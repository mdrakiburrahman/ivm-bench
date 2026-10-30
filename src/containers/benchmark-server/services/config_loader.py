"""Configuration loader — reads environment variables into BenchmarkConfig."""

import os
import json
from dataclasses import fields
from pathlib import Path

from models.config import BenchmarkConfig, ExtendedTPCDSOptions


def _insert_pct(batch_num: int, default: str) -> str:
    insert_pct = os.environ.get(f"BATCH_{batch_num}_INSERT_PCT")
    if insert_pct:
        return insert_pct
    return os.environ.get(f"BATCH_{batch_num}_PCT", default)


def _load_schedule() -> str:
    """Resolve the scheduling mode: explicit ``SCHEDULE`` wins, else the legacy
    ``PARALLEL`` flag maps to serial/parallel."""
    sched = os.environ.get("SCHEDULE", "").strip()
    if sched:
        return sched
    return "parallel" if os.environ.get("PARALLEL", "0") == "1" else "serial"


def load_config() -> BenchmarkConfig:
    """Build a BenchmarkConfig from environment variables."""
    values = dict(
        repeated_refresh=os.environ.get("REPEATED_REFRESH", "0") == "1",
        refresh_count=os.environ.get("REFRESH_COUNT", "20"),
        refresh_pct=os.environ.get("REFRESH_PCT", "1"),
        scale_factor=float(os.environ.get("SCALE_FACTOR", "3")),
        workload=os.environ.get("WORKLOAD") if os.environ.get("WORKLOAD") == "extended-tpcds" else os.environ.get("TPCDI_WORKLOAD", "standard"),
        extended_tpcds=ExtendedTPCDSOptions(**json.loads(os.environ.get("EXTENDED_TPCDS_OPTIONS", "{}"))),
        benchmark_runs=int(os.environ.get("BENCHMARK_RUNS", "1")),
        batch_2_days=int(os.environ.get("TPCDI_BATCH_2_DAYS", "0")),
        batch_1_pct=_insert_pct(1, "1"),
        batch_2_pct=_insert_pct(2, "0.001"),
        batch_3_pct=_insert_pct(3, "0.002"),
        batch_2_update_pct=os.environ.get("BATCH_2_UPDATE_PCT", "0"),
        batch_2_delete_pct=os.environ.get("BATCH_2_DELETE_PCT", "0"),
        batch_3_update_pct=os.environ.get("BATCH_3_UPDATE_PCT", "0"),
        batch_3_delete_pct=os.environ.get("BATCH_3_DELETE_PCT", "0"),
        parallel=os.environ.get("PARALLEL", "0") == "1",
        schedule=_load_schedule(),
        engines=[
            e.strip()
            for e in os.environ.get("ENGINES", "spark,spark-openivm,duckdb,duckdb-openivm,feldera").split(",")
            if e.strip()
        ],
        host_cores=int(os.environ["HOST_CORES"]) if os.environ.get("HOST_CORES") else None,
        host_memory_gb=int(os.environ["HOST_MEMORY"]) if os.environ.get("HOST_MEMORY") else None,
        repo_dir=os.environ.get("REPO_DIR", "/repo"),
    )

    # An OAT file owns workload, scale and batch settings. Validate its first
    # experiment before constructing a TPC-DI bootstrap configuration; otherwise
    # a fractional Extended TPC-DS scale fails before the sweep can even start.
    experiments_file = os.environ.get("BENCHMARK_EXPERIMENTS_FILE")
    if experiments_file:
        from models.experiments import parse_experiments_json
        experiments = parse_experiments_json(Path(experiments_file).read_text())
        if not experiments:
            raise ValueError("experiments JSON has no experiments")
        first = experiments[0]
        values.update({field.name:getattr(first,field.name) for field in fields(BenchmarkConfig)
                       if hasattr(first,field.name)})
    return BenchmarkConfig(**values)
