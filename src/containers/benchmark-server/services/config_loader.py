"""Configuration loader — reads environment variables into BenchmarkConfig."""

import os

from models.config import BenchmarkConfig


def load_config() -> BenchmarkConfig:
    """Build a BenchmarkConfig from environment variables."""
    return BenchmarkConfig(
        scale_factor=int(os.environ.get("SCALE_FACTOR", "3")),
        batch_1_pct=os.environ.get("BATCH_1_PCT", "1"),
        batch_2_pct=os.environ.get("BATCH_2_PCT", "0.001"),
        batch_3_pct=os.environ.get("BATCH_3_PCT", "0.002"),
        parallel=os.environ.get("PARALLEL", "0") == "1",
        engines=[
            e.strip()
            for e in os.environ.get("ENGINES", "spark,duckdb,openivm,feldera").split(",")
            if e.strip()
        ],
        host_cores=int(os.environ["HOST_CORES"]) if os.environ.get("HOST_CORES") else None,
        host_memory_gb=int(os.environ["HOST_MEMORY"]) if os.environ.get("HOST_MEMORY") else None,
        repo_dir=os.environ.get("REPO_DIR", "/repo"),
    )
