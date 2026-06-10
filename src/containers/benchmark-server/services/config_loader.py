"""Configuration loader — reads environment variables into BenchmarkConfig."""

import os

from models.config import BenchmarkConfig


def _insert_pct(batch_num: int, default: str) -> str:
    insert_pct = os.environ.get(f"BATCH_{batch_num}_INSERT_PCT")
    if insert_pct:
        return insert_pct
    return os.environ.get(f"BATCH_{batch_num}_PCT", default)


def load_config() -> BenchmarkConfig:
    """Build a BenchmarkConfig from environment variables."""
    return BenchmarkConfig(
        scale_factor=int(os.environ.get("SCALE_FACTOR", "3")),
        batch_1_pct=_insert_pct(1, "1"),
        batch_2_pct=_insert_pct(2, "0.001"),
        batch_3_pct=_insert_pct(3, "0.002"),
        batch_2_update_pct=os.environ.get("BATCH_2_UPDATE_PCT", "0"),
        batch_2_delete_pct=os.environ.get("BATCH_2_DELETE_PCT", "0"),
        batch_3_update_pct=os.environ.get("BATCH_3_UPDATE_PCT", "0"),
        batch_3_delete_pct=os.environ.get("BATCH_3_DELETE_PCT", "0"),
        parallel=os.environ.get("PARALLEL", "0") == "1",
        engines=[
            e.strip()
            for e in os.environ.get("ENGINES", "spark,spark-openivm,duckdb,duckdb-openivm,feldera").split(",")
            if e.strip()
        ],
        host_cores=int(os.environ["HOST_CORES"]) if os.environ.get("HOST_CORES") else None,
        host_memory_gb=int(os.environ["HOST_MEMORY"]) if os.environ.get("HOST_MEMORY") else None,
        repo_dir=os.environ.get("REPO_DIR", "/repo"),
    )
