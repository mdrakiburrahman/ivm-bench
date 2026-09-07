"""Shared source-cache identity for cloud engines."""

import os
from pathlib import Path


STAGING_TABLES = (
    "cash_transaction", "daily_market", "holding_history", "prospect",
    "trade", "watch_history", "account", "customer", "batch_date",
)
AUGMENTED_STAGING_TABLES = (
    "cash_transaction", "daily_market", "holding_history", "trade",
    "watch_history", "account", "customer",
)


def incremental_staging_tables():
    """Return the source tables updated by the configured workload."""
    if int(os.environ.get("TPCDI_BATCH_2_DAYS", "0")) > 0:
        return AUGMENTED_STAGING_TABLES
    return STAGING_TABLES


def generated_batch_dirs(raw_delta_dir: str, batch_num: int):
    """Return every expected immutable table directory or fail loudly."""
    root = Path(raw_delta_dir) / f"batch{batch_num}"
    tables = tuple((table, root / table) for table in incremental_staging_tables())
    missing = [table for table, path in tables if not path.is_dir()]
    if missing:
        raise RuntimeError(
            f"Generated batch {batch_num} is missing Delta tables: {', '.join(missing)}"
        )
    return tables


def batch_cache_root(cache_root: str, scale_factor: int, batch_num: int) -> str:
    """Return the cache root for one distinct generated source batch."""
    days = int(os.environ.get("TPCDI_BATCH_2_DAYS", "0"))
    if days > 0:
        batch = "batch1_augmented" if batch_num == 1 else f"batch{batch_num}_augmented_days={days}"
        return f"{cache_root.rstrip('/')}/sf={scale_factor}/{batch}"

    insert_pct = os.environ.get(f"BATCH_{batch_num}_INSERT_PCT", "").strip()
    pct = insert_pct or os.environ.get(f"BATCH_{batch_num}_PCT", "").strip()
    if not pct:
        raise RuntimeError(f"Batch {batch_num} insertion percentage is not configured")
    return f"{cache_root.rstrip('/')}/sf={scale_factor}/batch{batch_num}_pct={pct}"
