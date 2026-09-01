"""Shared source-cache identity for cloud engines."""

import os


def batch_cache_root(cache_root: str, scale_factor: int, batch_num: int) -> str:
    """Return the cache root for one generated batch percentage."""
    insert_pct = os.environ.get(f"BATCH_{batch_num}_INSERT_PCT", "").strip()
    pct = insert_pct or os.environ.get(f"BATCH_{batch_num}_PCT", "").strip()
    if not pct:
        raise RuntimeError(f"Batch {batch_num} insertion percentage is not configured")
    return f"{cache_root.rstrip('/')}/sf={scale_factor}/batch{batch_num}_pct={pct}"
