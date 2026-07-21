"""Synchronization helper for storage snapshots in parallel engine waves."""

from contextlib import contextmanager
from threading import Barrier, BrokenBarrierError
from typing import Iterator, Optional


@contextmanager
def storage_snapshot_barrier(barrier: Optional[Barrier]) -> Iterator[None]:
    """Keep storage I/O between two wave-wide barriers."""
    if barrier is None:
        yield
        return
    try:
        barrier.wait()
    except BrokenBarrierError as exc:
        raise RuntimeError("parallel storage barrier aborted") from exc
    try:
        yield
    finally:
        try:
            barrier.wait()
        except BrokenBarrierError as exc:
            raise RuntimeError("parallel storage barrier aborted") from exc
