"""Synchronization helper for storage snapshots in parallel engine waves."""

from contextlib import contextmanager
from threading import Barrier, BrokenBarrierError
from typing import Iterator, Optional


@contextmanager
def storage_snapshot_barrier(barrier: Optional[Barrier]) -> Iterator[None]:
    """Keep storage I/O outside every engine's timed batch window.

    The first wait lets every engine finish its timed batch before a faster
    peer starts a disk-heavy storage walk. The second prevents any engine from
    starting its next timed batch while a slower peer is still measuring.
    """
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
