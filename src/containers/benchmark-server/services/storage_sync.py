"""Synchronization helper for storage snapshots in parallel engine waves."""

from contextlib import contextmanager
from threading import Barrier, BrokenBarrierError
from typing import Iterator, Optional


class StorageMetricsError(RuntimeError):
    """Raised when an enabled storage snapshot is incomplete."""


def require_complete_storage_metrics(status: str, engine: str, batch_num: int) -> None:
    """Reject partial or failed snapshots after their artifact has been saved."""
    if status != "ok":
        raise StorageMetricsError(
            f"storage metrics for {engine} batch {batch_num} returned status={status}"
        )


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
