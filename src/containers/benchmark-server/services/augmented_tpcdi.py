"""Pure helpers for the accumulated-daily TPC-DI workload."""

from __future__ import annotations

from typing import Dict, Iterable

from models.experiments import ExperimentInputs


STANDARD_INCREMENTAL_BATCHES = 2


def required_incremental_batches(batch_2_days: int) -> int:
    """Return the DIGen horizon needed for one experiment.

    A measured Batch 2 containing ``N`` daily updates consumes DIGen batches
    2..N+1. IVM-Bench still executes Batch 3, so it consumes the following
    daily update (DIGen batch N+2). DIGen's parameter counts updates after
    Batch 1, hence N+1 incremental batches are required.
    """
    if batch_2_days < 0:
        raise ValueError("batch_2_days must be non-negative")
    if batch_2_days == 0:
        return STANDARD_INCREMENTAL_BATCHES
    return batch_2_days + 1


def digen_horizons_by_scale_factor(
    experiments: Iterable[ExperimentInputs],
) -> Dict[int, int]:
    """Compute one reusable DIGen horizon for every scale factor in a sweep."""
    horizons: Dict[int, int] = {}
    for experiment in experiments:
        required = required_incremental_batches(experiment.batch_2_days)
        horizons[experiment.scale_factor] = max(
            horizons.get(experiment.scale_factor, STANDARD_INCREMENTAL_BATCHES),
            required,
        )
    return horizons
