"""Validation shared by experiment inputs and runtime configuration."""

from decimal import Decimal, InvalidOperation


def validate_refresh(config):
    if config.workload not in {"standard", "databricks"}:
        raise ValueError("workload must be standard or databricks")
    try:
        count = int(config.refresh_count)
        if isinstance(config.refresh_count, bool) or count != Decimal(str(config.refresh_count)):
            raise ValueError
        pct = Decimal(str(config.refresh_pct))
        if not pct.is_finite() or pct <= 0:
            raise ValueError
    except (ValueError, InvalidOperation, OverflowError) as exc:
        raise ValueError("refresh_count must be an integer >= 1; refresh_pct must be finite and > 0") from exc
    if count < 1:
        raise ValueError("refresh_count must be an integer >= 1")
    config.refresh_count = count
    config.refresh_pct = str(pct)
    if config.repeated_refresh:
        if any(Decimal(str(getattr(config, field))) != 0 for field in (
            "batch_2_update_pct", "batch_2_delete_pct", "batch_3_update_pct", "batch_3_delete_pct",
        )):
            raise ValueError("repeated refresh is insert-only; mutation percentages must be zero")
        if hasattr(config, "feature_flags") and (
            config.feature_flags.compiler_bench or config.feature_flags.cost_model_bench
        ):
            raise ValueError("repeated refresh cannot be combined with compiler/cost model benchmarks")
        config.batch_1_pct = "100"
        # The existing dbt adapters use this flag to interpret trade CDC events.
        config.batch_2_days = 1 if config.workload == "databricks" else 0
