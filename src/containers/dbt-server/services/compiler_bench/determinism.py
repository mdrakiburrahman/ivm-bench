"""Conservative SQL nondeterminism checks used to gate correctness verdicts.

This mirrors LPTS's ``IsLikelyNondeterministicSQL`` text-level checks.  It is
deliberately conservative: a flagged query may happen to return the same rows
twice, but a mismatch is not sufficient evidence of an IVM correctness bug.
"""

from __future__ import annotations

import re
from typing import Optional


def _normalized(sql: str) -> str:
    return " " + " ".join(sql.lower().split()) + " "


def _contains_phrase(sql: str, phrase: str) -> bool:
    return _normalized(phrase) in _normalized(sql)


def _has_call(sql: str, name: str) -> bool:
    return re.search(
        rf"(?<![A-Za-z0-9_]){re.escape(name)}\(", sql, re.IGNORECASE
    ) is not None


def _has_window_call(sql: str, name: str) -> bool:
    return _has_call(sql, name) and _contains_phrase(sql, "over")


def nondeterminism_reason(sql: str) -> Optional[str]:
    """Return LPTS's conservative reason, or ``None`` if no pattern matches."""
    if _has_call(sql, "string_agg") or _has_call(sql, "group_concat"):
        return (
            "order-sensitive aggregate (string_agg/group_concat) may have a "
            "non-total ordering"
        )
    if _has_call(sql, "listagg"):
        return "order-sensitive aggregate (listagg) may have a non-total ordering"
    if _has_call(sql, "list") or _has_call(sql, "array_agg"):
        return "order-sensitive aggregate (list/array_agg) may have a non-total ordering"
    if _has_call(sql, "random"):
        return "volatile random() expression"
    if any(
        _has_call(sql, name)
        for name in ("uuid", "uuidv4", "uuidv7", "gen_random_uuid")
    ):
        return "volatile UUID generator"
    if _has_call(sql, "stats"):
        return "stats() depends on storage layout / statistics"
    if _has_call(sql, "nextval") or _has_call(sql, "currval"):
        return "sequence access (nextval/currval) is stateful"
    if (
        _has_call(sql, "now")
        or any(
            _contains_phrase(sql, phrase)
            for phrase in ("current_timestamp", "current_date", "current_time")
        )
        or any(
            _has_call(sql, name)
            for name in (
                "get_current_timestamp",
                "transaction_timestamp",
                "current_localtimestamp",
                "current_localtime",
            )
        )
    ):
        return "wall-clock/transaction time function"
    for name, reason in (
        ("row_number", "row_number over potentially tied ordering keys"),
        ("rank", "rank over potentially tied ordering keys"),
        ("dense_rank", "dense_rank over potentially tied ordering keys"),
    ):
        if _has_window_call(sql, name):
            return reason
    if any(
        _has_window_call(sql, name)
        for name in ("lag", "lead", "first_value", "last_value", "nth_value")
    ):
        return "window function over potentially tied ordering keys"
    if _contains_phrase(sql, "using sample") or _contains_phrase(sql, "tablesample"):
        return "row sampling (USING SAMPLE/TABLESAMPLE) returns a nondeterministic subset"
    if any(
        _contains_phrase(sql, phrase)
        for phrase in ("limit", "offset", "fetch first", "fetch next")
    ):
        return "LIMIT/OFFSET/FETCH selects an unspecified subset of rows"

    floating_aggregates = (
        "avg",
        "favg",
        "mean",
        "fsum",
        "kahan_sum",
        "sumkahan",
        "geomean",
        "stddev",
        "stddev_pop",
        "stddev_samp",
        "variance",
        "var_pop",
        "var_samp",
        "corr",
        "covar_pop",
        "covar_samp",
        "sem",
        "skewness",
        "kurtosis",
        "kurtosis_pop",
        "entropy",
        "regr_avgx",
        "regr_avgy",
        "regr_intercept",
        "regr_r2",
        "regr_slope",
        "regr_sxx",
        "regr_sxy",
        "regr_syy",
    )
    if any(_has_call(sql, name) for name in floating_aggregates):
        return "strict floating aggregate equality may depend on evaluation order"
    if re.search(
        r"\b(sum|product|geomean|fsum|kahan_sum)\s*\([^()]*\border\s+by\b",
        sql,
        re.IGNORECASE,
    ):
        return "ordered floating aggregate result may depend on summation order"
    if any(
        _has_call(sql, name)
        for name in (
            "approx_quantile",
            "approx_count_distinct",
            "reservoir_quantile",
            "approx_top_k",
        )
    ):
        return "approximate aggregate result is not exactly reproducible"
    lower = sql.lower()
    if "export_state" in lower and "blob" in lower:
        return "raw exported aggregate state materialized as BLOB is not byte-reproducible"
    return None
