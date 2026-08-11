"""Parse Databricks EXPLAIN CREATE MATERIALIZED VIEW eligibility output."""

import re
from typing import Optional, Tuple


_NOT_INCREMENTALIZABLE_MARKER = re.compile(
    r"cannot be incrementally refreshed", re.IGNORECASE,
)
_INCREMENTALIZABLE_MARKER = re.compile(
    r"\bcan be incrementally refreshed", re.IGNORECASE,
)
_DETAILED_INFO_SECTION = re.compile(
    r"==\s*Detailed Incrementalization Info\s*==\s*\n(.*?)(?:\n==|\Z)",
    re.IGNORECASE | re.DOTALL,
)
_REASON_BULLET = re.compile(r"^\s*-\s*(.+?)\s*$", re.MULTILINE)


def _non_incrementalizable_error(plan: str) -> Optional[str]:
    if not plan or not _NOT_INCREMENTALIZABLE_MARKER.search(plan):
        return None
    section = _DETAILED_INFO_SECTION.search(plan)
    if section:
        bullets = [
            bullet.strip()
            for bullet in _REASON_BULLET.findall(section.group(1))
            if bullet.strip()
        ]
        reasons = "; ".join(bullets) if bullets else "no detailed reason"
    else:
        reasons = "no detailed reason"
    return f"MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE: {reasons}"


def classify_incrementalization_plan(plan: str) -> Tuple[str, Optional[str]]:
    """Return incremental, full, or unknown from Databricks plan text."""
    error = _non_incrementalizable_error(plan)
    if error is not None:
        return "full", error
    if _INCREMENTALIZABLE_MARKER.search(plan or ""):
        return "incremental", None
    return "unknown", "EXPLAIN returned no incrementalization eligibility verdict"
