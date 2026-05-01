"""SQL analysis dataclasses for the sqlglot-based API."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class OperatorCounts:
    """Counts of SQL operators found in a query."""

    window_functions: int = 0
    join_inner: int = 0
    join_left: int = 0
    join_right: int = 0
    join_full: int = 0
    join_cross: int = 0
    join_left_anti: int = 0
    join_left_semi: int = 0
    join_right_anti: int = 0
    join_right_semi: int = 0
    cte: int = 0
    delete_update: int = 0
    aggregates: int = 0
    distinct: int = 0
    sort: int = 0
    subqueries: int = 0

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


@dataclass
class SQLAnalysis:
    """Complete SQL analysis result for a dbt model."""

    model_name: str
    unique_id: str
    compiled_sql: str
    ast_json: Optional[dict[str, Any]] = None
    operators: OperatorCounts = field(default_factory=OperatorCounts)
    parse_error: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "model_name": self.model_name,
            "unique_id": self.unique_id,
            "compiled_sql": self.compiled_sql,
            "ast_json": self.ast_json,
            "operators": self.operators.to_dict(),
            "parse_error": self.parse_error,
        }
