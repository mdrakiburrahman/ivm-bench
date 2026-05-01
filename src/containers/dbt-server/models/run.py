"""Run-related dataclasses for typed API responses."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, Optional


@dataclass
class NodeResult:
    """A single dbt node execution result."""

    run_id: Optional[str]
    unique_id: str
    name: str
    resource_type: str = ""
    execution_time_s: Optional[float] = None
    status: str = ""
    compiled_sql: str = ""
    depends_on: list[str] = field(default_factory=list)
    rows_affected: Optional[int] = None

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        return d

    @classmethod
    def from_db_row(cls, row: dict) -> NodeResult:
        depends_on = json.loads(row.get("depends_on") or "[]")
        return cls(
            run_id=row.get("run_id"),
            unique_id=row.get("unique_id", ""),
            name=row.get("name", ""),
            resource_type=row.get("resource_type", ""),
            execution_time_s=row.get("execution_time_s"),
            status=row.get("status", ""),
            compiled_sql=row.get("compiled_sql", ""),
            depends_on=depends_on,
            rows_affected=row.get("rows_affected"),
        )


@dataclass
class RunResult:
    """Full run result including nodes and edges."""

    run_id: str
    engine: str
    scale_factor: int
    full_refresh: bool
    status: str
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    duration_s: Optional[float] = None
    error: Optional[str] = None
    nodes: list[NodeResult] = field(default_factory=list)
    edges: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["nodes"] = [n.to_dict() for n in self.nodes]
        return d
