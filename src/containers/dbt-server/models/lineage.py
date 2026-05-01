"""Lineage dataclasses for the lineage API."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Literal


@dataclass
class LineageNode:
    """A node in the dbt lineage graph."""

    unique_id: str
    name: str
    resource_type: str
    schema: str = ""
    role: Literal["source", "intermediate", "target", "standalone"] = "standalone"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class LineageEdge:
    """A directed edge in the lineage graph."""

    source: str  # unique_id of upstream node
    target: str  # unique_id of downstream node

    def to_dict(self) -> dict[str, Any]:
        return {"from": self.source, "to": self.target}


@dataclass
class LineageGraph:
    """Full lineage graph with metadata."""

    engine: str
    nodes: list[LineageNode] = field(default_factory=list)
    edges: list[LineageEdge] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "engine": self.engine,
            "nodes": [n.to_dict() for n in self.nodes],
            "edges": [e.to_dict() for e in self.edges],
            "metadata": self.metadata,
        }
