"""Lineage handler — exposes dbt DAG as JSON with role annotations."""

from datetime import datetime, timezone

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from models.lineage import LineageEdge, LineageGraph, LineageNode
from services.dbt_compiler import get_manifest

bp = Blueprint("lineage", __name__)


def _build_lineage(engine: str) -> LineageGraph | None:
    """Build a full lineage graph from a dbt manifest."""
    manifest = get_manifest(engine)
    if not manifest:
        return None

    # Collect all nodes (models + sources + snapshots)
    all_nodes: dict[str, dict] = {}

    for uid, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") in ("model", "snapshot"):
            all_nodes[uid] = {
                "unique_id": uid,
                "name": node.get("name", uid.split(".")[-1]),
                "resource_type": node.get("resource_type", ""),
                "schema": node.get("schema", ""),
                "depends_on_nodes": node.get("depends_on", {}).get("nodes", []),
            }

    for uid, source in manifest.get("sources", {}).items():
        all_nodes[uid] = {
            "unique_id": uid,
            "name": source.get("name", uid.split(".")[-1]),
            "resource_type": "source",
            "schema": source.get("schema", ""),
            "depends_on_nodes": [],
        }

    # Build edges
    edges: list[LineageEdge] = []
    downstream_set: set[str] = set()  # nodes that have upstream deps
    upstream_set: set[str] = set()  # nodes that are depended upon

    for uid, node_data in all_nodes.items():
        for dep_uid in node_data["depends_on_nodes"]:
            if dep_uid in all_nodes:
                edges.append(LineageEdge(source=dep_uid, target=uid))
                downstream_set.add(uid)
                upstream_set.add(dep_uid)

    # Assign roles
    lineage_nodes: list[LineageNode] = []
    for uid, node_data in all_nodes.items():
        is_upstream = uid in upstream_set
        is_downstream = uid in downstream_set

        if node_data["resource_type"] == "source":
            role = "source"
        elif is_upstream and is_downstream:
            role = "intermediate"
        elif is_downstream and not is_upstream:
            role = "target"
        elif is_upstream and not is_downstream:
            role = "source"
        else:
            role = "standalone"

        lineage_nodes.append(LineageNode(
            unique_id=uid,
            name=node_data["name"],
            resource_type=node_data["resource_type"],
            schema=node_data["schema"],
            role=role,
        ))

    return LineageGraph(
        engine=engine,
        nodes=lineage_nodes,
        edges=edges,
        metadata={
            "node_count": len(lineage_nodes),
            "edge_count": len(edges),
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
    )


@bp.route("/lineage/<engine>")
def get_lineage(engine):
    """Return the full dbt lineage DAG for an engine with role annotations."""
    graph = _build_lineage(engine)
    if graph is None:
        return jsonify({"error": f"Could not build lineage for engine '{engine}'. dbt compile may have failed."}), 500

    return jsonify(graph.to_dict())


class LineageHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
