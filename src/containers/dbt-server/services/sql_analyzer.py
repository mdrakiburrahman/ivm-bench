"""SQL analysis service — uses sqlglot to parse and analyze compiled SQL."""

from __future__ import annotations

import logging
from typing import Any

import sqlglot
from sqlglot import exp

from models.sql_analysis import OperatorCounts, SQLAnalysis

logger = logging.getLogger(__name__)

# Dialect mapping for engines
ENGINE_DIALECTS = {
    "spark": "spark",
    "spark-openivm": "spark",
    "duckdb": "duckdb",
    "duckdb-openivm": "duckdb",
    "databricks-enzyme": "databricks",
    "feldera": None,  # sqlglot doesn't support Feldera; use generic
}


def analyze_sql(model_name: str, unique_id: str, sql_text: str, engine: str = "spark") -> SQLAnalysis:
    """
    Parse SQL with sqlglot and produce a comprehensive operator breakdown.
    """
    dialect = ENGINE_DIALECTS.get(engine)

    try:
        parsed = sqlglot.parse(sql_text, dialect=dialect)
    except sqlglot.errors.ParseError as e:
        return SQLAnalysis(
            model_name=model_name,
            unique_id=unique_id,
            compiled_sql=sql_text,
            parse_error=str(e),
        )

    if not parsed:
        return SQLAnalysis(
            model_name=model_name,
            unique_id=unique_id,
            compiled_sql=sql_text,
            parse_error="No statements parsed",
        )

    # Use the first (and typically only) statement
    tree = parsed[0]

    # Generate AST JSON
    try:
        ast_json = tree.dump()
    except Exception:
        ast_json = None

    # Count operators
    operators = _count_operators(tree)

    return SQLAnalysis(
        model_name=model_name,
        unique_id=unique_id,
        compiled_sql=sql_text,
        ast_json=ast_json,
        operators=operators,
    )


def _count_operators(tree: exp.Expression) -> OperatorCounts:
    """Walk the AST and count SQL operators."""
    counts = OperatorCounts()

    # Window functions
    counts.window_functions = len(list(tree.find_all(exp.Window)))

    # Joins — categorize by join type
    for join in tree.find_all(exp.Join):
        join_kind = (join.args.get("kind") or "").upper()
        join_side = (join.args.get("side") or "").upper()

        if join_kind == "CROSS":
            counts.join_cross += 1
        elif join_side == "LEFT" and join_kind == "ANTI":
            counts.join_left_anti += 1
        elif join_side == "LEFT" and join_kind == "SEMI":
            counts.join_left_semi += 1
        elif join_side == "RIGHT" and join_kind == "ANTI":
            counts.join_right_anti += 1
        elif join_side == "RIGHT" and join_kind == "SEMI":
            counts.join_right_semi += 1
        elif join_side == "RIGHT":
            counts.join_right += 1
        elif join_side == "FULL":
            counts.join_full += 1
        elif join_side == "LEFT":
            counts.join_left += 1
        else:
            # Default: INNER or unspecified
            counts.join_inner += 1

    # CTEs
    for cte_scope in tree.find_all(exp.With):
        counts.cte += len(list(cte_scope.find_all(exp.CTE)))

    # Delete / Update
    counts.delete_update = (
        len(list(tree.find_all(exp.Delete)))
        + len(list(tree.find_all(exp.Update)))
    )

    # Aggregates — count aggregate functions
    agg_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max, exp.ArrayAgg, exp.GroupConcat)
    for agg_type in agg_types:
        counts.aggregates += len(list(tree.find_all(agg_type)))

    # Distinct
    counts.distinct = len(list(tree.find_all(exp.Distinct)))

    # Sort (ORDER BY)
    counts.sort = len(list(tree.find_all(exp.Order)))

    # Sub-queries (SELECT inside another SELECT, excluding CTEs)
    all_selects = list(tree.find_all(exp.Select))
    # The first Select is the main query; others are subqueries
    # But CTEs also contain Selects, so we count Subquery nodes specifically
    counts.subqueries = len(list(tree.find_all(exp.Subquery)))

    return counts


def analyze_all_models(engine: str, models: dict[str, dict[str, Any]]) -> list[SQLAnalysis]:
    """Analyze all compiled models for an engine."""
    results = []
    for uid, model_data in models.items():
        analysis = analyze_sql(
            model_name=model_data["name"],
            unique_id=uid,
            sql_text=model_data["compiled_sql"],
            engine=engine,
        )
        results.append(analysis)
    return results
