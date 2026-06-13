"""Databricks MV refresh-metrics collector.

After each dbt batch finishes, we want to know — per MV — how many rows the
INCREMENTAL STRICT refresh actually inserted/updated/deleted, how long the
underlying Delta operation took, etc. This is captured by:

  - `SHOW TABLES IN <catalog>.<schema>` — discover every relation the dbt
    build materialised across every per-layer schema (bronze/silver/gold/
    work plus the historical `tpcdi_bench` / `tpcdi_src`). Enzyme MVs
    surface here as TABLE rows whose `tableType` is `MATERIALIZED_VIEW`.
  - For each, `DESCRIBE HISTORY <fqn> LIMIT <K>` — Delta op history with
    `operationMetrics` map containing executionTimeMs, numOutputRows,
    numTargetRows{Inserted,Updated,Deleted}, numFiles, etc.

The benchmark-server writes this JSON under
`mount/stats/<SF>/databricks-enzyme/refresh-history-batch<N>.json` so it
sits next to the container-level CPU/memory samples.
"""

import logging
from typing import Any, Dict, List

from services import databricks_enzyme_sources as src

logger = logging.getLogger(__name__)


def _candidate_schemas() -> List[str]:
    """All Databricks schemas we should sweep for MVs / Delta tables.

    Includes the per-layer schemas defined in the dbt project
    (bronze/silver/gold/work by default) plus the historical
    `tpcdi_bench` / `tpcdi_src` for backwards compatibility.
    """
    seen: set[str] = set()
    out: List[str] = []
    for s in [src.DBT_SCHEMA, src.SOURCE_SCHEMA, *src.LAYER_SCHEMAS]:
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
    return out


def _list_relations_in_schema(schema: str) -> List[Dict[str, str]]:
    """Return every relation in <catalog>.<schema> as {schema, name, table_type}.

    Tries information_schema.tables first (gives us table_type, including
    `MATERIALIZED VIEW`); falls back to SHOW TABLES IN if information_schema
    returns empty or errors (Databricks Serverless sometimes lazy-populates
    information_schema for very fresh schemas).
    """
    catalog = src.CATALOG
    out: List[Dict[str, str]] = []
    try:
        df = src._execute(
            "SELECT table_schema, table_name, table_type "
            f"FROM `{catalog}`.information_schema.tables "
            f"WHERE table_schema = '{schema}'"
        )
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                out.append({
                    "schema": str(row["table_schema"]),
                    "name": str(row["table_name"]),
                    "table_type": str(row.get("table_type", "") or ""),
                })
            return out
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/metrics] information_schema lookup failed for %s: %s",
            schema,
            exc,
        )
    try:
        df = src._execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
        if df is None or df.empty:
            return out
        for _, row in df.iterrows():
            name = row.get("tableName") or row.get("table_name") or ""
            if not name:
                continue
            out.append({
                "schema": str(row.get("database", schema) or schema),
                "name": str(name),
                "table_type": "",
            })
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/metrics] SHOW TABLES fallback failed for %s: %s",
            schema,
            exc,
        )
    return out


def _list_relations() -> List[Dict[str, str]]:
    """Return every relation across every candidate schema."""
    out: List[Dict[str, str]] = []
    for schema in _candidate_schemas():
        out.extend(_list_relations_in_schema(schema))
    return out


def _describe_history(fqn: str, limit: int = 10) -> List[Dict[str, Any]]:
    try:
        df = src._execute(f"DESCRIBE HISTORY {fqn} LIMIT {limit}")
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/metrics] DESCRIBE HISTORY %s failed: %s",
            fqn,
            exc,
        )
        return []
    if df is None or df.empty:
        return []
    rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        rec: Dict[str, Any] = {}
        for col in df.columns:
            val = row[col]
            try:
                if hasattr(val, "to_pylist"):
                    val = val.to_pylist()
                elif hasattr(val, "tolist"):
                    val = val.tolist()
            except Exception:
                pass
            if val is None or isinstance(val, (str, int, float, bool, list, dict)):
                rec[col] = val
            else:
                rec[col] = str(val)
        rows.append(rec)
    return rows


def collect_refresh_metrics(batch_num: int, limit: int = 10) -> dict:
    """Discover every relation in every dbt-output schema and capture its
    `DESCRIBE HISTORY` rows (most recent first). Returns a JSON-ready
    payload the benchmark-server writes to disk.
    """
    relations = _list_relations()
    catalog = src.CATALOG
    out_relations: List[Dict[str, Any]] = []
    for rel in relations:
        fqn = f"`{catalog}`.`{rel['schema']}`.`{rel['name']}`"
        history = _describe_history(fqn, limit=limit)
        out_relations.append({
            "fqn": fqn,
            "schema": rel["schema"],
            "name": rel["name"],
            "table_type": rel.get("table_type", ""),
            "history": history,
        })
    return {
        "status": "ok",
        "batch_num": batch_num,
        "catalog": catalog,
        "schemas_scanned": _candidate_schemas(),
        "relation_count": len(out_relations),
        "relations": out_relations,
    }
