"""Databricks MV refresh- and storage-metrics collector.

After each dbt batch finishes, we want to know — per MV — how many rows the
INCREMENTAL STRICT refresh actually inserted/updated/deleted, how long the
underlying Delta operation took, etc. This is captured by:

  - `SHOW TABLES IN <catalog>.<schema>` — discover every relation the dbt
    build materialised across every per-layer schema (bronze/silver/gold/
    work plus the per-experiment ``exp_<ts>_data`` source schema). Enzyme MVs
    surface here as TABLE rows whose `tableType` is `MATERIALIZED_VIEW`.
  - For each, `DESCRIBE HISTORY <fqn> LIMIT <K>` — Delta op history with
    `operationMetrics` map containing executionTimeMs, numOutputRows,
    numTargetRows{Inserted,Updated,Deleted}, numFiles, etc.

The benchmark-server writes this JSON under
`mount/stats/<SF>/databricks-enzyme/refresh-history-batch<N>.json` so it
sits next to the container-level CPU/memory samples.
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional

from services import databricks_enzyme_sources as src

logger = logging.getLogger(__name__)


def _candidate_schemas() -> List[str]:
    """All Databricks schemas we should sweep for MVs / Delta tables.

    Per-experiment isolation: this is the 5 ``exp_<ts>_*`` schemas for
    the active experiment_id (data + 4 layers). The MV refresh metrics
    of interest live in bronze/silver/gold; the source tables live in
    data; work is ephemeral. We include all 5 so the metrics endpoint
    matches whatever dbt actually wrote.
    """
    return list(src.all_experiment_schemas())


def _list_relations_in_schema(
    schema: str,
    execute: Optional[Callable[[str], Any]] = None,
    strict: bool = False,
) -> List[Dict[str, str]]:
    """Return every relation in <catalog>.<schema> as {schema, name, table_type}.

    Tries information_schema.tables first (gives us table_type, including
    `MATERIALIZED VIEW`); falls back to SHOW TABLES IN if information_schema
    returns empty or errors (Databricks Serverless sometimes lazy-populates
    information_schema for very fresh schemas).
    """
    catalog = src.CATALOG
    execute = execute or src._execute
    out: List[Dict[str, str]] = []
    try:
        df = execute(
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
        df = execute(f"SHOW TABLES IN `{catalog}`.`{schema}`")
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
        if strict:
            raise RuntimeError(
                f"could not enumerate Databricks relations in {schema}: {exc}"
            ) from exc
    # SHOW TABLES does not reliably expose relation type. Mark logical views
    # explicitly so storage collection does not issue ANALYZE TABLE against
    # them and turn an expected zero-byte relation into a false probe failure.
    try:
        views = execute(f"SHOW VIEWS IN `{catalog}`.`{schema}`")
        if views is not None and not views.empty:
            view_names = {
                str(
                    row.get("viewName")
                    or row.get("view_name")
                    or row.get("tableName")
                    or ""
                )
                for _, row in views.iterrows()
            }
            for rel in out:
                if rel["name"] in view_names:
                    rel["table_type"] = "VIEW"
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme/metrics] SHOW VIEWS fallback failed for %s: %s",
            schema,
            exc,
        )
    return out


def _remaining(deadline: Optional[float], default: float = 20.0) -> float:
    if deadline is None:
        return default
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("Databricks storage collection deadline exceeded")
    return min(default, remaining)


def list_relations(deadline: Optional[float] = None) -> List[Dict[str, str]]:
    """Return all experiment relations, normally with one catalog query."""
    schemas = _candidate_schemas()
    quoted = ", ".join(f"'{schema}'" for schema in schemas)
    sql = (
        "SELECT table_schema, table_name, table_type "
        f"FROM `{src.CATALOG}`.information_schema.tables "
        f"WHERE table_schema IN ({quoted})"
    )
    execute = src._execute
    if deadline is not None:
        execute = lambda statement: src.execute_isolated(  # noqa: E731
            statement, timeout_s=_remaining(deadline)
        )
    try:
        df = execute(sql)
        if df is not None and not df.empty:
            return [
                {
                    "schema": str(row["table_schema"]),
                    "name": str(row["table_name"]),
                    "table_type": str(row.get("table_type", "") or ""),
                }
                for _, row in df.iterrows()
            ]
    except Exception as exc:
        logger.warning("[databricks-enzyme/metrics] bulk relation lookup failed: %s", exc)

    out: List[Dict[str, str]] = []
    for schema in schemas:
        _remaining(deadline)
        out.extend(_list_relations_in_schema(schema, execute=execute, strict=True))
    return out


def _list_relations() -> List[Dict[str, str]]:
    """Backward-compatible alias for refresh-history callers."""
    return list_relations()


def analyze_storage(rel: Dict[str, str], deadline: Optional[float] = None):
    """Return complete storage metrics for one relation on an isolated connection."""
    fqn = f"`{src.CATALOG}`.`{rel['schema']}`.`{rel['name']}`"
    return src.execute_isolated(
        f"ANALYZE TABLE {fqn} COMPUTE STORAGE METRICS",
        timeout_s=_remaining(deadline, default=1800.0),
    )


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
    relations = list_relations()
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
