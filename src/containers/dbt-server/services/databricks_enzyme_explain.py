"""databricks-enzyme pre-flight incrementalizability gate.

Validates every dbt model in the `databricks-enzyme` project by sending

    EXPLAIN CREATE MATERIALIZED VIEW <sandbox>.<sanitized>
    REFRESH POLICY INCREMENTAL STRICT
    AS <compiled_sql>

to the Databricks Serverless SQL warehouse. The planner returns either a
plan (model is structurally incrementalizable) or throws
`MATERIALIZED_VIEW_NOT_INCREMENTALIZABLE: <reason>` (model is NOT). We
capture both and surface a single pass/fail verdict for the experiment.

Designed to run AFTER `init_sources(sf)` and BEFORE batch 1's timer so:
- failures fail the engine without polluting timing data, and
- artifacts under `mount/query-plan/<sf>/databricks-enzyme/
  explain-create-materialized-view/` give an at-a-glance view of which
  queries the planner accepted vs rejected.

Cross-model resolution: Silver references bronze, gold references both.
At pre-flight time only `exp_<ts>_data.*` (the per-experiment source
table schema) exists. So we walk the dbt DAG in
topological order and after a successful EXPLAIN, `CREATE OR REPLACE
VIEW` the model's compiled SQL at its *real* target name. Regular views
are metadata-only (no compute), and downstream EXPLAINs can then resolve
their FROM clauses. All stub views are explicitly DROPped at the end so
the immediately-following dbt build's `CREATE MATERIALIZED VIEW` can
land cleanly.
"""

from __future__ import annotations

import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from services import databricks_enzyme_sources as src
from services.dbt_compiler import get_manifest

logger = logging.getLogger(__name__)

ENGINE = "databricks-enzyme"

# Sandbox schema for the throwaway EXPLAIN target names. Created and
# DROPped CASCADE inside explain_all_models so nothing lingers between
# experiments. Distinct from SOURCE_SCHEMA so a parallel `init/<sf>`
# cleanup can never race with us.
_SANDBOX_SUFFIX = "_validate"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize(name: str) -> str:
    """Render an arbitrary string as a safe Databricks identifier fragment."""
    return re.sub(r"[^0-9a-zA-Z_]", "_", name)


def _sandbox_schema() -> str:
    """Per-experiment sandbox schema for throwaway EXPLAIN stub views.

    Lives inside the per-experiment ``data`` schema so a CASCADE cleanup
    of the experiment also drops any leftover stub views in case the
    pre-flight crashed mid-walk. Distinct from the layer schemas
    (bronze/silver/gold/work) so the stubs never collide with real MVs.
    """
    return f"{src.data_schema()}{_SANDBOX_SUFFIX}"


def _fq_sandbox(sanitized: str) -> str:
    return f"`{src.CATALOG}`.`{_sandbox_schema()}`.`{sanitized}`"


def _fq_real(schema: str, name: str) -> str:
    return f"`{src.CATALOG}`.`{schema}`.`{name}`"


def _strip_trailing_semicolon(sql: str) -> str:
    return sql.strip().rstrip(";").rstrip()


def _topo_sort(
    nodes: Dict[str, Dict[str, Any]],
    manifest: Dict[str, Any],
) -> List[str]:
    """Kahn's algorithm over the manifest's depends_on.nodes graph.

    `nodes` is the set of unique_ids we care about (non-ephemeral models).
    Edges from a node's `depends_on.nodes` that point outside `nodes`
    (sources, ephemeral models, tests) are ignored.
    """
    full_nodes = manifest.get("nodes", {})

    in_degree: Dict[str, int] = {uid: 0 for uid in nodes}
    children: Dict[str, List[str]] = {uid: [] for uid in nodes}

    for uid in nodes:
        node = full_nodes.get(uid, {})
        deps = node.get("depends_on", {}).get("nodes", []) or []
        for dep in deps:
            if dep in nodes:
                children[dep].append(uid)
                in_degree[uid] += 1

    ready = [uid for uid, d in in_degree.items() if d == 0]
    ready.sort()  # stable order across runs
    out: List[str] = []
    while ready:
        uid = ready.pop(0)
        out.append(uid)
        for child in sorted(children[uid]):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                ready.append(child)
        ready.sort()

    if len(out) != len(nodes):
        cyclic = sorted(set(nodes) - set(out))
        raise RuntimeError(
            f"[databricks-enzyme] dbt manifest has a cycle among models: "
            f"{cyclic[:10]}..."
        )
    return out


def _select_models(manifest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Pick non-ephemeral models with compiled SQL from the manifest.

    Returns a dict keyed by unique_id with fields the EXPLAIN sweep needs.
    """
    out: Dict[str, Dict[str, Any]] = {}
    for uid, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue
        materialized = (node.get("config") or {}).get("materialized")
        if materialized == "ephemeral":
            continue
        compiled_sql = node.get("compiled_code") or node.get("compiled_sql")
        if not compiled_sql:
            continue
        out[uid] = {
            "unique_id": uid,
            "name": node.get("name", uid.split(".")[-1]),
            "schema": node.get("schema", ""),
            "compiled_sql": compiled_sql,
            "materialized": materialized or "",
        }
    return out


# ---------------------------------------------------------------------------
# Sandbox lifecycle
# ---------------------------------------------------------------------------


def _ensure_sandbox() -> None:
    src._ensure_experiment_schemas()
    src._execute(
        f"CREATE SCHEMA IF NOT EXISTS `{src.CATALOG}`.`{_sandbox_schema()}`"
    )


def _drop_sandbox() -> None:
    try:
        src._execute(
            f"DROP SCHEMA IF EXISTS `{src.CATALOG}`.`{_sandbox_schema()}` CASCADE"
        )
    except Exception as exc:
        logger.warning(
            "[databricks-enzyme] DROP sandbox schema %s failed: %s",
            _sandbox_schema(),
            exc,
        )


def _drop_stub_views(stubs: List[Tuple[str, str]]) -> None:
    """`stubs` is a list of (schema, name) pairs to DROP VIEW IF EXISTS."""
    for schema, name in stubs:
        try:
            src._execute(f"DROP VIEW IF EXISTS {_fq_real(schema, name)}")
        except Exception as exc:
            logger.warning(
                "[databricks-enzyme] DROP stub VIEW %s.%s failed: %s",
                schema,
                name,
                exc,
            )


# ---------------------------------------------------------------------------
# Per-model EXPLAIN
# ---------------------------------------------------------------------------


def _explain_one(model: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Run EXPLAIN CREATE MATERIALIZED VIEW for one model.

    Returns (plan_text, None) on success or (None, error_text) on failure.
    """
    sanitized = _sanitize(f"{model['schema']}__{model['name']}")
    sandbox_target = _fq_sandbox(sanitized)
    compiled_sql = _strip_trailing_semicolon(model["compiled_sql"])

    explain_sql = (
        f"EXPLAIN CREATE MATERIALIZED VIEW {sandbox_target} "
        f"REFRESH POLICY INCREMENTAL STRICT AS\n{compiled_sql}"
    )
    try:
        df = src._execute(explain_sql)
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"

    if df is None or df.empty:
        return "", None
    plan = "\n".join(str(v) for v in df.iloc[:, 0].tolist()).rstrip()
    return plan, None


def _register_stub_view(model: Dict[str, Any]) -> None:
    """CREATE OR REPLACE VIEW at the model's real target name.

    Lets downstream EXPLAINs resolve `<schema>.<name>` without needing
    the actual MV to exist. Regular views are metadata-only.
    """
    fq = _fq_real(model["schema"], model["name"])
    compiled_sql = _strip_trailing_semicolon(model["compiled_sql"])
    src._execute(f"CREATE OR REPLACE VIEW {fq} AS\n{compiled_sql}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def explain_all_models(sf: int) -> Dict[str, Any]:
    """Pre-flight gate: EXPLAIN every dbt model under INCREMENTAL STRICT.

    Returns a dict with:
      status: "ok" | "error"
      scale_factor: int
      total_models: int
      passed: int
      failed: int
      skipped_ephemeral: list[unique_id]
      failures: list[{model, unique_id, error}]
      plans: list[{model, unique_id, schema, compiled_sql, plan, error}]
      elapsed_ms: int
    """
    started = time.time()

    manifest = get_manifest(ENGINE, force_compile=True)
    if not manifest:
        raise RuntimeError(
            "[databricks-enzyme] dbt compile failed for engine "
            f"'{ENGINE}'; cannot run EXPLAIN sweep"
        )

    models = _select_models(manifest)
    skipped_ephemeral = sorted(
        uid for uid, node in manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
        and (node.get("config") or {}).get("materialized") == "ephemeral"
    )

    if not models:
        return {
            "status": "ok",
            "scale_factor": sf,
            "total_models": 0,
            "passed": 0,
            "failed": 0,
            "skipped_ephemeral": skipped_ephemeral,
            "failures": [],
            "plans": [],
            "elapsed_ms": int((time.time() - started) * 1000),
        }

    order = _topo_sort(models, manifest)

    logger.info(
        "[databricks-enzyme] EXPLAIN sweep: %d models, %d ephemeral skipped, sf=%d",
        len(order), len(skipped_ephemeral), sf,
    )

    _ensure_sandbox()
    stubs: List[Tuple[str, str]] = []
    plans: List[Dict[str, Any]] = []
    failures: List[Dict[str, Any]] = []

    try:
        for uid in order:
            model = models[uid]
            plan, error = _explain_one(model)
            entry: Dict[str, Any] = {
                "model": f"{model['schema']}.{model['name']}",
                "unique_id": uid,
                "schema": model["schema"],
                "name": model["name"],
                "compiled_sql": model["compiled_sql"],
            }
            if error is not None:
                entry["plan"] = ""
                entry["error"] = error
                failures.append({
                    "model": entry["model"],
                    "unique_id": uid,
                    "error": error,
                })
                logger.warning(
                    "[databricks-enzyme] EXPLAIN rejected %s: %s",
                    entry["model"], error,
                )
            else:
                entry["plan"] = plan or ""
                entry["error"] = None
                try:
                    _register_stub_view(model)
                    stubs.append((model["schema"], model["name"]))
                except Exception as exc:
                    logger.warning(
                        "[databricks-enzyme] stub VIEW registration failed for %s: %s",
                        entry["model"], exc,
                    )
            plans.append(entry)
    finally:
        _drop_stub_views(stubs)
        _drop_sandbox()

    passed = sum(1 for p in plans if not p.get("error"))
    failed = len(failures)
    status = "ok" if failed == 0 else "error"

    elapsed_ms = int((time.time() - started) * 1000)
    logger.info(
        "[databricks-enzyme] EXPLAIN sweep complete sf=%d status=%s "
        "passed=%d failed=%d elapsed_ms=%d",
        sf, status, passed, failed, elapsed_ms,
    )
    return {
        "status": status,
        "scale_factor": sf,
        "total_models": len(plans),
        "passed": passed,
        "failed": failed,
        "skipped_ephemeral": skipped_ephemeral,
        "failures": failures,
        "plans": plans,
        "elapsed_ms": elapsed_ms,
    }
