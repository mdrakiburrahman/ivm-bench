"""DuckDB-backed REST routes for the Spark metrics A/B.

Runs DuckDB over the Parquet emitted by ``services/spark_metrics.py`` under
``mount/metrics/<sf>/processed/``. The route signatures + response shape are a
STABLE CONTRACT for openivm-spark:

  GET  /metrics/kpis?sf=&engine=&model=&batch=   headline KPI rows
  GET  /metrics/diff?sf=&model=&batch=           spark vs spark-openivm diff
  POST /metrics/query  {"sql": "...", "sf": "..."}  read-only DuckDB SQL
  GET  /metrics/artifact?sf=&run_id=             stream the zip (default latest)

All dataframe responses use ``{"schema": [...], "data": [...], "row_count": N}``.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from flask import Blueprint, Flask, jsonify, request, send_file

from handlers.base import BaseHandler

bp = Blueprint("metrics", __name__)

_TABLES = ("metrics_long", "metrics_by_model", "timeseries")
_ROW_CAP_DEFAULT = 1000
_ROW_CAP_MAX = 10000

# Read-only guard: reject anything that is not a single SELECT/WITH statement.
_FORBIDDEN_RE = re.compile(
    r"\b(attach|copy|insert|update|delete|create|drop|alter|pragma|install|"
    r"load|set|export|import|call|vacuum|checkpoint)\b",
    re.IGNORECASE,
)


def _repo_dir() -> str:
    return os.environ.get("REPO_DIR", "/repo")


def _processed_dir(sf: str) -> str:
    return os.path.join(_repo_dir(), "mount", "metrics", str(sf), "processed")


def _metrics_root(sf: str) -> str:
    return os.path.join(_repo_dir(), "mount", "metrics", str(sf))


def _open_con(sf: str):
    """In-memory DuckDB with the three Parquet tables materialised.

    External file access is disabled AFTER load so a user ``/metrics/query``
    body cannot read arbitrary host files via ``read_parquet`` / ``read_csv``.
    Returns ``(con, missing)`` where ``missing`` lists absent tables.
    """
    import duckdb

    processed = _processed_dir(sf)
    con = duckdb.connect(database=":memory:")
    missing: List[str] = []
    for tbl in _TABLES:
        path = os.path.join(processed, f"{tbl}.parquet")
        if os.path.exists(path):
            con.execute(
                f"CREATE TABLE {tbl} AS SELECT * FROM read_parquet(?)", [path]
            )
        else:
            con.execute(f"CREATE TABLE {tbl} (dummy INTEGER)")
            missing.append(tbl)
    # Lock the connection down: no external access, no config changes.
    try:
        con.execute("SET enable_external_access=false")
        con.execute("SET lock_configuration=true")
    except Exception:  # pragma: no cover - older duckdb
        pass
    return con, missing


def _df_payload(df) -> Dict[str, Any]:
    schema = [{"name": str(c), "type": str(t)} for c, t in zip(df.columns, df.dtypes)]
    return {
        "schema": schema,
        "data": df.to_dict("records"),
        "row_count": int(len(df)),
    }


def _kpis() -> Any:
    sf = request.args.get("sf")
    if not sf:
        return jsonify({"error": "sf query parameter is required"}), 400
    processed = _processed_dir(sf)
    if not os.path.exists(os.path.join(processed, "metrics_by_model.parquet")):
        return jsonify({"error": f"no processed metrics for sf={sf}"}), 404

    con, _missing = _open_con(sf)
    try:
        clauses: List[str] = []
        params: List[Any] = []
        for col, arg in (("engine", "engine"), ("dbt_model", "model")):
            val = request.args.get(arg)
            if val:
                clauses.append(f"{col} = ?")
                params.append(val)
        batch = request.args.get("batch")
        if batch not in (None, ""):
            clauses.append("batch = ?")
            params.append(int(batch))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        df = con.execute(
            "SELECT * FROM metrics_by_model" + where
            + " ORDER BY engine, batch, dbt_model",
            params,
        ).df()
    finally:
        con.close()
    return jsonify(_df_payload(df))


def _diff() -> Any:
    sf = request.args.get("sf")
    if not sf:
        return jsonify({"error": "sf query parameter is required"}), 400
    processed = _processed_dir(sf)
    if not os.path.exists(os.path.join(processed, "metrics_by_model.parquet")):
        return jsonify({"error": f"no processed metrics for sf={sf}"}), 404

    con, _missing = _open_con(sf)
    try:
        kpi_cols = [
            r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='metrics_by_model' "
                "AND column_name NOT IN ('engine','sf','run_id','batch','dbt_model')"
            ).fetchall()
        ]
        clauses: List[str] = []
        params: List[Any] = []
        model = request.args.get("model")
        if model:
            clauses.append("s.dbt_model = ?")
            params.append(model)
        batch = request.args.get("batch")
        if batch not in (None, ""):
            clauses.append("s.batch = ?")
            params.append(int(batch))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

        # spark (baseline) vs spark-openivm, joined per (batch, dbt_model).
        selects = ["s.batch AS batch", "s.dbt_model AS dbt_model"]
        for c in kpi_cols:
            selects.append(f"s.{c} AS spark_{c}")
            selects.append(f"o.{c} AS openivm_{c}")
            selects.append(f"(o.{c} - s.{c}) AS delta_{c}")
            selects.append(
                f"CASE WHEN s.{c} IS NULL OR s.{c} = 0 THEN NULL "
                f"ELSE o.{c} / s.{c} END AS ratio_{c}"
            )
        sql = (
            "SELECT " + ", ".join(selects)
            + " FROM (SELECT * FROM metrics_by_model WHERE engine='spark') s "
            + "FULL OUTER JOIN (SELECT * FROM metrics_by_model "
            + "WHERE engine='spark-openivm') o "
            + "ON s.batch = o.batch AND s.dbt_model = o.dbt_model"
            + where + " ORDER BY batch, dbt_model"
        )
        df = con.execute(sql, params).df()
    finally:
        con.close()
    return jsonify(_df_payload(df))


def _query() -> Any:
    body = request.get_json(silent=True) or {}
    sql = (body.get("sql") or "").strip().rstrip(";").strip()
    sf = body.get("sf")
    if not sql:
        return jsonify({"error": "body must include a non-empty 'sql'"}), 400
    if not sf:
        return jsonify({"error": "body must include 'sf'"}), 400
    if ";" in sql:
        return jsonify({"error": "only a single statement is allowed"}), 400
    if not re.match(r"(?is)^\s*(select|with)\b", sql):
        return jsonify({"error": "only read-only SELECT/WITH queries are allowed"}), 400
    if _FORBIDDEN_RE.search(sql):
        return jsonify({"error": "query contains a forbidden (DDL/DML) keyword"}), 400

    processed = _processed_dir(sf)
    if not any(
        os.path.exists(os.path.join(processed, f"{t}.parquet")) for t in _TABLES
    ):
        return jsonify({"error": f"no processed metrics for sf={sf}"}), 404

    try:
        cap = int(request.args.get("limit", body.get("limit", _ROW_CAP_DEFAULT)))
    except (TypeError, ValueError):
        cap = _ROW_CAP_DEFAULT
    cap = max(1, min(cap, _ROW_CAP_MAX))

    con, _missing = _open_con(sf)
    try:
        df = con.execute(f"SELECT * FROM ({sql}) AS _q LIMIT {cap}").df()
    except Exception as e:  # surface the DuckDB error to the caller
        con.close()
        return jsonify({"error": f"query failed: {e}"}), 400
    con.close()
    payload = _df_payload(df)
    payload["row_cap"] = cap
    payload["available_tables"] = list(_TABLES)
    return jsonify(payload)


def _artifact() -> Any:
    sf = request.args.get("sf")
    if not sf:
        return jsonify({"error": "sf query parameter is required"}), 400
    root = _metrics_root(sf)
    run_id = request.args.get("run_id")
    if run_id:
        zip_path = os.path.join(root, f"spark-metrics-{run_id}.zip")
    else:
        zip_path = os.path.join(root, "spark-metrics-latest.zip")
    if not os.path.exists(zip_path):
        return jsonify({"error": f"artifact not found: {os.path.basename(zip_path)}"}), 404
    return send_file(
        zip_path, mimetype="application/zip",
        as_attachment=True, download_name=os.path.basename(zip_path),
    )


@bp.route("/metrics/kpis", methods=["GET"])
def metrics_kpis() -> Any:
    return _kpis()


@bp.route("/metrics/diff", methods=["GET"])
def metrics_diff() -> Any:
    return _diff()


@bp.route("/metrics/query", methods=["POST"])
def metrics_query() -> Any:
    return _query()


@bp.route("/metrics/artifact", methods=["GET"])
def metrics_artifact() -> Any:
    return _artifact()


class MetricsHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
