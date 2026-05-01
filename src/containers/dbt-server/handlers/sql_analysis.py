"""SQL analysis handler — exposes sqlglot-based query analysis via REST API."""

from flask import Blueprint, Flask, jsonify

from handlers.base import BaseHandler
from services.dbt_compiler import get_compiled_models
from services.sql_analyzer import analyze_all_models, analyze_sql

bp = Blueprint("sql_analysis", __name__)


@bp.route("/sql/<engine>")
def get_all_sql_analysis(engine):
    """Return SQL analysis for ALL compiled models in an engine."""
    models = get_compiled_models(engine)
    if not models:
        return jsonify({
            "error": f"No compiled models found for engine '{engine}'. "
                     "Ensure the engine has a valid dbt project."
        }), 404

    analyses = analyze_all_models(engine, models)
    return jsonify({
        "engine": engine,
        "model_count": len(analyses),
        "models": [a.to_dict() for a in analyses],
    })


@bp.route("/sql/<engine>/<model_name>")
def get_model_sql_analysis(engine, model_name):
    """Return SQL analysis for a single named model. Returns 404 if not found."""
    models = get_compiled_models(engine)
    if not models:
        return jsonify({
            "error": f"No compiled models found for engine '{engine}'."
        }), 404

    # Find model by name (short name match)
    target_model = None
    for uid, model_data in models.items():
        if model_data["name"] == model_name:
            target_model = model_data
            break

    if target_model is None:
        available = sorted(m["name"] for m in models.values())
        return jsonify({
            "error": f"Model '{model_name}' not found for engine '{engine}'.",
            "available_models": available,
        }), 404

    analysis = analyze_sql(
        model_name=target_model["name"],
        unique_id=target_model["unique_id"],
        sql_text=target_model["compiled_sql"],
        engine=engine,
    )
    return jsonify(analysis.to_dict())


class SQLAnalysisHandler(BaseHandler):
    def register(self, app: Flask) -> None:
        app.register_blueprint(bp)
