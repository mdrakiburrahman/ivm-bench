import json
import re
from pathlib import Path, PurePosixPath

import sqlglot


PACKAGE_DIR = Path(__file__).resolve().parent
APP_ROOT = PACKAGE_DIR.parent
DBT_PROJECT_ROOT = APP_ROOT
if not (DBT_PROJECT_ROOT / "dbt-projects").exists() and (APP_ROOT / "dbt-server" / "dbt-projects").exists():
    DBT_PROJECT_ROOT = APP_ROOT / "dbt-server"
_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_SQL_KEYWORDS = re.compile(r"\b(SELECT|WITH|INSERT\s+INTO|CREATE\s+TABLE|DELETE\s+FROM|UPDATE\s+\w+\s+SET)\b", re.IGNORECASE)


def _relative_sql_path(name):
    relative = PurePosixPath(name)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError(f"Invalid catalog path: {name!r}")
    return relative


def _catalog_path(category, name):
    if category not in {"models", "macros", "inline"}:
        raise ValueError(f"Unsupported SQL category: {category!r}")
    relative = _relative_sql_path(name)
    return PACKAGE_DIR / "sql" / "spark" / category / relative.with_suffix(".sql")


def _load_json(name):
    with (PACKAGE_DIR / name).open(encoding="utf-8") as file:
        return json.load(file)


def _apply_override(engine, category, name):
    overrides = _load_json("overrides.json")
    key = f"{category}/{_relative_sql_path(name).with_suffix('.sql').as_posix()}"
    override = overrides.get(engine, {}).get(key)
    if override is None:
        return None
    if not override.get("reason") or not override.get("sql"):
        raise ValueError(f"Override {engine}:{key} requires both sql and reason")
    return override["sql"]


def _mask_jinja(sql):
    tokens = []

    def replace(match):
        token = f"SQLCATALOGTOKEN{len(tokens)}"
        tokens.append((token, match.group(0)))
        return token

    masked = _JINJA_EXPRESSION.sub(replace, sql)
    if "{%" in masked or "%}" in masked:
        raise ValueError("Jinja control blocks require an engine-specific SQL override")
    return masked, tokens


def _restore_jinja(sql, tokens):
    for token, original in tokens:
        sql = re.sub(rf"\b{token}\b", lambda _: original, sql, flags=re.IGNORECASE)
    return sql


def render_sql(engine, category, name):
    """Load catalog SQL and render it for one configured engine dialect."""
    override = _apply_override(engine, category, name)
    if override is not None:
        return override

    source_path = _catalog_path(category, name)
    source = source_path.read_text(encoding="utf-8")
    if category == "macros":
        return source

    dialect = _load_json("engines.json").get(engine)
    if dialect is None:
        raise ValueError(f"Unknown SQL catalog engine: {engine!r}")
    masked, tokens = _mask_jinja(source)
    rendered = sqlglot.transpile(masked, read="spark", write=dialect)[0]
    return _restore_jinja(rendered, tokens) + ("\n" if source.endswith("\n") else "")


def load_sql(engine, category, name):
    """Public loader used by both container services and local tooling."""
    return render_sql(engine, category, name)


def generate_model(engine, model, output_dir=None):
    """Render one catalog model to the existing dbt project layout."""
    relative = _relative_sql_path(model).with_suffix(".sql")
    destination_root = Path(output_dir) if output_dir else DBT_PROJECT_ROOT / "dbt-projects" / engine / "models"
    destination = destination_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_sql(engine, "models", model), encoding="utf-8")
    return destination


def is_sql_candidate(line):
    return bool(_SQL_KEYWORDS.search(line))
