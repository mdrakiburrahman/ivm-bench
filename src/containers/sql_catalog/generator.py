import argparse
import fnmatch
import json
import re
import sys
from pathlib import Path, PurePosixPath

import sqlglot
from sqlglot import exp


PACKAGE_DIR = Path(__file__).resolve().parent
APP_ROOT = PACKAGE_DIR.parent
DBT_PROJECT_ROOT = APP_ROOT
if not (DBT_PROJECT_ROOT / "dbt-projects").exists() and (
    APP_ROOT / "dbt-server" / "dbt-projects"
).exists():
    DBT_PROJECT_ROOT = APP_ROOT / "dbt-server"
DBT_PROJECTS_DIR = DBT_PROJECT_ROOT / "dbt-projects"
_CATEGORIES = ("models", "macros", "inline")
_JINJA_EXPRESSION = re.compile(r"\{\{.*?\}\}", re.DOTALL)
_PLACEHOLDER = re.compile(
    r"(?<!\{)\{[A-Za-z_][A-Za-z0-9_]*\}(?!\})|%\([A-Za-z_][A-Za-z0-9_]*\)s|%s|(?<!:):[A-Za-z_][A-Za-z0-9_]*|\?"
)
_PARAMETER = re.compile(r"(?<!\{)\{([A-Za-z_][A-Za-z0-9_]*)\}(?!\})")
_SQL_KEYWORDS = re.compile(
    r"\b(?:"
    r"SELECT|WITH|INSERT\s+INTO|"
    r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TABLE|VIEW|MATERIALIZED\s+VIEW|SCHEMA|DATABASE)|"
    r"DELETE\s+FROM|UPDATE\s+\w+\s+SET|"
    r"DROP\s+(?:TABLE|VIEW|SCHEMA|DATABASE|FUNCTION)|"
    r"ALTER\s+(?:TABLE|VIEW|SCHEMA|DATABASE)|MERGE\s+INTO|"
    r"REFRESH\s+(?:MATERIALIZED\s+VIEW|TABLE)|DESCRIBE(?:\s+HISTORY)?|"
    r"SHOW\s+(?:TABLES?|VIEWS?|COLUMNS?|DATABASES?|SCHEMAS?|CREATE)|"
    r"TRUNCATE\s+(?:TABLE\s+)?|CALL\s+\w+|EXPLAIN\s+(?:SELECT|WITH)|"
    r"PRAGMA\s+\w+|COPY\s+\w+"
    r")\b",
    re.IGNORECASE,
)
_SQL_STRING_START = r"(?:[rubf]{1,2})?(?:'{3}|\"{3}|'|\")"
_NULL_ORDERING = re.compile(r"\bNULLS\s+(?:FIRST|LAST)\b", re.IGNORECASE)
_SQL_STATEMENT_START = re.compile(
    r"^\s*(?!WITH\b)" + _SQL_KEYWORDS.pattern,
    re.IGNORECASE,
)
_SQL_WITH_START = re.compile(
    r"^\s*WITH\s+(?:RECURSIVE\s+)?"
    r"(?:[\w$]+|\"[^\"]+\"|`[^`]+`|\[[^\]]+\])"
    r"(?:\s*\([^)]*\))?\s+AS\s*\(",
    re.IGNORECASE,
)
_SQL_CALL_START = re.compile(
    r"\b(?:execute|executemany|executescript|execute_sql|execute_query|query|sql)"
    r"\s*\(\s*(?:\(\s*)*"
    + _SQL_STRING_START
    + r"\s*"
    + _SQL_KEYWORDS.pattern,
    re.IGNORECASE,
)
_SQL_ASSIGNMENT_START = re.compile(
    r"^\s*[\w.]+\s*=\s*(?:\(\s*)*"
    + _SQL_STRING_START
    + r"\s*"
    + _SQL_KEYWORDS.pattern,
    re.IGNORECASE,
)
_SQL_RETURN_START = re.compile(
    r"^\s*return\s+(?:\(\s*)*"
    + _SQL_STRING_START
    + r"\s*"
    + _SQL_KEYWORDS.pattern,
    re.IGNORECASE,
)
_EXEMPT_MARKER = re.compile(r"(?:^|\s)#\s*sql-catalog:\s*exempt\s+(\S.*?)\s*$")
_EXEMPT_PREFIX = re.compile(r"(?:^|\s)#\s*sql-catalog:\s*exempt\b")


def _relative_sql_path(name):
    if not isinstance(name, str) or not name:
        raise ValueError(f"Invalid catalog path: {name!r}")
    relative = PurePosixPath(name.replace("\\", "/"))
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError(f"Invalid catalog path: {name!r}")
    if relative.suffix == ".sql":
        relative = relative.with_suffix("")
    return relative


def _sql_path(root, name):
    relative = _relative_sql_path(name)
    path = (root / f"{relative.as_posix()}.sql").resolve()
    if root.resolve() not in path.parents:
        raise ValueError(f"Catalog path resolves outside its root: {name!r}")
    return path


def _catalog_path(category, name):
    if category not in _CATEGORIES:
        raise ValueError(f"Unsupported SQL category: {category!r}")
    return _sql_path(PACKAGE_DIR / "sql" / "spark" / category, name)


def _override_path(engine, category, name):
    if category not in _CATEGORIES:
        raise ValueError(f"Unsupported SQL category: {category!r}")
    return _sql_path(PACKAGE_DIR / "overrides" / engine / category, name)


def _load_engines():
    with (PACKAGE_DIR / "engines.json").open(encoding="utf-8") as file:
        engines = json.load(file)
    if not isinstance(engines, dict) or not engines:
        raise ValueError("engines.json must contain a non-empty engine-to-dialect mapping")
    return engines


def _parse_override(path):
    if not path.is_file():
        return None
    content = path.read_text(encoding="utf-8")
    header, separator, body = content.partition("\n")
    header = header.rstrip("\r")
    if header.startswith("-- catalog-override:"):
        reason = header.removeprefix("-- catalog-override:").strip()
        if not reason:
            raise ValueError(f"{path}: catalog override requires a reason")
        if not separator or not body.strip():
            raise ValueError(f"{path}: catalog override requires SQL after its header")
        return {"reason": reason, "sql": body, "skip": False}
    if header.startswith("-- catalog-skip:"):
        reason = header.removeprefix("-- catalog-skip:").strip()
        if not reason:
            raise ValueError(f"{path}: catalog skip requires a reason")
        if separator and body.strip():
            raise ValueError(f"{path}: catalog skip may contain only its header")
        return {"reason": reason, "sql": None, "skip": True}
    raise ValueError(
        f"{path}: first line must be '-- catalog-override: <reason>' "
        "or '-- catalog-skip: <reason>'"
    )


def _entry_names(prefixes=()):
    entries = set()
    for category in _CATEGORIES:
        canonical_root = PACKAGE_DIR / "sql" / "spark" / category
        if canonical_root.is_dir():
            for path in canonical_root.rglob("*.sql"):
                entries.add(
                    (category, path.relative_to(canonical_root).with_suffix("").as_posix())
                )
        entries.update(
            (path_category, name)
            for _, path_category, name, _ in _override_names(category=category)
        )
    return sorted(
        (category, name)
        for category, name in entries
        if _matches_prefix(category, name, prefixes)
    )


def _override_names(engine=None, category=None):
    root = PACKAGE_DIR / "overrides"
    if not root.is_dir():
        return []
    names = []
    for path in root.rglob("*.sql"):
        relative = path.relative_to(root)
        if len(relative.parts) < 3:
            continue
        path_engine, path_category = relative.parts[:2]
        if engine is not None and path_engine != engine:
            continue
        if category is not None and path_category != category:
            continue
        names.append(
            (
                path_engine,
                path_category,
                relative.relative_to(Path(path_engine) / path_category)
                .with_suffix("")
                .as_posix(),
                path,
            )
        )
    return sorted(names)


def _matches_prefix(category, name, prefixes):
    if not prefixes:
        return True
    relative = _relative_sql_path(name).as_posix()
    full = f"{category}/{relative}"
    for prefix in prefixes:
        normalized = str(prefix).replace("\\", "/").strip("/")
        if normalized.endswith(".sql"):
            normalized = normalized[:-4]
        if full == normalized or full.startswith(f"{normalized}/"):
            return True
        if relative == normalized or relative.startswith(f"{normalized}/"):
            return True
    return False


def _mask_tokens(sql):
    tokens = []

    def replace(match):
        token = f"SQLCATALOGTOKEN{len(tokens)}"
        while token in sql:
            token += "X"
        tokens.append((token, match.group(0)))
        return token

    masked = _JINJA_EXPRESSION.sub(replace, sql)
    masked = _PLACEHOLDER.sub(replace, masked)
    return masked, tokens


def _mask_jinja(sql):
    tokens = []

    def replace(match):
        token = f"SQLCATALOGTOKEN{len(tokens)}"
        while token in sql:
            token += "X"
        tokens.append((token, match.group(0)))
        return token

    masked = _JINJA_EXPRESSION.sub(replace, sql)
    if "{%" in masked or "%}" in masked:
        raise ValueError("Jinja control blocks require an engine-specific SQL override")
    return masked, tokens


def _restore_tokens(sql, tokens):
    for token, original in tokens:
        sql = sql.replace(token, original)
    return sql


def _restore_jinja(sql, tokens):
    return _restore_tokens(sql, tokens)


def _substitute_params(sql, params):
    if not params:
        return sql
    return _PARAMETER.sub(
        lambda match: str(params[match.group(1)])
        if match.group(1) in params
        else match.group(0),
        sql,
    )


def _transpile_sql(source, dialect):
    masked, tokens = _mask_tokens(source)
    statements = sqlglot.parse(masked, read="spark")
    rendered = []
    for statement in statements:
        if statement is None:
            continue
        if isinstance(statement, exp.Command):
            rendered.append(statement.sql(dialect="spark", pretty=True))
            continue
        source_statement = statement.sql(dialect="spark")
        if not _NULL_ORDERING.search(masked):
            for ordered in statement.find_all(exp.Ordered):
                if ordered.args.get("desc"):
                    ordered.set("nulls_first", True)
            source_statement = statement.sql(dialect="spark")
        output = sqlglot.transpile(
            source_statement,
            read="spark",
            write=dialect,
            pretty=True,
        )
        if not output:
            raise ValueError(f"SQLGlot produced no output for a {dialect} statement")
        rendered.append(output[0])
    result = ";\n".join(rendered)
    if source.rstrip().endswith(";") and result:
        result += ";"
    if source.endswith(("\n", "\r")) and result:
        result += "\n"
    return _restore_tokens(result, tokens)


def _render_canonical(engine, category, name, source):
    dialect = _load_engines().get(engine)
    if dialect is None:
        raise ValueError(f"Unknown SQL catalog engine: {engine!r}")
    if category == "macros" or dialect == "spark":
        return source
    if "{%" in source or "%}" in source:
        path = _override_path(engine, category, name)
        raise ValueError(
            f"{category}/{_relative_sql_path(name)} has Jinja control blocks; "
            f"add an engine override at {path}"
        )
    return _transpile_sql(source, dialect)


def _resolve_sql(engine, category, name):
    override_path = _override_path(engine, category, name)
    override = _parse_override(override_path)
    if override is not None:
        if override["skip"]:
            return None, override
        return override["sql"], override
    canonical_path = _catalog_path(category, name)
    if canonical_path.is_file():
        return canonical_path.read_text(encoding="utf-8"), None
    raise FileNotFoundError(
        f"No canonical SQL or engine override for {engine}:{category}/{_relative_sql_path(name)}"
    )


def render_sql(engine, category, name, **params):
    """Load catalog SQL and render it for one configured engine dialect."""
    engines = _load_engines()
    if engine not in engines:
        raise ValueError(f"Unknown SQL catalog engine: {engine!r}")
    if category not in _CATEGORIES:
        raise ValueError(f"Unsupported SQL category: {category!r}")
    source, override = _resolve_sql(engine, category, name)
    if override is not None and override["skip"]:
        raise ValueError(
            f"SQL catalog entry {engine}:{category}/{_relative_sql_path(name)} is skipped: "
            f"{override['reason']}"
        )
    if override is not None:
        rendered = source
    else:
        rendered = _render_canonical(engine, category, name, source)
    rendered = _substitute_params(rendered, params)
    if category == "inline" and rendered.endswith("\n"):
        rendered = rendered[:-1]
    return rendered


def load_sql(engine, category, name, **params):
    """Load one catalog entry, rendering its dialect and optional Python parameters."""
    return render_sql(engine, category, name, **params)


def _destination(engine, category, name, output_dir=None):
    relative = _relative_sql_path(name)
    root = Path(output_dir) if output_dir is not None else DBT_PROJECTS_DIR / engine / category
    return root / f"{relative.as_posix()}.sql"


def generate_model(engine, model, output_dir=None):
    """Render one catalog model to the existing dbt project layout."""
    destination = _destination(engine, "models", model, output_dir)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_sql(engine, "models", model), encoding="utf-8")
    return destination


def generate_macro(engine, macro, output_dir=None):
    """Render one catalog macro to a project directory or caller-supplied path."""
    destination = _destination(engine, "macros", macro, output_dir)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(render_sql(engine, "macros", macro), encoding="utf-8")
    return destination


def _generate_catalog_files(engine, category, prefixes=(), output_dir=None):
    if category not in {"models", "macros"}:
        raise ValueError(f"Unsupported generated SQL category: {category!r}")
    generated = []
    for entry_category, name in _entry_names(prefixes):
        if entry_category != category:
            continue
        override = _parse_override(_override_path(engine, category, name))
        if override is not None and override["skip"]:
            continue
        canonical = _catalog_path(category, name).is_file()
        if not canonical and override is None:
            continue
        destination = _destination(engine, category, name, output_dir)
        if output_dir is None and not destination.is_file() and override is None:
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(render_sql(engine, category, name), encoding="utf-8")
        generated.append(destination)
    return generated


def generate_models(engine, output_dir=None, prefixes=()):
    """Render catalog models applicable to an engine's existing project."""
    return _generate_catalog_files(engine, "models", prefixes, output_dir)


def generate_macros(engine, output_dir=None, prefixes=()):
    """Render catalog macros applicable to an engine's existing project."""
    return _generate_catalog_files(engine, "macros", prefixes, output_dir)


def generate(engine=None, prefixes=(), all_engines=False):
    """Generate applicable catalog models and macros for one or all engines."""
    engines = _load_engines()
    selected = list(engines) if all_engines else [engine]
    if not all_engines and engine not in engines:
        raise ValueError("Pass a configured engine or set all_engines=True")
    generated = []
    for engine_name in selected:
        generated.extend(generate_models(engine_name, prefixes=prefixes))
        generated.extend(generate_macros(engine_name, prefixes=prefixes))
    return generated


def _catalog_entries(prefixes=()):
    return _entry_names(prefixes)


def _exceptions():
    entries = []
    root = PACKAGE_DIR / "exceptions"
    if not root.is_dir():
        return entries
    for path in sorted(root.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            raise ValueError(f"{path}: exceptions file must contain a JSON list")
        for item in data:
            if (
                not isinstance(item, dict)
                or not isinstance(item.get("path"), str)
                or not item["path"]
                or not isinstance(item.get("reason"), str)
                or not item["reason"].strip()
            ):
                raise ValueError(f"{path}: each exception requires a path and reason")
            entries.append(item)
    return entries


def _is_exempt_path(path):
    normalized = str(path).replace("\\", "/")
    if "/tests/" in f"/{normalized}/":
        return True
    return any(fnmatch.fnmatch(normalized, item["path"]) for item in _exceptions())


def _line_marker(lines, index):
    for candidate in (index, index - 1):
        if candidate < 0 or candidate >= len(lines):
            continue
        line = lines[candidate].rstrip("\r\n")
        marker = _EXEMPT_MARKER.search(line)
        if marker:
            return marker.group(1).strip()
        if _EXEMPT_PREFIX.search(line):
            return ""
    return None


def _all_project_sql():
    engine_dirs = sorted(DBT_PROJECTS_DIR.iterdir()) if DBT_PROJECTS_DIR.is_dir() else ()
    for engine_dir in engine_dirs:
        if not engine_dir.is_dir():
            continue
        for category in ("models", "macros"):
            root = engine_dir / category
            if root.is_dir():
                for path in sorted(root.rglob("*.sql")):
                    yield (
                        engine_dir.name,
                        category,
                        path.relative_to(root).with_suffix("").as_posix(),
                        path,
                    )


def _normalized_sql(sql, dialect):
    if "{%" in sql or "%}" in sql:
        return None
    masked, tokens = _mask_tokens(sql)
    statements = sqlglot.parse(masked, read=dialect)
    normalized = []
    for statement in statements:
        if statement is None:
            continue
        if isinstance(statement, exp.Command):
            text = re.sub(r"\s+", " ", statement.sql(dialect=dialect)).strip()
        else:
            if dialect in {"spark", "postgres"} and not _NULL_ORDERING.search(masked):
                for ordered in statement.find_all(exp.Ordered):
                    if ordered.args.get("desc"):
                        ordered.set("nulls_first", True)
            for node in list(statement.find_all(exp.TsOrDsToDate)):
                node.replace(
                    exp.Cast(this=node.this.copy(), to=exp.DataType.build("DATE"))
                )
            text = statement.sql(dialect="spark")
        normalized.append(_restore_tokens(text, tokens))
    return ";\n".join(normalized)


def _override_is_unnecessary(engine, category, name, override):
    if override is None or override["skip"]:
        return False
    canonical_path = _catalog_path(category, name)
    if not canonical_path.is_file():
        return False
    canonical = canonical_path.read_text(encoding="utf-8")
    if category == "macros":
        return override["sql"] == canonical
    if "{%" in canonical or "%}" in canonical:
        return False
    dialect = _load_engines()[engine]
    try:
        expected = _render_canonical(engine, category, name, canonical)
        return _normalized_sql(expected, dialect) == _normalized_sql(override["sql"], dialect)
    except (ValueError, sqlglot.ParseError):
        return False


def check_catalog(prefixes=(), scan_project=True):
    """Return all catalog drift, stale skips, unnecessary overrides, and loose project SQL."""
    errors = []
    engines = _load_engines()
    entries = set(_catalog_entries(prefixes))
    try:
        _exceptions()
    except (OSError, ValueError, json.JSONDecodeError) as error:
        return [f"Invalid exceptions: {error}"]

    for engine, category, name, path in _override_names():
        if engine not in engines:
            errors.append(f"{path}: unknown engine {engine!r}")
            continue
        if category not in _CATEGORIES:
            errors.append(f"{path}: unsupported category {category!r}")
            continue
        if not _matches_prefix(category, name, prefixes):
            continue
        try:
            override = _parse_override(path)
        except (OSError, ValueError) as error:
            errors.append(str(error))
            continue
        entries.add((category, name))
        if override is not None and _override_is_unnecessary(engine, category, name, override):
            errors.append(f"{path}: unnecessary override; output is AST-equal to canonical SQL")

    for category, name in sorted(entries):
        canonical = _catalog_path(category, name)
        canonical_exists = canonical.is_file()
        for engine in engines:
            override_path = _override_path(engine, category, name)
            try:
                override = _parse_override(override_path)
            except (OSError, ValueError) as error:
                errors.append(str(error))
                continue
            if not canonical_exists and override is None:
                continue
            destination = _destination(engine, category, name)
            if override is not None and override["skip"]:
                if destination.is_file():
                    errors.append(f"{destination}: stale generated file for catalog skip")
                continue
            if category == "inline":
                try:
                    render_sql(engine, category, name)
                except (OSError, ValueError, FileNotFoundError) as error:
                    errors.append(f"{engine}:{category}/{name}: {error}")
                continue
            if override is not None and not destination.is_file():
                errors.append(f"{destination}: missing generated file for catalog override")
                continue
            if not destination.is_file():
                continue
            try:
                expected = render_sql(engine, category, name)
            except (OSError, ValueError, FileNotFoundError) as error:
                errors.append(f"{destination}: cannot render catalog SQL: {error}")
                continue
            actual = destination.read_text(encoding="utf-8")
            if actual != expected:
                errors.append(f"{destination}: differs from generated catalog output")

    if scan_project:
        for engine, category, name, path in _all_project_sql():
            if not _matches_prefix(category, name, prefixes):
                continue
            if engine not in engines:
                errors.append(f"{path}: project SQL uses unconfigured engine {engine!r}")
                continue
            if _is_exempt_path(path.relative_to(DBT_PROJECT_ROOT.parent).as_posix()):
                continue
            canonical_exists = _catalog_path(category, name).is_file()
            engine_override_exists = _override_path(engine, category, name).is_file()
            if not canonical_exists and not engine_override_exists:
                errors.append(f"{path}: project SQL has no canonical source or engine override")
    return sorted(set(errors))


def parity_report(prefixes=()):
    """Return parity rows and all engine/model AST differences."""
    engines = _load_engines()
    rows = []
    differences = []
    for category, name in _catalog_entries(prefixes):
        canonical_path = _catalog_path(category, name)
        canonical_exists = canonical_path.is_file()
        for engine, dialect in engines.items():
            override_path = _override_path(engine, category, name)
            override = _parse_override(override_path)
            destination = _destination(engine, category, name)
            if override is None and not canonical_exists:
                continue
            if override is None and category in {"models", "macros"} and not destination.is_file():
                continue
            if override is not None and override["skip"]:
                status = f"override({override['reason']})"
            elif override is not None:
                status = f"override({override['reason']})"
            elif category == "macros":
                canonical = canonical_path.read_text(encoding="utf-8")
                status = "equal" if canonical == render_sql(engine, category, name) else "DIFF"
            else:
                try:
                    final_sql = render_sql(engine, category, name)
                    canonical_sql = render_sql("spark", category, name)
                    if dialect == "spark" and final_sql == canonical_sql:
                        status = "equal"
                    else:
                        final_ast = _normalized_sql(final_sql, dialect)
                        canonical_ast = _normalized_sql(canonical_sql, "spark")
                        if final_ast is None or canonical_ast is None:
                            status = "equal" if final_sql == canonical_sql else "DIFF"
                        else:
                            status = "equal" if final_ast == canonical_ast else "DIFF"
                except (OSError, ValueError, sqlglot.ParseError) as error:
                    status = "DIFF"
                    differences.append(f"{engine}:{category}/{name}: {error}")
            rows.append((engine, category, name, status))
            if status == "DIFF":
                differences.append(f"{engine}:{category}/{name}: AST differs from canonical SQL")
    return rows, sorted(set(differences))


def overrides_ledger():
    ledger = []
    for engine, category, name, path in _override_names():
        override = _parse_override(path)
        ledger.append((engine, category, name, override["reason"]))
    return ledger


def is_sql_candidate(line):
    return any(
        pattern.search(line)
        for pattern in (
            _SQL_STATEMENT_START,
            _SQL_WITH_START,
            _SQL_CALL_START,
            _SQL_ASSIGNMENT_START,
            _SQL_RETURN_START,
        )
    )


def _print_rows(rows):
    print("engine\tcategory\tpath\tstatus")
    for engine, category, name, status in rows:
        print(f"{engine}\t{category}\t{name}\t{status}")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Generate and validate the SQL catalog.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser("generate", help="generate dbt project SQL")
    generate_group = generate_parser.add_mutually_exclusive_group(required=True)
    generate_group.add_argument("--engine", choices=sorted(_load_engines()))
    generate_group.add_argument("--all", action="store_true", dest="all_engines")
    generate_parser.add_argument("prefixes", nargs="*")

    for command in ("check", "parity"):
        command_parser = subparsers.add_parser(command, help=f"validate {command} for catalog SQL")
        command_parser.add_argument("--all", action="store_true")
        command_parser.add_argument("prefixes", nargs="*")

    subparsers.add_parser("overrides", help="list the engine override ledger")
    args = parser.parse_args(argv)

    if args.command == "generate":
        generated = generate(args.engine, args.prefixes, args.all_engines)
        for path in generated:
            print(path)
        return 0
    if args.command == "check":
        errors = check_catalog(args.prefixes, scan_project=True)
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        print("SQL catalog check passed.")
        return 0
    if args.command == "parity":
        rows, differences = parity_report(args.prefixes)
        _print_rows(rows)
        if differences:
            print("\n".join(differences), file=sys.stderr)
            return 1
        return 0
    for engine, category, name, reason in overrides_ledger():
        print(f"{engine}\t{category}/{name}\t{reason}")
    return 0
