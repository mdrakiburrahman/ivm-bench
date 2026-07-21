"""Translate a Feldera-style dbt project (DBSP / view-only SQL with static Delta
`+connectors` config) into a DbspNet program spec.

This is the DbspNet integration's "compile-only bypass": rather than a custom dbt adapter,
we read the model SQL files + `dbt_project.yml` connector config directly, resolve dbt
`{{ ref() }}` / `{{ source() }}` and the one `dbt_utils.generate_surrogate_key` macro the
project uses, topologically sort by dependency, and emit the JSON the DbspNet control
service accepts at POST /compile and POST /deploy:

    {
      "program": ["CREATE TABLE ...", "CREATE VIEW ... AS ...", ...],   # dependency order
      "outputs": ["dim_account", ...],                                   # output view names
      "inputs": [{"table": "...", "uri": "...", "mode": "..."}, ...],    # Delta input bindings
      "output_bindings": [{"view": "...", "uri": "...", "mode": "truncate"}, ...],
    }

Timing is unaffected: this runs once at deploy, like Feldera's compile step (excluded from
the measured batch duration).
"""

import json
import os
import re
import sys

import yaml

SURROGATE_NULL = "_dbt_utils_surrogate_key_null_"

_REF = re.compile(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}")
_SOURCE = re.compile(r"\{\{\s*source\(\s*'[^']+'\s*,\s*'([^']+)'\s*\)\s*\}\}")
_SURROGATE = re.compile(r"\{\{\s*dbt_utils\.generate_surrogate_key\(\s*\[([^\]]*)\]\s*\)\s*\}\}")


def _surrogate_key(field_list_src):
    fields = [f.strip().strip("'\"") for f in field_list_src.split(",") if f.strip()]
    parts = [f"coalesce(cast({f} as varchar), '{SURROGATE_NULL}')" for f in fields]
    joiner = " || '-' || "
    return "md5(cast(" + joiner.join(parts) + " as varchar))"


def render(sql):
    """Resolve dbt refs/sources/macros to plain SQL identifiers/expressions."""
    sql = _REF.sub(lambda m: m.group(1), sql)
    sql = _SOURCE.sub(lambda m: m.group(1), sql)
    sql = _SURROGATE.sub(lambda m: _surrogate_key(m.group(1)), sql)
    return sql


def refs_of(raw_sql):
    return set(_REF.findall(raw_sql)) | set(_SOURCE.findall(raw_sql))


def _read_models(models_dir):
    """name -> (is_source, raw_sql, layer). Source models live under sources/."""
    models = {}
    for root, _dirs, files in os.walk(models_dir):
        rel = os.path.relpath(root, models_dir).replace("\\", "/")
        layer = rel.split("/")[0] if rel != "." else ""
        for f in files:
            if not f.endswith(".sql"):
                continue
            name = f[:-4]
            with open(os.path.join(root, f), "r", encoding="utf-8") as fh:
                models[name] = (layer == "sources", fh.read(), layer)
    return models


def _topo_sort(models):
    """Return model names in dependency order (a model after every model it references)."""
    ordered, visiting, done = [], set(), set()

    def visit(name):
        if name in done:
            return
        if name in visiting:
            raise ValueError(f"dependency cycle at model '{name}'")
        visiting.add(name)
        for dep in sorted(refs_of(models[name][1])):
            if dep in models:
                visit(dep)
        visiting.discard(name)
        done.add(name)
        ordered.append(name)

    for name in sorted(models):
        visit(name)
    return ordered


def _connectors(project_dir):
    """Read `+connectors` config from dbt_project.yml → (inputs, output_bindings).

    inputs: [{"table", "uri", "mode"}] (delta_table_input on source models).
    output_bindings: [{"view", "uri", "mode"}] (delta_table_output on gold views).
    """
    with open(os.path.join(project_dir, "dbt_project.yml"), encoding="utf-8") as f:
        proj = yaml.safe_load(f)

    inputs, outputs = [], []

    def process(model_name, connector_list):
        for c in connector_list or []:
            transport = c.get("transport", {})
            cfg = transport.get("config", {})
            uri, mode = cfg.get("uri"), cfg.get("mode")
            if transport.get("name") == "delta_table_input":
                inputs.append({"table": model_name, "uri": uri, "mode": mode})
            elif transport.get("name") == "delta_table_output":
                outputs.append({"view": model_name, "uri": uri, "mode": mode or "truncate"})

    def walk(node):
        if not isinstance(node, dict):
            return
        for key, val in node.items():
            if key.startswith("+"):
                continue  # a config key (+materialized, +stored, …), not a model
            if isinstance(val, dict) and "+connectors" in val:
                process(key, val["+connectors"])
            else:
                walk(val)

    walk(proj.get("models", {}).get("tpcdi", {}))
    return inputs, outputs


def _models_dir(project_dir):
    """Resolve the model source directory, honoring dbt's `model-paths` in
    dbt_project.yml (default `models`, dbt's own default). This lets a project
    share another's models — the dbspnet project points model-paths at the
    feldera project so both engines run the byte-identical model set from a
    single source instead of a maintained copy. dbt requires model-paths under
    the project root; this translator does not, so a `../feldera/models` is fine.
    """
    with open(os.path.join(project_dir, "dbt_project.yml"), encoding="utf-8") as f:
        proj = yaml.safe_load(f)
    paths = proj.get("model-paths") or ["models"]
    return os.path.normpath(os.path.join(project_dir, paths[0]))


def build_program(project_dir):
    models = _read_models(_models_dir(project_dir))
    order = _topo_sort(models)

    statements = []
    gold_views = []
    for name in order:
        is_source, raw, layer = models[name]
        body = render(raw).strip()
        if is_source:
            statements.append(f"CREATE TABLE {name} (\n{body}\n)")
        else:
            statements.append(f"CREATE VIEW {name} AS\n{body}")
            if layer == "gold":
                gold_views.append(name)

    inputs, output_bindings = _connectors(project_dir)
    return {
        "program": statements,
        # All gold views — passed to /compile to validate they all compile.
        "outputs": gold_views,
        # Delta bindings for /deploy.
        "inputs": inputs,
        "output_bindings": output_bindings,
    }


if __name__ == "__main__":
    project = sys.argv[1] if len(sys.argv) > 1 else "."
    print(json.dumps(build_program(project), indent=2))
