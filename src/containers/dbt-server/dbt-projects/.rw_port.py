#!/usr/bin/env python3
"""Generate the RisingWave dbt project from the DuckDB one.

The two dialects are close (both PostgreSQL-flavoured), so this is a small,
auditable set of mechanical rewrites rather than a hand-port. Every rewrite here
was verified against RisingWave 3.0.2 before being encoded — see the comment on
each for what fails without it.

Not handled here: the five gold/analytics models, which need structural edits
(CROSS JOIN against a one-row aggregate, and window functions with an empty
PARTITION BY). Those are hand-written after this script runs.
"""
import re
import shutil
import sys
from pathlib import Path
from typing import List

HERE = Path(__file__).resolve().parent
SRC = HERE / "duckdb"
DST = HERE / "risingwave"

# 1. RisingWave's interval parser rejects the MILLISECOND unit entirely:
#      "Bind error: Invalid unit: millisecond"
#    `INTERVAL '1 second' / 1000` binds and yields the identical value
#    (verified: 2026-01-01 00:00:00 -> 2025-12-31 23:59:59.999).
INTERVAL_MS = (
    re.compile(r"INTERVAL\s+1\s+MILLISECOND", re.I),
    "INTERVAL '1 second' / 1000",
)

# 2. strptime() is DuckDB-only. RisingWave spells it to_date/to_timestamp with a
#    PostgreSQL format string.
STRPTIME = (
    re.compile(r"strptime\(\s*(.+?)\s*,\s*'%Y%m%d'\s*\)\s*::\s*DATE", re.I | re.S),
    r"to_date(\1, 'YYYYMMDD')",
)

# 3. A window function with an empty PARTITION BY is refused outright:
#      "Window function with empty PARTITION BY is not supported because of
#       potential bad performance"
#    Partitioning by a constant is one partition, i.e. identical semantics to no
#    PARTITION BY, and it plans.
EMPTY_PARTITION = (
    re.compile(r"OVER\s*\(\s*ORDER BY", re.I),
    "OVER (PARTITION BY 1 ORDER BY",
)

# 4. `CROSS JOIN <one-row global aggregate>` needs a streaming nested-loop join,
#    which RisingWave does not implement:
#      "Not supported: streaming nested-loop join"
#    Giving both sides a constant column turns it into an equi-join, which plans
#    as a hash join. The key never reaches the output because every one of these
#    models projects its columns explicitly.
CROSS_JOIN_CONST = (
    re.compile(
        r"FROM\s+(\w+)\s+(\w+)\s*\n(\s*)CROSS JOIN\s+(\w+)\s+(\w+)",
        re.I,
    ),
    lambda m: (
        f"FROM (SELECT *, 1 AS join_key FROM {m.group(1)}) {m.group(2)}\n"
        f"{m.group(3)}JOIN (SELECT *, 1 AS join_key FROM {m.group(4)}) {m.group(5)}"
        f" ON {m.group(2)}.join_key = {m.group(5)}.join_key"
    ),
)

# 5. RisingWave has stddev_samp/stddev_pop but not the bare `stddev` alias:
#      "function stddev(...) does not exist"
#    DuckDB's and PostgreSQL's STDDEV *is* STDDEV_SAMP, so this is a rename.
STDDEV = (re.compile(r"\bSTDDEV\s*\(", re.I), "STDDEV_SAMP(")

# 6. DuckDB's CASE coerces a TINYINT to BOOLEAN; RisingWave will not even cast
#    smallint to boolean ('cannot cast type "smallint" to "boolean"'), so the
#    comparison has to be written against the integer values it actually holds.
IS_CASH = (
    re.compile(
        r"case\s+t_is_cash\s+when\s+true\s+then\s+'Cash'\s+when\s+false\s+then\s+'Margin'\s+end",
        re.I | re.S,
    ),
    "case when t_is_cash = 1 then 'Cash' when t_is_cash = 0 then 'Margin' end",
)

# 7. RisingWave's parser rejects every length/precision-parameterised type, in
#    model SQL exactly as in DDL: "Feature is not yet implemented: unsupported
#    data type: NUMERIC(38,6)". The TPC-DI analytics models carry explicit
#    `CAST(... AS DECIMAL(38, 6))`, so the strip has to run over model text too.
#    Note the whitespace allowances: the source writes `DECIMAL(38, 6)`.
PARAM_DECIMAL = (
    re.compile(r"\b(DECIMAL|NUMERIC)\s*\(\s*\d+\s*,\s*\d+\s*\)", re.I),
    "DECIMAL",
)
PARAM_VARCHAR = (
    re.compile(r"\b(VARCHAR|CHAR|CHARACTER VARYING)\s*\(\s*\d+\s*\)", re.I),
    "VARCHAR",
)

REWRITES = [INTERVAL_MS, STRPTIME, EMPTY_PARTITION, CROSS_JOIN_CONST, STDDEV,
            IS_CASH, PARAM_DECIMAL, PARAM_VARCHAR]


# --- call-level rewrites -----------------------------------------------------
# These take an argument list apart, so a regex is not enough: the arguments run
# across newlines and carry nested parentheses.

def _split_args(text: str, start: int):
    """Given text and the index of a '(', return (args, index_after_close)."""
    depth = 0
    args: List[str] = []
    current = ""
    i = start
    while i < len(text):
        ch = text[i]
        if ch == "(":
            depth += 1
            if depth == 1:
                i += 1
                continue
        elif ch == ")":
            depth -= 1
            if depth == 0:
                args.append(current)
                return args, i + 1
        elif ch == "," and depth == 1:
            args.append(current)
            current = ""
            i += 1
            continue
        current += ch
        i += 1
    raise ValueError("unbalanced parentheses")


def _rewrite_calls(text: str, name: str, transform) -> str:
    """Apply `transform(args) -> str` to every call of `name` in `text`."""
    pattern = re.compile(r"\b" + name + r"\s*\(", re.I)
    out = text
    searched_from = 0
    while True:
        match = pattern.search(out, searched_from)
        if not match:
            return out
        open_paren = out.index("(", match.start())
        try:
            args, end = _split_args(out, open_paren)
        except ValueError:
            return out
        replacement = transform([a.strip() for a in args])
        if replacement is None:
            searched_from = match.end()
            continue
        out = out[: match.start()] + replacement + out[end:]
        # Resume just past the rewritten call's own name, not past the whole
        # replacement: these calls nest (ROUND(SUM(CAST(ROUND(x, 6) ...)))) and
        # skipping the replacement would leave the inner one untouched.
        searched_from = match.start() + len(name) + 1


def rewrite_round(text: str) -> str:
    """ROUND(x, digits) needs a DECIMAL first argument.

    'function round_digit(double precision, integer) does not exist' — the
    two-argument form is DECIMAL-only, and every ROUND in the analytics models
    is applied to a DOUBLE. Casting an already-DECIMAL argument is a no-op, so
    this is applied unconditionally.
    """
    def transform(args):
        if len(args) != 2:
            return None
        value, digits = args
        # Skip only when the argument is ENTIRELY a cast -- wrapping that would
        # be redundant. It is not enough that it *starts* with one: the analytics
        # models contain `CAST(SUM(..) AS DOUBLE) / NULLIF(COUNT(..), 0)`, whose
        # value is DOUBLE, and leaving it unwrapped fails with
        # "function round_digit(double precision, integer) does not exist".
        if _is_entirely_cast(value):
            return None
        return f"ROUND(CAST({value} AS NUMERIC), {digits})"

    return _rewrite_calls(text, "ROUND", transform)


def _is_entirely_cast(value: str) -> bool:
    """True when `value` is one CAST(...) spanning the whole expression."""
    stripped = value.strip()
    if not stripped.upper().startswith("CAST("):
        return False
    try:
        _, end = _split_args(stripped, stripped.index("("))
    except ValueError:
        return False
    return end == len(stripped)


def rewrite_regexp_matches(text: str) -> str:
    """regexp_matches() is a boolean in DuckDB and an array in RisingWave.

    Used bare in a CASE WHEN it fails with 'argument of CASE WHEN must be
    boolean'. RisingWave has PostgreSQL's ~ operator, which is the boolean test.
    """
    def transform(args):
        if len(args) != 2:
            return None
        subject, pattern = args
        return f"({subject}) ~ ({pattern})"

    return _rewrite_calls(text, "regexp_matches", transform)


CALL_REWRITES = [rewrite_round, rewrite_regexp_matches]

#: Models whose port is a rewrite of the model itself rather than a regex over
#: the DuckDB text. Copied over the generated file after the rewrites run.
OVERRIDES = HERE / ".rw-overrides"


PROFILES_YML = """tpcdi:
  target: risingwave
  outputs:
    risingwave:
      type: risingwave
      host: "{{ env_var('RISINGWAVE_HOST', 'risingwave') }}"
      port: "{{ env_var('RISINGWAVE_PORT', '4566') | int }}"
      user: "{{ env_var('RISINGWAVE_USER', 'root') }}"
      pass: "{{ env_var('RISINGWAVE_PASSWORD', '') }}"
      dbname: "{{ env_var('RISINGWAVE_DATABASE', 'dev') }}"
      schema: tpcdi
      threads: "{{ env_var('RISINGWAVE_THREADS', '1') | int }}"
      connect_timeout: 60
"""

# The DuckDB project opens with two DuckLake PRAGMAs and materialises every
# layer as a table. RisingWave has neither, and materialising the layers as
# MATERIALIZED VIEWs is the whole point: that is the IVM under test.
MODELS_BLOCK = """models:
  tpcdi:
    # Every layer is a RisingWave MATERIALIZED VIEW: that IS the incremental
    # view maintenance under test. Appending a batch to the source tables
    # propagates through the whole DAG with no REFRESH call — unlike the
    # DuckDB/Spark projects, where these same layers are `table` and get fully
    # recomputed. `work` stays ephemeral so dbt inlines it as a CTE.
    bronze:
      +schema: bronze
      +materialized: materialized_view
    silver:
      +schema: silver
      +materialized: materialized_view
    gold:
      +schema: gold
      +materialized: materialized_view
    work:
      +schema: work
      +materialized: ephemeral
"""

ON_RUN_START = re.compile(r"^on-run-start:\n(?:  - .*\n)+\n?", re.M)
MODELS_SECTION = re.compile(r"^models:\n(?:[ \t].*\n|\n)*", re.M)


def write_project_config(dst: Path) -> None:
    (dst / "profiles.yml").write_text(PROFILES_YML, encoding="utf-8")
    project = dst / "dbt_project.yml"
    text = project.read_text(encoding="utf-8")
    text = ON_RUN_START.sub("", text)
    text = MODELS_SECTION.sub(MODELS_BLOCK, text)
    project.write_text(text, encoding="utf-8")


def main() -> int:
    if not SRC.is_dir():
        print(f"source project missing: {SRC}", file=sys.stderr)
        return 1
    if DST.exists():
        shutil.rmtree(DST)
    shutil.copytree(SRC, DST, ignore=shutil.ignore_patterns(
        "target", "dbt_packages", "logs", "*.duckdb", "*.db"))

    changed = []
    for path in sorted((DST / "models").rglob("*.sql")):
        rel = path.relative_to(DST / "models").as_posix()
        text = original = path.read_text(encoding="utf-8")
        for pattern, replacement in REWRITES:
            text = pattern.sub(replacement, text)
        for rewrite in CALL_REWRITES:
            text = rewrite(text)
        if text != original:
            path.write_text(text, encoding="utf-8")
            changed.append(rel)

    overridden = []
    if OVERRIDES.is_dir():
        for src in sorted(OVERRIDES.rglob("*.sql")):
            rel = src.relative_to(OVERRIDES)
            dst = DST / "models" / rel
            if not dst.exists():
                print(f"override targets a model that does not exist: {rel}", file=sys.stderr)
                return 1
            shutil.copyfile(src, dst)
            overridden.append(rel.as_posix())

    write_project_config(DST)

    print(f"copied {SRC.name} -> {DST.name}")
    print(f"rewrote {len(changed)} models:")
    for rel in changed:
        print(f"  {rel}")
    print(f"hand-written overrides applied ({len(overridden)}):")
    for rel in overridden:
        print(f"  {rel}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
