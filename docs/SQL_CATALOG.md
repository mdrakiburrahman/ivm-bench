# SQL catalog

The shared `sql_catalog` package is the source of SQL executed by the benchmark.
Shared SQL is stored in Spark dialect and SQLGlot renders it for each engine.
The dbt-server image generates ignored models and macros into
`dbt-projects/<engine>/{models,macros}/`; inline SQL is loaded by the Python
service that executes it. Never commit generated project SQL.

## Layout and overrides

- Shared SQL used by two or more engines belongs at
  `src/containers/sql_catalog/sql/spark/<category>/<path>.sql`, where category
  is `models`, `macros`, or `inline`.
- Engine-only SQL and dialect/semantic differences belong at
  `src/containers/sql_catalog/overrides/<engine>/<category>/<path>.sql`.
  The first line must be `-- catalog-override: <specific reason>`. The generator
  strips that line and emits the remaining SQL verbatim; overrides are never
  transpiled.
- An intentional absence uses a file at the same override path containing only
  `-- catalog-skip: <reason>`. Generation emits nothing, and `check` reports a
  stale dbt project file if one remains.

An override reason names the engine constraint and SQL construct; avoid generic
reasons such as "engine differs". `overrides` prints the current override
ledger. `engines.json` maps every configured engine ID to its SQLGlot dialect.
`project_targets.json` groups models and macros by the engines that need them;
it contains paths only, not SQL. Add a target there when introducing a dbt
model or macro. An engine's absence from a group means that project does not
use that entry; a reasoned skip describes an intentional unsupported case.

## Rendering and porting

`load_sql(engine, category, name, **params)` resolves an engine override first,
then the shared Spark source. Spark-dialect output is byte-for-byte identical
to the canonical file. Other dialects use SQLGlot with `pretty=True`; macros
remain verbatim because they contain dbt Jinja. Jinja `{{ ... }}` expressions
and Python SQL placeholders (`{name}`, `%s`, `%(name)s`, `:name`, `?`) are
protected during transpilation. `{name}` placeholders are substituted only
when matching keyword parameters are supplied. Inline results remove exactly
one trailing newline; model and macro output preserve it.
Transpilation preserves Spark's effective null ordering when a target dialect
has different defaults; for example, `ORDER BY x DESC` becomes
`ORDER BY x DESC NULLS LAST` for PostgreSQL.

Jinja `{% ... %}` control blocks are never transpiled. The same-dialect engine
uses the canonical bytes; each other dialect requires an override file. A
missing override raises an error naming its expected path. Multi-statement
files are transpiled statement by statement, and opaque SQLGlot command
statements are passed through without dialect rewriting.

Inline catalog paths are
`inline/<service>/<module>/<name>.sql`. Service is `dbt_server`,
`benchmark_server`, `compiler_bench`, or `scripts`; module is the service-root
Python module path with slashes changed to underscores and `.py` removed.
For example, SQL executed by `handlers/chart.py` belongs under
`inline/benchmark_server/handlers_chart/<name>.sql`.

Run these from the repository root in a Python environment with SQLGlot
installed (for example, in the dbt-server container):

```bash
PYTHONPATH=src/containers python -m sql_catalog generate --engine spark models/bronze
PYTHONPATH=src/containers python -m sql_catalog generate --all
PYTHONPATH=src/containers python -m sql_catalog check models/bronze
PYTHONPATH=src/containers python -m sql_catalog check --all
PYTHONPATH=src/containers python -m sql_catalog parity --all
PYTHONPATH=src/containers python -m sql_catalog overrides
```

`check` verifies generated-file byte parity, stale skips, missing catalog
sources for project SQL, override reasons, and unnecessary AST-equal overrides.
`check` and `parity` fail on SQLGlot unsupported/fallback warnings or empty
rendered output; use a reasoned override or skip for engine-specific SQL that
SQLGlot cannot faithfully transpile. DuckDB `read_parquet` calls with a path list
are preserved opaquely because SQLGlot does not model their list arguments.
`parity` parses each engine's final SQL with its configured dialect and compares
the normalized Spark AST with the canonical source. It normalizes one-argument
`TO_DATE(value)`, constant nonzero modulo divisors wrapped in `NULLIF`, and
implicit descending null ordering (Spark/PostgreSQL default to `NULLS FIRST`);
explicit `NULLS LAST` remains distinct. Byte-identical Spark
SQL is equal without parsing, including syntax unsupported by SQLGlot.
Jinja control-block models are excluded from AST comparison. `generate` writes
the declared project targets even from an empty checkout; inline SQL is loaded
at runtime. Generate before running local dbt or `check`; the Docker image and
CI do this automatically. If a target is removed, delete its local generated
file as well; `check` reports undeclared or stale output.

Exceptions are reasoned JSON entries under
`src/containers/sql_catalog/exceptions/*.json`. Test paths (`*/tests/**`) are
exempt by rule. A single inline SQL statement may instead be marked on its
Python line or the line immediately above with
`# sql-catalog: exempt <specific reason>`; the reason is mandatory.

On the minimal devbox host, run parity, the focused tests, and the full SQL
catalog guard in the dbt-server container. Use `--base-ref main` instead of
`--all` to check only changed files.

```bash
docker compose -f docker/docker-compose.benchmark.spark.yml run --rm --no-deps \
  -v "$PWD:/workspace" -w /workspace \
  -e PYTHONPATH=/workspace/src/containers \
  --entrypoint python dbt-server -m sql_catalog parity --all
docker compose -f docker/docker-compose.benchmark.spark.yml run --rm --no-deps \
  -v "$PWD:/workspace" -w /workspace \
  -e PYTHONPATH=/workspace/src/containers \
  --entrypoint python dbt-server -m unittest discover \
  -s src/containers/dbt-server/tests -p test_sql_catalog.py -v
docker compose -f docker/docker-compose.benchmark.spark.yml run --rm --no-deps \
  -v "$PWD:/workspace" -w /workspace \
  -e PYTHONPATH=/workspace/src/containers \
  -e GIT_CONFIG_COUNT=1 -e GIT_CONFIG_KEY_0=safe.directory \
  -e GIT_CONFIG_VALUE_0=/workspace \
  --entrypoint python dbt-server tools/scripts/check-sql-catalog.py --all
```
