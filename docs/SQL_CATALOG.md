# SQL catalog

The shared `sql_catalog` package stores engine-neutral SQL under
`sql_catalog/sql/spark/` and renders it through SQLGlot. The same package is
copied into both the dbt-server and benchmark-server images. Generated dbt
models keep their existing `dbt-projects/<engine>/models/` paths so runtime
consumers do not need a path change.

## Porting recipe

- **Model:** add one canonical `.sql` file under `sql_catalog/sql/spark/models/<layer>/`.
  Convert the Spark version to the canonical form, then render with
  `sql_catalog.generate_model(engine, "<layer>/<name>")`. Preserve dbt Jinja
  expressions such as `ref()`; the renderer masks and restores them around
  SQLGlot parsing. Check AST parity for every configured engine with
  `python -m unittest discover -s src/containers/dbt-server/tests -p test_sql_catalog.py -v`.
- **Macro:** move the dbt macro to `sql_catalog/sql/spark/macros/` and load it with
  `load_sql(engine, "macros", "<name>")`. Macro Jinja is returned intact; SQL
  fragments requiring dialect syntax belong in a reasoned entry in
  `sql_catalog/overrides.json`.
- **Inline SQL:** move the statement to `sql_catalog/sql/spark/inline/<name>.sql`
  and call `load_sql(engine, "inline", "<name>")` at its execution site. Add
  only known semantic/dialect differences to `overrides.json`, each with both
  rendered SQL and a reason.

The path-based catalog has no shared mutable registry, so independent porters
can add separate model, macro, or inline files without merge conflicts. Add
unit-test fixture paths to `exceptions.json` only when a new fixture requires
it; production code is not excepted. Run the incremental guard with
`python tools/scripts/check-sql-catalog.py --base-ref main`.

The initial parity slice is `models/silver/date.sql`. Continue migration in
disjoint slices: bronze; silver; gold and work; compiler-bench inline SQL;
OpenIVM/Databricks/source inline SQL; benchmark-server inline SQL. Existing
engine SQL remains in place until its catalog entry has parity coverage.
