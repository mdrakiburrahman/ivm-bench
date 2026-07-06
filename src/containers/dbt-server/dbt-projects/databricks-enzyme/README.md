# databricks-enzyme dbt project

Materialises the TPC-DI dbt project as **Databricks Materialized Views**
with `REFRESH POLICY INCREMENTAL STRICT` so we can benchmark Enzyme
(Databricks' incremental view maintenance engine) against the other
engines in this harness.

## Per-experiment isolation (shared-nothing)

Every OAT experiment gets its own **microsecond-timestamped namespace**
so multiple experiments can hit the same Databricks workspace
concurrently without contention and a crashed experiment cannot leave
background MVs accruing refresh bills.

Layout per experiment (5 schemas, one fixed cache):

```
ivmbenchdbrx.exp_<ts>_data    — CTAS source tables (managed Delta + row tracking + CDF)
ivmbenchdbrx.exp_<ts>_bronze  — dbt bronze MVs
ivmbenchdbrx.exp_<ts>_silver  — dbt silver MVs
ivmbenchdbrx.exp_<ts>_gold    — dbt gold MVs
ivmbenchdbrx.exp_<ts>_work    — dbt ephemeral models (effectively unused)

ivmbenchdbrx._shared_cache.tpcdi_raw_cache  — Volume holding raw Delta
                                              files per SF, seeded once
                                              per SF and reused by every
                                              experiment thereafter.
```

`<ts>` = `int(time.time() * 1_000_000)` minted by the orchestrator in
`benchmark-server/services/orchestrator.py:_apply_experiment` and
propagated via the `DATABRICKS_EXPERIMENT_ID` env var.

### Lifecycle

1. **Experiment start (batch 1)** — `engine_runner._run_databricks_enzyme`:
   1. `POST /sources/databricks-enzyme/sweep-stale` — drops every
      `exp_<ts>_*` schema with a timestamp > 1 day ago, recovering
      from crashed prior experiments.
   2. `POST /sources/databricks-enzyme/cleanup-schema` — idempotent
      drop of this experiment's own 5 schemas (no-op on a fresh ID).
   3. `POST /sources/databricks-enzyme/init/<sf>` — seeds the shared
      cache if cold for this SF, then CTAS-copies every source table
      from the cache into `exp_<ts>_data.<table>` server-side.
   4. dbt build `--full-refresh` — custom MV materialization issues
      `CREATE MATERIALIZED VIEW exp_<ts>_<layer>.<model> REFRESH POLICY
      INCREMENTAL STRICT AS …`.

2. **Batches 2 / 3** — `INSERT INTO` from cached per-batch staging
   slice → dbt build (no `--full-refresh`) → `REFRESH MATERIALIZED VIEW
   exp_<ts>_<layer>.<model>` per model.

3. **Experiment end** — `engine_runner._databricks_enzyme_drop_mvs`
   calls `cleanup-schema`, which `DROP SCHEMA … CASCADE`s all 5
   per-experiment schemas. MVs + their backing pipelines + Delta
   tables all disappear; refresh bills stop.

### Schema-name macro

`macros/generate_schema_name.sql` reads `DATABRICKS_EXPERIMENT_ID` via
`env_var()` and prefixes `exp_<ts>_` onto whatever `+schema:` value
the layer config passes (`bronze` → `exp_<ts>_bronze`, etc.). It
raises a compile error if `DATABRICKS_EXPERIMENT_ID` is missing or
the placeholder `unset`, so a dbt-run without the orchestrator
present fails loud and fast.

## Refresh-policy knob

`dbt_project.yml`'s top-level `+refresh_policy: 'INCREMENTAL STRICT'`
flows into the custom `materialized_view` materialization. Override
per-layer or per-model via standard dbt config:

```yaml
models:
  tpcdi:
    +refresh_policy: 'INCREMENTAL STRICT'
    bronze:
      +refresh_policy: 'AUTO'   # if you want bronze to fall back
```

Valid values: `INCREMENTAL STRICT`, `INCREMENTAL`, `AUTO`, `FULL`.
See [Databricks docs](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-materialized-view-refresh-policy).

## Required Service Principal grants

The SP that the benchmark uses (`DATABRICKS_SPN_CLIENT_ID`) needs the
following on `<catalog>` (default `ivmbenchdbrx`):

| Grant                         | Purpose                                      |
|-------------------------------|----------------------------------------------|
| `USE CATALOG`                 | Access the catalog                           |
| `CREATE SCHEMA`               | Per-experiment + cache schemas               |
| `CREATE VOLUME`               | Cache volume + per-experiment volumes        |
| `CREATE TABLE`                | CTAS source tables                           |
| `CREATE MATERIALIZED VIEW`    | dbt build emits CREATE MV                    |
| `MODIFY` / `WRITE VOLUME`     | INSERT INTO + Volume file upload             |
| `SELECT` on `system.information_schema.schemata` | (optional — sweeper uses `SHOW SCHEMAS LIKE` instead, no grant needed) |

End-of-experiment teardown needs implicit DROP on its own-owned
schemas. The sweeper drops any `exp_<ts>_*` schema older than 1 day
regardless of owner — if you grant `OWN` on the catalog to the SP,
that's sufficient.
