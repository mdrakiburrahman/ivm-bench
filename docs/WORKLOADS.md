# Benchmark workloads

[Back to README](../README.md)

---

### Accumulated daily updates

Set `batch_2_days` to a positive integer to measure one Batch 2 containing
that many distinct consecutive days from Databricks' augmented TPC-DI window,
which starts on 2016-07-06. Batch 3 contains the immediately following day;
`0` preserves the standard three-batch workload. The self-contained converter
uses the same seven daily datasets and initial-state boundary as Databricks,
without importing its repository as a submodule. Daily mode always uses every
row for each selected day; the per-batch row-percentage knobs apply only to the
standard three-batch workload.

The built-in augmented sweeps map 10, 20, 30, 40, and 50 percent of the
365-day window to 37, 73, 110, 146, and 183 days. The
`smoke-augmented-daily.json` file is the focused SF3 integration test. Set
`BENCHMARK_RUNS=2` for two repetitions per point.

### Repeated percentage refreshes

Set `repeated_refresh: true` in an experiment to initialize once at the
requested `scale_factor`, then retain source and engine state across refreshes.
`workload` selects `standard` or `databricks` (the extended TPC-DI workload).
`refresh_count` defaults to **20**, and `refresh_pct` defaults to **1**:

```json
{"experiments": [{
  "scale_factor": 3,
  "repeated_refresh": true,
  "workload": "standard",
  "refresh_count": 20,
  "refresh_pct": "1",
  "engines": ["duckdb"]
}]}
```

Each refresh inserts `ceil(current_source_rows * refresh_pct / 100)` rows.
The denominator counts the source tables actually loaded by the engines,
including static references; empty initial staging tables contribute zero.
The unused raw `time` table is excluded. Percentages apply to the aggregate
source row count, rather than independently to each table. The table mix follows
the generated event stream. Integer row counts compound after every refresh;
the ideal growth target is `(1 + refresh_pct / 100)^refresh_count`.

Generation runs before engine initialization. It measures the initial workload
and available reservoir, then increases PDGF's native incremental horizon until
the actual reservoir covers every compounded budget. The scale factor and
initial table counts stay fixed. Extended mode first consumes the existing
historical daily event window, then native future events if needed. Native customer/account CDC sequence IDs are mapped to the native day's clock
for the CRM models; the original sequence controls stream cuts. Extended fact
adapters likewise receive event timestamps in their CDC fields.
The generator's date-reference horizon limits native extension to 1200 days;
requests beyond that bound fail explicitly.

Cuts may fall inside a native day. Within a day, customer/account events precede
trades, and trades precede holdings; events within each source retain sequence
order. Each generated event is consumed at most once, with its original keys
and relationships. Static references receive no inserts. Batch 1 is the initial
load; batches 2 through `refresh_count + 1` are the refreshes. Legacy batch
percentage/day knobs are replaced by this mode's row budgets; mutation knobs
must remain zero. With `repeated_refresh` omitted, the existing three-batch
behavior is unchanged.

Repeated Databricks runs partition `fact_market_history` by `sk_date_id`.
Its SF10 output has 190 million rows and otherwise occupies one highly
compressed file; date partitions spread that materialization across smaller
files. The query, row multiplicities, and selected refresh policy are preserved.
The partition layout appears in the dbt manifest and materialized-view DDL.

If a Databricks refresh fails with an executor OOM and its latest failed
pipeline update explicitly announces `WILL_SCALE_UP`, the harness retries the
refresh on the same materialized views. Databricks uses larger workers for
the next ad-hoc update ([vertical autoscaling documentation](https://docs.databricks.com/aws/en/ldp/auto-scaling)).
The source batch is appended once; retry attempts and backoff remain inside
the reported refresh duration. Failed attempts and pipeline events are saved.
Recovery uses the existing retry limit (two retries by default), excludes
initialization, and does not retry SQL errors or OOM without scaling evidence.

The opt-in `databricks-openivm` engine uses OpenIVM's compiler and Spark SQL
rewriter with the same dbt model SQL as `databricks-enzyme`. A local compiler
container sees schemas only; the Databricks SQL warehouse optimizes and executes
the generated SQL. Model results and maintenance state are managed Delta tables
with CDF, exposed through views that hide OpenIVM bookkeeping columns. The
backend records OpenIVM's effective refresh classification, including full
recompute where OpenIVM Spark requires it.

Each model remembers the Delta versions it has consumed. Refreshes read exact
CDF intervals in dependency order, including updates and deletes from upstream
models. Interrupted multi-statement refreshes mark the checkpoint dirty and
fail instead of replaying partially applied maintenance. The benchmark retains
the initial state and appends each source batch once. Enzyme and OpenIVM use
separate experiment schemas, and cleanup drops their respective schemas.

OpenIVM schedules ready models in dependency order with up to eight workers,
matching Enzyme's default model concurrency. Each worker owns a separate SQL
session and reuses its CDF temporary views only for an identical source range.
Source schemas and Delta versions are cached within a batch; a model's version
is invalidated after its writes finish, before its children can start. Failed
maintenance stops new work and waits for active writes before reporting failure.
Both Databricks engines use the same local orchestration and source-loader
resource allocation. OpenIVM additionally runs a schema-only compiler helper
limited to two CPUs and 4 GB. Remote SQL warehouse and managed MV pipeline
compute allocations are independent; matching model concurrency does not make
them an equal-resource SQL execution comparison.
Batch timings include source synchronization and completed model writes.
Enzyme's record-only incrementalization EXPLAIN sweep runs after the initial
batch timer stops, against the existing MVs. Its separate client elapsed time
is retained as `databricks_explain_s`; it does not replace or drop live models.

`mount/results/<sf>/databricks-openivm/` contains the checkpoint, generated
`sql-batch<N>.sql`, statement IDs and timings, bag-equivalence validation using
`EXCEPT ALL` in both directions, and observed Photon/task time from query
history. Photon metrics are collected outside the benchmark timer; missing
metrics are reported as partial rather than proof of native execution.
The batch result also retains validation details and client timings for source
loading, metadata SQL, compiler requests, and each model. Fields ending in
`work_s` sum spans across concurrent workers; they are not elapsed batch time.
On a validation mismatch, the report includes exact differing-row samples from
two frozen recomputations, compared with the maintained result and each other.
These diagnostics run outside the timer and do not override the original failure.
Use `sf10-databricks-openivm-enzyme.json` for the SF10 standard and extended
TPC-DI comparison against Enzyme, with two compounded 1% refreshes and eight
model workers for each engine.
`sf100-databricks-openivm-enzyme.json` uses the same settings at SF100.
`fig1-sf100-databricks-openivm.json` runs only Databricks OpenIVM with the
Figure 1 standard TPC-DI batches (100%, 1%, 0.01%), eight model workers and
exact validation, retaining individual model timings for comparison.
SF3 matches the existing repeated-refresh smoke scale; SF1 produces an invalid
native FINWIRE configuration (`minUpdateSize=2`, below PDGF's minimum of 3).

`refresh-plan.json` records generator sizing and initial, before, inserted, and
resulting counts for every source table and round. The server logs these counts
and archives them in `source-row-counts.json`; `results.csv` includes every
refresh and its before/after source totals. In repeated mode, `source_rows`
counts the rows actually loaded, including the separately stored audit table;
`generated_source_rows` and `generated_batches` retain the physical file counts,
including unused initial tables. The initial resulting total therefore matches
the first refresh's before total. Legacy row-count reporting is unchanged.
The SF10 DuckDB OpenIVM preset, `sf10-repeated-refresh-duckdb-openivm.json`,
contains both workloads as separate experiments sharing one baseline.

#### Concurrency and resource settings

Model concurrency is distinct from execution threads within an engine:

| Engine | Default model concurrency | Execution resources |
| --- | ---: | --- |
| Spark / Spark OpenIVM | 30 | Local Spark uses `local[*]`; default shuffle partitions and parallelism are 16. |
| Fabric / Fabric OpenIVM | 30 | Remote Fabric Spark; local container limits describe the orchestration client. |
| Databricks Enzyme / Databricks OpenIVM | 8 | Managed Databricks compute; local container limits describe the orchestration client. |
| DuckDB / DuckDB OpenIVM | Serial model execution | SQL threads use the allocated CPU count; engine memory is capped at 80% of the container allocation. |
| Feldera | 1 | 12 execution workers; maximum RSS is 95% of the container allocation. |
| RisingWave | 4 | Four dbt model threads; materialized-view backfill parallelism is 8. The engine allocation reserves 5 CPUs and 5 GiB for MinIO and its tuner. Batch snapshots are copied from Iceberg into mutable RisingWave tables. |

RisingWave materializes `market_volatility_by_symbol` before building
`market_volatility`. This engine-only helper prevents the global rankings from
processing every intermediate daily-market backfill update. Both views remain
incrementally maintained, and their creation and storage are included in the
benchmark measurements.

The existing RisingWave compute CPU metric counts the engine container only;
per-container diagnostic samples include MinIO, the tuner and dbt-server.

`spark_tunables.dbt_threads` caps only Spark/Fabric model concurrency. The SF10
all-engine repeated preset and the existing augmented/sweep presets set it to
4; `full.json` sets it to 4 at SF100. These limits do not change Enzyme's default.
Use the separate optional `databricks_dbt_threads` experiment field to override
Databricks model concurrency, for example `"databricks_dbt_threads": 1` for diagnostics.
The overrides are recorded separately in `results.csv`; blank means the engine
default. The focused Enzyme verification preset inherits 8; its diagnostic
preset explicitly requests 1 and labels that restriction.

Serial local-engine runs receive the available host allocation; parallel runs
divide it among engines. Spark and Feldera reserve part of their allocation for
the dbt client. Cloud CPU totals are not inferred from local container limits.
Repeated-refresh experiments are opt-in: the GCI and CI presets use the
existing three-batch benchmark.

Storage totals describe durable bytes owned by the active engine experiment:
`visible_output` is user-facing materialized output, `helper_data` is hidden
intermediate state required by the engine (for example Feldera DBSP storage or
OpenIVM delta/auxiliary data), `metadata` is catalogs and transaction/query
logs, and `source` is the engine's current managed source state. Shared raw
generators and reusable cloud caches are excluded. A partial relation/listing
failure is reported as `partial` (or `error` when nothing could be measured),
never as a successful zero-byte result. The overhead ratio is
`helper_data_bytes / visible_output_bytes`.

Databricks storage collection requires
`ANALYZE TABLE ... COMPUTE STORAGE METRICS` (Databricks Runtime 18 or newer)
and must run as the materialized-view owner. Databricks registers both a public
materialized-view alias and its physical `__materialization_mat_*` backing
table; the collector measures the backing table once and excludes the alias.
The active backing snapshot, including inseparable Enzyme-internal columns, is
reported as `visible_output`, making it an upper bound on user-visible bytes.
Enzyme auxiliary state is not separately observable from this physical backing
snapshot. The `event_log_*` tables, transaction logs, and associated metadata
are reported as `metadata`; no positive lower bound on separate Enzyme state is
claimed. Vacuumable and time-travel data retain the owning relation's category.
The per-relation artifact preserves the full active, vacuumable, time-travel,
transaction-log, and total byte/file breakdown.

Each sample also reports a base-table footprint. Engines that directly read
the generated Delta tables use that current snapshot; engines that copy data
into managed tables use their measured `source_bytes`. `results.csv` compares
OpenIVM engines with the same-batch vanilla engine where one exists (Spark,
DuckDB, and Fabric), and otherwise retains the raw Delta reference. The
`base_table_storage_overhead_*` columns are a storage proxy: paired comparisons
control for engine and format, while raw-reference comparisons can also include
compression, encoding, and file-layout differences. The byte value is
`base_table_bytes - base_table_baseline_bytes`; the ratio divides that difference
by `base_table_baseline_bytes`.

For append-only runs, `batch_N_pct` (or the alias `batch_N_insert_pct`) is the
insert percentage; for mixed-DML batches, `batch_N_update_pct` and
`batch_N_delete_pct` are optional mutation percentages applied before the
insert (batch 1 is always insert-only, defaults are `0`).

At the end, `benchmark.sh` cats `mount/oat-state/latest/results.csv`. The file
contains one row per experiment, engine, and batch with raw numeric timing and
storage values (columns abridged here):

```text
oat_run_id,run_status,experiment_index,experiment_label,engine,batch_num,duration_s,helper_data_bytes,base_table_bytes,base_table_baseline_bytes,base_table_storage_overhead_bytes
abc123,completed,0,smoke-sf3,spark,1,120.5,0,1048576,1048576,0
abc123,completed,0,smoke-sf3,spark-openivm,1,18.2,40960,1089536,1048576,40960
```

Artifacts: `mount/oat-state/latest/{chart-oat.png, chart-per-model.png, results.csv, outputs.json}`

For engines executed in controlled local containers, every batch reports
CPU-seconds by subtracting Docker's cumulative CPU counters at the exact batch
boundaries. DuckDB measures its in-process `dbt-server`; Feldera measures its
pipeline manager; Spark measures both its runtime and metastore. Periodic CPU,
memory, and network samples remain diagnostic artifacts but are not interpolated
to produce the reported CPU value. Results are exposed in `results.csv`.

Managed Databricks and Fabric engines are intentionally excluded from this
metric. Their autoscalers do not expose fixed, comparable compute allocations,
and local Docker statistics only measure the orchestration client. Their batch
metric is explicitly marked `excluded`; use repeated end-to-end duration and
within-engine incremental speedup for those systems. Databricks pipeline-flow
durations remain diagnostics and do not replace the end-to-end timer.

Each experiment runs 3 batches per engine (full load `batch1` → append `batch2` →
append `batch3`). Set `BENCHMARK_RUNS` greater than 1 to repeat the full 3-batch
benchmark from clean engine state and report averaged per-engine batch timings.
