<!-- PROJECT LOGO -->
<p align="center">
  <img src="https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-incremental.png" alt="Logo" width="30%">
  <h3 align="center">dbt - Incremental View Maintenance Benchmark</h3>
  <p align="center">
     Benchmarking engine-specific IVM implementations using a TPC-DI dbt project, <em>without</em> using <a href="https://docs.getdbt.com/docs/build/incremental-models">dbt incremental</a> (because watermark columns are unreliable).
     <br />
    <br />
    <a href="https://docs.getdbt.com/">dbt Docs</a>
    ·
    <a href="https://materializedview.io/p/everything-to-know-incremental-view-maintenance">Everything You Need to Know About Incremental View Maintenance</a>
    ·
    <a href="https://rakirahman.blob.core.windows.net/public/books/INCREMENTAL_VIEW_MAINTENANCE.pdf">Short doc on IVM</a>
    ·
    <a href="https://bit.ly/dbsp-paper-full">DBSP Paper</a>
    ·
    <a href="https://bit.ly/https://bit.ly/openivm-paper">OpenIVM Paper</a>
    <br />
    <br />
  </p>
</p>

<br>

Benchmarks Incremental View Maintenance capabilities on various Open Source engines with [the TPC-DI-based data model](https://www.tpc.org/tpcdi/) for **educational purposes**.

## Dev Setup

The only requirement is Docker.

See [`CONTRIBUTING.md`](CONTRIBUTING.md) on how to bootstrap a fresh Windows machine.

## Quickstart

### Benchmark

The benchmark harness is OAT (one-at-a-time): every run takes a JSON experiments
file via `BENCHMARK_EXPERIMENTS_FILE`. A 1-row JSON is a "single experiment";
an N-row JSON is a sweep. Built-in files live under
`src/containers/benchmark-server/experiments/`.

```bash
export GIT_ROOT=$(git rev-parse --show-toplevel)
export BENCHMARK_EXPERIMENTS_FILE="$GIT_ROOT/src/containers/benchmark-server/experiments/smoke.json"

# Optional global knobs (apply to the OAT loop itself):
export OAT_MIN_FREE_PCT=10   # skip remaining experiments when free disk < 10%
export PRESERVE_RAW=1        # keep mount/bin/ across sweeps (raw/<SF>/ is always wiped between experiments)
export STORAGE_METRICS=1     # collect per-engine storage artifacts after each batch (set 0 to disable)
export STORAGE_COLLECTION_TIMEOUT_S=1800 # optional storage-collection deadline
export BENCHMARK_RUNS=1      # repeat the full benchmark N times and average per-engine timings

sudo rm -rf ${GIT_ROOT}/mount
docker kill $(docker ps -q)

bash src/.scripts/benchmark.sh
```

Per-experiment knobs (scale factor, batch percentages, per-batch update/delete
mixes, engines, parallel mode, OpenIVM feature flags, Spark tunables) live
INSIDE the experiments JSON — each row inherits from a `baseline` block and
overrides only what varies. See `smoke.json` for the schema and the
[OAT sweeps](#oat-sweeps-one-at-a-time) section below for artifact layout.
Storage metrics are enabled by default via `STORAGE_METRICS=1` and are captured
outside the timed batch window. Environment feature flags form the baseline;
an explicit `feature_flags` value in the experiments JSON overrides that
baseline for the corresponding row.

### Accumulated daily updates

Set `batch_2_days` to a positive integer to measure one Batch 2 containing
that many distinct consecutive days from Databricks' augmented TPC-DI window,
which starts on 2016-07-06. Batch 3 contains the immediately following day;
`0` preserves the standard three-batch workload. The self-contained converter
uses the same seven daily datasets and initial-state boundary as Databricks,
without importing its repository as a submodule.

The built-in augmented sweeps map 10, 20, 30, 40, and 50 percent of the
365-day window to 37, 73, 110, 146, and 183 days. The
`smoke-augmented-daily.json` file is the focused SF3 integration test. Set
`BENCHMARK_RUNS=2` for two repetitions per point.

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

### Engines

| Engine         | Compose file                                         | Mode                 | Notes                                                                                                                                                                                                                                                                                         |
| -------------- | ---------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spark          | `docker/docker-compose.benchmark.spark.yml`          | Batch SQL            | Spark + MSSQL metastore                                                                                                                                                                                                                                                                       |
| DuckDB         | `docker/docker-compose.benchmark.duckdb.yml`         | Batch SQL            | In-process, initializes DuckLake source tables from generated Parquet files, then full-refreshes the dbt model DAG on each batch                                                                                                                                                              |
| DuckDB-OpenIVM | `docker/docker-compose.benchmark.duckdb-openivm.yml` | DuckLake IVM         | Built from source in a container (`docker/docker-compose.duckdb-openivm-build.yml`), creates DuckLake-backed materialized views, refreshes with `PRAGMA refresh`, validates with `EXCEPT ALL` after timing when `OPENIVM_VALIDATE=1`                                                          |
| Feldera        | `docker/docker-compose.benchmark.feldera.yml`        | Streaming IVM (DBSP) | `pipeline-manager` + Delta input connectors                                                                                                                                                                                                                                                   |
| Spark-OpenIVM  | `docker/docker-compose.benchmark.spark-openivm.yml`  | Spark IVM            | Spark + MSSQL metastore + the openivm-spark SQL extension. Built from source in a container (`docker/docker-compose.spark-openivm-build.yml`) at a pinned `mdrakiburrahman/openivm-spark` commit. dbt issues `CREATE MATERIALIZED VIEW` (batch 1) / `REFRESH MATERIALIZED VIEW` (batches 2-3) |

### Compiler bench (incrementalizability survey)

Off by default. A port of openivm's `benchmark/src/rewriter_benchmark.cpp` to
every engine: instead of timing one TPC-DI DAG, it pushes openivm's TPC-C query
corpus at an engine and reports what fraction its compiler can maintain
incrementally. Every run includes 2,186 native query shapes and 319
DuckLake-derived shapes, for a 2,505-query corpus.

Enable with the `compiler_bench` feature flag (env `COMPILER_BENCH=1`). When on,
an experiment runs the survey **instead of** the timed 3-batch benchmark —
creating and dropping thousands of views would otherwise pollute the timings.

```bash
BENCHMARK_EXPERIMENTS_FILE=src/containers/benchmark-server/experiments/compiler-bench.json \
  bash src/.scripts/benchmark.sh
```

Per query: run the `SELECT` → `CREATE MATERIALIZED VIEW` → ask the engine
whether it will maintain it incrementally → apply base-table deltas → refresh →
verify the view equals a re-run of the query. The phase a query stops at is its
result; phase codes match the C++ benchmark so runs are comparable with it.

Knobs live in a `compiler_bench` block in the experiments JSON:

| Knob | Default | Meaning |
| ---- | ------- | ------- |
| `scale_factor` | `3` | Separate from the experiment's SF — incrementalizability does not depend on data volume, so the survey stays small |
| `timeout_s` | `60` | Per-query wall-clock budget; exceeding it is a `timeout`, not a failure |
| `limit` | `0` | `0` = whole corpus. Use a small value to smoke-test |
| `verify` | `true` | Run the `EXCEPT ALL` correctness check |
| `classify_only` | `false` | Ask only for the planner verdict; do not create, refresh, or verify materialized views |
| `delta_batch_size` | `10` | Delta statements applied per query |
| `ducklake` | `false` | Back the base tables with DuckLake instead of plain DuckDB tables (DuckDB-family engines only) |

DuckDB-family engines receive OpenIVM's native SQL. Queries for other engine
dialects are translated via LPTS, which re-renders each query from DuckDB's
optimized logical plan into the target dialect. Translation is a one-time cached
step keyed on the corpus and LPTS revisions.

The measurement is therefore *incrementalizability of the LPTS-normalized query*,
which is not always the same as of the query as written — normalization can flip
a verdict. Measured against openivm's own C++ results on the TPC-C corpus, 4 of
85 FULL_REFRESH queries become incremental after normalization: e.g.
`query_0529`'s `S_I_ID IN (SELECT I_ID FROM top_items)` is a semi-join OpenIVM
declines, but LPTS flattens the optimized plan into explicit CTEs that it accepts.
Otherwise the port agrees with the C++ benchmark on 399/400 queries for both
classification and phase.

**Storage modes.** The timed `duckdb` / `duckdb-openivm` benchmark is
DuckLake-backed, and OpenIVM can classify a query differently when the scan is a
DuckLake scan, so both are worth surveying. Because the translated corpus uses
*unqualified* table names it is storage-agnostic: the same 2,505 queries run on
plain DuckDB tables or DuckLake-backed tables. The 319 `ducklake_*` shapes have
their source-only `dl.` qualifier removed and are always part of the corpus. Set
`ducklake: true` on an experiment row; results land under
`results/<engine>-ducklake/` so the two runs cannot overwrite each other.

DuckLake metadata is DuckDB-backed — the same `ATTACH '<db>.ducklake.db' AS dl
(TYPE ducklake)` the C++ benchmark uses, so verdicts stay comparable with the
reference. It is *not* the `ducklake:sqlite:` the timed duckdb-openivm engine
attaches, because OpenIVM's refresh cannot commit against DuckLake with SQLite
metadata at all: `PRAGMA refresh` fails with `Failed to commit DuckLake
transaction ... database is locked` on a single connection in a single process,
regardless of `openivm_cascade_refresh`. That looks worth reporting upstream.

The DuckDB-family engines run through **one persistent CLI worker** rather than a
process per phase, matching the C++ benchmark's fork-once worker: one connection
held across every phase of every query, re-spawned only after a crash (which is
what preserves crash isolation). Statements are bracketed by stdout markers with
stderr drained concurrently, so each statement's error stays attributable and one
bad query cannot end the session.

The engine sessions deliberately do **not** load icu, even though the translation
step does. icu changes string collation, which changes what `EXCEPT ALL`
considers equal, and turned 10 of 60 correct results into spurious
`verify_failed`. The C++ benchmark does not load it either.

Reading the numbers:

- Queries LPTS cannot express in a dialect are reported as `translation_failed`,
  never skipped — skipping would inflate an engine's success rate. Native
  DuckDB coverage is always all 2,505 queries.
- Because the dialects lose *different* queries, cross-engine percentages are
  only comparable over `summary.json → common_subset`. Per-engine percentages
  use that engine's own corpus.
- Every percentage names its denominator: `pct_of_attempted` divides by the whole
  corpus, `pct_of_mv_created` only by queries the engine accepted as a view.
- `classification: unknown` means the engine could not be interrogated. It is
  never reported as `full`, which would make an un-queryable engine look like a
  well-behaved full-refresh engine.
- `is_correct` is only set when verification actually ran. Full-recompute engines
  (`duckdb`, `spark`) report it empty, since comparing a just-recomputed table
  against the query that built it is tautological.
- Feldera is hardcoded `incremental`: DBSP has no full-recompute mode, so the
  informative measurement there is whether its compiler accepts the SQL at all.

Requires two builder images, both cached on their Dockerfile hash:
`docker/docker-compose.lpts-build.yml` (the standalone LPTS extension, which
exposes `PRAGMA lpts`) and the duckdb-openivm image (which ships the corpus).

The corpus is TPC-C, not TPC-DI, on purpose: it is a flat set of queries over 9
base tables, so each one stands alone. openivm's TPC-DI query set is a dbt DAG
whose queries read each other's outputs (`dim_company` reads `companies`,
`fact_*` read `dim_*`), which would require materializing the whole DAG in
topological order before any single query could be planned — and it is 45
queries rather than 2,505.

## Mount layout

| Path                                                                              | Description                                              |
| --------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `mount/raw/<SF>/delta/batch{1,2,3}/`                                              | Source Delta tables per batch                            |
| `mount/raw/<SF>/delta/staging/`                                                   | Unified staging (grows via `spark-batch-loader`)         |
| `mount/results/<SF>/<engine>/`                                                    | Engine output and engine-local state                     |
| `mount/results/<SF>/dbt-server/run-<engine>-batch<N>.json`                        | Per-batch benchmark results                              |
| `mount/bin/duckdb-openivm/duckdb`                                                 | DuckDB-OpenIVM binary (built by container)               |
| `mount/bin/spark-openivm/{openivm-extension.jar,openivm.duckdb_extension,duckdb}` | spark-openivm runtime artifacts (built by container)     |
| `mount/bin/lpts/lpts.duckdb_extension`                                            | LPTS extension for compiler-bench translation             |
| `mount/bin/duckdb-openivm/queries/tpcc/`                                          | compiler-bench query corpus (pinned to `OPENIVM_COMMIT`)  |
| `mount/compiler-bench/corpus/<dialect>/`                                          | Translated queries + `translation.csv`                   |
| `mount/compiler-bench/results/<engine>/`                                          | `results.csv` (per query) + `summary.json`               |
| `mount/metrics/<SF>/<engine>/spark-events/`                                       | Raw Spark event log                                      |
| `mount/metrics/<SF>/<engine>/executions.jsonl`                                    | Per-SQL-execution → dbt model/batch sidecar              |
| `mount/metrics/<SF>/processed/`                                                   | A/B Parquet + `spark-ab-diff.png` + `RESULTS.md`         |
| `mount/metrics/<SF>/spark-metrics-<run_id>.zip`                                   | Bundled A/B artifacts (+ `…-latest.zip`)                 |

### Spark metrics A/B

Both `spark` and `spark-openivm` capture homogeneous, Spark-native per-query
metrics via Spark's built-in **event log** (gated by the `spark_metrics_capture`
feature flag, default on). At the end of every experiment `benchmark-server`
parses both engines' logs (pandas + DuckDB), maps each Spark execution → dbt
model (from the physical-plan write target) and → batch (wall-clock windows),
and emits query-aligned Parquet (`metrics_long` / `metrics_by_model` /
`timeseries`), a **per-query lifecycle** A/B diff PNG (cumulative resource over
each query's runtime, `spark` vs `spark-openivm`), `RESULTS.md`, and a per-run
zip under `mount/metrics/<SF>/` (also copied to
`mount/oat-state/<run_id>/exp-NNN/metrics/`).
Query it via the DuckDB-backed `benchmark-server` routes
(`GET /metrics/kpis`, `GET /metrics/diff`, `POST /metrics/query`,
`GET /metrics/artifact`) or by reading the Parquet directly — both are a stable
contract for downstream (`openivm-spark`) consumption.

## Results

### Notes

- The Feldera initial run compiles a Rust Binary for all SQL in a pipeline - [see here](https://github.com/mdrakiburrahman/feldera/blob/dev/mdrrahman/research/.research/demo/docs/00-end-to-end.md#2-sql-submission-to-running-pipeline), which takes a long time for the pipeline start in batch 1
- Since duckdb runs in proc in the `dbt-server`, 2 additional cores are allocated for the server vs. other engines which run dedicated
- Feldera takes ALL queries in the model and compiles it into a single binary that represents a circuit. So when `dbt` runs, it's not query-by-query, but rather the circuit flushes as it proceeds. As a result, all "tables" finish around the same time roughly.
- All results below were generated on an `E32AS_v4` Azure VM. Do **not** commit results from your local machine as the hardware may vary.
- Docker is NOT very good at resource isolation, the screenshots below must be generated with `PARALLEL=0`
- The PNGs below are illustrative snapshots from prior runs. Current OAT runs emit `chart-oat.png` + `chart-per-model.png` into `mount/oat-state/<run_id>/` instead.

### Resource allocation

- The OSS engines run under an Azure VM at `Standard_E32as_v4` (32 cores).
- For closed source engines such as Databricks Enzyme with abstraction layers such as DBU, we use a:

  - [Heuristic UDF](https://github.com/mdrakiburrahman/ivm-bench/issues/22) `SELECT detect_cpu_and_ram()`
  - [VM mapping](https://docs.databricks.com/aws/en/compute/sql-warehouse/warehouse-behavior)

  To land on a `Small` SQL Warehouse resource to ~roughly match our 32 core machine (keeping in mind Databricks runs Drivers/Executors on seperate hosts).

### Query Heuristics

- Databricks Enzyme fails incrementalization for queries it cannot incrementalize with [`REFRESH POLICY INCREMENTAL STRICT`](https://community.databricks.com/t5/technical-blog/from-surprise-full-refreshes-to-predictable-bills-refresh-policy/ba-p/157365). It is possible that the other engines are either:

  1. Superior at incrementalizing a class of queries that Enzyme cannot
  2. Are incorrectly incrementalizing queries and failing silently

  In order to keep all IVM engine benchmarks in relative lockstep, we go ahead and write all queries to have similar IVM semantics with the lowest common denominator.

  In future, as per engine can expose an "incrementalizable dry run" similar to [`EXPLAIN CREATE MATERIALIZED VIEW`](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-qry-explain-materialized-view), we can dynamically set the refresh type to `auto` per engine or when an engine complains, make it run `full`. This way,
  engines that **can** incrementalize a query performantly will, and beat the competitor fairly.

### Benchmark Heuristics

![Heuristics](imgs/benchmark-heuristics.png)

### Scale Factor: 3 (100%, 1%, 2%)

![Results](imgs/scale-factor-3-100-1-2.png)

### Scale Factor: 25 (100%, 1%, 2%)

![Results](imgs/scale-factor-25-100-1-2.png)

### Scale Factor: 100 (100%, 1%, 2%)

![Results](imgs/scale-factor-100-100-1-2.png)

### OAT sweeps (one-at-a-time)

`benchmark.sh` is OAT-only. Every run takes an experiments JSON file passed
via `BENCHMARK_EXPERIMENTS_FILE` — a 1-row JSON is a minimal "single
experiment", an N-row JSON is a sweep. The server runs each experiment
serially with **disk-aware cleanup** — between experiments it wipes
`mount/raw/<SF>/` and the per-engine results / logs dirs while preserving
the dbt-server JSON, container stats, and `mount/bin/`. When free disk on
the WSL ext4 filesystem drops below `OAT_MIN_FREE_PCT` (default `10`), the
remaining experiments are skipped (the host never crashes).

```bash
# Run the built-in Scale-Factor sweep (12 SFs × spark + spark-openivm)
export BENCHMARK_EXPERIMENTS_FILE="$(git rev-parse --show-toplevel)/src/containers/benchmark-server/experiments/sf-sweep.json"
bash src/.scripts/benchmark.sh

# Or just smoke-test on SF=3 with the cheapest batch percentages
export BENCHMARK_EXPERIMENTS_FILE="$(git rev-parse --show-toplevel)/src/containers/benchmark-server/experiments/smoke.json"
bash src/.scripts/benchmark.sh
```

Per-OAT artifacts land under `mount/oat-state/<oat_run_id>/` with a
`mount/oat-state/latest` symlink:

| File                     | Description                                                              |
| ------------------------ | ------------------------------------------------------------------------ |
| `inputs.json`            | The experiments JSON as-loaded (provenance)                              |
| `outputs.json`           | Per-experiment aggregated outputs (status, walls, per-batch durations…)  |
| `chart-oat.png`          | Aggregate heatmap (rows = experiments, columns = batch × engine)         |
| `chart-per-model.png`    | Per-dbt-model heatmap with log₂(openivm / spark) per batch               |
| `results.csv`            | One row per experiment/engine/batch with raw timing and storage metrics  |
| `benchmark-server.log`   | Copy of the orchestrator log for that run                                |
| `exp-<NNN>/outputs.json` | One per experiment — same shape as a per-experiment entry in master      |
| `exp-<NNN>/source-row-counts.json` | Exact generated insert rows per batch and source table       |
| `exp-<NNN>/storage/storage-<engine>-batch<N>.json` | Immutable per-experiment storage snapshot; repeated runs are under `storage/repetition-<N>/` |

To write your own sweep, copy `experiments/smoke.json` as a starting point
and override `scale_factor` / `batch_*_pct` / `engines` / `parallel` /
the OpenIVM feature flags / Spark tunables on each row. Anything you don't
set inherits from the file's `baseline` block.

---

## Disclaimer

> Before adding a non-OSS engine here, be sure to check the [DeWitt clause](https://cube.dev/blog/dewitt-clause-or-can-you-benchmark-a-database).

- Databricks: [Eliminating the Anti-competitive DeWitt Clause for Database Benchmarking](https://www.databricks.com/blog/2021/11/08/eliminating-the-dewitt-clause-for-database-benchmarking.html)

---

As this repo's implementation of the TPC-DI is an unpublished and unofficial TPC Benchmark, the following is required legalese per TPC Fair Use Policy:

> The `ivm-bench` TPC-DI is derived from the TPC-DI source data to test Incremental View Maintenance and as such is NOT comparable to officially published TPC-DI results.
