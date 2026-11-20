# Engines and compiler benchmark

[Back to README](../README.md)

---

### Engines

| Engine         | Compose file                                         | Mode                 | Notes                                                                                                                                                                                                                                                                                         |
| -------------- | ---------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spark          | `docker/docker-compose.benchmark.spark.yml`          | Batch SQL            | Spark + MSSQL metastore                                                                                                                                                                                                                                                                       |
| DuckDB         | `docker/docker-compose.benchmark.duckdb.yml`         | Batch SQL            | In-process, initializes DuckLake source tables from generated Parquet files, then full-refreshes the dbt model DAG on each batch                                                                                                                                                              |
| DuckDB-OpenIVM | `docker/docker-compose.benchmark.duckdb-openivm.yml` | DuckLake IVM         | Built from source in a container (`docker/docker-compose.duckdb-openivm-build.yml`), creates DuckLake-backed materialized views, refreshes with `PRAGMA refresh`, validates with `EXCEPT ALL` after timing when `OPENIVM_VALIDATE=1`                                                          |
| Feldera        | `docker/docker-compose.benchmark.feldera.yml`        | Streaming IVM (DBSP) | `pipeline-manager` + Delta input connectors                                                                                                                                                                                                                                                   |
| RisingWave     | `docker/docker-compose.benchmark.risingwave.yml`     | Streaming IVM        | Native materialized views; immutable Iceberg snapshots are copied into RisingWave-owned mutable tables through its Iceberg connector                                                                                                                                                            |
| Spark-OpenIVM  | `docker/docker-compose.benchmark.spark-openivm.yml`  | Spark IVM            | Spark + MSSQL metastore + the openivm-spark SQL extension. Built from source in a container (`docker/docker-compose.spark-openivm-build.yml`) at a pinned `mdrakiburrahman/openivm-spark` commit. dbt issues `CREATE MATERIALIZED VIEW` (batch 1) / `REFRESH MATERIALIZED VIEW` (batches 2-3) |
| Materialize Emulator | `docker/docker-compose.benchmark.materialize-emulator.yml` | Streaming IVM | Pinned non-production Emulator, not production Materialize. Native tables receive Delta snapshots via bounded COPY; one set of materialized views is maintained across all batches. |

Materialize Emulator results are explicitly labeled and are **not representative
of production Materialize performance**. The Emulator image is subject to its
non-production license and limits. Its cluster size is chosen within the local
engine's CPU and memory allocation and the effective replica is recorded.
Batch timings include native source writes and a strict-serializable read of every
maintained output; same-engine compiled-definition multiset checks and storage
collection run after timing. These consistency checks do not replace independent
cross-engine result comparisons.
Storage includes persistent blobs and compute scratch; shard catalog ownership
separates source and output blobs from helper data and metadata. Temporary
compaction files count as helper data; scans retry if files disappear and fail
if a complete scan cannot be obtained. Counts are approximate: a live scan is
not an atomic snapshot and concurrent compaction can change attribution.
Compiler-bench, repeated refresh and the Databricks workload are unsupported.

Materialize truncates floating-point aggregate inputs to multiples of 2^-24
([aggregate precision](https://materialize.com/docs/sql/types/float/#aggregate-precision)).
Selected SQL overrides accumulate as `NUMERIC` before casting back to `DOUBLE`;
this improves accuracy but is not bitwise equivalent to IEEE floating-point
aggregation. Cross-engine checks use the repository's six-decimal float comparison.
Even at that precision, four-decimal model rounding can expose aggregation-order
differences: an SF3 Spark comparison found two `market_volatility` groups with
different rounded standard deviations. This remains a known parity discrepancy.
Materialize accepts `NUMERIC(29,4)` for Spark percentage expressions but
[ignores the precision](https://materialize.com/docs/sql/types/numeric/#precision):
the scale is retained, while Spark's 29-digit overflow limit is not enforced.
Timezone-aware Delta timestamps use Materialize `TIMESTAMPTZ`; Spark reports
these columns as `TIMESTAMP`. Value comparisons normalize them to UTC.

Use `src/containers/benchmark-server/experiments/materialize-emulator-smoke.json`
for a standalone three-batch TPC-DI run.

The existing Spark data generator defaults to Delta output. When RisingWave is
selected, the orchestrator configures `DATAGEN_OUTPUT_FORMATS=delta,iceberg`:
Spark writes batch-scoped Iceberg tables from the committed Delta snapshots to
MinIO, so other engines keep their existing Delta input. Snapshot IDs are
recorded alongside the Delta data and pinned during RisingWave loads. Later
batches mutate the owned tables before appending their batch-specific snapshots.

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
