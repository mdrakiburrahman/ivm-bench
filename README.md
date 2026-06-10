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

sudo rm -rf ${GIT_ROOT}/mount
docker kill $(docker ps -q)

bash src/.scripts/benchmark.sh
```

Per-experiment knobs (scale factor, batch percentages, per-batch update/delete
mixes, engines, parallel mode, OpenIVM feature flags, Spark tunables) live
INSIDE the experiments JSON — each row inherits from a `baseline` block and
overrides only what varies. See `smoke.json` for the schema and the
[OAT sweeps](#oat-sweeps-one-at-a-time) section below for artifact layout.

For append-only runs, `batch_N_pct` (or the alias `batch_N_insert_pct`) is the
insert percentage; for mixed-DML batches, `batch_N_update_pct` and
`batch_N_delete_pct` are optional mutation percentages applied before the
insert (batch 1 is always insert-only, defaults are `0`).

At the end, `benchmark.sh` cats `mount/oat-state/latest/RESULTS.md`:

```text
=== OAT sweep — status: completed ===

| # | label     | sf | b1   | b2     | b3     | engines              | parallel | wall   | status    |
|---|-----------|----|------|--------|--------|----------------------|----------|--------|-----------|
| 1 | smoke-sf3 | 3  | 1    | 0.001  | 0.002  | spark, spark-openivm | false    | 30m12s | completed |
```

Artifacts: `mount/oat-state/latest/{chart-oat.png, chart-per-model.png, RESULTS.md, outputs.json}`

### Engines

| Engine         | Compose file                                         | Mode                 | Notes                                                                                                                                                                                                                                                                                         |
| -------------- | ---------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spark          | `docker/docker-compose.benchmark.spark.yml`          | Batch SQL            | Spark + MSSQL metastore                                                                                                                                                                                                                                                                       |
| DuckDB         | `docker/docker-compose.benchmark.duckdb.yml`         | Batch SQL            | In-process, initializes DuckLake source tables from generated Parquet files, then full-refreshes the dbt model DAG on each batch                                                                                                                                                              |
| DuckDB-OpenIVM | `docker/docker-compose.benchmark.duckdb-openivm.yml` | DuckLake IVM         | Built from source in a container (`docker/docker-compose.duckdb-openivm-build.yml`), creates DuckLake-backed materialized views, refreshes with `PRAGMA refresh`, validates with `EXCEPT ALL` after timing when `OPENIVM_VALIDATE=1`                                                          |
| Feldera        | `docker/docker-compose.benchmark.feldera.yml`        | Streaming IVM (DBSP) | `pipeline-manager` + Delta input connectors                                                                                                                                                                                                                                                   |
| Spark-OpenIVM  | `docker/docker-compose.benchmark.spark-openivm.yml`  | Spark IVM            | Spark + MSSQL metastore + the openivm-spark SQL extension. Built from source in a container (`docker/docker-compose.spark-openivm-build.yml`) at a pinned `mdrakiburrahman/openivm-spark` commit. dbt issues `CREATE MATERIALIZED VIEW` (batch 1) / `REFRESH MATERIALIZED VIEW` (batches 2-3) |

## Mount layout

| Path                                                                              | Description                                          |
| --------------------------------------------------------------------------------- | ---------------------------------------------------- |
| `mount/raw/<SF>/delta/batch{1,2,3}/`                                              | Source Delta tables per batch                        |
| `mount/raw/<SF>/delta/staging/`                                                   | Unified staging (grows via `spark-batch-loader`)     |
| `mount/results/<SF>/<engine>/`                                                    | Engine output and engine-local state                 |
| `mount/results/<SF>/dbt-server/run-<engine>-batch<N>.json`                        | Per-batch benchmark results                          |
| `mount/bin/duckdb-openivm/duckdb`                                                 | DuckDB-OpenIVM binary (built by container)           |
| `mount/bin/spark-openivm/{openivm-extension.jar,openivm.duckdb_extension,duckdb}` | spark-openivm runtime artifacts (built by container) |

## Results

### Notes

- The Feldera initial run compiles a Rust Binary for all SQL in a pipeline - [see here](https://github.com/mdrakiburrahman/feldera/blob/dev/mdrrahman/research/.research/demo/docs/00-end-to-end.md#2-sql-submission-to-running-pipeline), which takes a long time for the pipeline start in batch 1
- Since duckdb runs in proc in the `dbt-server`, 2 additional cores are allocated for the server vs. other engines which run dedicated
- Feldera takes ALL queries in the model and compiles it into a single binary that represents a circuit. So when `dbt` runs, it's not query-by-query, but rather the circuit flushes as it proceeds. As a result, all "tables" finish around the same time roughly.
- All results below were generated on an `E32AS_v4` Azure VM. Do **not** commit results from your local machine as the hardware may vary.
- Docker is NOT very good at resource isolation, the screenshots below must be generated with `PARALLEL=0`
- The PNGs below are illustrative snapshots from prior runs. Current OAT runs emit `chart-oat.png` + `chart-per-model.png` into `mount/oat-state/<run_id>/` instead.

### Benchmark Heuristics

![Heuristics](imgs/benchmark-heuristics.png)

### Scale Factor: 25 (100%, 1%, 1%)

![Results](imgs/scale-factor-25-100-1-1.png)

### Scale Factor: 100 (100%, 1%, 1%)

![Results](imgs/scale-factor-100-100-1-1.png)

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

| File                       | Description                                                              |
| -------------------------- | ------------------------------------------------------------------------ |
| `inputs.json`              | The experiments JSON as-loaded (provenance)                              |
| `outputs.json`             | Per-experiment aggregated outputs (status, walls, per-batch durations…)  |
| `chart-oat.png`            | Aggregate heatmap (rows = experiments, columns = batch × engine)         |
| `chart-per-model.png`      | Per-dbt-model heatmap with log₂(openivm / spark) per batch               |
| `RESULTS.md`               | Markdown overview + per-input/output tables + per-model break-even table |
| `benchmark-server.log`     | Copy of the orchestrator log for that run                                |
| `exp-<NNN>/outputs.json`   | One per experiment — same shape as a per-experiment entry in master      |

To write your own sweep, copy `experiments/smoke.json` as a starting point
and override `scale_factor` / `batch_*_pct` / `engines` / `parallel` /
the OpenIVM feature flags / Spark tunables on each row. Anything you don't
set inherits from the file's `baseline` block.

---

## Disclaimer

> Before adding a non-OSS engine here, be sure to check the [DeWitt clause](https://cube.dev/blog/dewitt-clause-or-can-you-benchmark-a-database).

As this repo's implementation of the TPC-DI is an unpublished and unofficial TPC Benchmark, the following is required legalese per TPC Fair Use Policy:

> The `ivm-bench` TPC-DI is derived from the TPC-DI source data to test Incremental View Maintenance and as such is NOT comparable to officially published TPC-DI results.
