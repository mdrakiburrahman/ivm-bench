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

See [`contrib/README.md`](contrib/README.md) on how to bootstrap a fresh Windows machine.

## Quickstart

### Benchmark

```bash
export GIT_ROOT=$(git rev-parse --show-toplevel)
export SCALE_FACTOR=3                               # 3 - 2147483647
export BATCH_1_PCT=1                                # 1% of DIGen batch 1 data
export BATCH_2_PCT=0.001                            # 0.001% of DIGen batch 2 data
export BATCH_3_PCT=0.002                            # 0.002% of DIGen batch 3 data
export PARALLEL=1                                   # 0, 1
export ENGINES=spark,duckdb,duckdb-openivm,feldera  # Comma seperated engines to run

bash src/.scripts/benchmark.sh
```

At the end, you should see the benchmark-server stream results like:

```text
=== All benchmarks completed successfully ===

                 1           2           3
Duckdb:          00:00:22 -> 00:00:26 -> 00:00:26
Duckdb-openivm:  00:00:53 -> 00:00:33 -> 00:00:35
Feldera:         00:11:20 -> 00:00:39 -> 00:00:37
Spark:           00:03:32 -> 00:02:16 -> 00:02:14

====================== 00:16:51 ======================
```

Runs 3 batches per engine (Full load`batch1` → append `batch2` → append `batch3`).

### Engines

| Engine         | Compose file                                         | Mode                 | Notes                                                                                                                                                                                         |
| -------------- | ---------------------------------------------------- | -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spark          | `docker/docker-compose.benchmark.spark.yml`          | Batch SQL            | Spark + MSSQL metastore                                                                                                                                                                       |
| DuckDB         | `docker/docker-compose.benchmark.duckdb.yml`         | Batch SQL            | In-process, reads Delta via `delta` extension, writes via a hack with [this community extension](https://github.com/djouallah/delta_export) that blows up the Delta transaction log each time |
| DuckDB-OpenIVM | `docker/docker-compose.benchmark.duckdb-openivm.yml` | DuckLake IVM         | Built from source in a container (`docker/docker-compose.duckdb-openivm-build.yml`), creates DuckLake-backed materialized views, refreshes with `PRAGMA ivm`, validates with `EXCEPT ALL`     |
| Feldera        | `docker/docker-compose.benchmark.feldera.yml`        | Streaming IVM (DBSP) | `pipeline-manager` + Delta input connectors                                                                                                                                                   |

## Mount layout

| Path                                                       | Description                                      |
| ---------------------------------------------------------- | ------------------------------------------------ |
| `mount/raw/<SF>/delta/batch{1,2,3}/`                       | Source Delta tables per batch                    |
| `mount/raw/<SF>/delta/staging/`                            | Unified staging (grows via `spark-batch-loader`) |
| `mount/results/<SF>/<engine>/`                             | Engine output (Delta tables from dbt)            |
| `mount/results/<SF>/dbt-server/run-<engine>-batch<N>.json` | Per-batch benchmark results                      |
| `mount/bin/duckdb-openivm/duckdb`                          | DuckDB-OpenIVM binary (built by container)       |

## Results

### Notes

- The Feldera initial run compiles a Rust Binary for all SQL in a pipeline - [see here](https://github.com/mdrakiburrahman/feldera/blob/dev/mdrrahman/research/.research/demo/docs/00-end-to-end.md#2-sql-submission-to-running-pipeline), which takes a long time for the pipeline start in batch 1
- Since duckdb runs in proc in the `dbt-server`, 2 additional cores are allocated for the server vs. other engines which run dedicated

### Benchmark Heuristics

![Heuristics](imgs/benchmark-heuristics.png)

### Scale Factor: 3 (1%, 0.001%, 0.002%)

![Results](imgs/scale-factor-3-1-0_001-0_002.png)

---

## Disclaimer

As this repo's implementation of the TPC-DI is an unpublished and unofficial TPC Benchmark, the following is required legalese per TPC Fair Use Policy:

> The `ivm-bench` TPC-DI is derived from the TPC-DI source data to test Incremental View Maintenance and as such is NOT comparable to officially published TPC-DI results.
