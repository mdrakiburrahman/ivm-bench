# Data and mount layout

[Back to README](../README.md)

---

## Mount layout

| Path                                                                              | Description                                              |
| --------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `mount/raw/<SF>/delta/batch{1,2,3}/`                                              | Source Delta tables per batch                            |
| `mount/raw/<SF>/delta/.iceberg-snapshots.json`                                    | Delta and Iceberg snapshot identities for RisingWave    |
| `mount/raw/<SF>/iceberg/`                                                        | MinIO data for RisingWave's batch-scoped Iceberg inputs |
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
