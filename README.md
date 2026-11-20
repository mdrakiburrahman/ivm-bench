<p align="center">
  <img src="https://rakirahman.blob.core.windows.net/public/images/Misc/dbt-incremental.png" alt="dbt Incremental View Maintenance Benchmark" width="30%">
</p>

<h3 align="center">dbt - Incremental View Maintenance Benchmark</h3>

<p align="center">
Benchmarking engine-specific IVM implementations using a TPC-DI dbt project, <em>without</em> using <a href="https://docs.getdbt.com/docs/build/incremental-models">dbt incremental</a> (because watermark columns are unreliable).
</p>

<p align="center">
<a href="https://docs.getdbt.com/">dbt Docs</a> · <a href="https://materializedview.io/p/everything-to-know-incremental-view-maintenance">Everything You Need to Know About Incremental View Maintenance</a> · <a href="https://rakirahman.blob.core.windows.net/public/books/INCREMENTAL_VIEW_MAINTENANCE.pdf">Short doc on IVM</a> · <a href="https://bit.ly/dbsp-paper-full">DBSP Paper</a> · <a href="https://bit.ly/openivm-paper">OpenIVM Paper</a>
</p>

Benchmarks Incremental View Maintenance capabilities on various Open Source engines with [the TPC-DI-based data model](https://www.tpc.org/tpcdi/) for **educational purposes**.


The experimental [Extended TPC-DS workload](docs/extended-tpcds.md) adds a retail metadata extension and a 96-model, Fabric-inspired pipeline. All engines replay the same mutation plan and retain state across refreshes.

## Benchmark at a glance

TPC-DI data is generated and converted to Delta Lake, then materialized through
dbt across local Docker engines and managed cloud services.

![Docker, cloud, CI and GCI benchmark architecture](imgs/ivm-bench-architecture.drawio.png)

### Benchmark dashboard

The [IVM Bench dashboard](https://ivmbench.z9.web.core.windows.net/) visualizes published benchmark telemetry per commit: batch timings, engine resources, model coverage, and signals.

[![IVM Bench dashboard showing per-commit durations across three batches](imgs/ivm-bench-live-dashboard.jpg)](https://ivmbench.z9.web.core.windows.net/)

### Run a smoke benchmark

```bash
export GIT_ROOT=$(git rev-parse --show-toplevel)
export BENCHMARK_EXPERIMENTS_FILE="$GIT_ROOT/src/containers/benchmark-server/experiments/smoke.json"
bash src/.scripts/benchmark.sh
```

See [Quickstart](docs/QUICKSTART.md) for setup and benchmark options.

## Table of Contents

- [Quickstart](docs/QUICKSTART.md)
- [Docker, cloud, CI and GCI](docs/CI_VS_GCI.md)
- [Manual cloud CI](docs/CI.md)
- [Benchmark workloads](docs/WORKLOADS.md)
- [Engines and compiler benchmark](docs/ENGINES.md)
- [Mount layout](docs/MOUNT_LAYOUT.md)
- [Spark metrics A/B](docs/SPARK_METRICS.md)
- [Results](docs/RESULTS.md)
- [OAT sweeps](docs/OAT_SWEEPS.md)
- [SQL catalog](docs/SQL_CATALOG.md)
- [DBSP.NET engine](docs/DBSPNET.md)
- [Fabric validation](docs/FABRIC_VALIDATION.md)
- [OpenIVM profiling](docs/OPENIVM_PROFILING.md)
- [Benchmark disclaimer](docs/DISCLAIMER.md)
- [Contributing](CONTRIBUTING.md)
