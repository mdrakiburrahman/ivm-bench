# Copilot Instructions — ivm-bench

## What this repo is

A benchmark harness that generates TPC-DI data, converts it to Delta Lake, then materializes a dbt project against it via different engines.

The goal is to test the impact of Incremental View Maintenance on the dbt model runtime and find weak points per engine.

## Architecture

### Two-phase pipeline

**Phase 1 — Data generation** (`docker/docker-compose.datagen.yml`):

1. `tpc-di-gen`: Downloads DIGen.jar, runs PDGF to produce raw TPC-DI flat files → `mount/raw/<SF>/digen/`
2. `spark-digen-delta`: Scala Spark job reads flat files, writes Delta Lake tables (CDF enabled) → `mount/raw/<SF>/delta/`
3. Sequential: `spark-digen-delta` starts only after `tpc-di-gen` exits successfully. Both are idempotent (skip if output exists).

**Phase 2 — Benchmark orchestration** (three-tier):

```
benchmark.sh  →  benchmark-server (:9000)  →  dbt-server + engine stacks
  (thin CLI)       (orchestrator)                (per-engine workers)
```

1. `benchmark.sh` (`src/.scripts/benchmark.sh`) is a thin bash client. It starts the benchmark-server Docker stack, streams SSE progress, captures logs, and tears down on completion.
2. `benchmark-server` (`src/containers/ben# Copilot Instructions — ivm-bench

## What this repo is

A benchmark harness that generates TPC-DI data, converts it to Delta Lake, then materializes a dbt project against it via different engines.

The goal is to test the impact of Incremental View Maintenance on the dbt model runtime and find weak points per engine.

## Architecture

### Two-phase pipeline

**Phase 1 — Data generation** (`docker/docker-compose.datagen.yml`):

1. `tpc-di-gen`: Downloads DIGen.jar, runs PDGF to produce raw TPC-DI flat files → `mount/raw/<SF>/digen/`
2. `spark-digen-delta`: Scala Spark job reads flat files, writes Delta Lake tables (CDF enabled) → `mount/raw/<SF>/delta/`
3. Sequential: `spark-digen-delta` starts only after `tpc-di-gen` exits successfully. Both are idempotent (skip if output exists).

**Phase 2 — Benchmark orchestration** (three-tier):

```
benchmark.sh  →  benchmark-server (:9000)  →  dbt-server + engine stacks
  (thin CLI)       (orchestrator)                (per-engine workers)
```

1. `benchmark.sh` (`src/.scripts/benchmark.sh`) is a thin bash client. It starts the benchmark-server Docker stack, streams SSE progress, captures logs, and tears down on completion.
2. `benchmark-server` (`src/containers/benchmark-server/`) is the central orchestrator. It manages the full lifecycle: datagen, engine stack bring-up/teardown, batch loading, dbt runs, stats collection, and result persistence.
3. Per-engine stacks (`docker/docker-compose.benchmark.<engine>.yml`) each run a `dbt-server` instance plus any engine-specific services. The benchmark-server starts/stops these via Docker Compose from inside its container.

### Mount layout

All data flows through `mount/` which is gitignored:

- `mount/raw/<SF>/digen/` — Raw TPC-DI flat files (Phase 1 output)
- `mount/raw/<SF>/delta/` — Delta Lake source tables (input to dbt)
- `mount/results/<SF>/<engine>/` — Engine table output as Delta Lake
- `mount/results/<SF>/dbt-server/` — Benchmark metadata per engine: run results, delta stats, lineage, SQL analysis (consumed by chart generation)
- `mount/logs/<SF>/` — dbt-server logs per scale factor
- `mount/stats/<SF>/<engine>/` — Container resource stats (CPU, memory)
- `mount/metrics/<SF>/<engine>/{spark-events,executions.jsonl}` — Spark-native event log + per-execution→model/batch sidecar (`spark`/`spark-openivm` only, issue #36)
- `mount/metrics/<SF>/{processed/,spark-metrics-<run_id>.zip}` — A/B Parquet (`metrics_long`/`metrics_by_model`/`timeseries`) + `spark-ab-diff.png` + `RESULTS.md`, bundled per run
- `mount/benchmark-state/` — Persistent benchmark-server SQLite database

Each benchmark run cleans `mount/` except `mount/benchmark-state/`. See `services/orchestrator.py`.

### Spark metrics A/B (issue #36)

Homogeneous Spark-native metrics for `spark` + `spark-openivm` are captured via
Spark's event log (gated by the `spark_metrics_capture` feature flag, default on)
and post-processed by `benchmark-server` at the end of every experiment
(`services/spark_metrics.py`, wired in `orchestrator._emit_spark_metrics`). The
Parquet paths/schemas above and the DuckDB-backed routes — `GET /metrics/kpis`,
`GET /metrics/diff`, `POST /metrics/query` (read-only), `GET /metrics/artifact`
(`handlers/metrics.py`) — are a **stable contract** for `openivm-spark`, which
consumes results via REST or by reading the Parquet directly from the mount.

All engines included in the benchmark must report successful metrics.

If any do not despite being included in the benchmark, there could be a silent failure in query parsing or timeout, investigate the logs!

The detailed logs of all containers and `dbt` runs will be stored here:

```
.logs
mount/logs
```

### Do not commit without review

The repo owner reviews all changes before committing. Do not run `git commit` autonomously.

### Host environment constraints

The dev host only has bash, Docker, and minimal CLI tooling — as bootstrapped by `contrib/bootstrap-dev-env.sh`. Nothing more.

Do not assume it has Python or anything else. Do not try to install new software ad-hoc, and if you want to add something to `contrib/bootstrap-dev-env.sh`, ask explicit permission of the user.

Instead, if you need to test application code, spin up the relevant container and add things in there (e.g. `docker compose up dbt-server`) rather than installing dependencies on the host.

### During a `/plan`, always clarify any doubts or ambiguity with the end user

Do not make assumptions unless you are **crystal clear** on the user's intent.
chmark-server/`) is the central orchestrator. It manages the full lifecycle: datagen, engine stack bring-up/teardown, batch loading, dbt runs, stats collection, and result persistence.
3. Per-engine stacks (`docker/docker-compose.benchmark.<engine>.yml`) each run a `dbt-server` instance plus any engine-specific services. The benchmark-server starts/stops these via Docker Compose from inside its container.

### Mount layout

All data flows through `mount/` which is gitignored:

- `mount/raw/<SF>/digen/` — Raw TPC-DI flat files (Phase 1 output)
- `mount/raw/<SF>/delta/` — Delta Lake source tables (input to dbt)
- `mount/results/<SF>/<engine>/` — Engine table output as Delta Lake
- `mount/results/<SF>/dbt-server/` — Benchmark metadata per engine: run results, delta stats, lineage, SQL analysis (consumed by chart generation)
- `mount/logs/<SF>/` — dbt-server logs per scale factor
- `mount/stats/<SF>/<engine>/` — Container resource stats (CPU, memory)
- `mount/benchmark-state/` — Persistent benchmark-server SQLite database

Each benchmark run cleans `mount/` except `mount/benchmark-state/`. See `services/orchestrator.py`.

### Do not commit without review

The repo owner reviews all changes before committing. Do not run `git commit` autonomously.

### Host environment constraints

The dev host only has bash, Docker, and minimal CLI tooling — as bootstrapped by `contrib/bootstrap-dev-env.sh`. Nothing more.

Do not assume it has Python or anything else. Do not try to install new software ad-hoc, and if you want to add something to `contrib/bootstrap-dev-env.sh`, ask explicit permission of the user.

Instead, if you need to test application code, spin up the relevant container and add things in there (e.g. `docker compose up dbt-server`) rather than installing dependencies on the host.

### During a `/plan`, always clarify any doubts or ambiguity with the end user

Do not make assumptions unless you are **crystal clear** on the user's intent.

### OAT (one-at-a-time) sweep harness

`benchmark.sh` is OAT-only — it always runs an experiments JSON file passed
via `BENCHMARK_EXPERIMENTS_FILE`. The server runs each experiment serially
with disk-aware cleanup (`OAT_MIN_FREE_PCT`, default 10%). Per-run artifacts
(`chart-oat.png`, `chart-per-model.png`, `RESULTS.md`, `outputs.json`) land
under `mount/oat-state/<run_id>/` with a `mount/oat-state/latest` symlink and
are refreshed after every experiment so the run can be observed live.
Built-in sweeps: `experiments/sf-sweep.json` (12 SFs, spark vs spark-openivm)
and `experiments/smoke.json` (SF=3 dry-run). The experiments dataclass +
parser are in `models/experiments.py`; orchestrator dispatch is at
`services/orchestrator._run_oat`; per-experiment helpers live in
`services/oat_runner.py`.
