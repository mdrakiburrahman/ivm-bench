# Copilot Instructions — ivm-bench

## What this repo is

A benchmark harness that generates TPC-DI data, converts it to Delta Lake, then materializes a dbt project against it via different engines.

The goal is to test the impact of Incremental View Maintenance on the dbt model runtime and find weak points per engine.

## Architecture

### Two-phase pipeline

**Phase 1 — Data generation** (`docker-compose.datagen.yml`):

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
3. Per-engine stacks (`docker-compose.benchmark.<engine>.yml`) each run a `dbt-server` instance plus any engine-specific services. The benchmark-server starts/stops these via Docker Compose from inside its container.

### benchmark-server (`src/containers/benchmark-server/`)

Flask REST API on port 9000. See `app.py` and `handlers/` for full endpoint details. Key capabilities:

- **Benchmark lifecycle** — start a benchmark run, stream real-time SSE progress, query status and results.
- **Engine orchestration** — brings up/tears down per-engine Docker Compose stacks, runs a 3-batch benchmark with staged data. Batch execution semantics differ by engine (full-refresh vs incremental); see `services/engine_runner.py`. Also collects container stats, lineage, and SQL analysis.
- **Persistent state** — SQLite database at `mount/benchmark-state/benchmark.db` with `benchmark_runs` and `engine_batches` tables. This state survives container restarts. See `services/db.py`.
- **Resource calculation** — Allocates CPU/memory per engine based on `HOST_CORES`, `HOST_MEMORY`, and `PARALLEL` mode. In parallel mode, host resources are divided equally across engines. See `services/resource_calc.py` and `models/config.py`.
- **Docker-in-docker** — Mounts the host Docker socket. `benchmark.sh` exports `REPO_HOST_PATH`; compose mounts the repo at that path and passes it into benchmark-server as `REPO_DIR`. Uses `host.docker.internal` for networking to sibling containers. The repo must be mounted at the **same host path** inside the container.

### benchmark.sh (`src/.scripts/benchmark.sh`)

Thin CLI client (~100 lines). Environment variables:

| Variable                                    | Default                               | Purpose                        |
| ------------------------------------------- | ------------------------------------- | ------------------------------ |
| `SCALE_FACTOR`                              | `3`                                   | TPC-DI scale factor            |
| `BATCH_1_PCT`, `BATCH_2_PCT`, `BATCH_3_PCT` | (required)                            | Batch size percentages         |
| `PARALLEL`                                  | `0`                                   | `1` to run engines in parallel |
| `ENGINES`                                   | `spark,duckdb,duckdb-openivm,feldera` | Comma-separated engine list    |
| `HOST_CORES`                                | auto-detected                         | Override host CPU count        |
| `HOST_MEMORY`                               | auto-detected                         | Override host memory (GB)      |

### dbt-server (`src/containers/dbt-server/`)

Flask REST API that runs dbt builds, tracks progress, and collects results. Each engine gets its own dbt-server instance (port 5000 in serial mode, per-engine mapped ports in parallel mode; see `models/config.py`). See `app.py` and `handlers/` for the full endpoint list. Key endpoint groups:

- **Core runs** — `POST /run/<engine>`, `GET /runs`, `GET /runs/<id>`, progress polling (cursor-based) and SSE streaming.
- **Engine-specific** — Dedicated endpoints for engines that need custom workflows (e.g. compilation waits, pipeline stats).
- **Stats & analysis** — Container resource stats, Delta table stats, DAG lineage, compiled SQL extraction, chart generation.

Uses **in-memory SQLite** (ephemeral) — state is intentionally lost on container restart. Persistent benchmark state lives in benchmark-server's SQLite. See `services/db.py`.

### dbt project layout (`src/containers/dbt-server/dbt-projects/<engine>/`)

Each engine gets its own dbt project directory with `dbt_project.yml`, `profiles.yml`, `models/`, and `macros/`.

- **`models/bronze/`** — Pass-through from source tables, XML struct flattening (crm_customer_mgmt), fixed-width parsing (finwire CMP/SEC/FIN).
- **`models/silver/`** — SCD2 processing, window functions, joins across batches.
- **`models/gold/`** — Dimension and fact tables with surrogate keys, forward-fill.
- **`models/work/`** — Ephemeral intermediate models.
- Some engines have additional model directories (e.g. `sources/`) for engine-specific staging.

### Docker Compose files

| File                                      | Purpose                                                          |
| ----------------------------------------- | ---------------------------------------------------------------- |
| `docker-compose.datagen.yml`              | Phase 1: TPC-DI data generation + Delta conversion               |
| `docker-compose.base.yml`                 | Shared `dbt-server-base` service definition (used via `extends`) |
| `docker-compose.benchmark-server.yml`     | Benchmark orchestrator container                                 |
| `docker-compose.benchmark.<engine>.yml`   | Per-engine stacks (Spark, DuckDB, DuckDB-OpenIVM, Feldera)       |
| `docker-compose.batch-loader.yml`         | Spark batch-loader for staging incremental data                  |
| `docker-compose.duckdb-openivm-build.yml` | DuckDB-OpenIVM binary build step                                 |

All per-engine compose files extend `dbt-server-base` from `docker-compose.base.yml`. Engines with external services (Spark, Feldera) include those alongside dbt-server. Engines that run inside dbt-server itself (DuckDB, DuckDB-OpenIVM) only define the dbt-server service.

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

### Adding a new engine

1. Create a dbt project at `src/containers/dbt-server/dbt-projects/<engine>/` with `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`.
2. Create `docker-compose.benchmark.<engine>.yml` extending `dbt-server-base` from `docker-compose.base.yml`.
3. Register the engine in `src/containers/benchmark-server/models/config.py`: add entries to `ENGINE_PORTS`, `ENGINE_COMPOSE_FILES`, and `ENGINE_MAIN_SERVICES`.
4. Resource allocation is handled automatically by `src/containers/benchmark-server/services/resource_calc.py`.
5. If the engine needs custom run/wait logic, add methods in `src/containers/benchmark-server/services/engine_runner.py` and corresponding dbt-server handlers.
6. If the engine should be included by default, update the default `ENGINES` lists in `benchmark.sh`, `services/config_loader.py`, and `BenchmarkConfig`.
7. Results land in `mount/results/<SF>/<engine>/`.

### Do not commit without review

The repo owner reviews all changes before committing. Do not run `git commit` autonomously.

### Host environment constraints

The dev host only has bash, Docker, and minimal CLI tooling — as bootstrapped by `contrib/bootstrap-dev-env.sh`. Nothing more.

Do not assume it has Python or anything else. Do not try to install new software ad-hoc, and if you want to add something to `contrib/bootstrap-dev-env.sh`, ask explicit permission of the user.

Instead, if you need to test application code, spin up the relevant container and add things in there (e.g. `docker compose up dbt-server`) rather than installing dependencies on the host.

### During a `/plan`, always clarify any doubts or ambiguity with the end user

Do not make assumptions unless you are **crystal clear** on the user's intent.
