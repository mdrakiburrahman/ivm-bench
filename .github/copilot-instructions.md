# Copilot Instructions — ivm-bench

## What this repo is

A benchmark harness that generates TPC-DI data, converts it to Delta Lake, then materializes a dbt project against it via different engines.

The goal is to test the impact of Incremental View Maintenance on the dbt model runtime.

## Architecture

### Two-phase pipeline

**Phase 1 — Data generation** (`docker-compose.datagen.yml`):

1. `tpc-di-gen`: Downloads DIGen.jar, runs PDGF to produce raw TPC-DI flat files → `mount/raw/<SF>/digen/`
2. `spark-digen-delta`: Scala Spark job reads flat files, writes Delta Lake tables (CDF enabled) → `mount/raw/<SF>/delta/`
3. Sequential: `spark-digen-delta` starts only after `tpc-di-gen` exits successfully. Both are idempotent (skip if output exists).

**Phase 2 — dbt benchmark** (`docker-compose.benchmark.yml`):

1. `dbt-server`: Flask REST API that runs `dbt build` asynchronously, streams live progress, stores results in SQLite.
2. Various engines under test

### dbt-server REST API (`src/containers/dbt-server/app.py`)

| Endpoint                                 | Purpose                                                                                                           |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `POST /run/<engine>`                     | Trigger async dbt build. Body: `{"scale_factor": N, "full_refresh": bool}`. Returns `run_id`.                     |
| `GET /runs/<id>/progress?since=<cursor>` | Cursor-based live progress with `Retry-After` header. Parses dbt JSON log events (Q027, Q011, Q012) in real-time. |
| `GET /runs/<id>`                         | Final results: per-node DAG, timing, compiled SQL, edges for visualization.                                       |
| `GET /runs`                              | List all runs.                                                                                                    |

### dbt project layout (`src/containers/dbt-server/dbt-projects/<engine>/`)

Each engine gets its own dbt project directory.

- **`macros/register_sources.sql`** — `on-run-start` hook that creates external Delta tables in Spark's catalog via `CREATE TABLE IF NOT EXISTS ... USING DELTA LOCATION`.
- **`models/bronze/`** — 17 models: pass-through from source tables, XML struct flattening (crm_customer_mgmt), fixed-width parsing (finwire CMP/SEC/FIN).
- **`models/silver/`** — 14 models: SCD2 processing, window functions, joins across batches.
- **`models/gold/`** — 13 models: dimension and fact tables with surrogate keys, forward-fill.
- **`models/work/`** — Ephemeral intermediate models.

### Mount layout

All data flows through `mount/` which is gitignored:

- `mount/raw/<SF>/digen/` — Raw flat files (input)
- `mount/raw/<SF>/delta/` — Delta Lake source tables (input to dbt)
- `mount/results/<SF>/spark/` — Spark warehouse output (Delta tables from dbt)
- `mount/results/<SF>/dbt-server/` — SQLite state + `run-spark.json` results

### Adding a new engine

1. Create `src/containers/dbt-server/dbt-projects/<engine>/` with `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`.
2. The `POST /run/<engine>` endpoint automatically discovers projects by directory name.
3. Results land in `mount/results/<SF>/<engine>/`.
4. Update `benchmark.sh` to trigger the new engine.

### Do not commit without review

The repo owner reviews all changes before committing. Do not run `git commit` autonomously.

### During a `/plan`, always clarify any doubts or ambiguity with the end user

Do not make assumptions unless you are **crystal clear** on the user's intent.