# DbspNet engine integration

Adds **DbspNet** — a .NET DBSP implementation ([clast-project/dbsp-net](https://github.com/clast-project/dbsp-net))
— as an ivm-bench engine. Same class as Feldera (streaming IVM / DBSP, view-only SQL,
Delta input/output), so it slots in the same way.

## Design: compile-only bypass (no custom dbt adapter)

Rather than a `dbt-dbspnet` adapter, the integration reads the dbt project's model SQL +
`+connectors` config directly and hands a program to a **DbspNet control service** (the
`.NET` analogue of Feldera's `pipeline-manager`). This is timing-neutral: the deploy /
compile happens once and is excluded from the measured batch, exactly like Feldera's Rust
compile.

- **`dbt_to_program.py`** (`dbt-server/services/`) — translates the dbt project into a
  program: resolves `{{ ref() }}` / `{{ source() }}` and `dbt_utils.generate_surrogate_key`
  (the only jinja the project uses), topo-sorts, and emits
  `{program: ["CREATE TABLE …", "CREATE VIEW … AS …"], inputs, output_bindings}`.
- **DbspNet control service** (`src/DbspNet.Server` in the dbsp-net repo) — HTTP
  `/compile` (dry-run), `/deploy` (compile + wire Delta connectors), `/resume` (timer
  start), `/wait` (block until the batch drains + outputs are truncate-written), `/stats`,
  `/healthz`.
- **`dbspnet_client.py` + `handlers/dbspnet.py`** (`dbt-server/`) — the dbt-server drives
  the service: deploy → resume → wait. Simpler than `feldera_client.py` — a DbspNet batch
  drains to completion, so `/wait` just blocks (no stats-quiescence heuristics).

## Files

| File | Purpose |
|---|---|
| `src/containers/dbt-server/dbt-projects/dbspnet/` | dbt project (copy of `feldera/`; output URIs → `/data/processed/dbspnet`, no adapter profile) |
| `src/containers/dbt-server/services/dbt_to_program.py` | dbt project → program translator |
| `src/containers/dbt-server/services/dbspnet_client.py` | client for the DbspNet control service |
| `src/containers/dbt-server/handlers/dbspnet.py` | Flask blueprint: `/deploy /resume /wait /pause /stats /dbspnet` |
| `src/containers/dbspnet/Dockerfile` | builds the DbspNet control service — clones dbsp-net at a pinned commit (like duckdb-openivm), no sibling checkout |
| `docker/docker-compose.benchmark.dbspnet.yml` | `dbspnet-server` + `dbt-server` |
| `benchmark-server` registries | `config.py` (ports/compose/main-service), `engine_runner.py` (dispatch + batch methods), `chart.py`/`oat_chart.py` (order/colour/status) |
| `experiments/dbspnet.json` | run DbspNet head-to-head with Feldera at SF=3 |

## Prerequisites

None beyond Docker — like the other from-source engines, the build clones the engine repo
itself. `src/containers/dbspnet/Dockerfile` clones `clast-project/dbsp-net` at a pinned
`DBSPNET_COMMIT` and builds the control service. To bump the engine, override the build
arg `DBSPNET_COMMIT` (or `DBSPNET_REPO` for a fork).

## Run

```bash
export BENCHMARK_EXPERIMENTS_FILE=src/containers/benchmark-server/experiments/dbspnet.json
bash src/.scripts/benchmark.sh
```

## Validation status

**Validated (in the dbsp-net repo, no Docker):**
- The translator turns the 70-model project into a program; POSTing it to the control
  service's `/compile` returns `{ok:true, outputViews:18}` — **the whole TPC-DI DAG
  compiles as one incrementally-maintained DbspNet circuit.**
- The control service's deploy → resume → wait cycle runs multi-view programs over real
  local Delta tables end-to-end (per-batch truncate output matching a batch oracle).

**Needs validation in the Docker/WSL harness (not yet run):**
- The full 3-batch benchmark over TPC-DI data (needs the `spark-batch-loader` datagen).
- The `dbspnet-server` image build (clone-at-pinned-commit) and the cross-container
  dbt-server → service wiring.
- The `benchmark-server` registry edits under a real OAT run.

## Known follow-ons

- Two gold views have no output connector in the Feldera project (`fact_market_history`,
  `daily_market_pulse`) because truncate can't drain them at SF=100. They are `+stored`
  (computed but not written). This draft materialises only views **with** an output
  binding; matching Feldera's "compute-but-don't-write" for those two is a follow-on
  (integrate-only outputs).
- Per-view timings: `/wait` reports the whole-batch duration for every output node (DbspNet
  flushes the circuit as a whole, like Feldera); per-view times could be added later.
