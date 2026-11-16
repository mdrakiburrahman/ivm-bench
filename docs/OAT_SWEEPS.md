# OAT sweep artifacts

[Back to README](../README.md)

---

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
