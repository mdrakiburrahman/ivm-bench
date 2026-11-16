# Getting started

[Back to README](../README.md)

---

## Dev Setup

The only requirement is Docker.

See [`CONTRIBUTING.md`](../CONTRIBUTING.md) on how to bootstrap a fresh Windows machine.

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
export STORAGE_METRICS=1     # collect per-engine storage artifacts after each batch (set 0 to disable)
export STORAGE_COLLECTION_TIMEOUT_S=1800 # optional storage-collection deadline
export BENCHMARK_RUNS=1      # repeat the full benchmark N times and average per-engine timings

sudo rm -rf ${GIT_ROOT}/mount
docker kill $(docker ps -q)

bash src/.scripts/benchmark.sh
```

Per-experiment knobs (scale factor, batch percentages, per-batch update/delete
mixes, engines, parallel mode, OpenIVM feature flags, Spark tunables) live
INSIDE the experiments JSON — each row inherits from a `baseline` block and
overrides only what varies. See `smoke.json` for the schema and the
[OAT sweeps](OAT_SWEEPS.md#oat-sweeps-one-at-a-time) section below for artifact layout.
Storage metrics are enabled by default via `STORAGE_METRICS=1` and are captured
outside the timed batch window. Environment feature flags form the baseline;
an explicit `feature_flags` value in the experiments JSON overrides that
baseline for the corresponding row.
