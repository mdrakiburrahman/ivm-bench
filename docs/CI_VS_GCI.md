# GCI and CI workflows

[Back to README](../README.md)

---

### GCI vs CI

| Workflow | When | What | Knobs |
|---|---|---|---|
| [`gci.yaml`](../.github/workflows/gci.yaml) | Non-draft same-repo PR, or manual dispatch on the default branch | SF10 TPC-DI on every local engine ([`gci-sf10-local-engines.json`](../src/containers/benchmark-server/experiments/gci-sf10-local-engines.json)) | None |
| [`ci.yaml`](../.github/workflows/ci.yaml) | Ad hoc only | Every engine ([`ci-sf10-all-engines.json`](../src/containers/benchmark-server/experiments/ci-sf10-all-engines.json)), or any experiments file | All dispatch inputs |

Both fail unless every requested engine completes every batch. Run anything
else, such as cloud engines or the cost-model sweep, through CI. For cloud
credentials and dispatch commands, see [manual CI](CI.md):

```bash
gh workflow run ci.yaml --ref <branch> \
  -f experiments_file=src/containers/benchmark-server/experiments/cost-model-bench.json
```

Use `cost-model-bench-ladder.json` for the full cost-model sweep, or another
experiments JSON for a TPC-DI workload. Without `experiments_file`, CI
synthesises one experiment from its per-knob inputs.

Manual dispatches write results to the job summary by default. Set the optional
string input `pr_number` (for example, `-f pr_number=100`) to also post a results
comment, including the best-effort per-batch chart, on that pull request.
Dispatch uses `batch_1_pct` for the initial insert percentage; the duplicate
`batch_1_insert_pct` input is omitted to stay within GitHub's 25-input limit.
Experiments JSON files still accept `batch_1_insert_pct`.

To compare OpenIVM revisions, change both `ARG OPENIVM_COMMIT` defaults in
[`src/containers/duckdb-openivm/Dockerfile`](../src/containers/duckdb-openivm/Dockerfile)
to the same commit SHA, push a branch, and dispatch CI on it. Changing the pin
invalidates the cached builder image. Each run uses one OpenIVM revision;
use separate branches or dispatches to compare revisions. The revision must provide
`cost_model_benchmark` and support the selected arguments; DuckDB and the build
tooling remain separately pinned in that Dockerfile.

Cost-model CSVs are uploaded with the CI artifacts under
`mount/cost-model-bench/<run-id>/<experiment-index>-<label>/`. Scale factors
run sequentially, and the CI job timeout and cancellation rules apply.
A newer dispatch of the same branch and experiments file cancels the previous
run (an active run on `main` is kept), so wait for a run to finish before
dispatching the next revision if you need complete comparison results.


## Execution boundaries

GitHub Actions runs the benchmark job on the `self-hosted`, `Linux`, `X64`,
`ivm-bench-azure` runner. The benchmark orchestrator and local engines run in
Docker on that runner. This includes TPC-DI generation, the benchmark-server,
dbt-server, Spark, DuckDB, Feldera, DbspNet, RisingWave, and Materialize Emulator.

Databricks Enzyme and OpenIVM, and the Fabric JVM and OpenIVM engines, execute
on their respective cloud services; the local Docker containers orchestrate
those runs.

GCI is the fixed SF10 gate for the eight local engines listed above. Use `gh workflow run gci.yaml` to run the same preset on the default branch without a PR. Successful coverage-passing GCI runs publish to the benchmark dashboard; configurable manual CI runs do not.
CI is the configurable workflow used by manual dispatch; its
default SF10 preset adds Databricks and Fabric for twelve engines total. CI can
also run other experiment files, including cloud-engine and cost-model sweeps.

Materialize Emulator is a non-production deployment, not representative of
production Materialize performance; its results retain the Emulator label.

The GitHub Actions results comment includes a per-batch stacked bar chart.
