# IVM Bench dashboard

From `website\`, run `npm ci`, `npm run build:fixture`, then `npm run preview`. Open `http://127.0.0.1:4175/`. `npm run build:fixture` generates the deterministic fixtures before bundling. Only synthetic measurements are served by default; `VITE_PARQUET_URL` changes the default and `?data=` overrides it for one view.

Open **Data** to inspect provenance and supported panels, load another Parquet URL, or choose/drop a local file. A URL selection can be shared with the dashboard link; a local file stays in the current browser tab and is never uploaded. The preview includes `/data/one-commit.parquet` and `/data/incompatible.parquet` for exercising both outcomes.

The Data byte map uses physical offsets and compressed column-chunk sizes from the footer. Its highlights indicate HTTP bytes fetched in this browser session; the footer meter counts repeated transfers separately. Row groups without a single section identified by statistics remain visible as mixed or unknown.

## Parquet v1

[`contract.json`](contract.json) is the authoritative column catalog shared by the reader and generator. A file can contain one published commit or a retained history. Publication is independent of the site: only successful benchmark runs with a passing engine coverage verdict enter the dataset. On pull requests `commit_sha` is the head commit, not the merge commit. Manual GCI runs record the executed default-branch commit.

- Use Snappy compression and dictionary encoding where it reduces size. Store `ivmbench.schema_version` (`1.0`), `dataset` (`synthetic` or `ci`), `generated_at` (ISO 8601 UTC), `retention_days` (integer), `workload` (stable workload ID), and `section_rows` (JSON map from section to row count) as file key/value metadata.
- Put exactly one section in each row group. Emit min/max statistics for `section`, `committed_at`, and `commit_sha` (untruncated). Put eager sections first, with newer engine-batch groups first, and cluster detail sections by commit time. The reader skips groups when statistics exclude them, then checks decoded rows. A group can hold multiple commits to limit footer size.
- `run` and `engine_batch` are required; other sections are optional. A compatible empty file can preserve the required schema with zero rows. Unknown major versions are rejected; extra columns are allowed.
- Strings are `BYTE_ARRAY` annotated `STRING` or `UTF8`; timestamps are `INT64` annotated UTC millisecond timestamp. Nullable out-of-section or unavailable measurements are null, not zero. Byte and row counts use `INT64`. Durations are in seconds, memory in MB, percentage fields in percentage points, and ratios are unitless. `compute_cpu_time_s` is not wall time.
- Use one run row per commit (engine and batch null), engine-batch rows keyed by commit/engine/batch, and one model row per batch and model. Lineage uses one row per node with null `depends_on`, then one row per upstream dependency. Curated logs are capped at 60 sanitized entries per engine/commit and 300 characters per entry. Derived signals are never stored.

| Section | Benchmark source |
|---|---|
| `run` | GitHub PR head metadata and OAT `exp-000/outputs.json` |
| `engine_batch` | OAT `exp-000/outputs.json` wall time, compute and storage facts; sampled container resources |
| `model` | dbt-server `run-<engine>-batch<N>.json`; full-refresh classification when recorded |
| `operator` | dbt-server `OperatorCounts` from compiled SQL |
| `lineage` | dbt-server `LineageGraph` |
| `resource` | `mount/stats/<sf>/<engine>/container_stats.jsonl`, bucketed within the batch interval |
| `log` | Allowlisted messages in the OAT benchmark-server log; no raw exception details or payloads are exported |

The fixture's batch duration, CPU, and storage baselines use the successful SF10 benchmark artifact from [run 44259810551](https://github.com/openivm/ivm-bench/actions/runs/44259810551). Individual rows, commit identities, timings beyond the baseline, engine-version stories, and log messages are synthetic. Hyparquet also decoded Snappy/dictionary output from that artifact's DuckDB-produced `metrics_by_model.parquet`.

The optional `run.batch_insert_pct_json` column is a JSON object keyed by batch number, e.g. `{"1":100,"2":1,"3":2}`. Values are percentages (not fractions), and describe the actual insert workload for that commit. Missing, null, or invalid entries show as unavailable; older v1 files need no migration. The Overview plots `engine_batch.duration_s` independently for each batch on the same chronological commit axis.

The fixture pairs the Spark OpenIVM refresh regression with compaction and spill retries, DuckDB OpenIVM's coverage drop with planner fallbacks, RisingWave's memory and initial-load shift with connector pressure, and Feldera's storage amplification with deferred compaction. Synthetic `ERROR` excerpts show recovered operations within successful runs.

The local preview serves only `dist` on loopback. A remote Parquet source needs `HEAD`, `GET`, byte ranges, and a strong ETag. Cross-origin hosts must accept `Range` and `If-Match` and expose `Content-Range`, `Content-Length` and `ETag`. A change to the object mid-read fails rather than mixing revisions.

## Publishing

Successful same-repository GCI runs with passing coverage invoke `tools/dashboard-ci.sh`. The publisher reads the runner's storage settings from its environment, writes the per-commit snapshot first, and conditionally replaces `_server.parquet` with a 365-day window. An ETag conflict triggers a fresh list and merge, up to four attempts. Old snapshots are deleted after the merged file is published. Unrecorded engine versions and incrementalization measurements remain null; an empty log section means no matching, safe messages were captured.

Use `VITE_PARQUET_URL=https://ivmbench.blob.core.windows.net/parquet/_server.parquet npm run build` in `website/` for the production bundle. Only `build:fixture` copies synthetic Parquet to `dist`. The dashboard workflow checks PR builds, deploys on matching changes to `main`, and supports manual dispatch once it is registered on the default branch. The deployment and `cors` modes of `tools/dashboard-storage.sh` require Azure CLI, jq, and benchmark storage settings in the environment. The CORS mode adds the site and loopback origins without clearing unrelated rules.
