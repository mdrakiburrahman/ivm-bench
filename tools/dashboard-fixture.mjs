import { createHash } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import contract from '../website/contract.json' with { type: 'json' };
import { writeDashboard } from "./dashboard-parquet.mjs";

const folder = new URL('../website/public/data/', import.meta.url);
mkdirSync(folder, { recursive: true });

const engines = {
  spark: [[180, 148, 146], [1530, 1233, 1149], [1.62, 3.24, 4.86], 0, 0, 50, 9000, '3.5.7'],
  'spark-openivm': [[705, 267, 267], [1927, 1069, 1049], [2.51, 2.51, 2.51], .008, 49, 50, 10500, '0.15.3'],
  duckdb: [[31.4, 28.6, 28.6], [182, 176, 179], [1.87, 3.79, 5.72], 0, 0, 50, 3900, '1.4.2'],
  'duckdb-openivm': [[49.4, 36.5, 36.5], [270, 136, 137], [1.90, 1.90, 1.90], .00015, 49, 50, 5000, '0.15.3'],
  feldera: [[84.7, 7.1, 4.42], [828, 60, 34], [.503, .504, .505], 11.6, 70, 70, 7600, '0.93.1'],
  dbspnet: [[84.5, .745, .645], [845, 1.84, 1.69], [.561, .561, .562], 0, 50, 50, 4100, '0.6.1'],
  risingwave: [[3471, 78.3, 78.9], [13919, 138, 130], [119.15, 77.26, 77.52], 0, 50, 50, 17500, '2.7.1'],
  'materialize-emulator': [[362, 3.04, 3.03], [1685, 35.9, 38.1], [2.53, 2.53, 2.54], .003, 50, 50, 8200, '0.150.0'],
};
const subjects = [
  'Reuse the arrangement across refresh batches', 'Add source watermark to coverage report',
  'Collect per-container CPU time after teardown', 'Normalize Delta snapshot metadata',
  'Skip empty change sets before scheduling', 'Record helper-state bytes in storage summary',
  'Align customer dimension keys across adapters', 'Split initial load from incremental timing',
  'Reduce compaction work in the refresh path', 'Add trade history to lineage fixtures',
  'Pin emulator image and worker settings', 'Flush refresh metrics after transaction commit',
];
const names = ['dim_customer', 'dim_account', 'dim_security', 'dim_company', 'dim_broker', 'dim_date',
  'fact_trade', 'fact_cash_balances', 'fact_market_history', 'fact_holdings', 'fact_watches', 'prospect'];
const modelNameAt = m => m >= 34 && m < 46 ? names[m - 34] : `${m < 14 ? 'bronze' : m < 34 ? 'silver' : 'gold'}_${names[m % names.length]}_${String(m + 1).padStart(2, '0')}`;
const day = 86_400_000;
const end = Date.UTC(2026, 10, 19, 9, 40);
const generated = new Date(end + 3 * 3_600_000).toISOString();
let seed = 285593;
function random() {
  seed = (Math.imul(seed, 1664525) + 1013904223) >>> 0;
  return seed / 2 ** 32;
}
const jitter = (n, amplitude = .08) => Number((n * (1 + amplitude * (2 * random() - 1))).toPrecision(5));
const bytes = n => BigInt(Math.round(n * 1e9));
const days = Array.from({ length: 366 }, (_, i) => 365 - i).filter(age => age % 7 !== 3 && age % 7 !== 5);
const runs = [];

for (const [index, age] of days.entries()) {
  const date = end - age * day - Math.floor(random() * 4) * 3_600_000;
  const sha = createHash('sha1').update(`synthetic-ivm-bench:${age}`).digest('hex');
  const common = { commit_sha: sha, committed_at: new Date(date) };
  const sections = Object.fromEntries(Object.keys(contract.sections).map(section => [section, []]));
  const add = (section, row, engine, batch) => sections[section].push({ ...common, section, engine, batch, ...row });
  const subject = age === 0 ? 'Change state compaction thresholds' : age === 6 ? 'Tune incremental lookup scheduling' : subjects[index % subjects.length];
  add('run', {
    subject, author: ['Alex Chen', 'Samira Patel', 'Morgan Blake', 'Jamie Park'][index % 4],
    branch: index % 4 === 0 ? 'main' : `bench/${subject.toLowerCase().replaceAll(' ', '-')}`,
    pr_number: index % 4 === 0 ? null : 1000 + index, pr_title: index % 4 === 0 ? null : subject,
    run_id: `synthetic-${index + 1}`, run_attempt: 1,
    runner: index % 3 === 0 ? 'gci-runner-02' : 'gci-runner-01',
    started_at: new Date(date + 120_000), finished_at: new Date(date + 3 * 3_600_000),
    workflow: 'GCI', mode: 'gci', experiments_file: 'gci-sf10-local-engines.json',
    scale_factor: 10, workload: 'tpc-di/standard/sf10', batch_insert_pct_json: JSON.stringify({ 1: 100, 2: 1, 3: 2 }),
    engine_versions_json: JSON.stringify(Object.fromEntries(Object.entries(engines).map(([id, profile]) =>
      [id, id === 'duckdb-openivm' && age <= 6 ? '0.16.0' : profile[7]]))),
  });
  const baseSpark = [];
  for (const [engine, [duration, cpu, visible, amplification, covered, total, memory]] of Object.entries(engines)) {
    for (let b = 0; b < 3; b++) {
      const batch = b + 1;
      const wobble = 1 + .055 * Math.sin(index * 1.33 + engine.length * .4) + .038 * Math.sin(index * .44);
      const shift = (engine === 'duckdb-openivm' && b > 0 && age <= 6 ? .69 : 1)
        * (engine === 'spark-openivm' && b > 0 && (age <= 2 || (age >= 16 && age <= 19)) ? 1.42 : 1)
        * (engine === 'risingwave' && b === 0 && age === 0 ? 1.28 : 1)
        * ([22, 47, 90].includes(age) && index % 3 === 0 ? 1.23 : 1);
      const elapsed = jitter(duration[b] * wobble * shift, .055);
      const compute = jitter(cpu[b] * wobble * shift);
      const output = bytes(jitter(visible[b], .014));
      const helper = bytes(jitter(visible[b] * amplification * (engine === 'feldera' && age <= 2 ? 1.25 : 1), .035));
      const meta = bytes(jitter(.023));
      const source = bytes(jitter(1.38, .01));
      const incremental = b === 0 ? 0 : covered - (engine === 'duckdb-openivm' && age <= 2 ? 5 : engine === 'spark-openivm' && age >= 16 && age <= 19 ? 8 : 0);
      if (engine === 'spark') baseSpark.push(elapsed);
      const peak = jitter(memory * (engine === 'risingwave' && age <= 2 ? 1.22 : 1));
      const cpuPct = jitter(71 + cpu[b] % 160, .12);
      add('engine_batch', {
        status: 'succeeded', duration_s: elapsed, compute_cpu_time_s: compute,
        compute_cpu_stddev_s: jitter(compute * .04), compute_repetition_count: 3,
        visible_output_bytes: output, helper_data_bytes: helper, metadata_bytes: meta,
        source_bytes: source, total_bytes: output + helper + meta + source,
        physical_state_bytes: output + helper + meta,
        helper_over_visible: Number(helper) / Number(output),
        openivm_over_spark_duration_ratio: engine === 'spark-openivm' ? elapsed / baseSpark[b] : null,
        peak_memory_mb: peak, cpu_pct: cpuPct, throttled_s: jitter(elapsed * .006),
        models_total: total, models_incremental: incremental, models_failed: 0, models_skipped: 0,
      }, engine, batch);
      for (let m = 0; m < total; m++) {
        const layer = m < 14 ? 'bronze' : m < 34 ? 'silver' : 'gold';
        const modelName = modelNameAt(m);
        const id = `model.tpc_di.${modelName}`;
        const inc = m < incremental;
        add('model', {
          model_id: id, model_name: modelName, layer, resource_type: 'model', status: 'success',
          execution_time_s: jitter(elapsed / total * (.15 + (m % 9) / 5), .21),
          rows_affected: BigInt(Math.round(jitter((m + 1) * (b ? 350 : 26_000), .05))),
          incremental: inc, incremental_reason: inc ? 'Incremental plan' : b === 0 ? 'Initial load' : covered === 0 ? 'Baseline rebuild' : 'Full refresh required',
        }, engine, batch);
        if (b === 0) {
          const counts = Object.fromEntries(Object.keys(contract.sections.operator).filter(key => key !== 'model_id')
            .map((key, op) => [key, (m * (op + 7) + engine.length) % (op % 4 + 2) === 0 ? 1 + (m + op) % 4 : 0]));
          add('operator', { model_id: id, ...counts }, engine);
          const role = m < 14 ? 'source' : m < 34 ? 'intermediate' : 'target';
          const node = { model_id: id, model_name: modelName, layer, resource_type: 'model', role };
          add('lineage', node, engine);
          const upstream = m >= 34 ? [14 + (m - 34) % 20, 14 + (m - 27) % 20] : m >= 14 ? [(m - 14) % 14, (m - 9) % 14] : [];
          for (const dep of upstream) add('lineage', { ...node, depends_on: `model.tpc_di.${modelNameAt(dep)}` }, engine);
        }
      }
      for (let bucket = 0; bucket < 30; bucket++) {
        const wave = .32 + .65 * Math.sin(Math.PI * bucket / 31);
        add('resource', {
          sample_index: bucket, sample_at: new Date(date + 120_000 + (b * 5000 + bucket * elapsed / 29) * 1000),
          container: `${engine}-worker`, cpu_pct: jitter(cpuPct * wave, .19),
          memory_mb: jitter(peak * (.48 + wave * .49), .025), throttled_s: jitter(elapsed * .006 * bucket / 29),
        }, engine, batch);
      }
    }
    const excerpts = [
      ['INFO', 'orchestrator', 'engine-ready', 'Engine service is ready; starting batch 1.', 1],
    ];
    const worker = `${engine}-worker`;
    const warn = (fingerprint, message, count = 1, source = worker) => excerpts.push(['WARN', source, fingerprint, message, count]);
    const recovered = (fingerprint, message, count = 1) => excerpts.push(['ERROR', worker, fingerprint, message, count]);
    if (age % 7 === 0 && (engine === 'spark' || engine === 'spark-openivm')) {
      warn('shuffle-spill', 'Shuffle crossed its in-memory limit and spilled intermediate blocks to disk.', engine === 'spark-openivm' && age === 0 ? 6 : 2);
    }
    if (engine === 'spark-openivm' && (age <= 2 || (age >= 16 && age <= 19))) {
      warn('state-compaction-retry', 'State compaction exceeded its target; rescheduling partition merge.', age === 0 ? 8 : 3);
    }
    if (engine === 'duckdb' && age % 11 === 0) warn('temp-directory-spill', 'Hash aggregate spilled to the temporary directory after reaching its memory budget.', 2);
    if (engine === 'duckdb-openivm' && age <= 2) {
      warn('incremental-fallback', 'Five models fell back to full refresh after plan compatibility checks.', 5, 'dbt');
    }
    if (engine === 'feldera') {
      excerpts.push(['INFO', 'compiler', 'rust-build-complete', 'Rust pipeline compilation finished; starting ingestion.', 1]);
      if (age <= 2 || age % 13 === 0) warn('checkpoint-compaction-deferred', 'Checkpoint is retaining unmerged state; compaction deferred until ingestion drains.', age === 0 ? 5 : 2);
    }
    if (engine === 'dbspnet' && age % 9 === 0) warn('checkpoint-lag', 'Checkpoint exceeded its target while waiting for the worker queue to drain.', 2);
    if (engine === 'risingwave' && (age <= 2 || age % 15 === 0)) {
      warn('connector-backpressure', 'Source connector paused reads while the ingestion queue was over its high-water mark.', age === 0 ? 7 : 2);
      if (age <= 2) warn('memory-headroom', 'Worker memory approached its container limit; reduced ingestion concurrency.', age === 0 ? 4 : 1);
      if (age % 15 === 0) recovered('connector-timeout', 'Connector fetch timed out; resumed from the last acknowledged offset.', 2);
    }
    if (engine === 'materialize-emulator' && age % 17 === 0) {
      warn('progress-frontier-lag', 'Output progress frontier paused while the sink drained queued updates.');
      recovered('sink-ack-timeout', 'Sink acknowledgement timed out; retry confirmed the batch without duplicate writes.');
    }
    if (engine === 'spark-openivm' && age === 0) recovered('spill-retry', 'Temporary spill write failed; retry succeeded on the next attempt.', 2);
    if (engine === 'spark-openivm' && age === 1) warn('late-metrics', 'Metrics sidecar closed after the publish checkpoint.', 1, 'orchestrator');
    for (let b = 1; b <= 3; b++) {
      excerpts.push(['INFO', 'orchestrator', `batch-${b}-start`, `Starting batch ${b}; source snapshot is ready.`, 1]);
      excerpts.push(['INFO', `${engine}-worker`, `snapshot-${b}`, `Attached the source snapshot for batch ${b}.`, 1]);
      for (let model = 34; model < 43; model++) {
        const name = modelNameAt(model);
        excerpts.push(['INFO', 'dbt', `batch-${b}-${name}`, `Batch ${b}: model ${name} materialized successfully.`, 1]);
      }
      excerpts.push(['INFO', 'dbt', `models-complete-${b}`, `Completed ${total} model executions for batch ${b}.`, 1]);
      excerpts.push(['INFO', `${engine}-worker`, `checkpoint-${b}`, `Persisted batch ${b} execution statistics and storage state.`, 1]);
    }
    excerpts.push(['INFO', 'orchestrator', 'refresh-finished', 'Finished all 3 benchmark batches.', 1]);
    for (const [position, [level, sourceName, fingerprint, message, collapsed_count]] of excerpts.entries()) {
      add('log', { logged_at: new Date(date + 120_000 + position * 210_000), level, source: sourceName, message, fingerprint, collapsed_count }, engine);
    }
  }
  runs.push(sections);
}

const metadata = { dataset: "synthetic", generatedAt: generated, workload: "tpc-di/standard/sf10" };
writeDashboard(fileURLToPath(new URL("_server.parquet", folder)), runs, metadata);
writeDashboard(fileURLToPath(new URL("one-commit.parquet", folder)), runs.slice(-1), { ...metadata, legacy: true });
writeDashboard(fileURLToPath(new URL("incompatible.parquet", folder)), runs.slice(-1), { ...metadata, schemaVersion: "2.0" });
console.log(`Wrote ${runs.length} synthetic commits plus alternate fixtures.`);
