import { readFileSync } from 'node:fs';
import { basename, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import contract from '../website/contract.json' with { type: 'json' };
import { sections, writeDashboard } from './dashboard-parquet.mjs';

function read(root, path, optional = false) {
  try { return readFileSync(join(root, path), 'utf8'); }
  catch (error) {
    if (!optional || error.code !== 'ENOENT') throw error;
    return undefined;
  }
}
function json(root, path, optional) {
  const text = read(root, path, optional);
  return text === undefined ? undefined : JSON.parse(text);
}
const number = value => typeof value === 'number' && Number.isFinite(value) ? value : null;
const integer64 = value => Number.isSafeInteger(value) ? BigInt(value) : null;
const date = value => typeof value === 'string' && Number.isFinite(Date.parse(value)) ? new Date(value) : null;
const text = value => typeof value === 'string' ? value : null;

const logTemplates = [
  [/^Failed to capture storage metrics for ([a-z0-9-]+) batch \d+:/, 'storage-metrics', 'Failed to capture batch storage metrics.'],
  [/^Failed to fetch lineage for ([a-z0-9-]+):/, 'lineage-fetch', 'Failed to fetch lineage.'],
  [/^Failed to fetch sql analysis for ([a-z0-9-]+):/, 'sql-analysis-fetch', 'Failed to fetch SQL analysis.'],
  [/^SSE stream error for ([a-z0-9-]+):/, 'sse-stream', 'Run event stream reported an error.'],
  [/^Failed to check run result for ([a-z0-9-]+) \(poll\):/, 'run-poll', 'Failed to poll run result.'],
  [/^Engine ([a-z0-9-]+) cleanup [a-z_]+ failed:/, 'engine-cleanup', 'Engine cleanup step failed.'],
  [/^Engine ([a-z0-9-]+) failed$/, 'engine-failed', 'Engine execution failed.'],
];

export function curatedLogs(content, engines) {
  const grouped = new Map();
  for (const line of content.split(/\r?\n/)) {
    const record = /^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) \[(WARNING|ERROR)\] services\.engine_runner: (.*)$/.exec(line);
    if (!record) continue;
    const logged_at = date(`${record[1].replace(' ', 'T').replace(',', '.')}Z`);
    if (!logged_at) continue;
    for (const [pattern, fingerprint, message] of logTemplates) {
      const engine = pattern.exec(record[3])?.[1];
      if (!engines.has(engine)) continue;
      const level = record[2] === 'WARNING' ? 'WARN' : 'ERROR';
      const key = `${engine}:${level}:${fingerprint}`;
      const prior = grouped.get(key);
      grouped.set(key, { engine, logged_at, level, source: 'benchmark-server', fingerprint, message,
        collapsed_count: (prior?.collapsed_count ?? 0) + 1 });
      break;
    }
  }
  return [...Map.groupBy(grouped.values(), row => row.engine).values()].flatMap(rows =>
    rows.sort((a, b) => a.level.localeCompare(b.level) || b.logged_at - a.logged_at).slice(0, 60));
}

function selectSamples(samples, batch) {
  const start = batch.extra?.wall_window_start_ms;
  const end = batch.extra?.wall_window_end_ms;
  if (!Number.isFinite(start) || !Number.isFinite(end)) return [];
  return samples.filter(sample => sample.timestamp_s * 1000 >= start && sample.timestamp_s * 1000 <= end);
}

function resourceSummary(samples) {
  const first = new Map(), last = new Map();
  const peaks = Object.fromEntries(['cpu_pct', 'mem_mb'].map(field => {
    const values = samples.map(sample => number(sample[field])).filter(v => v !== null);
    return [field, values.length ? Math.max(...values) : null];
  }));
  for (const sample of samples) {
    if (!Number.isFinite(sample.cpu_throttled_time_ns)) continue;
    if (!first.has(sample.container)) first.set(sample.container, sample.cpu_throttled_time_ns);
    last.set(sample.container, sample.cpu_throttled_time_ns);
  }
  const throttled = [...last].flatMap(([container, value]) => value >= first.get(container) ? [value - first.get(container)] : []);
  return { ...peaks, throttled_s: throttled.length ? throttled.reduce((sum, n) => sum + n, 0) / 1e9 : null };
}

export function convert(root, identity) {
  const coverage = json(root, 'ci-results/engine-coverage.json');
  if (coverage.passed !== true || coverage.experiments?.length !== 1) throw new Error('Dashboard publication requires one coverage-passing experiment.');
  const base = 'mount/oat-state/latest';
  const overall = json(root, `${base}/outputs.json`);
  const experiment = json(root, `${base}/exp-000/outputs.json`);
  if (experiment.status !== 'completed') throw new Error('Benchmark experiment did not complete.');
  const { commit_sha, committed_at } = identity;
  if (!/^[0-9a-f]{40}$/.test(commit_sha) || !date(committed_at)) throw new Error('Missing PR head commit identity.');
  const scale = experiment.scale_factor;
  const inputs = experiment.inputs;
  if (!Number.isInteger(scale) || scale < 1 || typeof inputs?.workload !== 'string') throw new Error('Missing workload or scale factor.');
  const workload = `tpc-di/${inputs.workload}/sf${scale}`;
  const percentages = Object.fromEntries([1, 2, 3].flatMap(n => {
    const raw = inputs[`batch_${n}_pct`];
    const pct = typeof raw === 'string' && /^\d+(?:\.\d+)?$/.test(raw) ? Number(raw) : number(raw);
    return pct === null || pct < 0 || pct > 100 ? [] : [[n, pct]];
  }));
  const rows = Object.fromEntries(sections.map(section => [section, []]));
  const add = (section, values, engine = null, batch = null) =>
    rows[section].push({ ...values, section, engine, batch, commit_sha, committed_at: new Date(committed_at) });
  add('run', {
    ...Object.fromEntries(['subject', 'author', 'branch', 'pr_title', 'run_id', 'run_url', 'runner'].map(k => [k, text(identity[k])])),
    pr_number: number(identity.pr_number), run_attempt: number(identity.run_attempt),
    started_at: date(experiment.started_at), finished_at: date(experiment.ended_at),
    workflow: 'GCI', mode: 'gci', experiments_file: overall.experiments_file ? basename(overall.experiments_file) : null,
    scale_factor: scale, workload, batch_insert_pct_json: JSON.stringify(percentages),
  });
  const dbt = `mount/results/${scale}/dbt-server`;
  for (const [engine, result] of Object.entries(experiment.engines)) {
    if (!/^[a-z0-9-]+$/.test(engine) || coverage.experiments[0].engines?.[engine]?.passed !== true) {
      throw new Error('Result contains an engine that did not pass coverage.');
    }
    const log = read(root, `${base}/benchmark-server.log`, true);
    if (log) for (const { engine, ...entry } of curatedLogs(log, new Set(Object.keys(experiment.engines)))) add('log', entry, engine);
    const lineage = json(root, `${dbt}/lineage-${engine}.json`, true);
    const layer = new Map(lineage?.nodes?.map(node => [node.unique_id, node.schema]));
    const edges = Map.groupBy(lineage?.edges ?? [], edge => edge.to);
    for (const node of lineage?.nodes ?? []) {
      const values = { model_id: node.unique_id, model_name: node.name, layer: node.schema, resource_type: node.resource_type, role: node.role };
      add('lineage', values, engine);
      for (const edge of edges.get(node.unique_id) ?? []) add('lineage', { ...values, depends_on: edge.from }, engine);
    }
    for (const model of json(root, `${dbt}/sql-analysis-${engine}.json`, true)?.models ?? []) {
      if (!model.operators || model.parse_error) continue;
      const counts = Object.fromEntries(Object.keys(contract.sections.operator).filter(k => k !== 'model_id')
        .map(k => [k, number(model.operators[k])]));
      add('operator', { model_id: model.unique_id, ...counts }, engine);
    }
    const stats = read(root, `${base}/exp-000/compute/${engine}-container_stats.jsonl`, true)
      ?? read(root, `mount/stats/${scale}/${engine}/container_stats.jsonl`, true);
    const samples = stats?.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) ?? [];
    for (const batch of result.batches) {
      const n = batch.batch_num;
      if (!Number.isInteger(n) || n < 1 || batch.status !== 'completed' || number(batch.duration_s) === null) {
        throw new Error('Invalid batch in a coverage-passing experiment.');
      }
      const dbtRun = json(root, `${dbt}/run-${engine}-batch${n}.json`, true);
      const models = dbtRun?.nodes?.filter(node => node.resource_type === 'model');
      for (const model of models ?? []) {
        add('model', {
          model_id: model.unique_id, model_name: model.name, layer: layer.get(model.unique_id), resource_type: model.resource_type,
          status: model.status, execution_time_s: number(model.execution_time_s), rows_affected: integer64(model.rows_affected),
          incremental: dbtRun.full_refresh === true ? false : null,
          incremental_reason: dbtRun.full_refresh === true ? 'Run used dbt full refresh' : null,
        }, engine, n);
      }
      const batchSamples = selectSamples(samples, batch);
      const summary = resourceSummary(batchSamples);
      for (const [container, points] of Map.groupBy(batchSamples, sample => sample.container)) {
        if (!/^[a-z0-9_.-]+$/i.test(container)) continue;
        const buckets = Math.min(points.length, 30);
        for (let i = 0; i < buckets; i++) {
          const sample = points[Math.round(i * (points.length - 1) / Math.max(1, buckets - 1))];
          const base = points[0].cpu_throttled_time_ns;
          add('resource', {
            sample_at: new Date(sample.timestamp_s * 1000), sample_index: i, container,
            cpu_pct: number(sample.cpu_pct), memory_mb: number(sample.mem_mb),
            throttled_s: Number.isFinite(base) && sample.cpu_throttled_time_ns >= base ? (sample.cpu_throttled_time_ns - base) / 1e9 : null,
          }, engine, n);
        }
      }
      const compute = batch.extra?.compute_metrics, storage = batch.extra?.storage;
      add('engine_batch', {
        status: 'succeeded', duration_s: batch.duration_s, compute_cpu_time_s: number(compute?.cpu_time_s),
        compute_cpu_stddev_s: number(compute?.cpu_time_stddev_s), compute_repetition_count: number(compute?.repetition_count),
        ...Object.fromEntries(['visible_output_bytes', 'helper_data_bytes', 'metadata_bytes', 'source_bytes', 'total_bytes', 'physical_state_bytes']
          .map(k => [k, integer64(storage?.[k])])),
        helper_over_visible: number(storage?.overhead_ratio_helper_to_visible),
        peak_memory_mb: summary.mem_mb, cpu_pct: summary.cpu_pct, throttled_s: summary.throttled_s,
        models_total: models?.length ?? null, models_incremental: models && dbtRun.full_refresh === true ? 0 : null,
        models_failed: models?.filter(m => ['error', 'fail'].includes(m.status)).length ?? null,
        models_skipped: models?.filter(m => m.status === 'skipped').length ?? null,
      }, engine, n);
    }
  }
  if (!rows.engine_batch.length) throw new Error('No engine measurements were found.');
  return { rows, workload };
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [root, identityPath, output] = process.argv.slice(2);
  if (!root || !identityPath || !output) throw new Error('Usage: dashboard-convert.mjs <artifact root> <identity JSON> <output.parquet>');
  const { rows, workload } = convert(root, JSON.parse(readFileSync(identityPath, 'utf8')));
  console.log(writeDashboard(output, [rows], { dataset: 'ci', workload, generatedAt: new Date().toISOString() }));
}
