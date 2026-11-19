import type { Batch, Run } from './data.ts';

export function batchInsertPercent(run: Run, batch: number): number | undefined {
  if (typeof run.batch_insert_pct_json !== 'string') return;
  try {
    const values: unknown = JSON.parse(run.batch_insert_pct_json);
    if (!values || typeof values !== 'object' || Array.isArray(values)) return;
    const value = (values as Record<string, unknown>)[String(batch)];
    if (typeof value === 'number' && Number.isFinite(value) && value >= 0 && value <= 100) return value;
  } catch (error) {
    if (!(error instanceof SyntaxError)) throw error;
  }
}

export function batchHistory(runs: Run[], rows: Batch[], matches: (engine: string, run: Run) => boolean, query: string) {
  const commits = new Map(runs.map(run => [run.commit_sha, run]));
  const matched = new Set<string>();
  const byBatch = new Map<number, Map<string, Map<string, number>>>();
  const engines = new Set<string>();
  for (const row of rows) {
    const run = commits.get(row.commit_sha);
    if (!run || !matches(row.engine, run)) continue;
    matched.add(run.commit_sha);
    engines.add(row.engine);
    if (row.duration_s === null || !Number.isFinite(row.duration_s) || row.duration_s < 0) continue;
    const engineMap = byBatch.get(row.batch) ?? new Map<string, Map<string, number>>();
    const durations = engineMap.get(row.engine) ?? new Map<string, number>();
    durations.set(run.commit_sha, row.duration_s);
    engineMap.set(row.engine, durations);
    byBatch.set(row.batch, engineMap);
  }
  const domain = runs.filter(run => !query || matched.has(run.commit_sha)
    || `${run.commit_sha} ${run.subject} ${run.branch}`.toLowerCase().includes(query))
    .sort((a, b) => a.committed_at - b.committed_at || a.commit_sha.localeCompare(b.commit_sha));
  return {
    domain,
    engines: [...engines],
    charts: [1, 2, 3].map(batch => ({
      batch,
      insert: domain.map(run => batchInsertPercent(run, batch)),
      series: [...engines].map(engine => ({ engine, durations: domain.map(run => byBatch.get(batch)?.get(engine)?.get(run.commit_sha)) })),
    })),
  };
}

export const DAY = 86_400_000;
export const palette: Record<string, [string, string, string, boolean?]> = {
  spark: ['Spark', 'SP', '#cb640f'],
  'spark-openivm': ['Spark OpenIVM', 'SI', '#ab450e', true],
  duckdb: ['DuckDB', 'DD', '#a08212'],
  'duckdb-openivm': ['DuckDB OpenIVM', 'DI', '#7d770d', true],
  feldera: ['Feldera', 'FE', '#824ebe'],
  dbspnet: ['DBSP.NET', 'DB', '#07856b'],
  risingwave: ['RisingWave', 'RW', '#2775c6'],
  'materialize-emulator': ['Materialize Emulator', 'ME', '#bc4e80'],
};
export const engineInfo = (id: string) => palette[id] ?? [id, id.slice(0, 2).toUpperCase(), '#667d8c', false];
export const date = (time: number) => new Intl.DateTimeFormat('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' }).format(time);
export const signed = (n: number) => `${n >= .5 ? '+' : n <= -.5 ? '−' : ''}${Math.round(Math.abs(n))}%`;
export const seconds = (n: number) => n >= 100 ? n.toFixed(0) : n >= 10 ? n.toFixed(1) : n.toFixed(2);
const signalValue = (n: number, unit: string) => unit === 'models' ? `${Math.round(n)} models`
  : unit === 'MB' && n >= 1024 ? `${(n / 1024).toFixed(1)} GB` : `${seconds(n)} ${unit}`;
export const median = (data: number[]) => {
  if (!data.length) return undefined;
  const sorted = [...data].sort((a, b) => a - b);
  return (sorted[(sorted.length - 1) >> 1] + sorted[sorted.length >> 1]) / 2;
};
const finite = (n: unknown): n is number => typeof n === 'number' && Number.isFinite(n);
export interface Point { run: Run; value: number; expected?: number; delta?: number }
export interface Trend { engine: string; points: Point[] }
export interface Finding {
  engine: string; title: string; level: 'High' | 'Watch' | 'Gain'; point: Point; context: string; trace: Point[];
}
const duration = (field: string) => (rows: Batch[]) => {
  const a = rows.find(row => row.batch === 2)?.[field];
  const b = rows.find(row => row.batch === 3)?.[field];
  return finite(a) && finite(b) ? a + b : undefined;
};
const single = (field: string, batch: number) => (rows: Batch[]) => {
  const value = rows.find(row => row.batch === batch)?.[field];
  return finite(value) ? value : undefined;
};
const measures = {
  refresh: { title: 'Incremental refresh', extract: duration('duration_s'), unit: 's', threshold: .12 },
  initial: { title: 'Initial load', extract: single('duration_s', 1), unit: 's', threshold: .20 },
  cpu: { title: 'Refresh CPU time', extract: duration('compute_cpu_time_s'), unit: 'CPU s', threshold: .20 },
  memory: { title: 'Peak memory', extract: single('peak_memory_mb', 3), unit: 'MB', threshold: .16 },
  storage: { title: 'Helper state amplification', extract: single('helper_over_visible', 3), unit: '×', threshold: .22 },
  coverage: { title: 'Incremental coverage', extract: single('models_incremental', 3), unit: 'models', threshold: .06 },
} as const;
export type Measure = keyof typeof measures;

export function summarize(runs: Run[], batches: Batch[], measure: Measure = 'refresh'): Trend[] {
  const byCommit = new Map<string, Map<string, Batch[]>>();
  const ids = new Set<string>();
  for (const row of batches) {
    if (row.status !== 'succeeded' && row.status !== 'success') continue;
    ids.add(row.engine);
    const engines = byCommit.get(row.commit_sha) ?? new Map<string, Batch[]>();
    engines.set(row.engine, [...(engines.get(row.engine) ?? []), row]);
    byCommit.set(row.commit_sha, engines);
  }
  const ordered = [...Object.keys(palette).filter(id => ids.has(id)), ...[...ids].filter(id => !Object.hasOwn(palette, id)).sort()];
  const sorted = [...runs].sort((a, b) => a.committed_at - b.committed_at || a.commit_sha.localeCompare(b.commit_sha));
  return ordered.map(engine => {
    const histories = new Map<string, (number | undefined)[]>();
    const points = sorted.flatMap(run => {
      const history = histories.get(run.workload) ?? [];
      const values = history.filter(finite);
      const expected = values.length >= 3 ? median(values) : undefined;
      const value = measures[measure].extract(byCommit.get(run.commit_sha)?.get(engine) ?? []);
      histories.set(run.workload, [...history.slice(-9), value]);
      return value === undefined ? [] : [{ run, value, expected, delta: expected ? 100 * (value / expected - 1) : undefined }];
    });
    return { engine, points };
  });
}

export function findings(runs: Run[], batches: Batch[], latest: string): Finding[] {
  return findingsTimeline(runs,batches,new Set([latest])).get(latest) ?? [];
}

export function findingsTimeline(runs: Run[], batches: Batch[], selected: Set<string>) {
  const weight = { High: 3, Watch: 2, Gain: 1 };
  const found = new Map<string, Finding[]>();
  for (const key of Object.keys(measures) as Measure[]) for (const {engine,points} of summarize(runs,batches,key)) points.forEach((point,i) => {
    const { threshold, title, unit } = measures[key];
    if (!selected.has(point.run.commit_sha) || point.delta === undefined || Math.abs(point.delta) < threshold * 100 || point.expected === undefined) return;
    const worsening = key === 'coverage' ? point.delta < 0 : point.delta > 0;
    const level = worsening ? Math.abs(point.delta) >= 25 || key === 'coverage' ? 'High' : 'Watch' : 'Gain';
    const verb = key === 'coverage' || key === 'storage' || key === 'memory'
      ? point.delta > 0 ? ' increased' : ' decreased' : point.delta > 0 ? ' slower' : ' faster';
    const list = found.get(point.run.commit_sha) ?? [];
    list.push({
      engine, title: title + verb, level, point, trace: points.slice(Math.max(0,i-14),i+1),
      context: `${signalValue(point.value, unit)} vs ${signalValue(point.expected, unit)} trailing median`,
    });
    found.set(point.run.commit_sha,list);
  });
  for (const list of found.values()) list.sort((a,b)=>weight[b.level]-weight[a.level]||Math.abs(b.point.delta??0)-Math.abs(a.point.delta??0));
  return found;
}
