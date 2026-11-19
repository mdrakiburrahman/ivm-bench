import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { convert } from './dashboard-convert.mjs';

test('emits one sanitized row per engine and log fingerprint across a multi-engine run', t => {
  const root = mkdtempSync(join(tmpdir(), 'dashboard-convert-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const write = (name, value) => {
    const path = join(root, name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, typeof value === 'string' ? value : JSON.stringify(value));
  };
  const localEngines = ['spark', 'duckdb'];
  const engines = Object.fromEntries(localEngines.map(engine => [engine, {
    batches: [{ batch_num: 1, duration_s: 2, status: 'completed' }],
  }]));
  write('ci-results/engine-coverage.json', { passed: true, experiments: [{
    engines: Object.fromEntries(localEngines.map(engine => [engine, { passed: true }])),
  }] });
  const oat = 'mount/oat-state/latest';
  write(`${oat}/outputs.json`, {});
  write(`${oat}/exp-000/outputs.json`, { status: 'completed', scale_factor: 10, inputs: { workload: 'test' }, engines });
  const warning = engine => `2026-03-01 09:00:00,000 [WARNING] services.engine_runner: Failed to fetch lineage for ${engine}: private details`;
  write(`${oat}/benchmark-server.log`, ['spark', 'duckdb', 'spark', 'untested'].map(warning).join('\n'));
  const { rows } = convert(root, { commit_sha: 'a'.repeat(40), committed_at: '2026-03-01T08:00:00Z' });
  assert.deepEqual(rows.log.map(({ engine, fingerprint, message, collapsed_count }) =>
    [engine, fingerprint, message, collapsed_count]), [
    ['spark', 'lineage-fetch', 'Failed to fetch lineage.', 2],
    ['duckdb', 'lineage-fetch', 'Failed to fetch lineage.', 1],
  ]);
});
