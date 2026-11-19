import assert from 'node:assert/strict';
import { test } from 'node:test';
import type { Batch, Run } from './data.ts';
import { batchHistory, batchInsertPercent, findings, summarize } from './metrics.ts';

test('batch duration uses one SHA-ordered domain with gaps at missing measurements', () => {
  const run = (sha: string, time: number): Run => ({ commit_sha: sha, committed_at: time, subject: '', branch: 'main', workload: 'standard' });
  const [a, b, c] = [run('a', 1), run('b', 1), run('c', 2)];
  const row = (commit: Run, batch: number, duration_s: number | null): Batch => ({
    commit_sha: commit.commit_sha, committed_at: commit.committed_at, engine: 'spark', batch, duration_s,
  });
  const result = batchHistory([c, b, a], [row(a, 1, 200), row(c, 1, 300), row(b, 2, 0), row(c, 2, null), row(a, 3, 5)], () => true, '');
  assert.deepEqual(result.domain.map(r => r.commit_sha), ['a', 'b', 'c']);
  assert.deepEqual(result.charts.map(chart => chart.series[0].durations), [
    [200, undefined, 300], [undefined, 0, undefined], [5, undefined, undefined],
  ]);
});

test('batch insert percentages come from each run and stay unavailable for legacy or invalid data', () => {
  const run: Run = { commit_sha: 'a', committed_at: 1, subject: '', branch: 'main', workload: 'standard' };
  const withData = { ...run, batch_insert_pct_json: '{"1":100,"2":0,"3":2.5,"4":101}' };
  assert.deepEqual([1, 2, 3, 4].map(batch => batchInsertPercent(withData, batch)), [100, 0, 2.5, undefined]);
  assert.equal(batchInsertPercent(run, 2), undefined);
  assert.equal(batchInsertPercent({ ...run, batch_insert_pct_json: '{' }, 2), undefined);
  const later = { ...run, commit_sha: 'b', committed_at: 2, batch_insert_pct_json: '{"3":1}' };
  assert.deepEqual(batchHistory([withData, later], [], () => true, '').charts[2].insert, [2.5, 1]);
});

test('refresh baseline excludes current, other-workload, and incomplete batches', () => {
  const runs: Run[] = Array.from({ length: 7 }, (_, i) => ({
    commit_sha: `${i}`, committed_at: i, subject: 'Test fixture', branch: 'main', workload: i === 4 ? 'other' : 'standard',
  }));
  const rows: Batch[] = runs.flatMap((run, i) => (i === 5 ? [1, 2] : [1, 2, 3]).map(batch => ({
    commit_sha: run.commit_sha, committed_at: run.committed_at, engine: 'spark', batch, status: 'succeeded',
    duration_s: batch === 1 ? 3000 : [5, 7, 4, 6, 500, 300, 11][i],
  })));
  const last = summarize(runs, rows)[0].points.at(-1)!;
  assert.equal(last.value, 22);
  assert.equal(last.expected, 11);
  assert.equal(last.delta, 100);
  assert.equal(findings(runs, rows, '6')[0]?.level, 'High');
});
