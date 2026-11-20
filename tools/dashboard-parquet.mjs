import { createRequire } from 'node:module';
import contract from '../website/contract.json' with { type: 'json' };

const require = createRequire(new URL('../website/package.json', import.meta.url));
const { parquetWriteFile } = await import(require.resolve('hyparquet-writer'));
export const sections = Object.keys(contract.sections);
const indexed = new Set(['section', 'committed_at', 'commit_sha']);
export const requiredFields = Object.assign({}, contract.common, ...Object.values(contract.sections));
export const retentionDays = 365;

export function writeDashboard(filename, snapshots, { dataset, generatedAt, workload, legacy = false, schemaVersion = contract.version }) {
  const sorted = [...snapshots].sort((a, b) => +a.run[0].committed_at - +b.run[0].committed_at
    || a.run[0].commit_sha.localeCompare(b.run[0].commit_sha));
  const groups = sections.flatMap(section => {
    const source = section === 'engine_batch' ? sorted.toReversed() : sorted;
    const size = section === 'run' ? Math.max(1, source.length) : section === 'engine_batch' ? 24 : 18;
    return Array.from({ length: Math.ceil(source.length / size) }, (_, i) =>
      source.slice(i * size, (i + 1) * size).flatMap(snapshot => snapshot[section])).filter(group => group.length);
  });
  const rows = groups.flat();
  const counts = Object.fromEntries(sections.map(section => [section, sorted.reduce((n, snapshot) => n + snapshot[section].length, 0)]));
  const fields = legacy ? requiredFields : { ...requiredFields, ...contract.optional.run };
  parquetWriteFile({
    filename, codec: 'SNAPPY', rowGroupSize: groups.map(group => group.length), statisticsTruncateLength: 64,
    kvMetadata: Object.entries({
      'ivmbench.schema_version': schemaVersion, dataset, generated_at: generatedAt, retention_days: String(retentionDays),
      workload, section_rows: JSON.stringify(counts),
    }).map(([key, value]) => ({ key, value })),
    columnData: Object.entries(fields).map(([name, type]) => ({
      name, type: type === 'TIMESTAMP_MILLIS' ? 'TIMESTAMP' : type,
      data: rows.map(row => {
        const value = row[name];
        return type === 'TIMESTAMP_MILLIS' && typeof value === 'number' ? new Date(value) : value ?? null;
      }), nullable: !indexed.has(name), statistics: indexed.has(name), offsetIndex: false,
    })),
  });
  return counts;
}
