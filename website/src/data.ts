import { parquetMetadataAsync, parquetReadObjects } from 'hyparquet';
import type { AsyncBuffer, FileMetaData, SchemaElement } from 'hyparquet';
import contract from '../contract.json' with { type: 'json' };

export type Section = keyof typeof contract.sections;
type Cell = string | number | boolean | bigint | Date | null;
export type Row = Record<string, Cell>;
export type Run = Row & { commit_sha: string; committed_at: number; subject: string; branch: string; workload: string };
export type Batch = Row & { commit_sha: string; committed_at: number; engine: string; batch: number; duration_s: number | null };
export interface ByteRange { start: number; end: number }
export interface Meter { bytes: number; requests: number; size: number; ranges?: ByteRange[] }
const sections = Object.keys(contract.sections) as Section[];
const eager: Section[] = ['run', 'engine_batch'];
const fields = (section: Section) => ({ ...contract.common, ...contract.sections[section] });
const optionalRunColumns = Object.entries(contract.optional.run);
const core = ['section', 'commit_sha', 'committed_at', 'engine', 'batch'];

export function epoch(value: unknown): number {
  const n = value instanceof Date ? value.getTime() : typeof value === 'number' || typeof value === 'bigint' ? Number(value) : NaN;
  return Number.isSafeInteger(n) && Math.abs(n) <= 8.64e15 ? n : NaN;
}

function matches(field: SchemaElement | undefined, type: string) {
  if (!field) return false;
  const logical = field.logical_type;
  if (type === 'STRING') return field.type === 'BYTE_ARRAY' && (logical?.type === 'STRING' || (!logical && field.converted_type === 'UTF8'));
  if (type === 'TIMESTAMP_MILLIS') return field.type === 'INT64' && (logical?.type === 'TIMESTAMP'
    ? logical.isAdjustedToUTC && logical.unit === 'MILLIS' : !logical && field.converted_type === 'TIMESTAMP_MILLIS');
  return field.type === type && (!logical || (logical.type === 'INTEGER' && logical.isSigned && `INT${logical.bitWidth}` === type))
    && (!field.converted_type || field.converted_type === `INT_${type.slice(3)}`);
}

export function validate(metadata: FileMetaData) {
  const kv = new Map(metadata.key_value_metadata?.map(({ key, value }) => [key, value]));
  const version = kv.get('ivmbench.schema_version');
  function mismatch(message: string): never { throw new Error(`Schema mismatch: ${message}`); }
  if (!version || !/^1(?:\.\d+)?$/.test(version)) mismatch(`expected Parquet contract v1; got ${version ?? 'no version'}.`);
  const dataset = kv.get('dataset');
  if (dataset !== 'synthetic' && dataset !== 'ci') mismatch('dataset must be synthetic or ci.');
  const generatedAt = Date.parse(kv.get('generated_at') ?? '');
  const retention = Number(kv.get('retention_days'));
  const workload = kv.get('workload');
  if (!Number.isFinite(generatedAt) || !Number.isSafeInteger(retention) || retention < 1 || !workload) mismatch('missing or invalid provenance.');
  let declared: unknown;
  try { declared = JSON.parse(kv.get('section_rows') ?? 'null'); } catch { mismatch('invalid section_rows metadata.'); }
  if (!declared || typeof declared !== 'object' || Array.isArray(declared)) mismatch('missing section_rows metadata.');
  const counts = new Map<string, number>();
  for (const [section, count] of Object.entries(declared as Record<string, unknown>)) {
    if (typeof count !== 'number' || !Number.isSafeInteger(count) || count < 0) throw new Error(`Schema mismatch: invalid count for ${section}.`);
    counts.set(section, count);
  }
  if (BigInt([...counts.values()].reduce((sum, n) => sum + n, 0)) !== metadata.num_rows) mismatch('section row counts do not match the footer total.');
  const schema = new Map(metadata.schema.slice(1).map(field => [field.name, field]));
  if (metadata.schema[0].num_children !== schema.size || metadata.schema.length !== schema.size + 1) mismatch('expected a flat table with unique column names.');
  const panels = new Set<Section>();
  const unavailable = new Map<Section, string>();
  for (const section of sections) {
    const required = eager.includes(section);
    if (!required && !counts.get(section)) continue;
    const invalid = Object.entries(fields(section)).find(([name, type]) => !matches(schema.get(name), type));
    if (invalid && required) mismatch(`${section}.${invalid[0]} must be ${invalid[1]}.`);
    if (invalid) unavailable.set(section, `${invalid[0]} must be ${invalid[1]}`);
    else if (counts.get(section)) panels.add(section);
  }
  return { version, dataset, generatedAt, retention, workload, counts, panels, unavailable };
}

export function planGroups(metadata: FileMetaData, section: Section, filter: { since?: number; at?: number; sha?: string } = {}) {
  let offset = 0;
  return metadata.row_groups.flatMap(group => {
    const start = offset;
    offset += Number(group.num_rows);
    function bounds(key: string) {
      const stat = group.columns.find(c => c.meta_data?.path_in_schema[0] === key)?.meta_data?.statistics;
      return [stat?.min_value, stat?.max_value];
    }
    const [a, b] = bounds('section');
    const [first, last] = bounds('committed_at').map(epoch);
    const [minSha, maxSha] = bounds('commit_sha');
    if ((typeof a === 'string' && section < a) || (typeof b === 'string' && section > b)) return [];
    if (filter.since !== undefined && last < filter.since) return [];
    if (filter.at !== undefined && (filter.at < first || filter.at > last)) return [];
    if (filter.sha && ((typeof minSha === 'string' && filter.sha < minSha) || (typeof maxSha === 'string' && filter.sha > maxSha))) return [];
    return [{ start, end: offset }];
  });
}

export function parquetLayout(metadata: FileMetaData, size: number) {
  const groups = metadata.row_groups.map((group, i) => {
    const stats = group.columns.find(c => c.meta_data?.path_in_schema[0] === 'section')?.meta_data?.statistics;
    const section = typeof stats?.min_value === 'string' && stats.min_value === stats.max_value ? stats.min_value : 'mixed / unknown';
    const chunks = group.columns.flatMap(({ meta_data: m, file_path }) => {
      if (!m || file_path) return [];
      const start = Number(m.dictionary_page_offset ?? m.data_page_offset), end = start + Number(m.total_compressed_size);
      return Number.isSafeInteger(start) && Number.isSafeInteger(end) && start >= 4 && end <= size && end > start ? [{ start, end }] : [];
    }).sort((a, b) => a.start - b.start);
    const spans: ByteRange[] = [];
    for (const chunk of chunks) {
      const previous = spans.at(-1);
      if (previous && previous.end >= chunk.start) previous.end = Math.max(previous.end, chunk.end);
      else spans.push({ ...chunk });
    }
    return { id: i + 1, section, spans, bytes: chunks.reduce((n, x) => n + x.end - x.start, 0), columns: group.columns.length };
  });
  return { groups, footer: { start: Math.max(4, size - metadata.metadata_length - 8), end: size } };
}

async function request(url: string, init: RequestInit) {
  try { return await fetch(url, { ...init, credentials: 'omit', cache: 'no-store' }); } catch (error) {
    if (init.signal?.aborted) throw error;
    throw new Error('Cannot reach the Parquet URL. Check network access and CORS for HEAD, GET, Range and If-Match.', { cause: error });
  }
}

export async function openDataset(source: string | File, update: (meter: Meter) => void, signal?: AbortSignal) {
  const meter: Meter = { requests: 0, bytes: 0, size: 0, ranges: [] };
  let etag: string | null = null;
  let file: AsyncBuffer;
  if (typeof source !== 'string') {
    meter.size = source.size;
    file = { byteLength: source.size, slice: (start, end) => source.slice(start, end).arrayBuffer() };
  } else {
    const url = new URL(source, location.href);
    if (!['http:', 'https:'].includes(url.protocol)) throw new Error('Enter an HTTP or HTTPS Parquet URL.');
    const info = await request(url.href, { method: 'HEAD', signal });
    if (info.status === 404) throw new Error('No published runs: Parquet file not found (404).');
    if (!info.ok) throw new Error(`Parquet request failed (HTTP ${info.status}).`);
    const length = Number(info.headers.get('Content-Length'));
    if (!Number.isSafeInteger(length) || length < 8) throw new Error('Parquet host must expose a valid Content-Length.');
    etag = info.headers.get('ETag');
    if (!etag || etag.startsWith('W/')) throw new Error('Parquet host must expose a strong ETag to pin range reads.');
    const pinned = etag;
    meter.size = length;
    file = {
      byteLength: length,
      async slice(start, end = length) {
        const response = await request(url.href, {
          headers: { Range: `bytes=${start}-${end - 1}`, 'If-Match': pinned }, signal,
        });
        if (response.status === 412) throw new Error('The Parquet file changed during this read. Reload to use the new version.');
        if (response.status !== 206 || response.headers.get('Content-Range') !== `bytes ${start}-${end - 1}/${length}`) {
          await response.body?.cancel();
          throw new Error('Expected HTTP 206 with Content-Range. Check range support and CORS response headers.');
        }
        const data = await response.arrayBuffer();
        if (data.byteLength !== end - start) throw new Error('The Parquet range response was incomplete.');
        meter.requests++;
        meter.bytes += data.byteLength;
        meter.ranges!.push({start,end});
        update({ ...meter, ranges:[...meter.ranges!] });
        return data;
      },
    };
  }
  update({ ...meter });
  const metadata = await parquetMetadataAsync(file, { initialFetchSize: 64 * 1024 });
  const report = validate(metadata);
  const cached = new Map<string, Promise<Row[]>>();
  async function read(section: Section, filter: { since?: number; at?: number; sha?: string } = {}, selected?: string[]) {
    if (!report.panels.has(section)) return [];
    const optional = section === 'run' ? optionalRunColumns.filter(([name, type]) =>
      matches(metadata.schema.find(field => field.name === name), type)).map(([name]) => name) : [];
    const columns = selected ? [...new Set([...core, ...selected])] : [...Object.keys(fields(section)), ...optional];
    const results: Row[] = [];
    const groups = planGroups(metadata, section, filter);
    for (let i = 0; i < groups.length; i += 4) {
      const blocks = await Promise.all(groups.slice(i, i + 4).map(({ start, end }) => {
        const key = `${start}:${columns.join(',')}`;
        let pending = cached.get(key);
        if (!pending) {
          pending = parquetReadObjects({ file, metadata, columns, rowStart: start, rowEnd: end });
          cached.set(key, pending);
          void pending.catch(() => cached.delete(key));
        }
        return pending;
      }));
      for (const row of blocks.flat()) {
        if (row.section !== section || (filter.sha && row.commit_sha !== filter.sha)) continue;
        const at = epoch(row.committed_at);
        if (!Number.isFinite(at) || typeof row.commit_sha !== 'string' || !/^[0-9a-f]{40,64}$/.test(row.commit_sha)) {
          throw new Error(`Schema mismatch: invalid commit identity in ${section}.`);
        }
        if ((filter.since !== undefined && at < filter.since) || (filter.at !== undefined && at !== filter.at)) continue;
        if (section === 'engine_batch' && (typeof row.engine !== 'string' || typeof row.batch !== 'number' || !Number.isInteger(row.batch) || row.batch < 1)) {
          throw new Error('Schema mismatch: batch rows require an engine and a positive batch number.');
        }
        results.push({ ...row, committed_at: at });
      }
    }
    return results;
  }
  return { read, report, etag, size: meter.size, source, layout:parquetLayout(metadata,meter.size) };
}

export type Dataset = Awaited<ReturnType<typeof openDataset>>;
