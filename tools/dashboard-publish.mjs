import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { createRequire } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout } from 'node:timers/promises';
import { openDataset } from '../website/src/data.ts';
import { convert } from './dashboard-convert.mjs';
import { retentionDays, sections, writeDashboard } from './dashboard-parquet.mjs';

const require = createRequire(new URL('../website/package.json', import.meta.url));
const { BlobServiceClient, StorageSharedKeyCredential } = await import(require.resolve('@azure/storage-blob'));
const account = process.env.BENCHMARK_STORAGE_ACCOUNT_NAME;
const key = process.env.BENCHMARK_STORAGE_ACCOUNT_KEY;
const container = process.env.BENCHMARK_STORAGE_ACCOUNT_CONTAINER_PARQUET_PUBLIC;
const exactName = /^[a-f0-9]{40}\.parquet$/;
const days = 86_400_000;

async function publish(root, identityPath) {
  if (!account || !/^[a-z0-9]{3,24}$/.test(account) || !key || container !== 'parquet') {
    throw new Error('Missing or invalid benchmark storage settings.');
  }
  const identity = JSON.parse(await readFile(identityPath, 'utf8'));
  const { rows, workload } = convert(root, identity);
  const generatedAt = new Date().toISOString();
  const cutoff = Date.parse(generatedAt) - retentionDays * days;
  if (+rows.run[0].committed_at < cutoff) throw new Error('Commit is outside the retention window.');
  const service = new BlobServiceClient(`https://${account}.blob.core.windows.net`, new StorageSharedKeyCredential(account, key));
  const blobs = service.getContainerClient(container);
  const server = blobs.getBlockBlobClient('_server.parquet');
  const blobOptions = { blobHTTPHeaders: { blobContentType: 'application/vnd.apache.parquet', blobCacheControl: 'no-cache' } };
  const temp = await mkdtemp(join(tmpdir(), 'ivm-dashboard-'));
  try {
    const snapshot = join(temp, 'commit.parquet');
    const merged = join(temp, 'server.parquet');
    writeDashboard(snapshot, [rows], { dataset: 'ci', workload, generatedAt });
    await blobs.getBlockBlobClient(`${identity.commit_sha}.parquet`).uploadFile(snapshot, blobOptions);
    for (let attempt = 0; attempt < 4; attempt++) {
      try {
        const current = await server.getProperties().catch(error => {
          if (error.statusCode !== 404) throw error;
          return undefined;
        });
        const kept = [], expired = [];
        for await (const item of blobs.listBlobsFlat()) {
          if (!exactName.test(item.name)) continue;
          const blob = blobs.getBlobClient(item.name);
          const data = await blob.downloadToBuffer(0, undefined, { conditions: { ifMatch: item.properties.etag } });
          const source = await openDataset(new File([data], item.name), () => {});
          const runs = await source.read('run');
          if (source.report.dataset !== 'ci' || runs.length !== 1 || runs[0].commit_sha !== item.name.slice(0, 40)) {
            throw new Error(`Invalid snapshot ${item.name}.`);
          }
          if (runs[0].committed_at < cutoff) expired.push({ blob, etag: item.properties.etag });
          else kept.push(Object.fromEntries(await Promise.all(sections.map(async section =>
            [section, section === 'run' ? runs : await source.read(section)]))));
        }
        if (!kept.some(snapshot => snapshot.run[0].commit_sha === identity.commit_sha)) throw new Error('New snapshot was not listed.');
        writeDashboard(merged, kept, { dataset: 'ci', workload, generatedAt });
        await server.uploadFile(merged, {
          ...blobOptions,
          conditions: current ? { ifMatch: current.etag } : { ifNoneMatch: '*' },
        });
        for (const { blob, etag } of expired) {
          await blob.deleteIfExists({ conditions: { ifMatch: etag } }).catch(error => {
            if (error.statusCode !== 412) throw error;
          });
        }
        console.log(`Published ${identity.commit_sha}. _server.parquet contains ${kept.length} snapshots.`);
        return;
      } catch (error) {
        if (![404, 412].includes(error.statusCode) || attempt === 3) throw error;
        await setTimeout(250 * 2 ** attempt);
      }
    }
  } finally {
    await rm(temp, { recursive: true, force: true });
  }
}

try {
  const [root, identityPath] = process.argv.slice(2);
  if (!root || !identityPath) throw new Error('Usage: dashboard-publish.mjs <artifact root> <identity JSON>');
  await publish(root, identityPath);
} catch (error) {
  console.error(error.statusCode ? `Dashboard publish failed: Azure HTTP ${error.statusCode}.`
    : `Dashboard publish failed: ${error.message}`);
  process.exitCode = 1;
}
