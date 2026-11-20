import { createServer } from 'node:http';
import { createReadStream, statSync } from 'node:fs';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('./dist', import.meta.url));
const port = 4175;
const types = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.parquet': 'application/octet-stream' };

createServer((req, res) => {
  if (!['GET', 'HEAD'].includes(req.method)) { res.writeHead(405, { Allow: 'GET, HEAD' }).end(); return; }
  let pathname;
  try { pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname); }
  catch { res.writeHead(400).end(); return; }
  const file = resolve(root, `.${pathname === '/' ? '/index.html' : pathname}`);
  if (!file.startsWith(`${root}${sep}`)) { res.writeHead(404).end(); return; }
  let stat;
  try { stat = statSync(file); }
  catch (error) {
    if (error.code !== 'ENOENT' && error.code !== 'ENOTDIR') console.error(error.message);
    res.writeHead(error.code === 'ENOENT' || error.code === 'ENOTDIR' ? 404 : 500).end();
    return;
  }
  if (!stat.isFile()) { res.writeHead(404).end(); return; }
  const etag = `"${stat.size.toString(16)}-${stat.mtimeMs.toString(16)}"`;
  const headers = { ETag: etag, 'Accept-Ranges': 'bytes', 'Content-Type': types[extname(file)] ?? 'application/octet-stream', 'Cache-Control': 'no-cache' };
  if (req.headers['if-match'] && !req.headers['if-match'].split(',').some(tag => ['*', etag].includes(tag.trim()))) {
    res.writeHead(412, headers).end(); return;
  }
  const range = req.method === 'GET' ? req.headers.range : undefined;
  let start = 0, end = stat.size - 1;
  if (range) {
    const match = /^bytes=(\d*)-(\d*)$/.exec(range);
    if (match?.[1]) { start = Number(match[1]); end = match[2] ? Math.min(Number(match[2]), end) : end; }
    else if (match?.[2]) start = Math.max(0, stat.size - Number(match[2]));
    else start = stat.size;
    if (!Number.isSafeInteger(start) || start > end) {
      res.writeHead(416, { ...headers, 'Content-Range': `bytes */${stat.size}` }).end(); return;
    }
    headers['Content-Range'] = `bytes ${start}-${end}/${stat.size}`;
  }
  res.writeHead(range ? 206 : 200, { ...headers, 'Content-Length': Math.max(0, end - start + 1) });
  if (req.method === 'HEAD' || end < start) { res.end(); return; }
  const stream = createReadStream(file, { start, end });
  res.once('close', () => stream.destroy());
  stream.on('error', error => res.destroy(error)).pipe(res);
}).listen(port, '127.0.0.1', () => console.log(`IVM Bench: http://127.0.0.1:${port}/ (PID ${process.pid})`));
