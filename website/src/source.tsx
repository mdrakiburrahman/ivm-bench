import { useState } from 'preact/hooks';
import contract from '../contract.json' with { type: 'json' };
import type { Dataset, Meter, Run, Section } from './data.ts';
import { Panel, human, pretty } from './details.tsx';

const panels: Record<Section, string> = {
  run: 'Commit history and provenance',
  engine_batch: 'Overview, engine trends and comparisons',
  model: 'Model timings and incrementalization',
  operator: 'SQL operator heatmap',
  lineage: 'Model dependency explorer',
  resource: 'Container CPU and memory timelines',
  log: 'Curated logs and log-pattern changes',
};
const stamp = (value: number) => new Date(value).toISOString().replace('T', ' ').slice(0, 16) + ' UTC';
const count = (value: number) => value.toLocaleString('en-US');
function showSource(source: string | File) {
  if (typeof source !== 'string') return source.name;
  try {
    const url = new URL(source, location.href);
    return url.origin === location.origin ? url.pathname : url.origin + url.pathname;
  } catch { return 'Invalid URL'; }
}
const sections = ['run','engine_batch','model','operator','lineage','resource','log'];
const colors = ['#3675a6','#07857c','#7564a7','#aa712f','#416c9e','#53826f','#a45579'];
function ByteMap({ dataset, meter }: { dataset:Dataset; meter:Meter }) {
  const {groups,footer}=dataset.layout;
  const rows=[...new Set(groups.map(g=>g.section))];
  const [detail,setDetail]=useState('');
  const left=148, width=900, yFooter=26+rows.length*23;
  const x=(offset:number)=>left+offset/dataset.size*width;
  const rangeRect=(start:number,end:number,y:number,fill:string,key:string)=> <rect key={key} x={x(start)} y={y} width={Math.max(.15,x(end)-x(start))} height="12" rx="1" fill={fill}/>;
  return <Panel title="Parquet byte map" note="Physical offsets · compressed column chunks grouped by section">
    <div class="byte-map"><svg viewBox={`0 0 1067 ${yFooter+28}`} aria-label="Compressed row groups and fetched HTTP byte ranges">
      {Array.from({length:5},(_,i)=>dataset.size*i/4).map(n=><g key={n}><text x={x(n)} y="13" text-anchor={n===0?'start':n===dataset.size?'end':'middle'}>{human(n,'B')}</text><line x1={x(n)} x2={x(n)} y1="19" y2={yFooter+14} class="gridline"/></g>)}
      {rows.map((section,i)=>{
        const y=25+i*23, color=colors[sections.indexOf(section)]??'#798b99';
        return <g key={section}><text x="0" y={y+10}>{section}</text><rect x={left} y={y} width={width} height="12" rx="2" fill="var(--soft)"/>
          {groups.filter(g=>g.section===section).map(g=>{
            const label=`Row group ${g.id} · ${section} · ${human(g.bytes,'B')} compressed · ${g.columns} columns`;
            return <g key={g.id} tabIndex={0} role="button" aria-label={label} onFocus={()=>setDetail(label)} onMouseEnter={()=>setDetail(label)} onClick={()=>setDetail(label)} onKeyDown={e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();setDetail(label);}}}>
              {g.spans.map((s,j)=>rangeRect(s.start,s.end,y,color,`${j}`))}
              {g.spans.flatMap((s,j)=>(meter.ranges??[]).flatMap((f,k)=>{
                const start=Math.max(s.start,f.start),end=Math.min(s.end,f.end);
                return start<end?[rangeRect(start,end,y,'var(--focus)',`${j}:${k}`)]:[];
              }))}
            </g>;
          })}
        </g>;
      })}
      <text x="0" y={yFooter+10}>Footer / range reads</text><rect x={left} y={yFooter} height="12" width={width} rx="2" fill="var(--soft)"/>
      {rangeRect(footer.start,footer.end,yFooter,'#8a949d','footer')}
      {meter.ranges?.map((r,i)=>rangeRect(r.start,r.end,yFooter,'var(--focus)',`range-${i}`))}
    </svg></div>
    <div class="byte-caption"><span><i/> Bytes fetched by this session</span><span>{meter.requests} HTTP ranges · {(meter.bytes / 1024).toFixed(1)} KB transferred</span><span aria-live="polite">{detail || 'Hover or focus a row group to inspect its size.'}</span></div>
  </Panel>;
}

export function DataSource({ source, dataset, meter, runs, loading, error, choose, reset, canReset }: {
  source: string | File; dataset?: Dataset; meter: Meter; runs: Run[]; loading: boolean; error: string;
  choose: (next: string | File) => void; reset: () => void; canReset: boolean;
}) {
  const [url, setUrl] = useState('');
  const [drag, setDrag] = useState(false);
  const report = dataset?.report;
  return <div class="detail-stack">
    <Panel title="Parquet source" note="One file feeds all supported views">
      <div class="source-grid"><div class="source-current"><span class="eyebrow">CURRENT SOURCE</span><strong>{showSource(source)}</strong>
        <div class="source-meta"><span class={`coverage-pill ${error ? 'high' : loading ? 'watch' : report ? 'gain' : 'log-info'}`}>{error ? 'Rejected' : loading ? 'Validating' : report ? 'Compatible' : 'Not validated'}</span>
          <span class="muted">{typeof source === 'string' ? 'Remote range reads' : 'Local file · not uploaded'}</span></div>
        {error && <p class="source-error" role="alert">{error}</p>}
        {!error && loading && <p class="muted" role="status">Reading Parquet metadata and recent batches…</p>}
        {canReset && <button class="control" onClick={reset}>Reset to default source</button>}
      </div>
      <div class="source-choose"><form onSubmit={e => { e.preventDefault(); if (url.trim()) choose(url.trim()); }}>
        <label for="parquet-url" class="eyebrow">LOAD FROM URL</label>
        <div><input id="parquet-url" class="field" placeholder="https://host/path/benchmark.parquet" value={url} onInput={e => setUrl(e.currentTarget.value)} />
          <button type="submit" class="control" disabled={!url.trim()}>Load URL</button></div>
      </form>
      <div class={`dropzone${drag ? ' drag' : ''}`} onDragOver={e => { e.preventDefault(); setDrag(true); }} onDragLeave={() => setDrag(false)} onDrop={e => {
        e.preventDefault(); setDrag(false); const file = e.dataTransfer?.files[0]; if (file) choose(file);
      }}><label><span>Drop a Parquet file here or choose a file</span><input type="file" accept=".parquet,application/vnd.apache.parquet" onChange={e => {
        const file = e.currentTarget.files?.[0]; if (file) choose(file); e.currentTarget.value = '';
      }} /></label></div></div></div>
    </Panel>
    {report && dataset && <><Panel title="Dataset provenance" note="File metadata and HTTP representation">
      <dl class="provenance">
        <div><dt>Schema version</dt><dd>{report.version}</dd></div><div><dt>Dataset</dt><dd>{report.dataset === 'synthetic' ? 'Synthetic' : 'Live CI'}</dd></div>
        <div><dt>Generated at</dt><dd>{stamp(report.generatedAt)}</dd></div><div><dt>Retention</dt><dd>{report.retention} days</dd></div>
        <div><dt>Workload</dt><dd>{report.workload}</dd></div><div><dt>File size</dt><dd title={`${count(meter.size)} B`}>{human(meter.size,'B')}</dd></div>
        <div><dt>ETag</dt><dd><code>{dataset.etag ?? 'Not applicable'}</code></dd></div>
        <div><dt>Published window</dt><dd>{runs.length ? `${new Date(runs[0].committed_at).toISOString().slice(0, 10)} – ${new Date(runs.at(-1)!.committed_at).toISOString().slice(0, 10)} UTC` : 'No published runs'}</dd></div>
      </dl>
    </Panel><ByteMap dataset={dataset} meter={meter}/><Panel title="Validation report" note="Required sections and optional panels · groups classified by section statistics">
      <div class="table-wrap"><table class="data-table validation-table"><thead><tr><th>Section</th><th>Rows</th><th>Row groups</th><th>Compressed</th><th>Columns</th><th>Status</th><th>Enables</th></tr></thead><tbody>
        {Object.keys(contract.sections).map(key => {
          const section = key as Section, note = report.unavailable.get(section), rows = report.counts.get(section) ?? 0;
          const groups=dataset.layout.groups.filter(g=>g.section===section);
          return <tr key={section}><th><code>{section}</code></th><td>{count(rows)}</td><td>{groups.length||'—'}</td><td>{groups.length?human(groups.reduce((n,g)=>n+g.bytes,0),'B'):'—'}</td><td>{groups.length?Math.max(...groups.map(g=>g.columns)):'—'}</td><td><span class={`coverage-pill ${note ? 'watch' : rows ? 'gain' : 'log-info'}`}>{note ? 'Incompatible' : rows ? 'Available' : 'Absent'}</span></td>
            <td>{note ?? panels[section]}</td></tr>;
        })}
        {[...report.counts].filter(([key]) => !Object.hasOwn(contract.sections, key)).map(([key, rows]) =>
          <tr key={key}><th><code>{key}</code></th><td>{count(rows)}</td><td colSpan={3}>—</td><td><span class="coverage-pill log-info">Ignored</span></td><td>{pretty(key)} is not used by this dashboard</td></tr>)}
      </tbody></table></div>
    </Panel></>}
  </div>;
}
