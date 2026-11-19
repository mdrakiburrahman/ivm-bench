import { useState } from 'preact/hooks';
import contract from '../contract.json' with { type: 'json' };
import type { Dataset, Row, Run } from './data.ts';
import { epoch } from './data.ts';
import { Lineage, Panel, ReadState, EngineTag, githubRunUrl, num, pretty, useSection, versions } from './details.tsx';
import type { Nav } from './details.tsx';
import { DAY, date, engineInfo, seconds, signed } from './metrics.ts';
import type { Finding } from './metrics.ts';

type Props = { dataset: Dataset; runs: Run[]; run: Run; engines: string[]; filters: URLSearchParams; nav: Nav };
const str = (v: Row[string] | undefined) => String(v ?? '');
const operatorFields = Object.keys(contract.sections.operator).filter(k => k !== 'model_id');
const short = (sha: string) => sha.slice(0, 7);
function Choices({ runs, run, engine, engines, nav, page }: Props & { engine: string; page: string }) {
  return <div class="detail-controls">
    <label class="inline-field">Engine <select value={engine} onChange={e => { location.hash = nav(page, { engine: e.currentTarget.value }); }}>
      {page === 'signals' && <option value="">All engines</option>}
      {engines.map(id => <option key={id} value={id}>{engineInfo(id)[0]}</option>)}
    </select></label>
    <label class="inline-field">Published commit <select value={run.commit_sha} onChange={e => { location.hash = nav(page, { commit: e.currentTarget.value }); }}>
      {[...runs].reverse().map(r => <option key={r.commit_sha} value={r.commit_sha}>{short(r.commit_sha)} · {r.subject}</option>)}
    </select></label>
  </div>;
}
export function Heuristics(props: Props & { query: string }) {
  const { dataset, run, engines, filters, nav, query } = props;
  const engine = filters.get('engine') || (engines.includes('spark-openivm') ? 'spark-openivm' : engines[0]);
  const models = useSection(dataset, 'model', run);
  const batches = [...new Set(models.rows.filter(r => r.engine === engine && typeof r.batch === 'number').map(r => Number(r.batch)))].sort((a, b) => a - b);
  const requestedBatch = Number(filters.get('batch'));
  const batch = batches.includes(requestedBatch) ? requestedBatch : batches.includes(3) ? 3 : batches.at(-1) ?? 3;
  const operators = useSection(dataset, 'operator', run);
  const lineage = useSection(dataset, 'lineage', run);
  const [more, setMore] = useState(false);
  const [mode, setMode] = useState<'all' | 'full'>('all');
  const names = new Map(models.rows.filter(r => r.engine === engine).map(r => [r.model_id, str(r.model_name)]));
  const label = (id: Row[string]) => names.get(id) ?? str(id).split('.').at(-1) ?? '';
  const matched = (id: Row[string]) => label(id).toLowerCase().includes(query.trim().toLowerCase());
  const modelRows = models.rows.filter(r => r.engine === engine && r.batch === batch && matched(r.model_id));
  const layerByModel = new Map(models.rows.filter(r => r.engine === engine).map(r=>[r.model_id,str(r.layer)]));
  const order = ['bronze','silver','gold'];
  const opRows = operators.rows.filter(r => r.engine === engine && (r.batch == null || r.batch === batch) && matched(r.model_id))
    .sort((a,b)=>order.indexOf(layerByModel.get(a.model_id)??'')-order.indexOf(layerByModel.get(b.model_id)??''));
  const layerStats = new Map<string, { count: number; incremental: number; duration: number }>();
  for (const row of modelRows) {
    const layer = str(row.layer) || 'unclassified';
    const stats = layerStats.get(layer) ?? { count: 0, incremental: 0, duration: 0 };
    stats.count += typeof row.incremental === 'boolean' ? 1 : 0;
    stats.incremental += row.incremental === true ? 1 : 0; stats.duration += num(row.execution_time_s) ?? 0;
    layerStats.set(layer, stats);
  }
  const timings = [...layerStats.entries()];
  const maxDuration = Math.max(1, ...timings.map(([, s]) => s.duration));
  const byModel = new Map<string, Map<string,Row>>();
  for (const row of models.rows.filter(r=>r.batch===batch&&matched(r.model_id))) {
    const group=byModel.get(str(row.model_id))??new Map<string,Row>();
    group.set(str(row.engine),row); byModel.set(str(row.model_id),group);
  }
  const coverage = [...byModel.entries()].filter(([,group]) => mode === 'all' || [...group.values()].some(r=>r.incremental===false));
  return <div class="detail-stack">
    <Panel title="Model evidence" note={`${short(run.commit_sha)} · ${run.subject}`}>
      <Choices {...props} engine={engine} page="heuristics" />
      {dataset.report.panels.has('model') && <div class="detail-controls evidence-scope"><span class="muted">Batch</span>
        {batches.map(n => <a key={n} class={`control${batch === n ? ' current' : ''}`} href={nav('heuristics', { batch: `${n}` })}>Batch {n}</a>)}
        <span class="badge">{modelRows.length} invocations in scope</span></div>}
    </Panel>
    {dataset.report.panels.has('operator') && <Panel title="SQL operator heatmap" note="Compiled SQL · counts per model, not query-plan cost">
      <ReadState state={operators} />
      {opRows.length > 0 && <div class="table-wrap"><table class="data-table operator-table"><thead><tr><th>Model</th>
        {operatorFields.map(field => <th key={field}>{pretty(field)}</th>)}</tr><tr class="operator-totals"><th>Totals · {opRows.length} models</th>{operatorFields.map(field=><th key={field}>{opRows.reduce((n,r)=>n+(num(r[field])??0),0)}</th>)}</tr></thead><tbody>
        {opRows.slice(0, more ? undefined : 12).map((row,i) => <>{(i===0||layerByModel.get(row.model_id)!==layerByModel.get(opRows[i-1].model_id))&&<tr class="layer-divider"><th colSpan={operatorFields.length+1}>{layerByModel.get(row.model_id)||'Unclassified'}</th></tr>}<tr key={str(row.model_id)}><th><code>{label(row.model_id)}</code></th>
          {operatorFields.map(field => {
            const value = num(row[field]);
            return <td key={field} class={value ? 'operator-count' : 'muted'} style={{ '--strength': `${Math.min(58, (value ?? 0) * 11)}%` }}>{value===0?'':value ?? '—'}</td>;
          })}</tr></>)}
      </tbody></table></div>}
      {!opRows.length && !operators.loading && !operators.error && <p class="empty">No operator counts for this selection.</p>}
      {opRows.length > 12 && <div class="detail-controls"><button class="control" onClick={() => setMore(!more)}>{more ? 'Show first 12 models' : `Show all ${opRows.length} models`}</button></div>}
    </Panel>}
    <div class="detail-pair">
      {dataset.report.panels.has('model') && <Panel title="Per-layer model time" note={`Batch ${batch} · sum of model timings, not wall time`}>
        <ReadState state={models} />{timings.length ? <div class="layer-list">{timings.map(([layer, stats]) => <div key={layer}>
          <strong>{layer}</strong><div class="bar-track"><i style={{ width: `${100 * stats.duration / maxDuration}%` }} /></div>
          <b>{seconds(stats.duration)} s</b><span class="muted">{stats.count ? `${stats.incremental}/${stats.count} classified incremental` : 'Classification not recorded'}</span>
        </div>)}</div> : !models.loading && <p class="empty">No model timings for this selection.</p>}
      </Panel>}
      {dataset.report.panels.has('lineage') && <Panel title="Model lineage" note="Direct upstream and downstream dependencies">
        <ReadState state={lineage} /><Lineage key={`${run.commit_sha}:${engine}`} rows={lineage.rows.filter(row => row.engine === engine)} models={modelRows} />
      </Panel>}
    </div>
    {dataset.report.panels.has('model') && <Panel title="Incrementalization matrix" note={`Batch ${batch} · hover or focus a cell for its reason`}>
      <div class="detail-controls"><div class="segments"><button aria-pressed={mode === 'all'} onClick={() => setMode('all')}>All models</button><button aria-pressed={mode === 'full'} onClick={() => setMode('full')}>Full refresh only</button></div>
        <span class="muted">{coverage.length} / {byModel.size} models shown</span></div>
      <ReadState state={models} />
      {coverage.length ? <div class="table-wrap evidence-scroll"><table class="data-table incremental-table"><thead><tr><th>Model</th>
        {engines.map(id=>{const known=[...byModel.values()].map(group=>group.get(id)).filter(r=>typeof r?.incremental==='boolean'); return <th key={id}><b class="mono-head" title={engineInfo(id)[0]} style={{'--engine':engineInfo(id)[2]}}>{engineInfo(id)[1]}</b><span class="matrix-total" title="Incremental / classified models">{known.length ? `${known.filter(r=>r?.incremental===true).length}/${known.length}` : '—'}</span></th>})}</tr></thead><tbody>
        {coverage.map(([id,group]) => <tr key={id}><th><code>{str(group.values().next().value?.model_name) || label(id)}</code></th>
          {engines.map(e=>{const r=group.get(e);const reason=str(r?.incremental_reason)||'Not recorded'; const value=r?.incremental===true?'Incremental':r?.incremental===false?'Full refresh':'—';
            return <td key={e}><span tabIndex={r?0:undefined} title={reason} aria-label={`${value} · ${reason}`} class={`coverage-pill ${r?.incremental===true?'gain':r?.incremental===false?'watch':''}`}>{value}</span></td>})}</tr>)}
      </tbody></table></div> : !models.loading && <p class="empty">No matching model classifications.</p>}
    </Panel>}
    {!(['model', 'operator', 'lineage'] as const).some(section => dataset.report.panels.has(section)) && <Panel title="No heuristics sections"><p class="empty">This file does not contain model, operator, or lineage evidence.</p></Panel>}
  </div>;
}

export function logChanges(current: Row[], previous: Row[]) {
  const index = (rows: Row[]) => {
    const values = new Map<string, number>();
    for (const row of rows) {
      const key = `${row.engine}:${row.fingerprint}`;
      values.set(key, (values.get(key) ?? 0) + (num(row.collapsed_count) ?? 1));
    }
    return values;
  };
  const before = index(previous), after = index(current);
  return new Map([...after].flatMap(([key, count]) => {
    const prior = before.get(key);
    const kind = prior === undefined ? 'New' : count >= 3 && count >= prior * 2 ? 'Spike' : undefined;
    return kind ? [[key, { count, prior, kind }] as const] : [];
  }));
}
export function SignalEvidence(props: Props & { signals: Finding[]; timeline: Map<string,Finding[]>; inWindow: Run[]; feed: preact.ComponentChildren }) {
  const { dataset, runs, run, engines, filters, nav } = props;
  const engine = filters.get('engine') ?? '';
  const [search, setSearch] = useState(filters.get('log') ?? '');
  const level = filters.get('level') ?? '';
  const onlyNew = filters.get('new') === '1';
  const previous = [...runs].reverse().find(r => r.committed_at < run.committed_at && r.workload === run.workload);
  const logs = useSection(dataset, 'log', run);
  const baseline = useSection(dataset, 'log', previous);
  const [historyOpen, setHistoryOpen] = useState(false);
  const history = useSection(dataset, 'log', undefined, historyOpen);
  const [grouped, setGrouped] = useState(false);
  const sameEngine = (r: Row) => !engine || r.engine === engine;
  const current = logs.rows.filter(sameEngine);
  const previousRows = baseline.rows.filter(sameEngine);
  const changes = previous && !baseline.loading && !baseline.error ? logChanges(current, baseline.rows.filter(sameEngine)) : new Map();
  const keyOf = (r:Row) => `${r.engine}:${r.fingerprint}`;
  const currentKeys = new Set(current.map(keyOf));
  const vanished = previousRows.filter(r=>!currentKeys.has(keyOf(r)));
  const levelRank = (r: Row) => ['ERROR', 'FATAL'].includes(str(r.level)) ? 0 : ['WARN', 'WARNING'].includes(str(r.level)) ? 1 : 2;
  const logOrder = (a: Row, b: Row) => levelRank(a) - levelRank(b)
    || Number(changes.has(keyOf(b))) - Number(changes.has(keyOf(a)))
    || epoch(a.logged_at) - epoch(b.logged_at);
  const ranked = current.filter(row => levelRank(row) < 2 && changes.has(keyOf(row))).sort(logOrder);
  const visible = current.filter(row => (!level || row.level === level) && (!onlyNew || changes.get(`${row.engine}:${row.fingerprint}`)?.kind === 'New')
    && `${row.source} ${row.message} ${row.fingerprint}`.toLowerCase().includes(search.trim().toLowerCase()))
    .sort(logOrder);
  const stale = dataset.report.dataset === 'ci' && Date.now() - runs.at(-1)!.committed_at > 7 * DAY;
  const versionsNow = versions(run), versionsBefore = previous ? versions(previous) : new Map();
  const versionChanges = engines.filter(id => (!engine || id === engine) && versionsNow.has(id) && versionsBefore.has(id) && versionsNow.get(id) !== versionsBefore.get(id));
  const url = githubRunUrl(run);
  const affected = new Set(props.signals.map(s=>s.engine)).size;
  const severity = [['High','High'],['Medium','Watch'],['Low / gain','Gain']] as const;
  const levelCount = (kind:string) => current.filter(r=>r.level===kind).length;
  const patterns = new Map<string,{row:Row; seen:number; gone:boolean}>();
  for (const row of visible) {
    const key=keyOf(row), prev=patterns.get(key);
    patterns.set(key,{row,seen:(prev?.seen??0)+(num(row.collapsed_count)??1),gone:false});
  }
  if (grouped&&!onlyNew) for (const row of vanished.filter(r=>(!level||r.level===level)&&`${r.source} ${r.message} ${r.fingerprint}`.toLowerCase().includes(search.trim().toLowerCase()))) {
    const key=keyOf(row); patterns.set(key,{row,seen:0,gone:true});
  }
  const historyRuns=[...runs].filter(r=>r.committed_at<=run.committed_at).slice(-16);
  const occurrence = new Map<string,Map<string,number>>();
  if(historyOpen) for(const row of history.rows.filter(sameEngine)){
    const key=keyOf(row), map=occurrence.get(key)??new Map<string,number>(), sha=str(row.commit_sha);
    map.set(sha,(map.get(sha)??0)+(num(row.collapsed_count)??1));occurrence.set(key,map);
  }
  return <div class="detail-stack">
    <Panel title="Signal scope" note={`${short(run.commit_sha)} · ${run.subject}`}><Choices {...props} engine={engine} page="signals" />
      {url && <div class="detail-controls"><a class="control" href={url} target="_blank" rel="noopener noreferrer">Open GitHub run ↗</a></div>}
      {!url && dataset.report.dataset==='synthetic'&&<div class="detail-controls"><a class="control" href="https://github.com/openivm/ivm-bench/actions/runs/44259810551" target="_blank" rel="noopener noreferrer">Calibration run on GitHub ↗</a><span class="muted">Synthetic commits do not have workflow runs.</span></div>}
    </Panel>
    <Panel title="Severity and reach" note="Selected commit · performance and log evidence">
      <div class="severity-grid">{severity.map(([label,kind])=><div key={kind}><small>{label}</small><strong class={kind==='High'?'red':kind==='Gain'?'green':''}>{props.signals.filter(s=>s.level===kind).length}</strong></div>)}
      <div><small>Engines affected</small><strong>{affected}</strong></div><div><small>New warning / error patterns</small><strong>{ranked.filter(r=>changes.get(keyOf(r))?.kind==='New').length}</strong></div></div>
    </Panel>
    <Panel title="Signal timeline" note="Select a dot to inspect the commit · performance signals">
      <div class="table-wrap"><table class="data-table signal-grid"><thead><tr><th>Engine</th>{props.inWindow.map((r,i)=><th key={r.commit_sha} title={`${date(r.committed_at)} · ${short(r.commit_sha)}`}>{i%5===0||i===props.inWindow.length-1?date(r.committed_at):''}</th>)}</tr></thead><tbody>
      {engines.filter(id=>!engine||engine===id).map(id=><tr key={id}><th><EngineTag id={id}/></th>{props.inWindow.map(r=>{
        const found=(props.timeline.get(r.commit_sha)??[]).filter(s=>s.engine===id);
        const first=found[0], detail=`${short(r.commit_sha)} · ${r.subject} · ${versions(r).get(id)??'Version unavailable'} · ${found.length} signals${first?` · ${first.title}`:''}`;
        return <td key={r.commit_sha}><a class={`signal-dot ${first?.level.toLowerCase()??'none'}${r===run?' chosen':''}`} href={nav('signals',{commit:r.commit_sha})} title={detail} aria-label={`${engineInfo(id)[0]} · ${detail}`}>{found.length||'·'}</a></td>;
      })}</tr>)}</tbody></table></div>
    </Panel>
    {props.feed}
    {(stale || versionChanges.length > 0 || ranked.length > 0) && <Panel title="Run annotations" note={previous ? `Compared with ${short(previous.commit_sha)} · same workload` : 'No previous comparable run'}>
      <div class="annotation-list">
        {stale && <div><span class="severity watch">Stale</span><strong>No published run in over 7 days</strong></div>}
        {versionChanges.map(id => {
          const shift = props.signals.find(s => s.engine === id);
          return <div key={id}><span class="severity watch">Version</span><EngineTag id={id} /><span>{versionsBefore.get(id)} → {versionsNow.get(id)}</span>
            {shift && <span>{shift.title} on this commit ({signed(shift.point.delta ?? 0)} vs expected)</span>}</div>;
        })}
        {[...new Map(ranked.map(row => [`${row.engine}:${row.fingerprint}`, row])).entries()].map(([key, row]) => {
          const change = changes.get(key)!;
          return <div key={key}><span class="severity high">{change.kind} pattern</span><EngineTag id={str(row.engine)} /><code>{str(row.fingerprint)}</code>
            <span>{change.count} occurrences{change.prior ? ` vs ${change.prior}` : ''}</span></div>;
        })}
      </div>
    </Panel>}
    {dataset.report.panels.has('log') ? <Panel title="Curated engine logs" note={`${dataset.report.dataset === 'synthetic' ? 'Synthetic' : 'Sanitized'} excerpts · errors and warnings first · UTC · repeated lines collapsed`}>
      <div class="detail-controls">
        <div class="segments small" aria-label="Log level">{['','ERROR','WARN','INFO',...[...new Set(current.map(r=>str(r.level)))].filter(s=>!['ERROR','WARN','INFO'].includes(s))].map(k=><a key={k} class={k===level?'current':''} aria-current={k===level?'true':undefined} href={nav('signals',{level:k})}>{k||'All'} · {k?levelCount(k):current.length}</a>)}</div>
        <label class="inline-field">Text <input aria-label="Filter logs" class="field" placeholder="Message or source" value={search} onInput={e => setSearch(e.currentTarget.value)} /></label>
        <label class="inline-field"><input type="checkbox" checked={onlyNew} disabled={!previous || baseline.loading || !!baseline.error} onChange={e => { location.hash = nav('signals', { new: e.currentTarget.checked ? '1' : '' }); }} /> New in this commit</label>
        <span class="badge">{visible.length} excerpts</span>
        <div class="segments small"><button aria-pressed={!grouped} onClick={()=>setGrouped(false)}>Log stream</button><button aria-pressed={grouped} onClick={()=>{setGrouped(true);setHistoryOpen(true);}}>Group by pattern</button></div>
      </div>
      <ReadState state={logs} /><ReadState state={baseline} />
      {grouped&&<ReadState state={history}/>}
      {(grouped?patterns.size:visible.length) > 0 ? <div class="table-wrap evidence-scroll"><table class="data-table logs-table"><thead><tr><th>{grouped?'Pattern history':'UTC'}</th><th>Level</th><th>Engine</th><th>Source / message</th><th>Occurrences</th></tr></thead>
        <tbody>{(grouped?[...patterns.values()].sort((a,b)=>logOrder(a.row,b.row)):visible.map(row=>({row,seen:num(row.collapsed_count)??1,gone:false}))).map(({row,seen,gone}, i) => {
          const at = epoch(row.logged_at);
          const change = changes.get(`${row.engine}:${row.fingerprint}`);
          const severity=['ERROR','FATAL'].includes(str(row.level))?'high':['WARN','WARNING'].includes(str(row.level))?'watch':'log-info';
          const counts=historyRuns.map(r=>occurrence.get(keyOf(row))?.get(r.commit_sha)??0), max=Math.max(1,...counts);
          return <tr class={`log-row ${severity}-line`} key={`${row.engine}:${row.fingerprint}:${i}`}><td>{grouped&& !history.loading && !history.error?<svg class="log-trace" viewBox="0 0 96 24" aria-label="Occurrences by commit" role="img"><polyline fill="none" stroke="var(--teal)" points={counts.map((n,j)=>`${2+92*j/Math.max(1,counts.length-1)},${22-19*n/max}`).join(' ')}/>{counts.map((n,j)=><circle cx={2+92*j/Math.max(1,counts.length-1)} cy={22-19*n/max} r="2" fill="var(--teal)" key={j}><title>{short(historyRuns[j].commit_sha)} · {historyRuns[j].subject} · {versions(historyRuns[j]).get(str(row.engine))??'Version unavailable'} · {n}</title></circle>)}</svg>:<code>{grouped?'—':Number.isFinite(at)?new Date(at).toISOString().slice(11,19):'—'}</code>}</td>
            <td><span class={`severity ${severity}`}>{str(row.level)}</span></td>
            <td><EngineTag id={str(row.engine)} /></td><td><details><summary><small>{str(row.source)}</small> {str(row.message)}</summary><code>{str(row.fingerprint)} · {seen} occurrences in selected commit</code></details></td>
            <td>{seen} {gone?<span class="coverage-pill watch">Gone since previous</span>:change&&<span class="coverage-pill watch">{change.kind==='New'?'New in this commit':change.kind}</span>}</td></tr>;
        })}</tbody>
      </table></div> : !logs.loading && !logs.error && <p class="empty">No log excerpts match the current filters.</p>}
    </Panel> : <Panel title="No curated log section"><p class="empty">This file can still provide performance signals from published batch measurements.</p></Panel>}
  </div>;
}
