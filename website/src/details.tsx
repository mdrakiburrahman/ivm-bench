import { useEffect, useRef, useState } from 'preact/hooks';
import type { Dataset, Row, Run, Section } from './data.ts';
import { date, engineInfo, findings, median, seconds, signed, summarize } from './metrics.ts';
import { epoch } from './data.ts';
import type { Batch } from './data.ts';

export type Nav = (page: string, change?: Record<string, string>) => string;
export const num = (v: Row[string] | undefined) => typeof v === 'number' && Number.isFinite(v) ? v : undefined;
const text = (value: Row[string] | undefined) => String(value ?? '—');
const decimal = (n: number) => new Intl.NumberFormat('en-US', { maximumSignificantDigits: 3 }).format(n);
export function human(v: Row[string] | undefined, unit = '') {
  if (typeof v !== 'number' && typeof v !== 'bigint') return '—';
  const n = Number(v);
  if (unit === 'B' || unit === 'MB') {
    const scale = unit === 'B' ? n >= 1e9 ? [1e9, 'GB'] : n >= 1e6 ? [1e6, 'MB'] : n >= 1e3 ? [1e3, 'KB'] : [1, 'B'] : n >= 1024 ? [1024, 'GB'] : [1, 'MB'];
    return `${decimal(n / Number(scale[0]))} ${scale[1]}`;
  }
  return `${unit === 'models' ? Math.round(n) : decimal(n)}${unit ? ` ${unit}` : ''}`;
}
const fmt = human;
const isFullRefreshBaseline = (id: string, incremental: number | undefined) => incremental === 0 && (id === 'spark' || id === 'duckdb');
const pct = (n: number | undefined, base: number | undefined) => n !== undefined && base ? 100 * (n / base - 1) : undefined;
const fmtDelta = (v: number | undefined) => v === undefined ? '—' : signed(v);
const short = (sha: string) => sha.slice(0, 7);
export const pretty = (key: string) => key.replaceAll('_', ' ');
export function versions(run: Run) {
  try {
    const obj: unknown = JSON.parse(String(run.engine_versions_json ?? '{}'));
    return obj && typeof obj === 'object' && !Array.isArray(obj) ? new Map(Object.entries(obj).filter(([, v]) => typeof v === 'string')) : new Map<string, string>();
  } catch { return new Map<string, string>(); }
}
export function useSection(dataset: Dataset, section: Section, run?: Run, all = false) {
  const key = all ? '*' : run?.commit_sha;
  const [state, set] = useState<{ dataset: Dataset; key?: string; section: Section; rows: Row[]; error?: string }>();
  useEffect(() => {
    if (!key || !dataset.report.panels.has(section)) return;
    let active = true;
    dataset.read(section, !all && run ? { at: run.committed_at, sha: run.commit_sha } : {}).then(rows => {
      if (active) set({ dataset, key, section, rows });
    }).catch((error: Error) => {
      if (active) set({ dataset, key, section, rows: [], error: error.message });
    });
    return () => { active = false; };
  }, [dataset, section, key, all, run]);
  const current = state?.key === key && state?.dataset === dataset && state?.section === section ? state : undefined;
  return { rows: current?.rows ?? [], error: current?.error, loading: !!key && dataset.report.panels.has(section) && !current };
}
export function ReadState({ state }: { state: ReturnType<typeof useSection> }) {
  return state.error ? <p class="empty" role="alert">{state.error}</p> : state.loading ? <p class="empty" role="status">Reading Parquet row groups…</p> : null;
}
export function Panel({ title, note, children }: { title: string; note?: string; children: preact.ComponentChildren }) {
  return <section class="panel detail-panel"><div class="panel-head"><div class="panel-title"><h2>{title}</h2><span class="muted">{note}</span></div></div>{children}</section>;
}
export function EngineTag({ id }: { id: string }) {
  const [label, mono, color] = engineInfo(id);
  return <span class="engine-tag" style={{ '--engine': color }}><b>{mono}</b>{label}</span>;
}
type PlotSet = { name: string; color: string; dashed?: boolean; points: { x: number; y: number; tip: string }[] };
function Plot({ sets, label, unit, time = false, log = false, step = false, limit, marks = [] }: {
  sets: PlotSet[]; label: string; unit: string; time?: boolean; log?: boolean; step?: boolean; limit?: number; marks?: { x: number; version: string }[];
}) {
  const [active, setActive] = useState<{ x: number; y: number; tip: string }>();
  const [width, setWidth] = useState(700);
  const plot = useRef<SVGSVGElement>(null);
  const hasData = sets.some(s => s.points.length > 0);
  useEffect(() => {
    const node = plot.current;
    if (!node) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(160, entry.contentRect.width)));
    observer.observe(node);
    return () => observer.disconnect();
  }, [hasData]);
  const all = sets.flatMap(s => s.points);
  if (!all.length) return <p class="empty">No measurements in scope.</p>;
  const min = Math.min(...all.map(p => p.x)), max = Math.max(...all.map(p => p.x));
  const highest = Math.max(...all.map(p => p.y), .01);
  const targetStep = limit ? Math.max(1, limit / 5) : highest / 4;
  const magnitude = 10 ** Math.floor(Math.log10(targetStep));
  const tick = ((limit ? [1, 2, 5, 10] : [1, 2, 2.5, 5, 10]).find(n => n * magnitude >= targetStep) ?? 10) * magnitude;
  const low = log ? 10 ** Math.floor(Math.log10(Math.min(...all.filter(p => p.y > 0).map(p => p.y), 1))) : 0;
  const high = log ? Math.max(low * 10, 10 ** Math.ceil(Math.log10(highest))) : limit ?? Math.ceil(highest / tick) * tick;
  const top = Math.max(high, .01);
  const left = 60, right = width - 25;
  const x = (n: number) => left + (right - left) * (n - min) / (max - min || 1);
  const y = (n: number) => 136 - 111 * (log ? Math.log(Math.max(n, low) / low) / Math.log(top / low) : n / top);
  const ticks = log ? Array.from({ length: Math.round(Math.log10(top / low)) + 1 }, (_, i) => low * 10 ** i)
    : Array.from({ length: Math.floor(top / tick + 1e-9) + 1 }, (_, i) => tick * i);
  const tickCount = width < 375 ? 3 : 5;
  const domain = max === min ? [min] : Array.from({ length: tickCount }, (_, i) => min + (max - min) * i / (tickCount - 1));
  return <div class="mini-plot" onMouseLeave={() => setActive(undefined)}><svg ref={plot} viewBox={`0 0 ${width} 160`} aria-label={label} preserveAspectRatio="none">
    {ticks.map(t => <g key={t}><line x1={left} x2={right} y1={y(t)} y2={y(t)} class="gridline" /><text x={left - 7} y={y(t) + 4} text-anchor="end">{fmt(t, unit)}</text></g>)}
    {domain.map(t => <text key={t} x={x(t)} y="153" text-anchor="middle">{time ? date(t) : `${Math.round(t)}${label.includes('Container') ? ' s' : ''}`}</text>)}
    {marks.filter(m => m.x >= min && m.x <= max).map(({ x: at, version }) => <g key={at}><line x1={x(at)} x2={x(at)} y1="23" y2="136" class="crosshair" /><text x={Math.min(right - 20, x(at) + 3)} y="17">{version}</text></g>)}
    {sets.map(s => <g key={s.name}><path class="plot-line" fill="none" stroke={s.color} stroke-dasharray={s.dashed ? '5 3' : undefined}
      d={s.points.map((p, i) => !i ? `M${x(p.x)},${y(p.y)}` : step ? `H${x(p.x)}V${y(p.y)}` : `L${x(p.x)},${y(p.y)}`).join('')} />
      {s.points.map((p, i) => <circle key={i} cx={x(p.x)} cy={y(p.y)} r={active === p ? 4 : 2.65} fill={s.color} tabIndex={0} aria-label={`${p.tip} · ${fmt(p.y, unit)}`} onBlur={() => setActive(undefined)} onFocus={() => setActive(p)} onMouseEnter={() => setActive(p)} />)}</g>)}
    {active && <line pointer-events="none" class="crosshair" x1={x(active.x)} x2={x(active.x)} y1="24" y2="136" />}
  </svg>{active && <div class="mini-tooltip" role="status">{active.tip} <strong>· {fmt(active.y, unit)}</strong></div>}
    <div class="plot-meta"><span>{sets.map(s => <span class="plot-key" key={s.name}><i style={{ background: s.color }} />{s.name}</span>)}</span></div></div>;
}
export function Lineage({ rows, models = [], baseline = [] }: { rows: Row[]; models?: Row[]; baseline?: Row[] }) {
  const [selected, set] = useState<string>();
  const nodes = rows.filter(r => r.depends_on == null);
  const focus = nodes.find(r => r.model_id === selected) ?? nodes.find(r => r.role === 'target') ?? nodes[0];
  if (!focus) return <p class="empty">No lineage nodes for this engine and commit.</p>;
  const edges = rows.filter(r => r.depends_on != null);
  const walk = (key: 'model_id'|'depends_on', other: 'model_id'|'depends_on') => {
    const seen = new Set([focus.model_id]), frontier = [focus.model_id];
    while (frontier.length) {
      const id = frontier.pop();
      for (const edge of edges) if (edge[key] === id && !seen.has(edge[other])) { seen.add(edge[other]); frontier.push(edge[other]); }
    }
    return seen;
  };
  const connected = new Set([...walk('model_id','depends_on'), ...walk('depends_on','model_id')]);
  const groups = ['bronze','silver','gold','other'].map(layer => ({
    layer,
    nodes: nodes.filter(n => layer === 'other' ? !['bronze','silver','gold'].includes(text(n.layer)) : n.layer === layer)
      .sort((a,b)=>Number(connected.has(b.model_id))-Number(connected.has(a.model_id))).slice(0,5),
  })).filter(group => group.nodes.length);
  const byId = new Map(groups.flatMap((group,i) => group.nodes.map((n,j) => [n.model_id, {x: 8 + i * 172, y: 25 + j * 38}] as const)));
  const modelById = new Map(models.map(r=>[r.model_id,r]));
  const old = new Map(baseline.map(r=>[r.model_id,num(r.execution_time_s)]));
  const width = groups.length * 172;
  return <div class="lineage"><label class="inline-field">Focus model <select value={text(focus.model_id)} onChange={e => set(e.currentTarget.value)}>
    {nodes.map(n => <option key={text(n.model_id)} value={text(n.model_id)}>{text(n.model_name)}</option>)}
  </select></label><div class="dag-scroll"><svg class="lineage-dag" viewBox={`0 0 ${width} 218`} aria-label="Layered dbt model dependency graph">
    {groups.map(({layer},i)=><text key={layer} class="dag-label" x={8+i*172} y="14">{layer.toUpperCase()}</text>)}
    {edges.map((row,i)=>{
      const a=byId.get(row.depends_on), b=byId.get(row.model_id);
      if(!a||!b || a.x>=b.x) return null;
      const x=a.x+143, y=a.y+14, dest=b.y+14, midpoint=(x+b.x)/2;
      return <path key={i} class={connected.has(row.depends_on)&&connected.has(row.model_id)?'dag-edge active':'dag-edge'} d={`M${x} ${y} C${midpoint} ${y},${midpoint} ${dest},${b.x} ${dest}`} />;
    })}
    {groups.flatMap(({nodes})=>nodes.map(n=>{
      const {x,y}=byId.get(n.model_id)!;
      const model=modelById.get(n.model_id), delta=pct(num(model?.execution_time_s),old.get(n.model_id));
      const state=delta!==undefined&&Math.abs(delta)>=8 ? delta>0?'slow':'fast' : model?.incremental===true?'fast' : model?.incremental===false?'full':'';
      const label=text(n.model_name);
      return <g key={text(n.model_id)} class={`dag-node ${state}${n===focus?' chosen':''}${connected.has(n.model_id)?'':' dim'}`} tabIndex={0} role="button" aria-label={`${label} · ${text(n.layer)}`}
        onClick={()=>set(text(n.model_id))} onKeyDown={e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();set(text(n.model_id));}}}>
        <title>{label} · {delta===undefined? text(model?.incremental_reason):signed(delta)}</title>
        <rect x={x} y={y} width="143" height="28" rx="5"/><text x={x+7} y={y+18}>{label.length>23?`${label.slice(0,21)}…`:label}</text>
      </g>;
    }))}
  </svg></div><div class="heat-key"><span>Blue edge: in focus</span><span class="fast">Incremental / faster</span><span class="slow">Slower</span><span class="watch">Full refresh</span></div></div>;
}
function ModelBars({ rows, baseline = [] }: { rows: Row[]; baseline?: Row[] }) {
  const [limit, setLimit] = useState(12);
  const ranked = [...rows].filter(r => num(r.execution_time_s) !== undefined).sort((a, b) => num(b.execution_time_s)! - num(a.execution_time_s)!);
  const max = num(ranked[0]?.execution_time_s) || 1;
  const byModel = new Map(baseline.map(r => [r.model_id, num(r.execution_time_s)]));
  if (!ranked.length) return <p class="empty">No model timings for this selection.</p>;
  return <div class="model-bars">{ranked.slice(0, limit).map(row => {
    const n = num(row.execution_time_s)!;
    const delta = pct(n, byModel.get(row.model_id));
    return <div class="model-bar" key={text(row.model_id)}><code title={text(row.model_name)}>{text(row.model_name)}</code><span class="muted">{text(row.layer)}</span><div class="bar-track"><i style={{ width: `${100 * n / max}%` }} /></div><strong>{fmt(n, 's')}</strong><span class={delta && delta > 0 ? 'red' : 'green'}>{fmtDelta(delta)}</span></div>;
  })}{ranked.length > limit && <button class="control" onClick={() => setLimit(ranked.length)}>Show all {ranked.length} models</button>}</div>;
}

interface PageProps { dataset: Dataset; runs: Run[]; run: Run; filters: URLSearchParams; nav: Nav; engines: string[]; from: number; query: string }

export function Engines({ dataset, runs, run, filters, nav, engines, from, query }: PageProps) {
  const requested = filters.get('engine');
  const engine = requested && engines.includes(requested) ? requested : '';
  const details = useSection(dataset, 'engine_batch', run);
  const archive = useSection(dataset, 'engine_batch', undefined, true);
  const [log, setLog] = useState(false);
  const models = useSection(dataset, 'model', engine ? run : undefined);
  const resources = useSection(dataset, 'resource', engine ? run : undefined);
  const previous = [...runs].reverse().find(r => r.committed_at < run.committed_at && r.workload === run.workload);
  const priorResources = useSection(dataset, 'resource', engine ? previous : undefined);
  const comparison = useSection(dataset, 'model', engine ? previous : undefined);
  const batch = Number(filters.get('batch')) || 3;
  const current = details.rows.filter(r => r.engine === engine);
  const thirdBatch = current.find(r => r.batch === 3);
  const incrementalCount = num(thirdBatch?.models_incremental);
  const modelCount = num(thirdBatch?.models_total);
  const failedCount = num(thirdBatch?.models_failed);
  const active = current.find(r => r.batch === batch);
  const runMap = new Map(runs.map(r => [r.commit_sha, r]));
  const history = archive.rows.filter(r => r.engine === engine && num(r.committed_at)! >= from).sort((a, b) => num(a.committed_at)! - num(b.committed_at)!);
  const color = engineInfo(engine)[2];
  const versionMarks = runs.flatMap((r, i) => {
    const version = versions(r).get(engine);
    const before = i ? versions(runs[i - 1]).get(engine) : undefined;
    return version && before && version !== before ? [{ x: r.committed_at, version }] : [];
  });
  const samples = summarize(runs, archive.rows as Batch[]);
  const activeFindings = findings(runs, archive.rows as Batch[], run.commit_sha);
  const fleet = engines.map(id => {
    const rows = details.rows.filter(r => r.engine === id);
    const first = rows.find(r => r.batch === 1), second = rows.find(r => r.batch === 2), third = rows.find(r => r.batch === 3);
    const values = rows.map(r => num(r.duration_s) ?? 0);
    const trend = samples.find(s => s.engine === id)?.points ?? [];
    const p = trend.find(s => s.run.commit_sha === run.commit_sha);
    const cpu = [second, third].map(r => num(r?.compute_cpu_time_s));
    return { id, values, point: p, trend: trend.filter(p => p.run.committed_at >= from), third,
      metrics: [num(first?.duration_s), p?.value, cpu.every(v => v !== undefined) ? cpu[0]! + cpu[1]! : undefined,
        num(third?.peak_memory_mb), num(third?.helper_over_visible), num(third?.models_incremental)] };
  });
  const scopedModels = (rows: Row[]) => rows.filter(r => r.engine === engine && r.batch === batch);
  const oldModels = scopedModels(comparison.rows);
  const priorById = new Map(oldModels.map(r => [r.model_id, num(r.execution_time_s)]));
  const sampled = resources.rows.filter(r => r.engine === engine && r.batch === batch).sort((a, b) => num(a.sample_index)! - num(b.sample_index)!);
  const regressions = scopedModels(models.rows).flatMap(r => {
    const delta = pct(num(r.execution_time_s), priorById.get(r.model_id));
    return delta !== undefined && delta >= 10 ? [{ r, delta }] : [];
  }).sort((a, b) => b.delta - a.delta).slice(0, 6);
  const trend = (field: string, batches: number[]) => batches.map((n, i) => ({
    name: `Batch ${n}`, color: [color, `color-mix(in srgb, ${color} 68%, var(--text))`, `color-mix(in srgb, ${color} 70%, var(--card))`][i], dashed: n > 1,
    points: history.filter(r => r.batch === n && num(r[field]) !== undefined).map(r => {
      const commit = runMap.get(text(r.commit_sha));
      const version = commit && versions(commit).get(engine);
      return { x: num(r.committed_at)!, y: num(r[field])!, tip: `${short(text(r.commit_sha))} · ${commit?.subject ?? ''} · ${version ?? 'Version unavailable'}` };
    }),
  }));
  const timeline = (field: string) => {
    const series = (rows: Row[], commit: Run, name: string, dashed = false) => {
      const sorted = rows.filter(r => r.engine === engine && r.batch === batch).sort((a,b) => epoch(a.sample_at) - epoch(b.sample_at));
      const start = epoch(sorted[0]?.sample_at);
      return { name, color: dashed ? '#798b9b' : color, dashed, points: sorted.flatMap(r => {
        const x = (epoch(r.sample_at) - start) / 1000, y = num(r[field]);
        return !Number.isFinite(x) || y === undefined ? [] : [{ x, y, tip: `${short(commit.commit_sha)} · ${commit.subject} · ${versions(commit).get(engine) ?? 'Version unavailable'} · ${text(r.container)}` }];
      }) };
    };
    return [...(previous ? [series(priorResources.rows, previous, 'Previous commit', true)] : []), series(sampled, run, 'Selected commit')];
  };
  const reference = (field: string) => median(archive.rows.filter(r => r.engine === engine && r.batch === batch && num(r.committed_at)! < run.committed_at
    && runMap.get(text(r.commit_sha))?.workload === run.workload)
    .sort((a,b) => num(b.committed_at)! - num(a.committed_at)!).slice(0, 10).flatMap(r => typeof r[field] === 'bigint' ? [Number(r[field])] : num(r[field]) === undefined ? [] : [num(r[field])!]));
  const visible = engines.filter(id => `${id} ${engineInfo(id)[0]}`.toLowerCase().includes(query.toLowerCase()));
  return <div class="detail-stack">
    <Panel title={engine ? engineInfo(engine)[0] : 'Engine fleet'} note={`${short(run.commit_sha)} · ${run.subject}`}>
      {engine ? <div class="detail-controls"><a class="control" href={nav('engines', { engine: '', batch: '' })}>All engines</a>
        <select aria-label="Engine" value={engine} onChange={e => { location.hash = nav('engines', { engine: e.currentTarget.value }); }}>
          {engines.map(id => <option key={id} value={id}>{engineInfo(id)[0]}</option>)}
        </select><select aria-label="Engine commit" value={run.commit_sha} onChange={e => { location.hash = nav('engines', { commit: e.currentTarget.value }); }}>
          {[...runs].reverse().map(r => <option key={r.commit_sha} value={r.commit_sha}>{short(r.commit_sha)} · {r.subject}</option>)}
        </select>{versions(run).get(engine) && <span class="badge">v{versions(run).get(engine)}</span>}
        <a class="control" href={nav('signals', { engine, commit: run.commit_sha })}>Logs and signals →</a>
      </div> : <div class="fleet-grid">{fleet.filter(f => visible.includes(f.id)).map(f => {
        const { id, point, trend, third } = f;
        const total = num(third?.models_total), inc = num(third?.models_incremental);
        const max = Math.max(.01, ...f.values), peak = Math.max(.01, ...trend.map(p => p.value));
        const delta = point?.delta;
        const signalCount = activeFindings.filter(s => s.engine === id).length;
        return <a class="fleet-card" style={{ '--engine': engineInfo(id)[2] }} href={nav('engines', { engine: id })} key={id}><div class="fleet-heading"><EngineTag id={id} />{versions(run).get(id) && <span class="badge">v{versions(run).get(id)}</span>}</div>
          <div class="fleet-heading"><div class="reading"><strong>{point ? seconds(point.value) : '—'}</strong><span>s · batches 2 + 3</span></div><b class={`delta ${delta === undefined ? 'flat' : delta > 0 ? 'slow' : 'fast'}`}>{fmtDelta(delta)}</b></div>
          <svg class="fleet-spark" viewBox="0 0 160 25" preserveAspectRatio="none" aria-label="Combined duration history, batches 2 and 3"><polyline class="plot-line" fill="none" stroke="var(--engine)" points={trend.map((p,i) => `${i * 160 / Math.max(1, trend.length - 1)},${24 - p.value / peak * 22}`).join(' ')} /></svg>
          <div class="fleet-batches">{f.values.map((n,i) => <div key={i} title={`Batch ${i + 1} · ${fmt(n,'s')}`}><span>B{i + 1}</span><div class="bar-track"><i style={{width:`${100*n/max}%`}} /></div></div>)}</div>
          <div class="fleet-heading muted"><span>{fmt(num(third?.peak_memory_mb), 'MB')} peak</span><span>{signalCount} {signalCount === 1 ? 'signal' : 'signals'}</span></div>
          <div class="fleet-heading muted"><span>{inc === undefined || total === undefined ? 'Coverage not recorded' : isFullRefreshBaseline(id, inc) ? 'n/a · full refresh' : `${inc} / ${total} incremental`}</span><span class="fleet-arrow">→</span></div>
          {inc !== undefined && total !== undefined && <div class="bar-track"><i style={{width:`${total ? inc / total * 100 : 0}%`}} /></div>}
        </a>;
      })}</div>}
      <ReadState state={details} />
    </Panel>
    {!engine && <Panel title="Fleet comparison" note="Batch 1 initial load · batches 2 + 3 refresh and CPU · batch 3 resource and coverage">
      <ReadState state={archive} />
      <div class="detail-controls">{[['spark-openivm','spark'],['duckdb-openivm','duckdb']].flatMap(([a,b]) => {
        const x = fleet.find(f=>f.id===a)?.point?.value, y = fleet.find(f=>f.id===b)?.point?.value;
        return x && y ? [<span class="badge" key={a}>{engineInfo(a)[0]} vs {engineInfo(b)[0]} · {fmt(x >= y ? x/y : y/x,'×')} {x >= y ? 'slower' : 'faster'}</span>] : [];
      })}</div>
      <div class="table-wrap"><table class="data-table fleet-table"><thead><tr>{['Engine','Initial load','Incremental refresh','CPU time','Peak memory','Helper / visible','Coverage'].map(k => <th key={k}>{k}</th>)}</tr></thead><tbody>
      {fleet.filter(f=>visible.includes(f.id)).map(f=><tr key={f.id}><th><a href={nav('engines',{engine:f.id})}><EngineTag id={f.id}/></a></th>{f.metrics.map((n,i)=>{
        const fullRefresh = i === 5 && isFullRefreshBaseline(f.id, n);
        return <td key={i}>{n === undefined || (i === 4 && n === 0) || fullRefresh ? <span class="muted">{fullRefresh ? 'n/a · full refresh' : '—'}</span>
          : <><span>{fmt(n,['s','s','CPU s','MB','×','models'][i])}</span><div class="bar-track"><i style={{width:`${100*n/Math.max(.00001,...fleet.map(e=>e.metrics[i]??0))}%`,background:engineInfo(f.id)[2]}}/></div></>}</td>;
      })}</tr>)}
      </tbody></table></div>
    </Panel>}
    {engine && <>
      <div class="detail-pair"><Panel title="Duration by commit" note="Wall clock · dashed marker = version change"><ReadState state={archive} /><div class="detail-controls"><div class="segments small"><button aria-pressed={!log} onClick={()=>setLog(false)}>Linear</button><button aria-pressed={log} onClick={()=>setLog(true)}>Log scale</button></div></div><Plot label="Batch durations over time" unit="s" time log={log} marks={versionMarks} sets={trend('duration_s', [1, 2, 3])} /></Panel>
        <Panel title="Incremental coverage over time" note="Batch 3 · model count"><ReadState state={archive} /><Plot label="Incremental model coverage" unit="models" time step limit={Math.max(1, ...history.map(r=>num(r.models_total)??0))} sets={trend('models_incremental', [3])} />
        {incrementalCount !== undefined && modelCount !== undefined && failedCount !== undefined &&
          <Breakdown values={[[incrementalCount,'Incremental','#159375'],[Math.max(0,modelCount-incrementalCount-failedCount),'Full refresh','#b78032'],[failedCount,'Failed','#c43957']]} unit="models" />}</Panel></div>
      <Panel title="Batch measurements" note="Select a batch to scope the evidence">
        <div class="detail-controls">{[...new Set(current.map(r => Number(r.batch)))].sort((a,b) => a-b).map(n => <a class={`control${n === batch ? ' current' : ''}`} key={n} href={nav('engines', { batch: `${n}` })}>Batch {n}</a>)}</div>
        <div class="fact-grid">{[
          ['Duration','duration_s','s'], ['CPU time','compute_cpu_time_s','CPU s'], ['Peak memory','peak_memory_mb','MB'],
          ['Helper / visible','helper_over_visible','×'], ['Visible output','visible_output_bytes','B'], ['Helper state','helper_data_bytes','B'],
        ].map(([label, field, unit]) => {
          const measurement = active?.[field];
          const value = (field === 'helper_over_visible' || field === 'helper_data_bytes') && (measurement === 0 || measurement === 0n) ? undefined : measurement;
          const diff = pct(typeof value === 'bigint' ? Number(value) : num(value), reference(field));
          return <div key={field}><small>{label}</small><strong>{fmt(value,unit)}</strong><span class={`tile-delta ${diff === undefined ? 'muted' : diff > 0 ? 'red' : 'green'}`}>{diff === undefined ? 'No comparable baseline' : `${signed(diff)} vs trailing median`}</span></div>;
        })}</div>
        <Breakdown values={[[Number(active?.visible_output_bytes??0),'Visible output','#128578'],[Number(active?.helper_data_bytes??0),'Helper state','#8255ad'],[Number(active?.metadata_bytes??0),'Metadata','#8799a6']]} unit="B" hideZero />
      </Panel>
      {dataset.report.panels.has('resource') && <div class="detail-pair">
        <Panel title="Container CPU" note={`Batch ${batch} · sampled`}><ReadState state={resources} /><Plot sets={timeline('cpu_pct')} label="Container CPU" unit="%" /></Panel>
        <Panel title="Container memory" note={`Batch ${batch} · sampled`}><ReadState state={resources} /><Plot sets={timeline('memory_mb')} label="Container memory" unit="MB" /></Panel>
      </div>}
      {dataset.report.panels.has('model') && <div class="detail-pair"><Panel title="Slowest models" note={`Batch ${batch}`}><ReadState state={models} /><ModelBars rows={scopedModels(models.rows)} baseline={oldModels} /></Panel>
        <Panel title="Regressed models" note={previous ? `≥10% slower vs ${short(previous.commit_sha)}` : 'No previous comparable commit'}><ReadState state={comparison} />
          {regressions.length ? <div class="rank-list">{regressions.map(({ r, delta }) => <div key={text(r.model_id)}><code>{text(r.model_name)}</code><b class="red">{signed(delta)}</b></div>)}</div> : !comparison.loading && <p class="empty">No comparable model regressions in this batch.</p>}</Panel></div>}
    </>}
  </div>;
}
function Breakdown({ values, unit, hideZero = false }: { values: [number,string,string][]; unit: string; hideZero?: boolean }) {
  const total = values.reduce((n,[v])=>n+v,0);
  if (!total) return null;
  return <div class="breakdown"><div class="break-bar">{values.map(([value,label,color])=><i key={label} title={`${label} · ${fmt(value,unit)}`} style={{width:`${value/total*100}%`,background:color}} />)}</div>
    <div>{values.map(([v,label,color])=><span class={`plot-key${hideZero && v === 0 ? ' muted' : ''}`} key={label}><i style={{background:color}}/>{label} · {hideZero && v === 0 ? '—' : fmt(v,unit)}</span>)}</div></div>;
}

export function Commits({ dataset, runs, run, filters, nav, engines, from, query }: PageProps) {
  const history = useSection(dataset, 'engine_batch', undefined, true);
  const detail = filters.has('commit');
  const choice = filters.get('compare') ?? 'previous';
  const comparable = [...runs].reverse().filter(r => r.commit_sha !== run.commit_sha && r.workload === run.workload);
  const other = choice === 'previous' || choice === 'main'
    ? comparable.find(r => r.committed_at < run.committed_at && (choice !== 'main' || r.branch === 'main'))
    : comparable.find(r => r.commit_sha === choice);
  const engine = filters.get('engine') ?? engines[0] ?? '';
  const batch = Number(filters.get('batch')) || 3;
  const models = useSection(dataset, 'model', detail ? run : undefined);
  const baseline = useSection(dataset, 'model', detail ? other : undefined);
  const lineage = useSection(dataset, 'lineage', detail ? run : undefined);
  const grouped = new Map<string, Map<string, Row>>();
  for (const row of history.rows) {
    const sha = text(row.commit_sha);
    if (!grouped.has(sha)) grouped.set(sha, new Map());
    grouped.get(sha)!.set(`${row.engine}:${row.batch}`, row);
  }
  const refresh = (sha: string, id: string) => {
    const rows = grouped.get(sha);
    const a = num(rows?.get(`${id}:2`)?.duration_s), b = num(rows?.get(`${id}:3`)?.duration_s);
    return a === undefined || b === undefined ? undefined : a + b;
  };
  const branch = filters.get('branch') ?? '';
  const visible = [...runs].reverse().filter(r => r.committed_at >= from && (!branch || (branch === 'main' ? r.branch === 'main' : r.pr_number != null))
    && `${r.commit_sha} ${r.subject} ${r.branch} ${r.author} ${r.pr_number ?? ''}`.toLowerCase().includes(query.toLowerCase()));
  const previousByCommit = new Map<string, Run>();
  const latestWorkload = new Map<string, Run>();
  for (const r of runs) {
    const prev = latestWorkload.get(r.workload);
    if (prev) previousByCommit.set(r.commit_sha, prev);
    latestWorkload.set(r.workload, r);
  }
  const scope = (rows: Row[]) => rows.filter(r => r.engine === engine && r.batch === batch);
  const batchNumbers = [...new Set([...(grouped.get(run.commit_sha)?.values() ?? [])].map(r => Number(r.batch)))].sort((a,b)=>a-b);
  const tone = (d: number | undefined) => d === undefined ? 'missing' : d >= 6 ? 'slow' : d <= -6 ? 'fast' : 'steady';
  const tint = (d:number|undefined) => ({'--strength':`${Math.min(32,8+Math.abs(d??0)*.55)}%`});
  const runLink = githubRunUrl(run);
  const pr = num(run.pr_number);
  const elapsed = (epoch(run.finished_at)-epoch(run.started_at))/1000;
  return <div class="detail-stack">{detail && <Panel title={run.subject} note={`${short(run.commit_sha)} · ${date(run.committed_at)} UTC`}>
    <div class="commit-meta">{[['Author', text(run.author)],['Branch',run.branch],['Runner',text(run.runner)],['Duration',Number.isFinite(elapsed)?fmt(elapsed,'s'):'—']].map(([label,value])=>
      <div key={label}><small>{label}</small><strong>{value}</strong></div>)}
      <div><small>Pull request</small>{runLink && pr ? <a href={`${new URL(runLink).origin}${new URL(runLink).pathname.split('/actions/runs/')[0]}/pull/${pr}`} target="_blank" rel="noopener noreferrer">#{pr} ↗</a> : <strong>{pr ? `#${pr}` : '—'}</strong>}</div>
      <div><small>Workflow run</small>{runLink ? <a href={runLink} target="_blank" rel="noopener noreferrer">GitHub run ↗</a> : <strong>{text(run.run_id)}</strong>}</div>
    </div><div class="detail-controls">{[...versions(run)].map(([id,version])=><span class="badge" key={id}>{engineInfo(id)[1]} · {version}</span>)}</div>
  </Panel>}<Panel title={detail ? 'Engine × batch' : 'Published commits'} note={detail ? 'Wall clock · change vs comparison commit' : `${visible.length} in scope · batches 2 + 3 delta vs previous comparable commit`}>
    <ReadState state={history} />
    {detail ? <><div class="detail-controls"><a class="control" href={nav('commits', { commit: '', compare: '' })}>All commits</a>
      <label class="inline-field">Compare against <select value={choice} onChange={e => { location.hash = nav('commits', { compare: e.currentTarget.value }); }}>
        <option value="previous">Previous published commit</option><option value="main">Previous main commit</option>
        {comparable.map(r => <option key={r.commit_sha} value={r.commit_sha}>{short(r.commit_sha)} · {r.subject}</option>)}
      </select></label><span class="muted">{other ? `${short(other.commit_sha)} · ${other.subject}` : 'No comparable commit'}</span>
    </div><div class="table-wrap"><table class="data-table"><thead><tr><th>Engine</th>{batchNumbers.map(n => <th key={n}>Batch {n}</th>)}</tr></thead><tbody>
      {engines.map(id => <tr key={id}><th><EngineTag id={id} /></th>{batchNumbers.map(n => {
        const k = `${id}:${n}`;
        const value = num(grouped.get(run.commit_sha)?.get(k)?.duration_s);
        const delta = pct(value, num(other ? grouped.get(other.commit_sha)?.get(k)?.duration_s : undefined));
        return <td key={n}><a class={`heat-cell matrix-cell ${tone(delta)}${id === engine && n === batch ? ' selected' : ''}`} style={tint(delta)} href={nav('commits', { engine: id, batch: `${n}` })}>{fmt(value, 's')} <small>{delta && Math.abs(delta)>=.5 ? delta > 0 ? '↑ ' : '↓ ' : ''}{fmtDelta(delta)}</small></a></td>;
      })}</tr>)}
    </tbody></table></div></> : <><div class="detail-controls"><div class="segments small" aria-label="Branch type">{[['','All'],['main','Main'],['pr','Pull requests']].map(([key,title])=><a key={key} class={key===branch?'current':''} aria-current={key===branch?'true':undefined} href={nav('commits',{branch:key})}>{title}</a>)}</div></div>
    <div class="table-wrap"><table class="data-table commit-list"><thead><tr><th>Commit</th><th>Author</th><th>Branch</th><th>PR</th><th>UTC day</th>
      {engines.map(id => <th key={id} title={engineInfo(id)[0]}><b class="mono-head" style={{'--engine':engineInfo(id)[2]}}>{engineInfo(id)[1]}</b></th>)}</tr></thead><tbody>
      {visible.map(r => <tr key={r.commit_sha}><th><a href={nav('commits', { commit: r.commit_sha })}><code>{short(r.commit_sha)}</code> {r.subject}</a></th><td>{r.author}</td><td><span class="branch-name" title={r.branch}>{r.branch}</span></td><td>{r.pr_number ? `#${r.pr_number}` : '—'}</td><td>{date(r.committed_at)}</td>{engines.map(id => {
        const prior = previousByCommit.get(r.commit_sha);
        const delta = pct(refresh(r.commit_sha, id), prior ? refresh(prior.commit_sha, id) : undefined);
        return <td key={id}><span class={`heat-cell ${tone(delta)}`} style={tint(delta)}>{fmtDelta(delta)}</span></td>;
      })}</tr>)}
    </tbody></table>{!visible.length && <p class="empty">No matching commits in this date range.</p>}</div></>}
  </Panel>
  {detail && <div class="detail-pair">
    {dataset.report.panels.has('model') && <Panel title="Model timing waterfall" note={`${engineInfo(engine)[0]} · batch ${batch} · duration ranked`}><ReadState state={models} /><ReadState state={baseline} /><ModelBars rows={scope(models.rows)} baseline={scope(baseline.rows)} /></Panel>}
    {dataset.report.panels.has('lineage') && <Panel title="Model lineage" note={`${engineInfo(engine)[0]} · focus a node to trace dependencies`}><ReadState state={lineage} /><Lineage key={`${run.commit_sha}:${engine}`} rows={lineage.rows.filter(r => r.engine === engine)} models={scope(models.rows)} baseline={scope(baseline.rows)} /></Panel>}
  </div>}
  </div>;
}
export function githubRunUrl(run: Run) {
  if (typeof run.run_url !== 'string') return undefined;
  try { const url=new URL(run.run_url); return url.origin==='https://github.com' && /^\/[^/]+\/[^/]+\/actions\/runs\/\d+\/?$/.test(url.pathname) ? url.href : undefined; }
  catch { return undefined; }
}
