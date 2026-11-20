import { render } from 'preact';
import { useEffect, useMemo, useRef, useState } from 'preact/hooks';
import { openDataset } from './data.ts';
import type { Batch, Dataset, Meter, Run } from './data.ts';
import { batchHistory, DAY, date, engineInfo, findings, findingsTimeline, seconds, signed, summarize } from './metrics.ts';
import type { Finding, Point, Trend } from './metrics.ts';
import { Commits, Engines } from './details.tsx';
import { BatchOverview } from './batch-overview.tsx';
import { Heuristics, SignalEvidence } from './evidence.tsx';
import { DataSource } from './source.tsx';
import './style.css';

const icons = {
  logo: 'M12 2 22 7 12 12 2 7 12 2ZM2 12l10 5 10-5M2 17l10 5 10-5',
  Overview: 'M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z',
  Engines: 'M3 7l9-4 9 4-9 4-9-4Zm0 5 9 4 9-4M3 17l9 4 9-4',
  Commits: 'M2 12h6m8 0h6M16 12a4 4 0 1 1-8 0 4 4 0 0 1 8 0',
  Heuristics: 'M4 20v-6m5 6V7m6 13V4m5 16v-9',
  Signals: 'M12 3 2 20h20L12 3Zm0 6v5m0 2.5v.5',
  Data: 'M3 6c0-4 18-4 18 0s-18 4-18 0Zm0 0v12c0 4 18 4 18 0V6M3 12c0 4 18 4 18 0',
  moon: 'M20 15a9 9 0 0 1-11-11 9 9 0 1 0 11 11Z',
  sun: 'M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8Zm0-6v3m0 14v3M2 12h3m14 0h3M5 5l2 2m10 10 2 2M5 19l2-2M17 7l2-2',
  search: 'M15 15l5 5M17 10a7 7 0 1 1-14 0 7 7 0 0 1 14 0',
  pulse: 'M2 12h5l3-7 4 14 3-7h5',
  back: 'M20 12H4m7-7-7 7 7 7',
  forward: 'M4 12h16m-7-7 7 7-7 7',
};
function Icon({ name, size = 17 }: { name: keyof typeof icons; size?: number }) {
  return <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d={icons[name]} /></svg>;
}
const pageNames = ['Overview', 'Engines', 'Commits', 'Heuristics', 'Signals', 'Data'] as const;
type Page = typeof pageNames[number];
const defaultSource = import.meta.env.VITE_PARQUET_URL ?? './data/_server.parquet';
const locationSource = () => new URLSearchParams(location.search).get('data') ?? defaultSource;
function route() {
  const [name, query] = location.hash.slice(1).split('?');
  return { page: pageNames.find(p => p.toLowerCase() === name) ?? 'Overview', filters: new URLSearchParams(query) };
}
function href(page: Page, params: URLSearchParams, change: Record<string, string> = {}) {
  const next = new URLSearchParams(params);
  if (page === 'Signals' && route().page !== 'Signals' && !Object.hasOwn(change, 'engine')) next.delete('engine');
  for (const [k, v] of Object.entries(change)) if (v) next.set(k, v); else next.delete(k);
  return `#${page.toLowerCase()}${next.size ? `?${next}` : ''}`;
}
function setFilter(page: Page, params: URLSearchParams, change: Record<string, string>, replace = false) {
  const next = href(page, params, change);
  if (next === location.hash) return;
  if (replace) { history.replaceState(history.state, '', next); window.dispatchEvent(new Event('hashchange')); }
  else location.hash = next;
}
type NavigationEntry = { trail: string; index: number };
const navigationEntry = (): NavigationEntry | undefined => history.state?.ivmNavigation;
function usePageHistory(onLocation: () => void) {
  const cursor = useRef(navigationEntry() ?? { trail: `ivm-nav-${performance.timeOrigin}`, index: 0 });
  const limit = useRef(Math.max(cursor.current.index, Number(sessionStorage.getItem(cursor.current.trail)) || 0));
  const [bounds, setBounds] = useState({ back: cursor.current.index > 0, forward: cursor.current.index < limit.current });
  function sync() {
    const entry = navigationEntry();
    if (entry?.trail === cursor.current.trail) cursor.current = entry;
    else {
      // Native hash links create entries without state. Stamp them before a traversal can revisit them.
      cursor.current = { ...cursor.current, index: cursor.current.index + 1 };
      history.replaceState({ ...history.state, ivmNavigation: cursor.current }, '');
      limit.current = cursor.current.index;
      sessionStorage.setItem(cursor.current.trail, String(limit.current));
    }
    setBounds({ back: cursor.current.index > 0, forward: cursor.current.index < limit.current });
    onLocation();
  }
  useEffect(() => {
    history.replaceState({ ...history.state, ivmNavigation: cursor.current }, '');
    window.addEventListener('hashchange', sync);
    window.addEventListener('popstate', sync);
    return () => { window.removeEventListener('hashchange', sync); window.removeEventListener('popstate', sync); };
  }, []);
  return { ...bounds, sync };
}
function Spark({ points, color, dashed }: { points: Point[]; color: string; dashed?: boolean }) {
  if (!points.length) return null;
  const low = Math.min(...points.map(p => p.value));
  const high = Math.max(...points.map(p => p.value));
  const coords = points.map((p, i) => [2 + 116 * i / Math.max(points.length - 1, 1), 29 - 24 * (p.value - low) / (high - low || 1)]);
  const line = coords.map(([x, y]) => `${x.toFixed(2)},${y.toFixed(2)}`).join(' ');
  const [x, y] = coords.at(-1)!;
  return <svg class="spark" viewBox="0 0 120 34" preserveAspectRatio="none" aria-hidden="true">
    <polygon points={`2,33 ${line} ${x},33`} fill={color} fill-opacity=".09" />
    <polyline points={line} fill="none" stroke={color} stroke-dasharray={dashed ? '4 2' : undefined} stroke-width="1.45" vector-effect="non-scaling-stroke" />
    <circle cx={x} cy={y} r="2" fill={color} />
  </svg>;
}
const dateKey = (time: number) => new Date(time).toISOString().slice(0, 10);
const stamp = (time: number) => `${date(time)}, ${new Date(time).toISOString().slice(11, 16)} UTC`;
const heading = (name: string, subtitle: string, icon: keyof typeof icons = 'pulse') =>
  <div class="panel-title"><span class="panel-icon"><Icon name={icon} /></span><h2>{name}</h2><span class="muted">{subtitle}</span></div>;

function Heatmap({ series, from, until, below, selected, select }: { series: Trend[]; from: number; until: number; below: boolean; selected?: string; select: (sha: string) => void }) {
  const scroll = useRef<HTMLDivElement>(null);
  const allDays = Array.from({ length: Math.floor((until - from) / DAY) + 1 }, (_, i) => from + i * DAY);
  const shown = below ? series.filter(s => s.points.some(p => (p.delta ?? 0) >= 10)) : series;
  const hasRows = shown.length > 0;
  useEffect(() => {
    const node = scroll.current;
    if (node) node.scrollLeft = node.scrollWidth;
  }, [from, until, hasRows]);
  if (!shown.length) return <p class="empty">No engines are 10% or more slower than expected in this window.</p>;
  return <div ref={scroll} class="heat-scroll" role="region" aria-label="Refresh heatmap, scroll to see earlier dates" tabIndex={0}><table class="heat"><thead><tr><th scope="col">Engine</th>
    {allDays.map((t, i) => <th scope="col" key={t}><span>{i === 0 || i === allDays.length - 1 || (i % 7 === 0 && i < allDays.length - 3) ? date(t) : ''}</span></th>)}
  </tr></thead><tbody>{shown.map(({ engine, points }) => {
    const [label, monogram, color] = engineInfo(engine);
    const days = new Map(points.map(p => [dateKey(p.run.committed_at), p]));
    return <tr key={engine}><th scope="row"><i style={{ color }}>{monogram}</i>{label}</th>
      {allDays.map(t => {
        const p = days.get(dateKey(t));
        const delta = p?.delta;
        const visible = delta !== undefined && (!below || delta >= 10);
        const tone = !visible ? 'missing' : delta >= 6 ? 'slow' : delta <= -6 ? 'fast' : 'steady';
        const detail = p ? `${date(t)} · ${p.run.commit_sha.slice(0, 7)} · ${p.run.subject} · ${seconds(p.value)} s${delta !== undefined ? ` (${signed(delta)} vs expected)` : ''}` : `${date(t)} · No published commit`;
        return <td key={t}>{p && (!below || visible)
          ? <button class={`heat-cell ${tone}${selected === p.run.commit_sha ? ' selected' : ''}`} title={detail} aria-label={`${label} · ${detail}`} onClick={() => select(p.run.commit_sha)} style={{ '--strength': `${visible ? Math.min(35, 7 + Math.abs(delta) * .55) : 0}%` }}>{visible ? signed(delta) : '·'}</button>
          : <span class="heat-cell missing" title={detail} />}</td>;
      })}
    </tr>;
  })}</tbody></table></div>;
}

function SignalList({ items, select, expanded }: { items: Finding[]; select: (sha: string) => void; expanded?: boolean }) {
  if (!items.length) return <p class="empty">No active signals in this scope.</p>;
  return <div class="signal-list">{(expanded ? items : items.slice(0, 4)).map(item => <article key={`${item.engine}:${item.title}`} class="finding">
    <div class="finding-main"><div class="finding-title"><span class={`severity ${item.level.toLowerCase()}`}>{item.level}</span><strong>{item.title}</strong></div>
      <div class="finding-desc">{engineInfo(item.engine)[0]} · {item.context}</div>
      <div class="chips"><span>{signed(item.point.delta ?? 0)} vs expected</span><button onClick={() => select(item.point.run.commit_sha)}>{item.point.run.commit_sha.slice(0, 7)}</button></div>
    </div>
    <Spark points={item.trace} color={item.level === 'High' ? '#d83c54' : item.level === 'Watch' ? '#bf7115' : '#118461'} />
  </article>)}</div>;
}

function App() {
  const [{ page, filters }, setRoute] = useState(route);
  const [theme, setTheme] = useState(() => localStorage.getItem('ivm-theme') === 'dark' ? 'dark' : 'light');
  const [dataset, setDataset] = useState<Dataset>();
  const [runs, setRuns] = useState<Run[]>([]);
  const [batches, setBatches] = useState<Batch[]>([]);
  const [meter, setMeter] = useState<Meter>({ bytes: 0, requests: 0, size: 0 });
  const [error, setError] = useState('');
  const [loaded, setLoaded] = useState(false);
  const [source, setSource] = useState<string | File>(locationSource);
  const [reload, retry] = useState(0);
  const searchVersion = useRef(location.search);
  const search = useRef<HTMLInputElement>(null);
  const q = filters.get('q') ?? '';
  const range = filters.get('range') === '14' ? 14 : filters.get('range') === '60' ? 60 : 30;
  const selected = filters.get('commit') ?? undefined;
  const engine = filters.get('engine') ?? '';
  const below = filters.get('heat') === 'slow';
  const navigation = usePageHistory(() => {
    setRoute(route());
    if (searchVersion.current !== location.search) {
      searchVersion.current = location.search;
      setSource(locationSource());
    }
  });
  function choose(next: string | File, reset = false) {
    const url = new URL(location.href);
    if (typeof next === 'string' && !reset) url.searchParams.set('data', next);
    else url.searchParams.delete('data');
    if (url.href !== location.href) { history.pushState(null, '', url); navigation.sync(); }
    searchVersion.current = location.search;
    if (next === source) retry(n => n + 1);
    setSource(next);
  }
  const latest = runs.at(-1);
  const end = latest ? Date.parse(`${dateKey(latest.committed_at)}T00:00:00Z`) + DAY - 1 : 0;
  const from = end - range * DAY + 1;
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') { e.preventDefault(); search.current?.focus(); } };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
  useEffect(() => { document.documentElement.dataset.theme = theme; localStorage.setItem('ivm-theme', theme); }, [theme]);
  useEffect(() => {
    const ctrl = new AbortController();
    setLoaded(false);
    setDataset(undefined);
    setRuns([]);
    setBatches([]);
    setMeter({ bytes: 0, requests: 0, size: 0 });
    setError('');
    openDataset(source, setMeter, ctrl.signal).then(async data => {
      const loaded = await data.read('run') as Run[];
      if (ctrl.signal.aborted) return;
      setDataset(data);
      setRuns(loaded.sort((a, b) => a.committed_at - b.committed_at || a.commit_sha.localeCompare(b.commit_sha)));
    }).catch(err => { if (!ctrl.signal.aborted) setError(err.message); });
    return () => ctrl.abort();
  }, [source, reload]);
  useEffect(() => {
    if (!dataset) return;
    let active = true;
    setLoaded(false);
    setError('');
    const selectedTime = page === 'Signals' && selected ? runs.find(r => r.commit_sha === selected)?.committed_at : undefined;
    dataset.read('engine_batch', { since: Math.min(from, selectedTime ?? from) - 24 * DAY }, [
      'status', 'duration_s', 'compute_cpu_time_s', 'peak_memory_mb', 'helper_over_visible', 'models_incremental',
    ]).then(rows => {
      if (active) { setBatches(rows as Batch[]); setLoaded(true); }
    }).catch(err => { if (active) setError(err.message); });
    return () => { active = false; };
  }, [dataset, from, page === 'Signals' ? selected : undefined, runs]);
  const matches = (id: string, run?: Run) => (!engine || engine === id)
    && (!q || `${id} ${engineInfo(id)[0]} ${run ? `${run.commit_sha} ${run.subject} ${run.branch}` : ''}`.toLowerCase().includes(q.trim().toLowerCase()));
  const totalSeries = useMemo(() => summarize(runs, batches), [runs, batches]);
  const series = totalSeries.map(s => ({ ...s, points: s.points.filter(p => p.run.committed_at >= from && matches(s.engine, p.run)) })).filter(s => s.points.length);
  const signalCommit = page === 'Signals' && selected ? runs.find(r => r.commit_sha === selected) ?? latest : latest;
  const allSignals = useMemo(() => signalCommit ? findings(runs, batches, signalCommit.commit_sha) : [], [runs, batches, signalCommit]);
  const signals = allSignals.filter(s => matches(s.engine, s.point.run));
  const inWindow = useMemo(() => runs.filter(r=>r.committed_at>=from),[runs,from]);
  const overviewHistory = page === 'Overview' ? batchHistory(inWindow, batches, matches, q.trim().toLowerCase()) : undefined;
  const selectedBatch = Number(filters.get('batch')) || 1;
  const timeline = useMemo(() => page==='Signals' ? findingsTimeline(runs,batches,new Set(inWindow.map(r=>r.commit_sha))) : new Map<string,Finding[]>(),[page,runs,batches,inWindow]);
  const count = overviewHistory?.domain.length ?? new Set(series.flatMap(s => s.points.map(p => p.run.commit_sha))).size;
  const engineCount = overviewHistory?.engines.length ?? series.length;
  const commit = runs.find(r => r.commit_sha === selected);
  const nav = (name: string, change?: Record<string, string>) => href(pageNames.find(p => p.toLowerCase() === name) ?? 'Overview', filters, change);
  const select = (sha: string) => setFilter(page, filters, { commit: sha });
  const remove = (key: string) => setFilter(page, filters, { [key]: '' });
  return <div class="shell">
    <aside class="rail" aria-label="Application navigation">
      <a class="brand" href="#overview" aria-label="IVM Bench"><Icon name="logo" size={23} /></a>
      <nav>{pageNames.map(name => <a href={href(name, filters)} key={name} class={page === name ? 'active' : ''} aria-current={page === name ? 'page' : undefined}><Icon name={name} size={19} /><span>{name}</span></a>)}</nav>
      <button class="theme" onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}><Icon name={theme === 'light' ? 'moon' : 'sun'} size={18} /><span>{theme === 'light' ? 'Dark' : 'Light'}</span></button>
    </aside>
    <main>
      <header class="topbar">
        <div class="page-heading">
          <nav class="page-history" aria-label="Page history">
            <button type="button" class="control" disabled={!navigation.back} title="Back" aria-label="Back" onClick={() => history.back()}><Icon name="back" /></button>
            <button type="button" class="control" disabled={!navigation.forward} title="Forward" aria-label="Forward" onClick={() => history.forward()}><Icon name="forward" /></button>
          </nav>
          <div><div class="eyebrow">IVM BENCH <span> / </span> {page.toUpperCase()}</div><h1>{page === 'Overview' ? 'Performance overview' : page === 'Signals' ? 'Performance signals' : page}</h1></div>
        </div>
        <div class="controls">{page !== 'Data' && <>
          <label class="search"><Icon name="search" size={15} /><input ref={search} aria-label="Search engines and commits" placeholder="Search engine or commit" value={q}
            onInput={e => setFilter(page, filters, { q: e.currentTarget.value }, true)} /><kbd>Ctrl K</kbd></label>
          <div class="segments" aria-label="Date window">{[14, 30, 60].map(n => <button key={n} aria-pressed={range === n} onClick={() => setFilter(page, filters, { range: n === 30 ? '' : `${n}` })}>{n} d</button>)}</div>
          <a class="reset" href={`#${page.toLowerCase()}`}>Reset filters</a></>}
          <span class="asof">{latest ? <><i /> as of {stamp(latest.committed_at)}</> : 'Awaiting snapshot'}</span>
          {dataset && <span class="dataset">{dataset.report.dataset === 'synthetic' ? 'SYNTHETIC' : 'LIVE'}</span>}
        </div>
      </header>
      <div class="content">
        {page === 'Data' ? <DataSource source={source} dataset={dataset} meter={meter} runs={runs} loading={!loaded && !error} error={error}
          choose={choose} reset={() => choose(defaultSource, true)} canReset={source !== defaultSource || new URLSearchParams(location.search).has('data')} />
          : error ? <section class="panel state" role="alert"><h2>{error.startsWith('No published runs') ? 'No published runs' : 'Cannot load the dataset'}</h2><p>{error}</p><div class="state-actions"><button class="control" onClick={() => retry(n => n + 1)}>Reload dataset</button><a class="control" href={nav('data')}>Change data source</a></div></section>
          : !loaded ? <section class="panel state" role="status"><h2>Loading benchmark history</h2><p>Reading Parquet metadata and recent engine batches.</p></section>
          : !latest || !batches.length ? <section class="panel state"><h2>No published runs</h2><p>The Parquet file has no engine results to display.</p></section>
          : <>
            <div class="scope"><span><strong>{dataset?.report.workload.split('/')[0].toUpperCase() ?? 'IVM'}</strong> <b>·</b> SF {String(latest.scale_factor ?? '—')} <b>·</b> {String(latest.mode ?? '—').toUpperCase()}</span>
              {(page === 'Overview' || page === 'Engines' || page === 'Commits') && <><span>{count} {count === 1 ? 'commit' : 'commits'} <b>·</b> {engineCount} {engineCount === 1 ? 'engine' : 'engines'}</span><span class="scope-date">{date(from)} – {date(end)} <b>·</b> UTC</span></>}
              {engine && <button class="filter-chip" onClick={() => remove('engine')}>{engineInfo(engine)[0]} ×</button>}</div>
            {page === 'Overview' && <>
              <section class={`banner${signals.length ? '' : ' quiet'}`}>
                <Icon name="Signals" size={21} /><div class="banner-text"><strong>{signals.length} active {signals.length === 1 ? 'signal' : 'signals'}</strong>
                  <span class="banner-label">vs trailing median · latest published commit</span>
                  <div class="banner-chips">{signals.slice(0, 3).map(s => <span key={`${s.engine}:${s.title}`}><i />{engineInfo(s.engine)[0]} · {s.title.toLowerCase()}</span>)}</div>
                </div><a href={href('Signals', filters)}>View active signals <span aria-hidden="true">→</span></a>
              </section>
              {commit && <div class="selection"><Icon name="Commits" /><code>{commit.commit_sha.slice(0, 7)}</code><strong>{commit.subject}</strong><span>{stamp(commit.committed_at)}</span><button onClick={() => setFilter(page, filters, { commit: '', batch: '' })} aria-label="Clear selected commit">×</button></div>}
              {overviewHistory?.domain.length ? <BatchOverview key={`${engine}:${from}:${q}`} history={overviewHistory} chosen={selected ?? null} chosenBatch={selectedBatch}
                select={(sha, batch) => setFilter(page, filters, { commit: sha, batch: String(batch) })}
                detail={(sha, batch) => href('Commits', filters, { commit: sha, batch: String(batch), q: '' })} />
                : <section class="panel state"><h2>No matching commits</h2><p>Clear the search or filters to see more commits.</p></section>}
              <div class="lower">
                <section class="panel heat-panel">
                  <div class="panel-head">{heading('Batches 2 + 3 by UTC day', 'Combined refresh · % vs expected')}<div class="segments small">
                    <button aria-pressed={!below} onClick={() => remove('heat')}>All</button><button aria-pressed={below} onClick={() => setFilter(page, filters, { heat: 'slow' })}>Below expected</button>
                  </div></div>
                  <Heatmap series={series} from={from} until={end} below={below} selected={selected} select={select} />
                  <div class="heat-key"><span class="fast">Faster</span><span class="steady">Near baseline</span><span class="slow">Slower</span><span>Blank: no comparable run</span></div>
                </section>
                <section class="panel">{<div class="panel-head">{heading('Active signals', 'Ranked by severity', 'Signals')}</div>}<SignalList items={signals} select={select} /></section>
              </div>
            </>}
            {dataset && (page === 'Engines' || page === 'Commits') && (() => {
              const Component = page === 'Engines' ? Engines : Commits;
              return <Component dataset={dataset} runs={runs} run={commit ?? latest} from={from} engines={totalSeries.map(s => s.engine)} filters={filters} nav={nav} query={q} />;
            })()}
            {dataset && page === 'Heuristics' && <Heuristics dataset={dataset} runs={runs} run={commit ?? latest} engines={totalSeries.map(s => s.engine)} filters={filters} nav={nav} query={q} />}
            {dataset && page === 'Signals' && <div class="detail-stack">
              <SignalEvidence dataset={dataset} runs={runs} run={commit ?? latest} engines={totalSeries.map(s => s.engine)} filters={filters} nav={nav} signals={signals} timeline={timeline} inWindow={inWindow}
                feed={<section class="panel"><div class="panel-head">{heading('Performance signals', 'Grouped by engine · trailing 10 comparable runs', 'Signals')}</div>
                  {signals.length ? [...new Set(signals.map(s=>s.engine))].map(id=><div key={id}><div class="group-heading">{engineInfo(id)[0]}</div><SignalList items={signals.filter(s=>s.engine===id)} select={select} expanded /></div>) : <SignalList items={[]} select={select} />}</section>} />
            </div>}
          </>}
      </div>
      <footer><span><Icon name="Data" size={13} /> Only successful runs are published.</span>{meter.size > 0 && <span class="meter" aria-label="Parquet range request meter">{meter.requests} range requests, {(meter.bytes / 1024).toFixed(1)} KB fetched, {(meter.bytes / meter.size * 100).toFixed(1)}% of file</span>}</footer>
    </main>
  </div>;
}

render(<App />, document.getElementById('app')!);
