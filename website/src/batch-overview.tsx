import { useEffect, useRef, useState } from 'preact/hooks';
import type { Run } from './data.ts';
import { date, engineInfo, seconds } from './metrics.ts';
import type { batchHistory } from './metrics.ts';
import { versions } from './details.tsx';

type History = ReturnType<typeof batchHistory>;
const short = (run: Run) => run.commit_sha.slice(0, 7);
function insertLabel(values: (number | undefined)[]) {
  const known = new Set(values.filter(v => v !== undefined));
  if (!known.size) return 'Insert % unavailable';
  if (known.size > 1) return 'Insert % varies by commit';
  return `${[...known][0]}% insert${values.includes(undefined) ? ' · some commits unreported' : ''}`;
}

export function BatchOverview({ history, chosen, chosenBatch, select, detail }: {
  history: History; chosen: string | null; chosenBatch: number;
  select: (sha: string, batch: number) => void; detail: (sha: string, batch: number) => string;
}) {
  const { domain, engines, charts } = history;
  const stack = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(1000);
  const [hover, setHover] = useState<{ index: number; batch: number }>();
  const [focus, setFocus] = useState<{ index: number; batch: number }>();
  const [lead, setLead] = useState<string>();
  const [pinned, setPinned] = useState<string>();
  useEffect(() => {
    const node = stack.current;
    if (!node) return;
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width - 2));
    observer.observe(node);
    return () => observer.disconnect();
  }, []);

  const active = hover ?? focus;
  const selected = domain.findIndex(run => run.commit_sha === chosen);
  const index = active?.index ?? selected;
  const run = domain[index];
  const versionMap = run ? versions(run) : undefined;
  const left = 60, right = 42, top = 14, bottom = 134;
  const plotWidth = Math.max(1, width - left - right);
  const x = (i: number) => left + plotWidth * (domain.length === 1 ? .5 : i / Math.max(1, domain.length - 1));
  const maxTicks = Math.max(2, Math.floor(plotWidth / 106));
  const tickCount = Math.min(maxTicks, domain.length);
  const ticks = Array.from({ length: tickCount }, (_, i) => Math.round(i * (domain.length - 1) / Math.max(1, tickCount - 1)));
  const highlight = lead ?? pinned;

  return <div class="batch-overview" ref={stack}>
    <section class="panel batch-intro">
      <div class="panel-head"><div class="panel-title"><h2>Elapsed time by commit</h2><span class="muted">Wall clock · lower is better</span></div></div>
      <div class="legend" aria-label="Highlight an engine across all batches">{engines.map(engine => {
        const [label, , color, dash] = engineInfo(engine);
        return <button key={engine} aria-pressed={pinned === engine} onMouseEnter={() => setLead(engine)} onMouseLeave={() => setLead(undefined)}
          onFocus={() => setLead(engine)} onBlur={() => setLead(undefined)} onClick={() => setPinned(pinned === engine ? undefined : engine)}>
          <i class={dash ? 'dash' : ''} style={{ color }} />{label}</button>;
      })}</div>
    </section>
    {charts.map(({ batch, insert, series }) => {
      const max = Math.max(1, ...series.flatMap(s => s.durations).filter(v => v !== undefined));
      const scale = max >= 3600 ? { divisor: 3600, label: 'h' } : max >= 120 ? { divisor: 60, label: 'min' } : { divisor: 1, label: 's' };
      const rawStep = max / scale.divisor / 4;
      const magnitude = 10 ** Math.floor(Math.log10(rawStep));
      const step = ([1, 2, 2.5, 5, 10].find(v => v * magnitude >= rawStep) ?? 10) * magnitude;
      const intervals = Math.ceil(max / scale.divisor / step);
      const y = (v: number) => bottom - (bottom - top) * v / (intervals * step * scale.divisor);
      const inspected = run && (active?.batch === batch || (!active && chosenBatch === batch));
      const detailRun = selected >= 0 ? domain[selected] : domain.at(-1);
      const height = batch === 3 ? 165 : 146;
      const hasData = series.some(s => s.durations.some(v => v !== undefined));
      return <section class="panel batch-card" key={batch}>
        <div class="panel-head">
          <div class="panel-title"><h2>Batch {batch}</h2><span class="badge">{insertLabel(insert)}</span></div>
          {detailRun && <a class="batch-link" href={detail(detailRun.commit_sha, batch)}>View batch <span aria-hidden="true">→</span></a>}
        </div>
        <div class="batch-plot">
          <svg class="batch-svg" width="100%" height={height} viewBox={`0 0 ${width} ${height}`} role="img" aria-label={`Batch ${batch} elapsed time by commit`}>
            {ticks.map(i => <line key={i} x1={x(i)} x2={x(i)} y1={top} y2={bottom} class="gridline batch-guide" />)}
            {hasData && Array.from({ length: intervals + 1 }, (_, i) => <g key={i}>
              <line x1={left} x2={width - right} y1={y(i * step * scale.divisor)} y2={y(i * step * scale.divisor)} class={i ? 'gridline' : 'zero'} />
              <text x={left - 8} y={y(i * step * scale.divisor) + 4} text-anchor="end">{+(i * step).toPrecision(4)} {scale.label}</text>
            </g>)}
            {series.map(({ engine, durations }) => {
              const [, , color, dash] = engineInfo(engine);
              let drawing = false;
              const path = durations.map((v, i) => {
                if (v === undefined) { drawing = false; return ''; }
                const point = `${drawing ? 'L' : 'M'}${x(i)},${y(v)}`;
                drawing = true;
                return point;
              }).join('');
              return <g key={engine} data-engine={engine} opacity={highlight && highlight !== engine ? .16 : 1}>
                <path d={path} class="plot-line" fill="none" stroke={color} stroke-dasharray={dash ? '5 3' : undefined} />
                {durations.map((v, i) => v !== undefined && (i === index || (durations[i - 1] === undefined && durations[i + 1] === undefined))
                  ? <circle key={i} cx={x(i)} cy={y(v)} r="2.8" fill={color} class="batch-point" /> : null)}
              </g>;
            })}
            {run && <line class="crosshair" x1={x(index)} x2={x(index)} y1={top} y2={bottom} />}
            {batch === 3 && ticks.map(i => <text class="batch-sha" key={domain[i].commit_sha} x={x(i)} y={155} text-anchor="middle">{short(domain[i])}</text>)}
          </svg>
          {!hasData && <div class="batch-no-data muted">No Batch {batch} measurements in scope</div>}
          <div class="batch-targets" style={{ left: `${left - 12}px`, right: `${right - 12}px` }} onMouseLeave={() => setHover(undefined)}>
            {domain.map((r, i) => {
              const start = i ? (x(i - 1) + x(i)) / 2 : left - 12;
              const end = i === domain.length - 1 ? width - right + 12 : (x(i) + x(i + 1)) / 2;
              return <button key={r.commit_sha} type="button" class="batch-target" style={{ left: `${start - left + 12}px`, width: `${end - start}px` }}
                aria-label={`Batch ${batch} · ${short(r)} · ${date(r.committed_at)} · ${r.subject}`}
                aria-pressed={selected === i && chosenBatch === batch}
                onMouseEnter={() => setHover({ index: i, batch })} onFocus={() => setFocus({ index: i, batch })}
                onBlur={() => setFocus(undefined)} onClick={() => select(r.commit_sha, batch)}
                onKeyDown={e => {
                  if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
                  e.preventDefault();
                  const target = e.key === 'ArrowLeft' ? e.currentTarget.previousElementSibling : e.currentTarget.nextElementSibling;
                  if (target instanceof HTMLElement) target.focus();
                }} />;
            })}
          </div>
          {inspected && <div class={`batch-tooltip${x(index) > width / 2 ? ' flip' : ''}`} style={{ left: `${x(index)}px` }}>
            <strong><code>{short(run)}</code> · {date(run.committed_at)} UTC</strong><div>{run.subject}</div>
            <div class="batch-tip-context">{insertLabel([insert[index]])}</div>
            {series.map(({ engine, durations }) => <div class="tip-value" key={engine}>
              <span>{engineInfo(engine)[0]} <small>{versionMap?.get(engine)}</small></span>
              <b>{durations[index] === undefined ? '—' : `${seconds(durations[index])} s`}</b>
            </div>)}
          </div>}
        </div>
      </section>;
    })}
  </div>;
}
