#!/usr/bin/env python3
"""Preserve paired observations without mixing batch latency and model work."""

import csv
import json
from pathlib import Path
import re
import statistics
import sys


def read_csv(path):
    with path.open() as f:
        return list(csv.DictReader(f))


def write_csv(path, rows):
    if rows:
        with path.open('w') as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def interval_union(intervals):
    total = 0
    end = 0
    for start, stop in sorted(intervals):
        total += max(0, stop - max(start, end))
        end = max(end, stop)
    return total


def summarize(root):
    manifest = json.loads((root/'manifest.json').read_text())
    batches, models, cli = [], [], []
    for phase in manifest['phases']:
        if phase.get('status') != 'passed':
            continue
        directory = root/phase['label']/'dbt-server'
        engine = phase['engine']
        seen = set()
        for row in phase['batches']:
            batch = int(row['batch_num'])
            common = dict(phase=phase['label'], engine=engine, variant=phase['variant'],
                          scale_factor=phase['scale'], validation=phase['validate'], batch=batch)
            run = json.loads((directory/f'run-{engine}-batch{batch}.json').read_text())
            nodes = {n['name']: n for n in run['nodes'] if n.get('resource_type') == 'model'}
            assert len(nodes) == 49
            intervals = []
            if engine == 'spark-openivm':
                exported = read_csv(directory/f'spark-openivm-profile-batch{batch}.csv')
                fresh = [r for r in exported if r['refresh_id'] not in seen]
                seen.update(r['refresh_id'] for r in exported)
                commands = [r for r in fresh if r['step_name'] == 'command_span']
                for command in commands:
                    name = command['view_name'].split('.')[-1].strip('`"')
                    detail = dict(re.findall(r'(start_epoch_ms|end_epoch_ms)=(\d+)', command['detail']))
                    intervals.append((int(detail['start_epoch_ms'])/1000,
                                      int(detail['end_epoch_ms'])/1000))
                    models.append(dict(**common, model=name,
                                       command_s=float(command['duration_ms'])/1000,
                                       client_s=nodes[name]['execution_time_s'],
                                       boundary='command entry through durable finalization; final diagnostic write excluded'))
            else:
                exported = read_csv(directory/f'openivm-cli-timings-batch{batch}.csv')
                identity = lambda r: (r['timestamp_s'], r['sql'], r['attempt'])
                fresh = [r for r in exported if identity(r) not in seen]
                seen.update(identity(r) for r in exported)
                commands = []
                for call in fresh:
                    cli.append(dict(**common, **call))
                    if re.match(r'\s*PRAGMA\s+refresh_options\b', call['sql'], re.I):
                        name = re.findall(r"'((?:''|[^'])*)'", call['sql'])[-1]
                    else:
                        match = re.search(r'MATERIALIZED\s+VIEW\s+([^\s]+)\s+AS\b', call['sql'], re.I)
                        if not match:
                            continue
                        name = match.group(1).replace('"', '').replace('`', '').split('.')[-1]
                    commands.append(call)
                    models.append(dict(**common, model=name,
                                       command_s=float(call['operation_sql_s']),
                                       client_s=nodes[name]['execution_time_s'],
                                       boundary='native MV SQL command, including publication/commit; CLI setup/startup/exit excluded'))
            assert len(commands) == 49
            work = [m['command_s'] for m in models if m['phase'] == phase['label'] and m['batch'] == batch]
            batches.append(dict(**common, engine_batch_s=float(row['duration_s']),
                                command_model_sum_s=sum(work),
                                model_active_wall_s=interval_union(intervals) if intervals else '',
                                model_window_makespan_s=(max(e for _,e in intervals)-min(s for s,_ in intervals)) if intervals else '',
                                source_rows=row['source_rows'],
                                container_cpu_s=row['compute_cpu_time_s']))
    write_csv(root/'batch-observations.csv', batches)
    write_csv(root/'model-observations.csv', models)
    write_csv(root/'native-cli-observations.csv', cli)
    summaries = []
    keys = sorted({(r['engine'],r['scale_factor'],r['validation'],r['variant'],r['batch']) for r in batches})
    for engine, scale, validation, variant, batch in keys:
        selected = [r for r in batches if (r['engine'],r['scale_factor'],r['validation'],r['variant'],r['batch']) ==
                    (engine,scale,validation,variant,batch)]
        for metric in ['engine_batch_s', 'command_model_sum_s', 'model_active_wall_s', 'model_window_makespan_s']:
            values = [r[metric] for r in selected if r[metric] != '']
            if values:
                summaries.append(dict(engine=engine, scale_factor=scale, validation=validation,
                                      variant=variant, batch=batch, metric=metric, n=len(values),
                                      mean=statistics.mean(values), median=statistics.median(values),
                                      minimum=min(values), maximum=max(values)))
    write_csv(root/'summary.csv', summaries)
    (root/'measurement-contract.txt').write_text(
        'Engine batch elapsed follows the existing Fig. 1 harness boundary.\n'
        'Sum of model commands is work-duration, not batch latency. Batches remain separate.\n'
        'Spark command_span includes durable backup/cleanup but excludes its final diagnostic write.\n'
        'Native command SQL includes publication/commit; process setup and exit remain separate.\n'
        'CLI phase offsets are parent-observed output arrival times and include observer scheduling lag.\n'
        'Old Spark query_span is incomplete and cannot substitute for whole-command timing.\n'
        'Native CLI exports are cumulative; calls are deduplicated by timestamp, SQL and attempt.\n'
        'Fresh sessions per repetition; shared host caches are not evicted.\n'
        'No slow observations are discarded. Two repetitions per variant are exploratory.\n')


if __name__ == '__main__':
    summarize(Path(sys.argv[1]))
