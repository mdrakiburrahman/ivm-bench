#!/usr/bin/env python3
"""Package complete native SQL and verify every profiled refresh statement."""

import csv
import hashlib
import json
from pathlib import Path
import re
import sys
import zipfile


def rows(path):
    with path.open(newline='') as f:
        return list(csv.DictReader(f))


def view_identity(sql):
    refresh = re.search(r"PRAGMA\s+refresh_options\s*\(([^)]*)\)", sql, re.I)
    if refresh:
        return '.'.join(re.findall(r"'((?:''|[^'])*)'", refresh[1]))
    create = re.search(r'CREATE\s+(?:OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW\s+([^\s]+)\s+AS\b', sql, re.I)
    return create[1].replace('"', '') if create else None


def decode_view(name):
    match = re.fullmatch(r'__openivm_mv_([0-9a-f]+)_([0-9a-f]+)_([0-9a-f]+)_', name)
    return '.'.join(bytes.fromhex(x).decode() for x in match.groups()) if match else name


def extract_batch(directory, batch, archive, seen_requests, seen_refreshes):
    requests = rows(directory/f'openivm-query-requests-batch{batch}.csv')
    requests = {r['invocation_id']:r for r in requests if r['invocation_id'] not in seen_requests}
    seen_requests.update(requests)
    logs = rows(directory/f'openivm-query-log-batch{batch}.csv')
    by_request = {key:[] for key in requests}
    for row in logs:
        if row['invocation_id'] in by_request:
            by_request[row['invocation_id']].append(row)
    profiles = rows(directory/f'openivm-profile-batch{batch}.csv')
    fresh = [r for r in profiles if r['refresh_id'] not in seen_refreshes]
    seen_refreshes.update(r['refresh_id'] for r in profiles)
    run = json.loads((directory/f'run-duckdb-openivm-batch{batch}.json').read_text())
    models = {n['name'] for n in run['nodes'] if n.get('resource_type') == 'model'}
    captured = set()
    by_view = {}
    records = []
    for invocation,request in requests.items():
        identity = view_identity(request['submitted_sql'])
        if identity:
            captured.add(identity.split('.')[-1])
            by_view.setdefault(identity.split('.')[-1], []).extend(by_request[invocation])
        if not by_request[invocation]:
            raise RuntimeError(f'Invocation {invocation} has no executed SQL trace')
        folder = f'batch{batch}/' + (identity.replace('.', '/') if identity else 'auxiliary') + f'/{invocation}'
        archive.writestr(folder+'/request.sql', request['submitted_sql'])
        trace = ['-- Observed nested query trace; do not replay outer and inner commands together.\n']
        for row in by_request[invocation]:
            sql = row['message']
            trace.append(f"\n-- sequence={row['sequence']}, connection={row['connection_id']}, query={row['query_id']}\n{sql}\n")
        archive.writestr(folder+'/observed.sql', ''.join(trace))
    if captured != models:
        raise RuntimeError(f'Batch {batch} model coverage differs: missing={models-captured}, extra={captured-models}')
    for row in fresh:
        if row['step_name'] != 'execute_refresh_sql_stmt':
            continue
        view = decode_view(row['view_name'])
        size = int(re.search(r'bytes=(\d+)', row['detail'])[1])
        prefix = row['detail'].split(', sql=', 1)[1].removesuffix('...')
        matches = [r['message'] for r in by_view.get(view.split('.')[-1], [])
                   if len(r['message'].encode()) == size and
                   re.sub(r'\s+', ' ', r['message'].strip().strip(';')).startswith(prefix)]
        if not matches:
            raise RuntimeError(f'Full SQL missing for batch {batch}, {view}, step {row["step_order"]}')
        if len(set(matches)) != 1:
            raise RuntimeError(f'Ambiguous full SQL match for {view}, step {row["step_order"]}')
        sql = matches[0]
        name = f'batch{batch}/{view.replace(".", "/")}/refresh/{int(row["step_order"]):04d}.sql'
        archive.writestr(name, sql)
        records.append(dict(view=view, step_order=row['step_order'], bytes=size,
                            sha256=hashlib.sha256(sql.encode()).hexdigest(), path=name))
    return dict(batch=batch, model_count=len(models), invocation_count=len(requests),
                executed_sql_records=sum(map(len, by_request.values())),
                refresh_statements=len(records), statements=records)


def main():
    csv.field_size_limit(sys.maxsize)
    root = Path(__file__).resolve().parents[3]
    output = root/'mount/oat-state/native-full-sql'
    output.mkdir(parents=True, exist_ok=True)
    found = False
    for directory in sorted((root/'mount/results').glob('*/dbt-server')):
        if not (directory/'openivm-query-log-batch1.csv').exists():
            continue
        found = True
        scale = directory.parent.name
        seen_requests,seen_refreshes = set(),set()
        summary = {'scale_factor': scale, 'batches': []}
        with zipfile.ZipFile(output/f'full-native-sql-sf{scale}.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
            for batch in [1,2,3]:
                result = extract_batch(directory, batch, archive, seen_requests, seen_refreshes)
                if result['model_count'] != 49:
                    raise RuntimeError('TPC-DI capture must cover all 49 models')
                summary['batches'].append(result)
            archive.writestr('manifest.json', json.dumps(summary, indent=2)+'\n')
        (output/f'coverage-sf{scale}.json').write_text(json.dumps(summary, indent=2)+'\n')
        print(f'SF{scale}: all 49 models captured in every batch; '
              f'{sum(b["refresh_statements"] for b in summary["batches"])} full refresh statements verified', flush=True)
    if not found:
        raise RuntimeError('No native full-SQL captures found')


if __name__ == '__main__':
    main()
