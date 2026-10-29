#!/usr/bin/env python3
"""Run correctness-gated paired OpenIVM experiments on one GCI worker."""

import csv
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
NATIVE_DOCKER = ROOT / 'src/containers/duckdb-openivm/Dockerfile'
csv.field_size_limit(sys.maxsize)


def replace_pin(text, key, value):
    if not re.fullmatch(r'[0-9a-f]{40}', value):
        raise ValueError(f'{key} must be an immutable full commit SHA')
    result, count = re.subn(r'(?m)^ARG ' + re.escape(key) + r'=\S+', f'ARG {key}={value}', text)
    if count != 2:
        raise ValueError(f'expected two build/runtime {key} arguments, found {count}')
    return result


def oat_ids():
    ids = set()
    for path in (ROOT/'mount/oat-state').glob('*'):
        try:
            uuid.UUID(path.name)
        except ValueError:
            continue
        if path.is_dir():
            ids.add(path.name)
    return ids


def copy_outputs(destination, oat_id, sf, engine):
    shutil.copytree(ROOT/'mount/oat-state'/oat_id, destination/'oat')
    for source, name in [
        (ROOT/f'mount/results/{sf}/dbt-server', 'dbt-server'),
        (ROOT/f'mount/stats/{sf}/{engine}', 'container-stats'),
        (ROOT/f'mount/query-plan/{sf}', 'query-plans'),
    ]:
        if source.exists():
            shutil.copytree(source, destination/name)


def verify_results(destination, sf, engine, validate):
    result = json.loads((destination/'dbt-server/benchmark-results.json').read_text())
    if result['status'] != 'completed' or result['repetition_count'] != 1:
        raise RuntimeError('paired phases require one complete, unaveraged repetition')
    with (destination/'oat/results.csv').open() as f:
        rows = list(csv.DictReader(f))
    if len(rows) != 3 or {int(r['batch_num']) for r in rows} != {1, 2, 3}:
        raise RuntimeError('expected three complete fresh batch records')
    for row in rows:
        if (row['batch_status'] != 'completed' or row['batch_error'] or
                row['engine'] != engine or int(row['scale_factor']) != sf):
            raise RuntimeError(f'invalid benchmark result: {row}')
        if row['openivm_validate'].lower() != str(validate).lower():
            raise RuntimeError('validation setting differs from the paired design')
    if validate:
        for batch in [1, 2, 3]:
            path = destination/f'dbt-server/validation-{engine}-batch{batch}.json'
            data = json.loads(path.read_text())
            if (data['status'] != 'passed' or data.get('failures') or
                    data['models_checked'] != 49 or len(data['results']) != 49 or
                    any(r['status'] != 'pass' for r in data['results'])):
                raise RuntimeError(f'correctness validation did not cover all models: {path}')
    seen = set()
    for batch in [1, 2, 3]:
        with (destination/f'dbt-server/openivm-cli-timings-batch{batch}.csv').open() as f:
            exported = list(csv.DictReader(f))
        identity = lambda r: (r['timestamp_s'], r['sql'], r['attempt'])
        fresh = [r for r in exported if identity(r) not in seen]
        seen.update(identity(r) for r in exported)
        commands = [r for r in fresh if re.match(
            r'\s*(CREATE\s+(OR\s+REPLACE\s+)?MATERIALIZED\s+VIEW|PRAGMA\s+refresh_options\b)',
            r['sql'], re.I)]
        if len(commands) != 49 or len({r['sql'] for r in commands}) != 49 or any(
                r['returncode'] != '0' or r['attempt'] != '0' or
                any(not r.get(k) for k in ['ready_offset_s', 'sql_started_offset_s',
                                          'sql_finished_offset_s', 'exit_offset_s'])
                for r in commands):
            raise RuntimeError(f'missing native command/process timing coverage in batch {batch}')
        for call in fresh:
            samples = json.loads(call['process_samples_json'])
            if not samples or not any(s.get('process_io') is not None for s in samples):
                raise RuntimeError(f'missing native process I/O samples in batch {batch}')
    return rows


def phases(study, sf):
    out = []
    for variant in study.get('validate_variants', 'AB'):
        out.append((f'native-validate-{variant}', 'duckdb-openivm', variant, 10, True,
                    study['native_control' if variant == 'A' else 'native_treatment']))
    for index, variant in enumerate(study.get('measurement_order', 'ABBA')):
        out.append((f'native-{variant}{1 if index < 2 else 2}', 'duckdb-openivm', variant, sf, False,
                    study['native_control' if variant == 'A' else 'native_treatment']))
    return out


def main():
    template = json.loads(Path(os.environ['BENCHMARK_EXPERIMENTS_FILE']).read_text())
    study = template['_attribution']
    sf = int(os.environ.get('SCALE_FACTOR', '100'))
    output = ROOT/'native-abba-results'/f'paired-{os.environ.get("GITHUB_RUN_ID", "manual")}'
    output.mkdir(parents=True, exist_ok=False)
    original_native = NATIVE_DOCKER.read_text()
    manifest = {
        'bench_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        'study': study, 'worker': platform.node(), 'cpu_count': os.cpu_count(),
        'meminfo': Path('/proc/meminfo').read_text().splitlines()[:3],
        'order': study.get('measurement_order', 'ABBA'), 'phases': [],
        'cold_warm_policy': 'Fresh engine state/session per repetition; no host page-cache eviction.',
    }
    reference_counts = {}
    try:
        for label, engine, variant, scale, validate, sha in phases(study, sf):
            destination = output/label
            destination.mkdir()
            NATIVE_DOCKER.write_text(replace_pin(original_native, 'OPENIVM_COMMIT', sha))
            config = {
                '_design_notes': template['_design_notes'], 'baseline': template['baseline'],
                'experiments': [{'label': label, 'scale_factor': scale, 'engines': [engine],
                                 'feature_flags': {'openivm_validate': validate}}],
            }
            config_path = ROOT/'src/containers/benchmark-server/experiments/native-abba-current.json'
            config_path.write_text(json.dumps(config, indent=2)+'\n')
            (destination/'experiment.json').write_text(config_path.read_text())
            (destination/'native-Dockerfile').write_text(NATIVE_DOCKER.read_text())
            diff = subprocess.check_output(['git', 'diff', '--', str(NATIVE_DOCKER)],
                                           cwd=ROOT, text=True)
            (destination/'pin-diff.patch').write_text(diff)
            before = oat_ids()
            record = {'label': label, 'engine': engine, 'variant': variant, 'scale': scale,
                      'validate': validate, 'commit': sha, 'started_at': time.time(),
                      'loadavg_before': Path('/proc/loadavg').read_text().strip()}
            manifest['phases'].append(record)
            (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
            print(f'::notice title=Native paired retry::{label}, {engine}, SF{scale}, {sha}', flush=True)
            env = dict(os.environ, BENCHMARK_EXPERIMENTS_FILE=str(config_path), BENCHMARK_RUNS='1')
            result = subprocess.run(['bash', 'src/.scripts/benchmark.sh'], cwd=ROOT, env=env)
            new_ids = oat_ids()-before
            record.update(returncode=result.returncode, completed_at=time.time())
            if len(new_ids) != 1:
                raise RuntimeError(f'{label} produced {len(new_ids)} fresh OAT runs')
            oat_id = new_ids.pop()
            record['oat_id'] = oat_id
            copy_outputs(destination, oat_id, scale, engine)
            if result.returncode != 0:
                raise RuntimeError(f'{label} failed; see archived OAT diagnostics')
            record['batches'] = verify_results(destination, scale, engine, validate)
            counts = json.loads((destination/'oat/exp-000/source-row-counts.json').read_text())
            key = (engine, scale)
            if key in reference_counts and reference_counts[key] != counts:
                raise RuntimeError(f'{label} has different source data from its paired control')
            reference_counts[key] = counts
            record['status'] = 'passed'
            (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
            summary = os.environ.get('GITHUB_STEP_SUMMARY')
            if summary:
                with open(summary, 'a') as f:
                    f.write(f'\n- **{label}** passed all three batches; validation={validate}.\n')
        manifest['status'] = 'passed'
    except BaseException as error:
        manifest['status'] = 'failed'
        manifest['error'] = str(error)
        if manifest['phases']:
            manifest['phases'][-1]['status'] = 'failed'
        raise
    finally:
        NATIVE_DOCKER.write_text(original_native)
        (output/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
        subprocess.run(['python3', str(Path(__file__).with_name('summarize_native_abba.py')),
                        str(output)], cwd=ROOT, check=True)


if __name__ == '__main__':
    main()
