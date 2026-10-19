"""Read existing Fabric Spark history; never create sessions or submit SQL."""
import importlib.util
import json
import os
from pathlib import Path
import re
import tempfile
import urllib.error
import urllib.parse
import urllib.request

OUT = Path(tempfile.mkdtemp(prefix='fabric-runtime-audit-'))
TARGETS = {
    'previous-39162066384': '9fe53c3b-a673-45f4-aea5-593af2d50379',
    'current-39233643421': '4f4f79e6-b11e-49e5-8e4e-3c3bc0094522',
}
CORE_FIELDS = ('id', 'isActive', 'totalCores', 'maxTasks', 'totalTasks',
               'failedTasks', 'completedTasks', 'totalDuration', 'totalGCTime',
               'totalInputBytes', 'totalShuffleRead', 'totalShuffleWrite',
               'addTime', 'removeTime', 'removeReason', 'memoryUsed', 'diskUsed')
STAGE_FIELDS = ('stageId', 'attemptId', 'status', 'numTasks', 'numCompleteTasks',
                'numFailedTasks', 'submissionTime', 'completionTime',
                'executorRunTime', 'executorCpuTime', 'inputBytes', 'outputBytes',
                'shuffleReadBytes', 'shuffleWriteBytes', 'memoryBytesSpilled', 'diskBytesSpilled')
CONF = {'spark.master', 'spark.app.id', 'spark.executor.cores', 'spark.executor.memory',
        'spark.executor.instances', 'spark.driver.cores', 'spark.driver.memory',
        'spark.dynamicAllocation.enabled', 'spark.dynamicAllocation.minExecutors',
        'spark.dynamicAllocation.maxExecutors', 'spark.dynamicAllocation.initialExecutors',
        'spark.sql.shuffle.partitions', 'spark.sql.adaptive.enabled',
        'spark.sql.adaptive.coalescePartitions.enabled',
        'spark.sql.autoBroadcastJoinThreshold', 'spark.sql.adaptive.autoBroadcastJoinThreshold'}


def pick(row, fields):
    return {key: row[key] for key in fields if key in row}


def main():
    OUT.mkdir(exist_ok=True)
    source = Path(__file__).resolve().parents[3] / 'src/containers/imds-router/imds_relay_router.py'
    spec = importlib.util.spec_from_file_location('relay_router', source)
    router = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(router)
    relay = os.environ['IMDS_RELAY_URL']
    if urllib.parse.urlsplit(relay).scheme != 'https':
        raise ValueError('HTTPS relay required')
    params = {'resource': 'https://api.fabric.microsoft.com'}
    if os.environ.get('UAMI_CLIENT_ID'):
        params['client_id'] = os.environ['UAMI_CLIENT_ID']
    sas = router._generate_sas_token(router._relay_sas_uri(relay), router.RELAY_SENDER_KEY, router.RELAY_KEY_NAME)
    request = urllib.request.Request(relay + '?' + urllib.parse.urlencode(params),
                                     headers={'ServiceBusAuthorization': sas})
    with urllib.request.urlopen(request, timeout=65) as response:
        token = json.load(response)['access_token']
    workspace = os.environ['FABRIC_WORKSPACE_ID']
    base = f'https://api.fabric.microsoft.com/v1/workspaces/{workspace}'
    calls = []

    def get(path):
        # Fixed-origin GETs only. Neither exception text nor response bodies are logged.
        request = urllib.request.Request(base + path, headers={'Authorization': 'Bearer ' + token})
        try:
            with urllib.request.urlopen(request, timeout=45) as response:
                data = json.load(response)
                calls.append({'path': path.split('?')[0], 'status': response.status})
                return data
        except urllib.error.HTTPError as error:
            entry = {'path': path.split('?')[0], 'status': error.code}
            try:
                body = json.loads(error.read(4096))
                code = body.get('errorCode') or body.get('error', {}).get('code')
                if isinstance(code, str) and re.fullmatch(r'[A-Za-z0-9_]{1,80}', code):
                    entry['error_code'] = code
            except (ValueError, AttributeError):
                pass
            calls.append(entry)
        except Exception as error:
            calls.append({'path': path.split('?')[0], 'error_type': type(error).__name__})
        return None

    sessions = []
    continuation = None
    for _ in range(12):
        path = '/spark/livySessions'
        if continuation:
            path += '?' + urllib.parse.urlencode({'continuationToken': continuation})
        data = get(path)
        if not data:
            break
        sessions.extend(data.get('value', []))
        continuation = data.get('continuationToken')
        found = {s.get('item', {}).get('itemId') for s in sessions}
        if not continuation or set(TARGETS.values()).issubset(found):
            break
    # Do not export other pipelines' names, queries, identities, or signed log links.
    matches = {label: [s for s in sessions if s.get('item', {}).get('itemId') == item]
               for label, item in TARGETS.items()}
    report = {'session_count_seen': len(sessions), 'targets': {}}
    for label, selected in matches.items():
        records = []
        for session in selected:
            item = session['item']['itemId']
            livy = session.get('livyId', '')
            app = session.get('sparkApplicationId', '')
            if not re.fullmatch(r'[0-9a-fA-F-]{36}', livy) or not re.fullmatch(r'application_\d+_\d+', app):
                continue
            prefix = f'/lakehouses/{item}/livySessions/{livy}/applications/{app}'
            record = {'session': pick(session, ('sparkApplicationId', 'state', 'livyId', 'capacityId',
                      'runtimeVersion', 'submittedDateTime', 'startDateTime', 'endDateTime',
                      'queuedDuration', 'runningDuration', 'totalDuration',
                      'driverCores', 'driverMemory', 'executorCores', 'executorMemory',
                      'numExecutors', 'dynamicAllocationMaxExecutors', 'isDynamicAllocationEnabled',
                      'isHighConcurrency', 'attemptNumber')),
                      'available_session_fields': sorted(session)}
            resource_uri = session.get('livySessionItemResourceUri')
            if resource_uri:
                record['resource_uri_path'] = urllib.parse.urlsplit(resource_uri).path
            detail = get(f'/lakehouses/{item}/livySessions/{livy}')
            if detail:
                record['detailed_resources'] = pick(detail, ('driverCores', 'driverMemory',
                    'executorCores', 'executorMemory', 'numExecutors', 'dynamicAllocationMaxExecutors',
                    'isDynamicAllocationEnabled', 'isHighConcurrency', 'attemptNumber'))
            get(f'/items/{item}')
            env = get(prefix + '/environment')
            if env:
                record['spark_properties'] = {k: v for k, v in env.get('sparkProperties', []) if k in CONF}
                record['runtime'] = env.get('runtime', {})
            executors = get(prefix + '/allexecutors')
            if isinstance(executors, list):
                record['executors'] = [pick(row, CORE_FIELDS) for row in executors]
            stages = get(prefix + '/stages?details=false')
            if isinstance(stages, list):
                record['stages'] = [pick(row, STAGE_FIELDS) for row in stages]
            usage = get(prefix + '/resourceUsage')
            if usage:
                record['resource_usage'] = pick(usage, ('duration', 'idleTime', 'coreEfficiency', 'capacityExceeded'))
                record['resource_usage']['data'] = pick(usage.get('data', {}),
                    ('timestamps', 'allocatedCores', 'idleCores', 'runningCores', 'isPartials'))
            records.append(record)
        report['targets'][label] = records
    report['calls'] = calls
    (OUT / 'history.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'session_count_seen': len(sessions),
                      'target_matches': {k: len(v) for k, v in report['targets'].items()},
                      'request_statuses': [r.get('status', r.get('error_type')) for r in calls]}))


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        OUT.mkdir(exist_ok=True)
        (OUT / 'failure.json').write_text(json.dumps({'error_type': type(error).__name__}) + '\n')
        print(json.dumps({'error_type': type(error).__name__}))
        raise SystemExit(1)
