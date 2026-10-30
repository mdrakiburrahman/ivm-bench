"""SQL-engine execution of the immutable Extended TPC-DS row-image plan.

The workload owns ordering, checkpoints and validation. Connections own dialect,
remote file placement and engine materialization commands.
"""
import json
import os
import time
from pathlib import Path

import jinja2
import sqlglot
from sqlglot import exp
import yaml

from services.compiler_bench.engines import _verify_probe

SQL_ENGINES = ("spark", "spark-openivm", "fabric-jvm-35", "fabric-openivm-jvm-35",
               "databricks-enzyme", "databricks-openivm")


def identifier(name):
    return '.'.join('`' + part.replace('`', '``') + '`' for part in name.split('.'))


def translate(sql, relations=None):
    tree = sqlglot.parse_one(sql, read='duckdb')
    # Spark requires a literal interval and has a separate null-on-error parser.
    for interval in tree.find_all(exp.Interval):
        value = interval.this
        sign = 1
        while isinstance(value, (exp.Paren, exp.Neg)):
            if isinstance(value, exp.Neg):
                sign *= -1
            value = value.this
        if isinstance(value, exp.Literal) and not value.is_string:
            interval.set('this', exp.Literal.string(str(sign*int(value.this))))
        elif not isinstance(value, exp.Literal):
            interval.replace(exp.Mul(this=interval.this.copy(), expression=exp.Interval(this=exp.Literal.string('1'), unit=interval.args['unit'].copy())))
    for function in list(tree.find_all(exp.Anonymous)):
        if function.name.upper() == 'TRY_STRPTIME':
            source, pattern = function.expressions
            if not isinstance(pattern, exp.Literal) or pattern.this != '%Y%m%d':
                raise ValueError('unsupported workload timestamp format')
            function.replace(exp.Anonymous(this='TRY_TO_TIMESTAMP', expressions=[source.copy(), exp.Literal.string('yyyyMMdd')]))
    # Scope-aware traversal keeps CTE names and aliases local to their query.
    from sqlglot.optimizer.scope import traverse_scope
    for scope in traverse_scope(tree):
        for _, source in scope.sources.items():
            if isinstance(source, exp.Table) and source.name in (relations or {}):
                replacement = sqlglot.to_table(identifier(relations[source.name]), dialect='spark')
                for key in ('this', 'db', 'catalog'):
                    source.set(key, replacement.args.get(key))
    return tree.sql(dialect='spark')


class Connection:
    def __init__(self, engine, directory, manifest, options, *, initialize=False):
        self.engine = engine
        self.timeout = options['statement_timeout_s']
        self.schema = 'xt_' + manifest['plan_id'] + '_' + engine.replace('-', '_') + '_' + str(os.environ.get('DATABRICKS_EXPERIMENT_ID') or os.environ.get('FABRIC_RUN_ID') or 'local').replace('-', '_')
        self.remote_root = str(directory)
        self.statements = []
        if engine.startswith('databricks-'):
            from services import databricks_enzyme_sources as src
            self.catalog = src.CATALOG
            self.handle = src.open_connection()
            if initialize:
                ws = src._workspace_client()
                src._ensure_cache_schema(ws)
                self.remote_root = src._cache_volume_root() + '/extended-tpcds/' + manifest['plan_id']
                src._sync_dir(ws, directory, self.remote_root)
            else:
                self.remote_root = src._cache_volume_root() + '/extended-tpcds/' + manifest['plan_id']
        else:
            from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
            from dbt.adapters.fabricspark.connections import FabricSparkConnectionManager
            from dbt.adapters.contracts.connection import Connection as DbtConnection
            from services.dbt_runner import _inject_fabric_resolved, PROJECTS_DIR
            if engine.startswith('fabric-'):
                from services import fabric
                if initialize:
                    fabric.provision_run(openivm='openivm' in engine)
                    fabric._azcopy(directory, 'Files/extended-tpcds/' + manifest['plan_id'] + '/plan', fabric.resolve_cache_lakehouse())
                self.remote_root = fabric.onelake_abfss('Files/extended-tpcds/' + manifest['plan_id'] + '/plan', fabric.resolve_cache_lakehouse())
            env = os.environ.copy()
            _inject_fabric_resolved(env)
            profile = Path(PROJECTS_DIR) / engine / 'profiles.yml'
            rendered = jinja2.Environment(undefined=jinja2.StrictUndefined).from_string(profile.read_text()).render(env_var=lambda key, default=None: env.get(key, default))
            values = yaml.safe_load(rendered)['tpcdi']['outputs'][engine]
            values = {k:v for k,v in values.items() if k not in ('type','threads')}
            values.update(statement_timeout=int(self.timeout), reuse_session=True)
            credentials = FabricSparkCredentials.from_dict(values)
            dbt_connection = DbtConnection(type='fabricspark', name='extended-tpcds', credentials=credentials)
            self.handle = FabricSparkConnectionManager.open(dbt_connection).handle
            self.catalog = None
        if initialize:
            self.execute('CREATE SCHEMA ' + identifier(self.namespace))
        self.use(self.namespace)

    @property
    def namespace(self):
        return (self.catalog + '.' if self.catalog else '') + self.schema

    def use(self, schema):
        self.execute('USE ' + identifier(schema))

    def execute(self, sql):
        start = time.monotonic()
        record = {'sql':sql, 'status':'failed'}
        try:
            cursor = self.handle.cursor()
            cursor.execute(sql)
            rows = [list(r) for r in cursor.fetchall()] if cursor.description else []
            record['status'] = 'completed'
            return rows
        except Exception as error:
            record['error'] = str(error)
            raise
        finally:
            record['duration_s'] = time.monotonic() - start
            self.statements.append(record)

    def parquet(self, image):
        # Paths come from the checksum-verified local manifest, never SQL text.
        path = self.remote_root.rstrip('/') + '/' + image['path']
        return 'parquet.`' + path.replace('`','``') + '`'

    def close(self):
        self.handle.close()


def replay(connection, manifest, directory, batch_num):
    from services.extended_tpcds import mutations
    images = manifest['initial'].values() if batch_num == 1 else (
        image for op in manifest['batches'][batch_num-2]['operations']
        for image in (op['before'], op['after']) if image)
    for image in images:
        if mutations.file_checksum(directory / image['path']) != image['sha256']:
            raise ValueError('source image checksum mismatch: ' + image['path'])
    if batch_num == 1:
        for table, image in manifest['initial'].items():
            properties = "'delta.enableChangeDataFeed'='true'"
            if connection.engine.startswith("databricks-"):
                properties += ", 'delta.enableRowTracking'='true'"
            connection.execute(f"CREATE TABLE {identifier(table)} USING DELTA TBLPROPERTIES ({properties}) AS SELECT * FROM {connection.parquet(image)}")
        return
    for operation in manifest['batches'][batch_num-2]['operations']:
        table = operation['table']
        if table not in manifest['schemas']:
            raise ValueError('unknown source: ' + table)
        target = identifier(table)
        before, after = operation['before'], operation['after']
        if before:
            expected = connection.parquet(before)
            actual = f'SELECT * FROM {target} WHERE _source_row_id IN (SELECT _source_row_id FROM {expected})'
            mismatch = connection.execute(f'SELECT count(*) FROM ((SELECT * FROM {expected} EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL SELECT * FROM {expected}))')[0][0]
            if mismatch:
                raise ValueError('before-image mismatch: ' + table)
        if operation['kind'] == 'delete':
            connection.execute(f'DELETE FROM {target} WHERE _source_row_id IN (SELECT _source_row_id FROM {connection.parquet(before)})')
        elif operation['kind'] == 'update':
            connection.execute(f'MERGE INTO {target} t USING {connection.parquet(after)} a ON t._source_row_id=a._source_row_id WHEN MATCHED THEN UPDATE SET *')
        elif operation['kind'] == 'insert':
            connection.execute(f'INSERT INTO {target} SELECT * FROM {connection.parquet(after)}')
        else:
            raise ValueError('unknown source operation')
        duplicates = connection.execute(f'SELECT count(*)-count(DISTINCT _source_row_id) FROM {target}')[0][0]
        if duplicates:
            raise ValueError('duplicate source IDs: ' + table)


def queries(manifest, options, namespace):
    from services.extended_tpcds import pipeline
    names = pipeline.selected_models()
    relations = {name:namespace+'.'+name for name in [*manifest['schemas'], *names]}
    return {name:translate(pipeline.render_model(name, manifest['source_configuration'], options['query_cohort']), relations) for name in names}


def build(connection, model_queries, state, *, initial=False, reference=False):
    materializations = state.setdefault('materializations', {})
    durations = {}
    for name, query in model_queries.items():
        started = time.monotonic()
        target = identifier(name)
        maintained = not reference and ('openivm' in connection.engine or connection.engine == 'databricks-enzyme')
        if maintained:
            if connection.engine == 'databricks-openivm':
                raise ValueError('Databricks OpenIVM must use the compiled SQL maintenance path')
            if initial:
                policy = ' REFRESH POLICY AUTO' if connection.engine == 'databricks-enzyme' else ''
                connection.execute(f'CREATE MATERIALIZED VIEW {target}{policy} AS {query}')
            else:
                connection.execute(f'REFRESH MATERIALIZED VIEW {target}')
            classification = 'unknown'
        else:
            if initial:
                connection.execute(f"CREATE TABLE {target} USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true') AS {query}")
            else:
                # Keep identity so maintained consumers see source changes.
                connection.execute(f'INSERT OVERWRITE {target} {query}')
            classification = 'full'
        durations[name] = time.monotonic() - started
        materializations[name] = {'kind':'materialized_view' if maintained else 'table', 'classification':classification}
    return durations


def build_databricks_openivm(connection, model_queries, manifest, state, initial):
    from services import databricks_openivm as ivm
    from services.db import DB_LOCK, get_db
    run_id = 'extended-tpcds-' + manifest['plan_id']
    with DB_LOCK:
        db = get_db()
        db.execute("INSERT OR IGNORE INTO runs(run_id,engine,scale_factor,created_at) VALUES (?,?,?,datetime('now'))", (run_id,connection.engine,manifest['source_configuration'].get('base_scale_factor',0)))
        db.commit()
        db.close()
    known = {connection.namespace+'.'+name:ivm.physical(connection.namespace+'.'+name) for name in model_queries}
    known.update({connection.namespace+'.'+name:connection.namespace+'.'+name for name in manifest['schemas']})
    by_relation = {connection.namespace+'.'+name:'model.extended_tpcds.'+name for name in model_queries}
    models = state.setdefault('models', {})
    materializations = state.setdefault('materializations', {})
    metadata = ivm.BatchMetadata()
    executor = ivm.SqlExecutor()
    durations = {}
    try:
        for name, query in model_queries.items():
            uid = by_relation[connection.namespace+'.'+name]
            parts = connection.namespace.split('.')
            node = {'name':name, 'database':parts[0], 'schema':parts[1], 'compiled_code':query}
            started = time.monotonic()
            executor.model_id = uid
            models[uid] = ivm._build_model(executor, metadata, uid, node, known, models, by_relation, run_id, initial, {'compiler_client_s':0})
            metadata.invalidate_version(known[connection.namespace+'.'+name])
            durations[name] = time.monotonic()-started
            materializations[name] = {'kind':'compiled_sql', 'classification':'full' if models[uid]['refresh_type']==3 else 'incremental', 'refresh_type_name':models[uid].get('refresh_type_name')}
        return durations
    finally:
        connection.statements.extend(executor.statements)
        executor.close()


def run_batch(engine, batch_num, manifest_path, work_dir, options):
    manifest = json.loads(manifest_path.read_text())
    work_dir.mkdir(parents=True, exist_ok=True)
    checkpoint = work_dir / 'sql-checkpoint.json'
    state = {'batch_num':0, 'plan_id':manifest['plan_id'], 'options':options, 'dirty':False} if not checkpoint.exists() else json.loads(checkpoint.read_text())
    if state['dirty'] or state['batch_num'] != batch_num-1 or state['plan_id'] != manifest['plan_id'] or state['options'] != options:
        raise ValueError('SQL engine checkpoint is dirty or does not match the requested batch/plan/options')
    connection = Connection(engine, manifest_path.parent, manifest, options, initialize=batch_num==1)
    try:
        state['dirty'] = True
        checkpoint.write_text(json.dumps(state))
        started = time.monotonic()
        replay(connection, manifest, manifest_path.parent, batch_num)
        source_s = time.monotonic() - started
        started = time.monotonic()
        model_queries = queries(manifest, options, connection.namespace)
        durations = (build_databricks_openivm(connection, model_queries, manifest, state, batch_num==1) if engine=='databricks-openivm' else build(connection, model_queries, state, initial=batch_num==1))
        model_s = time.monotonic() - started
        counts = {name:connection.execute('SELECT count(*) FROM '+identifier(name))[0][0] for name in model_queries}
        state.update(batch_num=batch_num, dirty=False, namespace=connection.namespace)
        checkpoint.write_text(json.dumps(state, indent=2))
        result = {'status':'completed', 'engine':engine, 'batch_num':batch_num, 'plan_id':manifest['plan_id'],
                  'duration_s':source_s+model_s, 'source_replay_wall_s_including_guards':source_s,
                  'model_wall_s_including_cli':model_s, 'model_sql_duration_s':durations, 'model_rows':counts,
                  'materializations':state['materializations'], 'classification_counts':{kind:sum(m['classification']==kind for m in state['materializations'].values()) for kind in {m['classification'] for m in state['materializations'].values()}},
                  'source_storage':'delta', 'timing_semantics':'source replay and complete SQL commands; setup, row counts and validation excluded',
                  'configuration':manifest['source_configuration'], 'options':options}
        (work_dir/f'batch{batch_num}.json').write_text(json.dumps(result, indent=2))
        return result
    finally:
        (work_dir/f'sql-batch{batch_num}.json').write_text(json.dumps(connection.statements, indent=2))
        connection.close()


def validate_batch(engine, batch_num, manifest_path, work_dir, options):
    manifest = json.loads(manifest_path.read_text())
    state = json.loads((work_dir/'sql-checkpoint.json').read_text())
    if state['dirty'] or state['batch_num'] != batch_num or state['plan_id'] != manifest['plan_id']:
        raise ValueError('validation snapshot does not match the committed batch')
    connection = Connection(engine, manifest_path.parent, manifest, options)
    reference = state['namespace'] + '_reference'
    try:
        connection.execute('CREATE SCHEMA '+identifier(reference))
        connection.use(reference)
        for n in range(1,batch_num+1):
            replay(connection, manifest, manifest_path.parent, n)
        model_queries = queries(manifest, options, reference)
        build(connection, model_queries, {}, initial=True, reference=True)
        checks = {}
        for name in model_queries:
            actual = identifier(state['namespace']+'.'+name)
            expected = identifier(reference+'.'+name)
            columns = [(r[0],r[1]) for r in connection.execute('DESCRIBE '+actual) if r[0] and not str(r[0]).startswith('#')]
            probe = _verify_probe(actual, 'SELECT * FROM '+expected, columns)
            checks[name] = connection.execute(probe)[0][0]
        return {'status':'passed' if not any(checks.values()) else 'failed', 'models_checked':len(checks),
                'differences':checks, 'comparison':'independent source replay and full DAG recomputation; EXCEPT ALL; floats rounded to 10 decimals'}
    finally:
        connection.use(state['namespace'])
        connection.execute('DROP SCHEMA '+identifier(reference)+' CASCADE')
        connection.close()


def cleanup(engine, manifest_path, work_dir, options):
    """Drop this engine's isolated schema, including failed partial builds."""
    manifest = json.loads(manifest_path.read_text())
    if engine.startswith('fabric-'):
        from services import fabric
        return fabric.teardown_run()
    connection = Connection(engine, manifest_path.parent, manifest, options)
    try:
        if engine == 'spark-openivm':
            from services.extended_tpcds import pipeline
            for name in reversed(pipeline.selected_models()):
                connection.execute('DROP MATERIALIZED VIEW IF EXISTS '+identifier(name))
        connection.execute('DROP SCHEMA IF EXISTS '+identifier(connection.namespace)+' CASCADE')
        return {'status':'ok'}
    finally:
        connection.close()
