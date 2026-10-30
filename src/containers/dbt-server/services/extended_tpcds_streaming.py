"""Feldera/DBSP.NET execution over the shared Extended TPC-DS mutation plan.

Every model is a materialized, observable output. Delta sources keep their identity
across batches; source mutation and compile spans remain separate from drain time.
"""
import json
import hashlib
import os
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake
import requests
import sqlglot
from sqlglot import exp
from sqlglot.dialects.duckdb import DuckDB

STREAM_ENGINES = ('feldera','dbspnet')


class FelderaDialect(DuckDB):
    class Generator(DuckDB.Generator):
        def trycast_sql(self, expression):
            return f"SAFE_CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"

        def regexpreplace_sql(self, expression):
            modifiers = expression.args.get('modifiers')
            if modifiers is not None and modifiers.this != 'g':
                raise ValueError('unsupported regex modifiers in workload')
            return self.func('REGEXP_REPLACE',expression.this,expression.expression,expression.args.get('replacement'))

        def strtotime_sql(self, expression):
            if expression.args['format'].this != '%Y%m%d':
                raise ValueError('unsupported timestamp format in workload')
            return f"CAST(PARSE_DATE('%Y%m%d', {self.sql(expression, 'this')}) AS TIMESTAMP)"

        def jsonextractscalar_sql(self, expression):
            path=expression.expression.expressions
            if len(path)!=2 or not isinstance(path[1],exp.JSONPathKey):
                raise ValueError('unsupported JSON path in workload')
            key=exp.Literal.string(path[1].this).sql()
            return f"CAST(PARSE_JSON({self.sql(expression, 'this')})[{key}] AS VARCHAR)"


def render(sql, engine):
    tree=sqlglot.parse_one(sql,read='duckdb')
    if engine=='dbspnet':
        return tree.sql(dialect='duckdb')
    for cast in tree.find_all(exp.DataType):
        if cast.this==exp.DataType.Type.TIMESTAMPTZ:
            # The only zoned workload value is the frozen report-date clock at UTC.
            cast.set('this',exp.DataType.Type.TIMESTAMP)
    for function in list(tree.find_all(exp.Anonymous)):
        if function.name.upper()=='TRY_STRPTIME':
            source,pattern=function.expressions
            if pattern.this!='%Y%m%d':
                raise ValueError('unsupported timestamp format in workload')
            function.replace(exp.Cast(this=exp.Anonymous(this='PARSE_DATE',expressions=[pattern.copy(),source.copy()]),to=exp.DataType.build('TIMESTAMP')))
    for interval in list(tree.find_all(exp.Interval)):
        value=interval.this
        if not isinstance(value,exp.Literal):
            interval.replace(exp.Mul(this=value.copy(),expression=exp.Interval(this=exp.Literal.string('1'),unit=interval.args['unit'].copy())))
    return tree.sql(dialect=FelderaDialect)


def program(manifest, work_dir, engine):
    from services.extended_tpcds import pipeline
    sql, inputs, outputs = [], [], []
    for name, fields in manifest['schemas'].items():
        sql.append('CREATE TABLE '+name+' ('+', '.join('"'+c+'" '+t for c,t in fields.items())+')')
        inputs.append({'table':name, 'uri':str(work_dir/'sources'/name), 'mode':'snapshot_and_follow'})
    for name in pipeline.selected_models():
        body=pipeline.render_model(name,manifest['source_configuration'],manifest['_query_cohort'])
        # The target compiler must accept the whole DAG; no models are pruned.
        sql.append('CREATE VIEW '+name+' AS '+render(body,engine))
        outputs.append({'view':name,'uri':str(work_dir/'outputs'/name),'mode':'truncate'})
    return {'program':sql,'inputs':inputs,'outputs':outputs}


def _reader(path):
    parquet=pq.ParquetFile(path)
    return pa.RecordBatchReader.from_batches(parquet.schema_arrow,parquet.iter_batches(batch_size=65536))


def _source(connection, path):
    table=DeltaTable(str(path))
    files=table.file_uris()
    if files:
        connection.read_parquet(files).create_view('xt_source',replace=True)
    else:
        connection.register('xt_source',table.to_pyarrow_table())


def replay(manifest, directory, work_dir, batch_num):
    from services.extended_tpcds import mutations
    images=manifest['initial'].values() if batch_num==1 else (image for op in manifest['batches'][batch_num-2]['operations'] for image in (op['before'],op['after']) if image)
    for image in images:
        if mutations.file_checksum(directory/image['path'])!=image['sha256']:
            raise ValueError('source image checksum mismatch: '+image['path'])
    if batch_num==1:
        for name,image in manifest['initial'].items():
            write_deltalake(str(work_dir/'sources'/name),_reader(directory/image['path']),mode='error')
        return
    with duckdb.connect() as guard:
        for op in manifest['batches'][batch_num-2]['operations']:
            name=op['table']
            if name not in manifest['schemas']:
                raise ValueError('unknown source: '+name)
            path=work_dir/'sources'/name
            before,after=op['before'],op['after']
            if not any(image and image['rows'] for image in (before,after)):
                continue
            if before:
                _source(guard,path)
                guard.read_parquet(str(directory/before['path'])).create_view('xt_before',replace=True)
                actual='SELECT * FROM xt_source WHERE _source_row_id IN (SELECT _source_row_id FROM xt_before)'
                mismatch=guard.execute(f'SELECT count(*) FROM ((SELECT * FROM xt_before EXCEPT ALL {actual}) UNION ALL ({actual} EXCEPT ALL SELECT * FROM xt_before))').fetchall()[0][0]
                if mismatch:
                    raise ValueError('before-image mismatch: '+name)
            if op['kind']=='insert':
                write_deltalake(str(path),_reader(directory/after['path']),mode='append')
            else:
                image=after or before
                merge=DeltaTable(str(path)).merge(_reader(directory/image['path']),predicate='target._source_row_id=source._source_row_id',source_alias='source',target_alias='target')
                if op['kind']=='update':
                    merge.when_matched_update_all().execute()
                elif op['kind']=='delete':
                    merge.when_matched_delete().execute()
                else:
                    raise ValueError('unknown mutation operation')
            _source(guard,path)
            if guard.execute('SELECT count(*)-count(DISTINCT _source_row_id) FROM xt_source').fetchall()[0][0]:
                raise ValueError('duplicate source IDs: '+name)


def _dbsp_request(path, body=None, timeout=1800):
    response=requests.post(os.environ.get('DBSPNET_URL','http://dbspnet-server:8080')+path,json=body,timeout=timeout)
    # Preserve the engine's diagnostic body, including non-JSON failures.
    if not response.ok:
        raise RuntimeError(f'DBSP.NET {path}: HTTP {response.status_code}: {response.text[:4000]}')
    return response.json()


def deploy(spec, engine):
    if engine=='dbspnet':
        return _dbsp_request('/deploy',spec)
    from dbt.adapters.feldera.pipeline_manager import PipelineStateManager
    from feldera.rest.feldera_client import FelderaClient
    from services.extended_tpcds import quote
    from jinja2 import Environment
    import yaml
    profile=Path('/app/dbt-projects/feldera/profiles.yml')
    options=yaml.safe_load(Environment().from_string(profile.read_text()).render(env_var=lambda k,d=None:os.environ.get(k,d)))['tpcdi']['outputs']['feldera']
    manager=PipelineStateManager()
    for statement,binding in zip(spec['program'],spec['inputs']):
        connector=[{'name':binding['table']+'_input','transport':{'name':'delta_table_input','config':{'uri':binding['uri'],'mode':'snapshot_and_follow','transaction_mode':'always','num_parsers':8}}}]
        manager.register_table('tpcdi',binding['table'],statement+" WITH ('connectors'="+quote(json.dumps(connector))+")")
    views=spec['program'][len(spec['inputs']):]
    for statement,binding in zip(views,spec['outputs']):
        connector=[{'name':binding['view']+'_output','transport':{'name':'delta_table_output','config':{'uri':binding['uri'],'mode':'truncate'}}}]
        statement=statement.replace('CREATE VIEW ','CREATE MATERIALIZED VIEW ',1)
        # Feldera's WITH options precede AS for a view definition.
        prefix,body=statement.split(' AS ',1)
        manager.register_view('tpcdi',binding['view'],prefix+" WITH ('connectors'="+quote(json.dumps(connector))+") AS "+body)
    started=time.monotonic()
    manager.deploy(FelderaClient(options['host']),'tpcdi',compilation_profile=options['compilation_profile'],workers=options['workers'],timeout=options['timeout'],max_rss_mb=options.get('max_rss_mb'),dev_tweaks=options.get('dev_tweaks'))
    return {'compileTimeS':time.monotonic()-started,'workers':options['workers']}


def run_batch(engine,batch_num,manifest_path,work_dir,options):
    manifest=json.loads(manifest_path.read_text())
    manifest['_query_cohort']=options['query_cohort']
    work_dir.mkdir(parents=True,exist_ok=True)
    checkpoint=work_dir/'stream-checkpoint.json'
    state=json.loads(checkpoint.read_text()) if checkpoint.exists() else {'batch_num':0,'plan_id':manifest['plan_id'],'options':options,'dirty':False}
    if state['dirty'] or state['batch_num']!=batch_num-1 or state['plan_id']!=manifest['plan_id'] or state['options']!=options:
        raise ValueError('streaming engine checkpoint is dirty or does not match the batch/plan/options')
    spec=program(manifest,work_dir,engine)
    sql_identity=hashlib.sha256(json.dumps(spec,sort_keys=True).encode()).hexdigest()
    if batch_num>1 and state.get('sql_identity') != sql_identity:
        raise ValueError('streaming program changed after initialization')
    state['sql_identity']=sql_identity
    state['dirty']=True
    checkpoint.write_text(json.dumps(state))
    if engine=='feldera' and batch_num>1:
        from services import feldera_client as feldera
        if not feldera.pause_pipeline():
            raise RuntimeError('could not pause Feldera before source mutation')
    started=time.monotonic()
    replay(manifest,manifest_path.parent,work_dir,batch_num)
    source_s=time.monotonic()-started
    (work_dir/'program.json').write_text(json.dumps(spec,indent=2))
    compilation=deploy(spec,engine) if batch_num==1 else {}
    started=time.monotonic()
    no_changes = batch_num>1 and not any(image and image['rows'] for op in manifest['batches'][batch_num-2]['operations'] for image in (op['before'],op['after']))
    if no_changes:
        drain = {'durationS':0.0,'status':'no_input_changes'}
    elif engine=='dbspnet':
        _dbsp_request('/resume')
        drain=_dbsp_request('/wait',timeout=86400)
    else:
        from services import feldera_client as feldera
        stats=feldera.get_stats()
        if not stats:
            raise RuntimeError('Feldera stats unavailable before resume')
        baseline_steps=stats.get('global_metrics',{}).get('total_initiated_steps',0)
        if not feldera.resume_pipeline():
            raise RuntimeError('could not resume Feldera')
        success,elapsed,per_output=feldera.poll_output_completion(baseline_steps=baseline_steps)
        if not success:
            raise RuntimeError('Feldera did not drain all outputs')
        drain={'durationS':elapsed,'per_output_completion_s':per_output}
    elapsed=time.monotonic()-started
    rows={}
    with duckdb.connect() as counter:
        for output in spec['outputs']:
            _source(counter,Path(output['uri']))
            rows[output['view']]=counter.execute('SELECT count(*) FROM xt_source').fetchall()[0][0]
    state.update(batch_num=batch_num,dirty=False)
    checkpoint.write_text(json.dumps(state,indent=2))
    result={'status':'completed','engine':engine,'batch_num':batch_num,'plan_id':manifest['plan_id'],
            'duration_s':source_s+elapsed,'source_replay_wall_s_including_guards':source_s,'model_wall_s_including_cli':elapsed,
            'compile':compilation,'program':spec,'drain':drain,'model_rows':rows,'model_sql_duration_s':{name:None for name in rows},
            'classification_counts':{'incremental':len(rows)},'source_storage':'delta','options':options,
            'configuration':manifest['source_configuration'],
            'timing_semantics':'source mutation plus whole-pipeline resume-to-drain; compilation, output row counts and validation excluded; per-model SQL time unavailable'}
    (work_dir/f'batch{batch_num}.json').write_text(json.dumps(result,indent=2))
    return result


def validate_batch(engine,batch_num,manifest_path,work_dir,options):
    from services.extended_tpcds import mutations,pipeline,quote
    from services.compiler_bench.engines import _verify_probe
    manifest=json.loads(manifest_path.read_text())
    state=json.loads((work_dir/'stream-checkpoint.json').read_text())
    if state['dirty'] or state['batch_num']!=batch_num or state['plan_id']!=manifest['plan_id']:
        raise ValueError('validation snapshot differs from the committed stream batch')
    checks={}
    with duckdb.connect() as reference:
        mutations.load_initial(reference,manifest,manifest_path.parent)
        for batch in manifest['batches'][:batch_num-1]:
            mutations.apply_batch(reference,manifest,manifest_path.parent,batch)
        pipeline.build(reference,cohort=options['query_cohort'])
        for name in pipeline.selected_models():
            _source(reference,work_dir/'outputs'/name)
            columns=reference.execute('DESCRIBE '+name).fetchall()
            checks[name]=reference.execute(_verify_probe('xt_source','SELECT * FROM '+name,[(r[0],r[1]) for r in columns])).fetchall()[0][0]
    return {'status':'passed' if not any(checks.values()) else 'failed','models_checked':len(checks),'differences':checks,
            'comparison':'fresh DuckDB source replay and full recomputation; EXCEPT ALL; float values rounded to 10 decimals'}
