"""Fabric-DS on native continuously maintained PostgreSQL-wire engines."""
import hashlib
import io
import json
import os
import tempfile
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_values
import sqlglot
from sqlglot import exp
from sqlglot.dialects.postgres import Postgres
from sql_catalog import load_sql

ENGINES = ('materialize-emulator', 'risingwave')


class NativePostgres(Postgres):
    class Generator(Postgres.Generator):
        TRANSFORMS = {**Postgres.Generator.TRANSFORMS,
                      exp.TryCast:lambda self,e:self.trycast_sql(e),
                      exp.JSONExtractScalar:lambda self,e:self.jsonextractscalar_sql(e)}
        def trycast_sql(self, expression):
            # Never silently turn a nullable conversion into a throwing CAST.
            return f"TRY_CAST({self.sql(expression, 'this')} AS {self.sql(expression, 'to')})"

        def jsonextractscalar_sql(self, expression):
            path = expression.expression.expressions
            if len(path) != 2 or not isinstance(path[1], exp.JSONPathKey):
                raise ValueError('unsupported workload JSON path')
            key = exp.Literal.string(path[1].this)
            return f"(CAST({self.sql(expression.this)} AS JSONB) ->> {self.sql(key)})"


def render(query, engine):
    from services.extended_tpcds_streaming import lower_first_value_nulls
    tree = sqlglot.parse_one(query, read='duckdb')
    lower_first_value_nulls(tree, engine)
    for function in list(tree.find_all(exp.Anonymous)):
        if function.name.upper() == 'TRY_STRPTIME':
            value, pattern = function.expressions
            if pattern.this != '%Y%m%d':
                raise ValueError('unsupported workload date format')
            # Guard the compact format: native date parsers also accept shorter
            # strings that DuckDB's explicit eight-digit format rejects.
            text = value.sql(dialect=NativePostgres)
            replacement = (f"CASE WHEN {text} ~ '^[0-9]{{8}}$' THEN TRY_CAST("
                           f"SUBSTRING({text},1,4)||'-'||SUBSTRING({text},5,2)||'-'||SUBSTRING({text},7,2) AS DATE) END")
            function.replace(sqlglot.parse_one(replacement, read='postgres'))
    query = tree.sql(dialect=NativePostgres, identify=True, unsupported_level=sqlglot.ErrorLevel.RAISE)
    if engine == 'risingwave':
        from services.compiler_bench.engines_risingwave import to_risingwave_sql
        query = to_risingwave_sql(query)
    return query


def ident(name):
    return '"'+name.replace('"','""')+'"'


@contextmanager
def connect(engine, timeout):
    materialize = engine == 'materialize-emulator'
    prefix = 'MATERIALIZE' if materialize else 'RISINGWAVE'
    connection = psycopg2.connect(
        host=os.environ.get(prefix+'_HOST', engine),
        port=int(os.environ.get(prefix+'_PORT', '6875' if materialize else '4566')),
        user=os.environ.get(prefix+'_USER', 'materialize' if materialize else 'root'),
        password=os.environ.get(prefix+'_PASSWORD', ''),
        dbname=os.environ.get(prefix+'_DATABASE', 'materialize' if materialize else 'dev'),
        connect_timeout=10)
    connection.autocommit = True
    try:
        with connection.cursor() as cursor:
            cursor.execute("SET TIME ZONE 'UTC'")
            cursor.execute(f"SET statement_timeout = '{int(timeout*1000)}ms'")
            if materialize:
                cursor.execute('SET TRANSACTION ISOLATION LEVEL STRICT SERIALIZABLE')
        yield connection
    finally:
        connection.close()


def source_type(kind, engine):
    # UUIDs are physical replay identities, not analytical model columns.
    if kind.upper() == 'UUID':
        return 'TEXT'
    tree = exp.DataType.build(kind, dialect='duckdb')
    result = tree.sql(dialect='postgres')
    if engine == 'risingwave':
        from services.compiler_bench.engines_risingwave import strip_type_params
        result = strip_type_params(result)
    return result


def load_image(connection, path, table, fields, engine):
    from services.materialize_sources import _copy_field, COPY_CHUNK_BYTES
    names = list(fields)
    target = ident(table)+' ('+', '.join(map(ident,names))+')'
    copied = 0
    chunk = io.BytesIO()
    with connection.cursor() as cursor:
        def flush():
            if chunk.tell():
                chunk.seek(0)
                cursor.copy_expert('COPY '+target+' FROM STDIN',chunk)
                chunk.seek(0);chunk.truncate()
        for batch in pq.ParquetFile(path).iter_batches(batch_size=4096):
            values = []
            for row in batch.to_pylist():
                for name,kind in fields.items():
                    if kind.upper()=='UUID' and row[name] is not None:
                        value=row[name]
                        row[name]=str(uuid.UUID(bytes=value)) if isinstance(value,bytes) else str(value)
                values.append(tuple(row[name] for name in names))
            if engine == 'materialize-emulator':
                for row in values:
                    line=('\t'.join(_copy_field(value) for value in row)+'\n').encode()
                    if len(line)>COPY_CHUNK_BYTES:
                        raise ValueError('source row exceeds bounded COPY chunk')
                    if chunk.tell()+len(line)>COPY_CHUNK_BYTES:flush()
                    chunk.write(line)
            else:
                # Bound the SQL AST as well as client RAM; large multi-row
                # INSERTs can exhaust the RisingWave parser's stack.
                execute_values(cursor,'INSERT INTO '+target+' VALUES %s',values,page_size=128)
            copied += len(values)
        flush()
    return copied


def replay(connection, manifest, directory, batch, engine):
    from services.extended_tpcds import mutations
    images = list(manifest['initial'].items()) if batch==1 else []
    if batch>1:
        for op in manifest['batches'][batch-2]['operations']:
            if op['kind']!='insert' or op['before']:
                raise ValueError('PostgreSQL-wire Fabric-DS currently requires insert-only mutations')
            images.append((op['table'],op['after']))
    for table,image in images:
        if table not in manifest['schemas'] or mutations.file_checksum(directory/image['path'])!=image['sha256']:
            raise ValueError('unknown source or image checksum mismatch: '+table)
    with connection.cursor() as cursor:
        for table,image in images:
            fields=manifest['schemas'][table]
            if batch==1:
                cursor.execute('CREATE TABLE '+ident(table)+' ('+', '.join(ident(c)+' '+source_type(t,engine) for c,t in fields.items())+')')
            copied=load_image(connection,directory/image['path'],table,fields,engine)
            if copied!=image['rows']:
                raise ValueError('source image row count mismatch: '+table)
        if engine=='risingwave':cursor.execute(load_sql('risingwave','inline','dbt_server/services_risingwave_sources/flush'))
        for table,_ in images:
            cursor.execute('SELECT count(*)-count(DISTINCT _source_row_id) FROM '+ident(table))
            if cursor.fetchone()[0]:raise ValueError('duplicate physical source identities: '+table)


def run_batch(engine,batch_num,manifest_path,work_dir,options):
    from services.extended_tpcds import pipeline
    from services.extended_tpcds_profile import BatchProfile
    manifest=json.loads(manifest_path.read_text())
    work_dir.mkdir(parents=True,exist_ok=True)
    checkpoint=work_dir/'pg-checkpoint.json'
    schema='xt_'+manifest['plan_id']+'_'+engine.replace('-','_')
    queries={name:render(pipeline.render_model(name,manifest['source_configuration'],options['query_cohort']),engine)
             for name in pipeline.selected_models()}
    identity=hashlib.sha256(json.dumps(queries,sort_keys=True).encode()).hexdigest()
    state={'batch_num':0,'dirty':False,'plan_id':manifest['plan_id'],'sql_identity':identity,'options':options} if not checkpoint.exists() else json.loads(checkpoint.read_text())
    if state['dirty'] or state['batch_num']!=batch_num-1 or state['plan_id']!=manifest['plan_id'] or state['sql_identity']!=identity or state['options']!=options:
        raise ValueError('PostgreSQL engine checkpoint does not match batch/plan/model definitions')
    state['dirty']=True
    checkpoint.write_text(json.dumps(state))
    durations={};counts={}
    with connect(engine,options['statement_timeout_s']) as connection, connection.cursor() as cursor:
        if batch_num==1:cursor.execute('CREATE SCHEMA '+ident(schema))
        cursor.execute('SET search_path = '+ident(schema))
        profile=BatchProfile(engine,batch_num,queries)
        started=time.monotonic()
        replay(connection,manifest,manifest_path.parent,batch_num,engine)
        source_s=time.monotonic()-started
        started=time.monotonic()
        try:
            for name,query in queries.items():
                start=time.monotonic()
                if batch_num==1:
                    cursor.execute('CREATE MATERIALIZED VIEW '+ident(name)+' AS '+query)
                    durations[name]=time.monotonic()-start
                else:durations[name]=None
            if engine=='risingwave':cursor.execute(load_sql('risingwave','inline','dbt_server/services_risingwave_sources/flush'))
            for name in queries:
                # Materialize's strict-serializable read fences each maintained
                # output after source writes, including empty outputs.
                cursor.execute('SELECT 1 FROM '+ident(name)+' LIMIT 1');cursor.fetchall()
        except Exception as error:
            (work_dir/f'batch{batch_num}-failure.json').write_text(json.dumps({'model':name,'error':str(error),'model_queries':queries,'completed_models':durations},indent=2))
            raise
        model_s=time.monotonic()-started
        profiling=profile.finish(durations)
        for name in queries:
            cursor.execute('SELECT count(*) FROM '+ident(name));counts[name]=cursor.fetchone()[0]
    state.update(batch_num=batch_num,dirty=False,schema=schema)
    checkpoint.write_text(json.dumps(state,indent=2))
    materializations={name:{'kind':'materialized_view',
                      'classification':'incremental' if any(sqlglot.parse_one(query,read='postgres').find_all(exp.Table)) else 'static'}
                      for name,query in queries.items()}
    result={'status':'completed','engine':engine,'batch_num':batch_num,'plan_id':manifest['plan_id'],
            'duration_s':source_s+model_s,'source_replay_wall_s_including_guards':source_s,'model_wall_s_including_cli':model_s,
            'model_sql_duration_s':durations,'model_queries':queries,'model_rows':counts,'profiling':profiling,
            'materializations':materializations,
            'classification_counts':{kind:sum(m['classification']==kind for m in materializations.values()) for kind in ('incremental','static')},
            'source_storage':'native_tables',
            'options':options,'configuration':manifest['source_configuration'],
            'timing_semantics':'source insertion plus native MV creation/completion fences; counts and independent validation excluded; per-model refresh time unavailable'}
    (work_dir/f'batch{batch_num}.json').write_text(json.dumps(result,indent=2))
    return result


def validate_batch(engine,batch_num,manifest_path,work_dir,options):
    from services.extended_tpcds import mutations,pipeline
    from services.compiler_bench.engines import _verify_probe
    manifest=json.loads(manifest_path.read_text());state=json.loads((work_dir/'pg-checkpoint.json').read_text())
    if state['dirty'] or state['batch_num']!=batch_num or state['plan_id']!=manifest['plan_id']:
        raise ValueError('validation snapshot does not match committed PostgreSQL batch')
    checks={}
    with tempfile.TemporaryDirectory(dir=work_dir,prefix='validation-') as scratch, \
         duckdb.connect(str(Path(scratch)/'reference.duckdb')) as reference, \
         connect(engine,options.get('validation_timeout_s',14400)) as connection, connection.cursor() as cursor:
        reference.execute("SET TimeZone='UTC'")
        mutations.load_initial(reference,manifest,manifest_path.parent)
        for batch in manifest['batches'][:batch_num-1]:mutations.apply_batch(reference,manifest,manifest_path.parent,batch)
        pipeline.build(reference,cohort=options['query_cohort'])
        cursor.execute('SET search_path = '+ident(state['schema']))
        for name in pipeline.selected_models():
            columns=reference.execute('DESCRIBE '+ident(name)).fetchall()
            arrow_schema=reference.execute('SELECT * FROM '+ident(name)+' LIMIT 0').fetch_arrow_table().schema
            reference.execute('CREATE OR REPLACE TABLE xt_actual AS SELECT * FROM '+ident(name)+' WHERE false')
            cursor.execute('SELECT * FROM '+ident(name))
            if len(cursor.description)!=len(columns):raise ValueError('output schema width differs: '+name)
            while rows:=cursor.fetchmany(4096):
                arrays=[pa.array(values,type=field.type) for values,field in zip(zip(*rows),arrow_schema)]
                reference.register('xt_chunk',pa.Table.from_arrays(arrays,schema=arrow_schema))
                reference.execute('INSERT INTO xt_actual SELECT * FROM xt_chunk')
                reference.unregister('xt_chunk')
            checks[name]=reference.execute(_verify_probe('xt_actual','SELECT * FROM '+ident(name),[(r[0],r[1]) for r in columns],engine='duckdb')).fetchone()[0]
            (work_dir/f'validation{batch_num}.json').write_text(json.dumps({'differences':checks,'models_checked':len(checks),'model':name}))
    return {'status':'passed' if not any(checks.values()) else 'failed','models_checked':len(checks),'differences':checks,
            'comparison':'independent source replay and full DAG; EXCEPT ALL by position; floats rounded to ten decimals'}
