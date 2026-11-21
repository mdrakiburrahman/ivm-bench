"""Execute all translated Spark queries against an independent DuckDB replay.

This checks SQL translation on a tiny synthetic fixture. It does not exercise
OpenIVM maintenance, Delta source replay or a remote engine connection.
"""
import argparse
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import time

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
sys.path.insert(0,str(HERE.parents[1]/'containers/dbt-server'))
import duckdb
import generate
import mutations
import pipeline
from test_workload import fixture
from services.extended_tpcds_sql import translate


def normalize(value):
    if isinstance(value,datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(value,(float,Decimal)):
        if not math.isfinite(float(value)):
            return str(value)
        return Decimal(str(value)).quantize(Decimal('0.0000000001'),rounding=ROUND_HALF_UP)
    return value


def verify(output, mode):
    from pyspark.sql import SparkSession
    import pyspark,sqlglot
    os.environ['TZ']='UTC'
    time.tzset()
    spark=SparkSession.builder.master('local[2]').appName('extended-tpcds-sql-acceptance').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').config('spark.driver.memory','4g').config('spark.sql.session.timeZone','UTC').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    code=[Path(__file__),HERE.parents[1]/'containers/dbt-server/services/extended_tpcds_sql.py',HERE/'generate.py',HERE/'mutations.py',HERE/'test_workload.py',*sorted((HERE/'models').glob('*.sql'))]
    result={'status':'running','scope':__doc__.strip(),'spark':pyspark.__version__,'duckdb':duckdb.__version__,'sqlglot':sqlglot.__version__,
            'java':spark.sparkContext._jvm.java.lang.System.getProperty('java.version'),
            'workers':2,'shuffle_partitions':2,'timezone':'UTC','mode':mode,
            'comparison':'row multisets; numbers rounded half away from zero to 10 decimals; aware timestamps normalized to naive UTC',
            'source_sha256':{str(p.relative_to(HERE.parents[2])):hashlib.sha256(p.read_bytes()).hexdigest() for p in code},'snapshots':[]}
    cached={}
    try:
        with tempfile.TemporaryDirectory() as tmp,fixture() as planner,duckdb.connect() as reference:
            directory=Path(tmp)/'plan'
            configuration=generate.prepare(planner,'base',report_date=date(2002,12,31))
            steps=[] if mode=='initial' else [{'insert_pct':'20','update_pct':'30','delete_pct':'20'}]*2
            manifest=mutations.plan(planner,directory,mode='mixed',steps=steps)
            mutations.load_initial(reference,manifest,directory)
            for batch_num in range(1,len(steps)+2):
                if batch_num>1:
                    mutations.apply_batch(reference,manifest,directory,manifest['batches'][batch_num-2])
                for name in mutations.sources():
                    path=Path(tmp)/f'{name}-{batch_num}.parquet'
                    reference.execute('COPY '+name+' TO '+generate.quote(path)+' (FORMAT PARQUET)')
                    spark.read.parquet(str(path)).createOrReplaceTempView(name)
                pipeline.build(reference)
                snapshot={'batch_num':batch_num,'models':[]}
                result['snapshots'].append(snapshot)
                for name in pipeline.selected_models():
                    query=translate(pipeline.render_model(name,configuration))
                    frame=spark.sql(query).localCheckpoint(eager=True)
                    frame.createOrReplaceTempView(name)
                    if name in cached:
                        cached[name].unpersist(blocking=True)
                    cached[name]=frame
                    expected=Counter(tuple(normalize(v) for v in r) for r in reference.execute('SELECT * FROM '+name).fetchall())
                    actual=Counter(tuple(normalize(v) for v in r) for r in frame.collect())
                    difference=sum((expected-actual).values())+sum((actual-expected).values())
                    snapshot['models'].append({'model':name,'rows':sum(expected.values()),'differences':difference,'spark_sql_sha256':hashlib.sha256(query.encode()).hexdigest()})
                    print(f'batch {batch_num} {name}: rows={sum(expected.values())} differences={difference}',flush=True)
                    if difference:
                        raise ValueError(f'{name}: {difference} row differences')
            result['status']='passed'
    except Exception as error:
        result.update(status='failed',error=str(error))
        raise
    finally:
        result['models_checked']=sum(len(s['models']) for s in result['snapshots'])
        output.write_text(json.dumps(result,indent=2)+'\n')
        spark.stop()


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['initial','mixed'],default='mixed')
    args=parser.parse_args()
    if args.output.exists():
        parser.error('output must be a new file')
    verify(args.output,args.mode)
