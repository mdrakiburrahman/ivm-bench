import io
import json
import os
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import MagicMock as Mock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0,str(Path(__file__).parents[1]))
os.environ['EXTENDED_TPCDS_WORKLOAD_DIR']=str(Path(__file__).parents[3]/'workloads/extended-tpcds')
from services import extended_tpcds_postgres as pg


class PostgresWorkloadTests(unittest.TestCase):
    def test_all_96_models_render_without_dropping_safe_casts_or_null_semantics(self):
        from services.extended_tpcds import pipeline
        for engine in pg.ENGINES:
            queries={n:pg.render(pipeline.render_model(n,{'report_date':'2002-12-31','history_months':12},'all'),engine)
                     for n in pipeline.selected_models()}
            self.assertEqual(len(queries),96)
            self.assertIn('TRY_CAST(',queries['meta_ingestion'])
            self.assertNotIn('IGNORE NULLS',queries['int_channel_status_latest'])
            self.assertIn('IS NULL',queries['int_channel_status_latest'])

    def test_window_rewrite_preserves_nulls_duplicates_and_whole_partition_results(self):
        source='SELECT k,t,v,FIRST_VALUE(v IGNORE NULLS) OVER (PARTITION BY k ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first FROM input'
        with duckdb.connect() as db:
            db.execute('CREATE TABLE input(k INT,t INT,v VARCHAR)')
            db.execute("INSERT INTO input VALUES (1,1,NULL),(1,2,'a'),(1,2,'a'),(2,1,NULL),(NULL,1,'b')")
            expected=db.execute(source).fetchall()
            for engine in pg.ENGINES:
                rendered=pg.render(source,engine)
                lowered=pg.sqlglot.transpile(rendered,read='postgres',write='duckdb')[0]
                self.assertCountEqual(db.execute(lowered).fetchall(),expected)
        with self.assertRaisesRegex(ValueError,'whole-partition'):
            pg.render('SELECT FIRST_VALUE(v IGNORE NULLS) OVER (ORDER BY t) FROM input','materialize-emulator')

    def test_nullable_casts_dates_and_json_keep_the_reference_results(self):
        source="SELECT TRY_CAST(v AS INT),CAST(TRY_STRPTIME(d,'%Y%m%d') AS DATE),json_extract_string(j,'$.x') FROM input"
        with duckdb.connect() as db:
            db.execute('CREATE TABLE input(v VARCHAR,d VARCHAR,j VARCHAR)')
            db.execute("INSERT INTO input VALUES ('12','20021231','{\"x\":\"café\"}'),('invalid','20020230','{}'),(NULL,'01',NULL),('2',NULL,'{\"x\":null}')")
            expected=db.execute(source).fetchall()
            for engine in pg.ENGINES:
                lowered=pg.sqlglot.transpile(pg.render(source,engine),read='postgres',write='duckdb')[0]
                self.assertCountEqual(db.execute(lowered).fetchall(),expected)

    def test_copy_is_bounded_and_preserves_uuid_nulls_and_escaped_values(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'rows.parquet';token=uuid.uuid4()
            pq.write_table(pa.table({'id':[str(token),str(token)+'b'],'v':['a\tb\nc\\d',None]}),path)
            connection=Mock();cursor=connection.cursor.return_value.__enter__.return_value
            chunks=[]
            cursor.copy_expert.side_effect=lambda statement,stream:chunks.append(stream.read())
            self.assertEqual(pg.load_image(connection,path,'events',{'id':'UUID','v':'VARCHAR'},'materialize-emulator'),2)
            data=b''.join(chunks).decode()
            self.assertIn('a\\tb\\nc\\\\d',data)
            self.assertIn('\\N',data)
            self.assertIn(str(token),data)

    def test_risingwave_inserts_use_bounded_values_without_retries(self):
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'rows.parquet';pq.write_table(pa.table({'id':range(300)}),path)
            connection=Mock()
            with patch.object(pg,'execute_values') as execute:
                self.assertEqual(pg.load_image(connection,path,'events',{'id':'BIGINT'},'risingwave'),300)
            self.assertEqual(execute.call_args.kwargs['page_size'],128)
            self.assertEqual(len(execute.call_args.args[2]),300)

    def test_source_checksum_and_unsupported_mutations_fail_before_writes(self):
        from services.extended_tpcds import mutations
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);p=root/'rows.parquet';pq.write_table(pa.table({'id':[1]}),p)
            image={'path':p.name,'rows':1,'sha256':'wrong'}
            manifest={'schemas':{'events':{'id':'BIGINT'}},'initial':{'events':image},
                      'batches':[{'operations':[{'kind':'delete','table':'events','before':image,'after':None}]}]}
            connection=Mock()
            with self.assertRaisesRegex(ValueError,'checksum'):
                pg.replay(connection,manifest,root,1,'risingwave')
            with self.assertRaisesRegex(ValueError,'insert-only'):
                pg.replay(connection,manifest,root,2,'risingwave')
            connection.cursor.assert_not_called()

    def test_connection_closes_even_when_session_setup_fails(self):
        with patch.object(pg.psycopg2,'connect') as factory:
            factory.return_value.cursor.return_value.__enter__.return_value.execute.side_effect=RuntimeError('setup failed')
            with self.assertRaisesRegex(RuntimeError,'setup failed'):
                with pg.connect('risingwave',100):pass
            factory.return_value.close.assert_called_once()

    def test_broken_or_repeated_checkpoint_is_rejected_before_connecting(self):
        from services.extended_tpcds import pipeline
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);p=root/'manifest.json'
            p.write_text(json.dumps({'plan_id':'p','source_configuration':{}}))
            (root/'pg-checkpoint.json').write_text(json.dumps({'batch_num':1,'dirty':True,'plan_id':'p'}))
            with patch.object(pipeline,'selected_models',return_value=[]),patch.object(pg,'connect') as connect:
                with self.assertRaisesRegex(ValueError,'checkpoint'):
                    pg.run_batch('risingwave',2,p,root,{'query_cohort':'all'})
                connect.assert_not_called()

    def test_rejected_model_keeps_dirty_checkpoint_and_never_becomes_a_table(self):
        from services.extended_tpcds import pipeline
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);p=root/'manifest.json'
            p.write_text(json.dumps({'plan_id':'p','source_configuration':{},'schemas':{},'initial':{}}))
            connection=Mock();cursor=connection.cursor.return_value.__enter__.return_value
            def execute(sql):
                if sql.startswith('CREATE MATERIALIZED VIEW'):raise RuntimeError('unsupported window')
            cursor.execute.side_effect=execute
            with patch.object(pg,'connect') as connect,patch.object(pipeline,'selected_models',return_value=['m']), \
                 patch.object(pipeline,'render_model',return_value='SELECT 1 AS x'):
                connect.return_value.__enter__.return_value=connection
                with self.assertRaisesRegex(RuntimeError,'unsupported window'):
                    pg.run_batch('materialize-emulator',1,p,root,{'query_cohort':'all','statement_timeout_s':300})
            self.assertTrue(json.loads((root/'pg-checkpoint.json').read_text())['dirty'])
            self.assertEqual(json.loads((root/'batch1-failure.json').read_text())['model'],'m')
            self.assertFalse(any(call.args[0].startswith('CREATE TABLE') for call in cursor.execute.call_args_list))
