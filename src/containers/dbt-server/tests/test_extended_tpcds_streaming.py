import importlib.util
import json
import hashlib
import os
import stat
from pathlib import Path
import sys
import tempfile
import unittest
from decimal import Decimal, ROUND_DOWN
from unittest.mock import Mock,patch

APP=Path(__file__).parents[1]
sys.path.insert(0,str(APP))
os.environ['EXTENDED_TPCDS_WORKLOAD_DIR']=str(APP.parents[1]/'workloads/extended-tpcds')
spec=importlib.util.spec_from_file_location('extended_tpcds_streaming_tested',APP/'services/extended_tpcds_streaming.py')
module=importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ExtendedTPCDSStreamingTest(unittest.TestCase):
    def test_feldera_round_uses_explicit_precision_and_preserves_signed_half_ties(self):
        query = 'SELECT ROUND(value), ROUND(value, 1), ROUND(value, -1) FROM amounts'
        rendered = module.render(query, 'feldera')
        self.assertIn('ROUND("value", 0)', rendered)
        self.assertIn('ROUND("value", 1)', rendered)
        self.assertIn('ROUND("value", -1)', rendered)
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE amounts(value DECIMAL(28,10))')
            c.execute('INSERT INTO amounts VALUES (0.5),(1.5),(2.5),(-0.5),(-1.5),(-2.5),(0.49),(0.51),(NULL)')
            self.assertEqual(c.execute(rendered).fetchall(), c.execute(query).fetchall())
            self.assertNotEqual(c.execute('SELECT round_even(value,0) FROM amounts').fetchall(),
                                c.execute('SELECT round(value) FROM amounts').fetchall())
        self.assertIn('ROUND("value")', module.render(query, 'dbspnet'))

    def test_feldera_monthly_snapshot_keeps_half_units_after_latest_event_deduplication(self):
        from services.extended_tpcds import pipeline
        query = pipeline.render_model('fact_engagement_status_monthly_snapshot',
                                      {'report_date':'2002-12-31','history_months':12})
        rendered = module.render(query, 'feldera')
        self.assertIn('ROUND("effective_units_count", 0)', rendered)
        with module.duckdb.connect() as c:
            dimensions = ['account_offer_key','account_usage_key','platform_key','preference_key',
                          'product_tier_key','product_version_key','engagement_type_key','mode_key']
            c.execute('CREATE TABLE int_engagement_status_transaction AS SELECT '
                      "'2002-01-02'::TIMESTAMP AS event_time, household_key, "
                      + ', '.join('1 AS '+name for name in dimensions)
                      + ', CAST(household_key AS VARCHAR) AS engagement_path, effective_units '
                      'FROM (VALUES (1,0.5::DOUBLE),(2,2.5),(3,-0.5),(4,-2.5)) t(household_key,effective_units)')
            c.execute("INSERT INTO int_engagement_status_transaction SELECT "
                      "'2002-01-01'::TIMESTAMP, household_key, " + ', '.join(dimensions)
                      + ', engagement_path, 100.0 FROM int_engagement_status_transaction')
            c.execute("INSERT INTO int_engagement_status_transaction SELECT '2002-01-02'::TIMESTAMP, 5, "
                      + ', '.join('1' for _ in dimensions)
                      + ", 'fraction-'||n, 1.0/6 FROM range(3) t(n)")
            expected = [(1,1,1),(2,1,3),(3,1,-1),(4,1,-3),(5,3,1)]
            # Feldera converts DOUBLE via its shortest decimal string, then
            # truncates to the target scale; DuckDB's cast rounds instead.
            c.create_function('feldera_decimal',lambda value: Decimal(str(value)).quantize(
                Decimal('0.0000000001'),rounding=ROUND_DOWN),['DOUBLE'],'DECIMAL(28,10)')
            decimal_cast = 'CAST("effective_units" AS DECIMAL(28, 10))'
            unrounded = rendered.replace('ROUND("effective_units", 10)', '"effective_units"')
            unrounded = unrounded.replace(decimal_cast, 'feldera_decimal("effective_units")')
            self.assertEqual(c.execute('SELECT effective_units_count FROM ('
                + unrounded.replace('SAFE_CAST(', 'TRY_CAST(')
                + ') WHERE household_key=5').fetchone()[0],0)
            feldera_cast = rendered.replace('CAST(ROUND("effective_units", 10) AS DECIMAL(28, 10))',
                                             'feldera_decimal(ROUND("effective_units", 10))')
            for sql in (query,rendered.replace('SAFE_CAST(', 'TRY_CAST('),
                        feldera_cast.replace('SAFE_CAST(', 'TRY_CAST(')):
                rows = c.execute('SELECT household_key, engagement_total_count, effective_units_count '
                                 'FROM ('+sql+') ORDER BY household_key').fetchall()
                self.assertEqual(rows, expected)

    def test_validation_compares_one_disk_backed_snapshot_and_preserves_failure_progress(self):
        from services import extended_tpcds as workload
        from services.compiler_bench import engines
        for fail in (False,True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as folder:
                root=Path(folder);work=root/'work';work.mkdir();manifest=root/'manifest.json'
                manifest.write_text(json.dumps({'plan_id':'p','schemas':{},'initial':{},'batches':[]}))
                (work/'stream-checkpoint.json').write_text(json.dumps({'plan_id':'p','batch_num':1,'dirty':False}))
                output=work/'outputs/v'
                module.write_deltalake(str(output),module.pa.table({'key':[2],'value':['new'],
                    '__feldera_op':['i'],'__feldera_ts':[1]}),mode='error')
                def build(reference,**kwargs):
                    self.assertEqual(reference.execute('SELECT current_database()').fetchone()[0],'reference')
                    reference.execute("CREATE TABLE v AS SELECT 2::BIGINT AS key, 'new' AS value")
                original_probe=engines._verify_probe
                def probe(actual,sql,columns):
                    self.assertEqual(actual,'xt_actual')
                    # The output advances after its snapshot. Both EXCEPT ALL
                    # directions must still read the same captured rows.
                    module.write_deltalake(str(output),module.pa.table({'key':[2,3],'value':['new','later'],
                        '__feldera_op':['d','i'],'__feldera_ts':[2,2]}),mode='append')
                    if fail:
                        raise RuntimeError('forced comparison failure')
                    return original_probe(actual,sql,columns)
                with patch.object(workload.mutations,'load_initial'), \
                     patch.object(workload.pipeline,'build',side_effect=build), \
                     patch.object(workload.pipeline,'selected_models',return_value=['v']), \
                     patch.object(engines,'_verify_probe',side_effect=probe):
                    if fail:
                        with self.assertRaisesRegex(RuntimeError,'forced comparison failure'):
                            module.validate_batch('feldera',1,manifest,work,{'query_cohort':'all'})
                    else:
                        result=module.validate_batch('feldera',1,manifest,work,{'query_cohort':'all'})
                        self.assertEqual(result['status'],'passed')
                        self.assertEqual(result['differences'],{'v':0})
                        self.assertEqual(result['output_encodings'],{'v':'feldera_cdc'})
                saved=json.loads((work/'validation1.json').read_text())
                self.assertEqual(saved['status'],'error' if fail else 'passed')
                self.assertEqual(saved['model'],'v')
                self.assertEqual(list(work.glob('validation-*')),[])
                if fail:
                    self.assertEqual(saved['stage'],'full bag comparison')
                    self.assertEqual(saved['error'],'forced comparison failure')

    def test_delta_output_reader_folds_cdc_bags_with_nulls_deletes_and_duplicates(self):
        with tempfile.TemporaryDirectory() as folder, module.duckdb.connect() as connection:
            rows=[(1,'gone','i'),(1,'gone','d'),(2,None,'i'),(2,None,'i'),(2,None,'d'),
                  (3,'old','i'),(3,'old','d'),(3,'new','i'),(4,'duplicate','i'),(4,'duplicate','i')]
            table=module.pa.table({'key':[r[0] for r in rows],'value':[r[1] for r in rows],
                '__feldera_op':[r[2] for r in rows],'__feldera_ts':list(range(len(rows)))})
            module.write_deltalake(folder,table,mode='error')
            module._source(connection,Path(folder))
            self.assertEqual(connection.execute('SELECT * FROM xt_source ORDER BY key').fetchall(),
                             [(2,None),(3,'new'),(4,'duplicate'),(4,'duplicate')])
            self.assertEqual([r[0] for r in connection.description],['key','value'])

    def test_delta_output_reader_rejects_unkeyed_updates_and_negative_counts(self):
        for operation in ('u','d'):
            with self.subTest(operation=operation), tempfile.TemporaryDirectory() as folder, module.duckdb.connect() as connection:
                module.write_deltalake(folder,module.pa.table({'key':[1],'__feldera_op':[operation],'__feldera_ts':[1]}),mode='error')
                module._source(connection,Path(folder))
                with self.assertRaisesRegex(module.duckdb.Error,'Feldera CDC'):
                    connection.execute('SELECT * FROM xt_source').fetchall()

    def test_external_writer_can_access_output_parent_before_deploy_with_restrictive_umask(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);work=root/'work';manifest=root/'manifest.json'
            manifest.write_text(json.dumps({'plan_id':'p','schemas':{},'initial':{},'batches':[]}))
            def check_outputs(_spec,_engine):
                self.assertEqual(stat.S_IMODE((work/'outputs').stat().st_mode),0o777)
                self.assertEqual(work.stat().st_mode & 0o055,0o055)
                self.assertFalse((work/'stream-checkpoint.json').stat().st_mode & stat.S_IWOTH)
                self.assertTrue(json.loads((work/'stream-checkpoint.json').read_text())['dirty'])
                raise RuntimeError('stop after checking external writer access')
            old_umask=os.umask(0o077)
            try:
                with patch.object(module,'program',return_value={'program':[],'inputs':[],'outputs':[]}), \
                     patch.object(module,'replay'),patch.object(module,'deploy',side_effect=check_outputs), \
                     self.assertRaisesRegex(RuntimeError,'external writer access'):
                    module.run_batch('feldera',1,manifest,work,{'query_cohort':'all'})
            finally:
                os.umask(old_umask)

    def image(self,root,name,ids,values):
        path=root/(name+'.parquet')
        module.pq.write_table(module.pa.table({'_source_row_id':ids,'value':values}),path,compression='zstd')
        return {'path':path.name,'rows':len(ids),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}

    def test_real_delta_replay_preserves_identity_and_retracts_updates_and_deletes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);work=root/'work'
            initial=self.image(root,'initial',['a','b'],[1,2])
            before_update=self.image(root,'update_before',['a'],[1]);after_update=self.image(root,'update_after',['a'],[3])
            before_delete=self.image(root,'delete_before',['b'],[2]);insert=self.image(root,'insert',['c'],[4])
            manifest={'schemas':{'events':{'_source_row_id':'VARCHAR','value':'INTEGER'}},'initial':{'events':initial},'batches':[{'operations':[
                {'table':'events','kind':'update','before':before_update,'after':after_update},
                {'table':'events','kind':'delete','before':before_delete,'after':None},
                {'table':'events','kind':'insert','before':None,'after':insert}]}]}
            module.replay(manifest,root,work,1)
            table_id=module.DeltaTable(str(work/'sources/events')).metadata().id
            module.replay(manifest,root,work,2)
            table=module.DeltaTable(str(work/'sources/events'))
            self.assertEqual(table.metadata().id,table_id)
            self.assertEqual(sorted((r['_source_row_id'],r['value']) for r in table.to_pyarrow_table().to_pylist()),[('a',3),('c',4)])
            with self.assertRaisesRegex(ValueError,'before-image mismatch'):
                module.replay(manifest,root,work,2)

    def test_checksum_rejection_happens_before_source_writes(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);image=self.image(root,'initial',['a'],[1]);image['sha256']='wrong'
            with patch.object(module,'write_deltalake') as write:
                with self.assertRaisesRegex(ValueError,'checksum'):
                    module.replay({'initial':{'events':image}},root,root/'work',1)
                write.assert_not_called()

    def test_dbsp_error_keeps_original_server_failure(self):
        response=Mock(ok=False,status_code=500,text='System.OutOfMemoryException: aggregate resize')
        with patch.object(module.requests,'post',return_value=response):
            with self.assertRaisesRegex(RuntimeError,'OutOfMemoryException'):
                module._dbsp_request('/wait')
        response.json.assert_not_called()

    def test_feldera_wait_uses_pre_resume_progress_for_fast_batches(self):
        from services import feldera_client
        stats={'global_metrics':{'total_initiated_steps':2,'total_completed_steps':2,'pipeline_complete':True},'outputs':[]}
        with patch.object(feldera_client,'get_stats',return_value=stats) as get_stats:
            success,_,_=feldera_client.poll_output_completion(baseline_steps=1)
        self.assertTrue(success)
        self.assertEqual(get_stats.call_count,1)

    def test_feldera_translation_preserves_null_on_error_and_global_regex(self):
        sql="SELECT TRY_CAST(x AS INTEGER), TRY_STRPTIME(d,'%Y%m%d'), json_extract_string(j,'$.CustomerPath'), regexp_replace(x,'a','b','g')"
        actual=module.render(sql,'feldera')
        self.assertIn('SAFE_CAST("x" AS INT)',actual)
        self.assertIn('PARSE_DATE(\'%Y%m%d\', "d")',actual)
        self.assertIn('PARSE_JSON("j")[\'CustomerPath\']',actual)
        self.assertIn('REGEXP_REPLACE("x", \'a\', \'b\')',actual)
        self.assertNotIn('TRY_STRPTIME',actual)

    def test_feldera_position_preserves_argument_order_unicode_and_nulls(self):
        query = 'SELECT STRPOS(haystack, needle) FROM strings'
        rendered = module.render(query, 'feldera')
        self.assertIn('POSITION("needle" IN "haystack")', rendered)
        self.assertNotIn('STRPOS(', rendered)
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE strings(haystack VARCHAR, needle VARCHAR)')
            c.executemany('INSERT INTO strings VALUES (?, ?)', [
                ('x/order-lines/a', '/order-lines/'), ('none', '/order-lines/'),
                ('☃é/order-lines/a', '/order-lines/'), ('same', 'same'),
                ('a', ''), ('', 'a'), (None, 'a'), ('a', None), (None, None)])
            self.assertCountEqual(c.execute(rendered).fetchall(), c.execute(query).fetchall())

    def test_feldera_order_line_model_preserves_distinct_counts_and_path_filters(self):
        from services.extended_tpcds import pipeline
        query = pipeline.render_model('int_engagement_order_line_counts', {'report_date':'2002-12-31', 'history_months':12})
        rendered = module.render(query, 'feldera')
        self.assertNotIn('STRPOS(', rendered)
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE stg_entity_registry(entity_path VARCHAR, rn INT, entity_type VARCHAR)')
            c.executemany('INSERT INTO stg_entity_registry VALUES (?, ?, ?)', [
                ('/a/order-lines/x',1,'order_lines'), ('/a/order-lines/x',1,'order_lines'),
                ('/a/order-lines/y',1,'order_lines'), ('/a/order-lines/test',1,'order_lines'),
                ('/☃/order-lines/é',1,'order_lines'), ('/order-lines/z',1,'order_lines'),
                ('invalid',1,'order_lines'), (None,1,'order_lines'),
                ('/a/order-lines/z',2,'order_lines'), ('/a/order-lines/z',1,'other')])
            expected = [('/a',2), ('/☃',1)]
            self.assertCountEqual(c.execute(query).fetchall(), expected)
            self.assertCountEqual(c.execute(rendered).fetchall(), expected)

    def test_feldera_quotes_reserved_columns_and_preserves_bag_results(self):
        query = 'SELECT date, year, month, day FROM events'
        actual = module.render(query, 'feldera')
        self.assertEqual(actual, 'SELECT "date", "year", "month", "day" FROM "events"')
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE events(date DATE, year INTEGER, month INTEGER, day INTEGER)')
            c.execute("INSERT INTO events VALUES ('2002-12-31', 2002, 12, 31), (NULL, NULL, NULL, NULL)")
            self.assertCountEqual(c.execute(actual).fetchall(), c.execute(query).fetchall())

    def test_feldera_uses_null_last_order_for_whole_partition_first_value(self):
        query = 'SELECT FIRST_VALUE(v IGNORE NULLS) OVER (PARTITION BY k ORDER BY t ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM events'
        actual = module.render(query, 'feldera')
        self.assertIn('FIRST_VALUE("v") OVER', actual)
        self.assertIn('ORDER BY "v" IS NULL NULLS LAST,', actual)
        self.assertNotIn('IGNORE NULLS', actual)
        self.assertNotIn('"v" IGNORE NULLS)', actual)
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE events(k INT, t INT, v INT)')
            c.execute('INSERT INTO events VALUES (1, 1, NULL), (1, 2, 7), (1, 3, 9)')
            duck_sql = module.sqlglot.parse_one(actual).sql(dialect='duckdb')
            self.assertCountEqual(c.execute(duck_sql).fetchall(), c.execute(query).fetchall())

    def test_feldera_null_skipping_matches_all_three_workload_windows_after_mutations(self):
        from services.extended_tpcds import pipeline
        query = pipeline.render_model('int_channel_status_latest', {'report_date':'2002-12-31', 'history_months':12})
        rendered = module.render(query, 'feldera')
        self.assertNotIn('IGNORE NULLS', rendered)
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE stg_engagement_channel_history(assessment_key VARCHAR, ingestion_time TIMESTAMP, effective_start TIMESTAMP, store_eligibility BOOLEAN, catalog_eligibility BOOLEAN, web_eligibility BOOLEAN)')
            changes = [
                "INSERT INTO stg_engagement_channel_history VALUES ('a','2002-12-31','2002-12-31',NULL,NULL,NULL), ('a','2002-12-30','2002-12-30',FALSE,NULL,TRUE), ('a','2002-12-29','2002-12-29',TRUE,FALSE,NULL), ('b',NULL,NULL,NULL,NULL,NULL), ('b',NULL,NULL,NULL,NULL,NULL), (NULL,NULL,NULL,TRUE,NULL,FALSE)",
                "INSERT INTO stg_engagement_channel_history VALUES ('a','2003-01-01','2003-01-01',NULL,TRUE,NULL), ('b',NULL,NULL,FALSE,FALSE,FALSE)",
                "UPDATE stg_engagement_channel_history SET web_eligibility=NULL WHERE assessment_key='a'",
                "DELETE FROM stg_engagement_channel_history WHERE ingestion_time='2003-01-01'",
            ]
            for change in changes:
                c.execute(change)
                self.assertCountEqual(c.execute(rendered).fetchall(), c.execute(query).fetchall())

    def test_feldera_rejects_prefix_and_excluded_peer_frames_for_null_skipping(self):
        frames = ('ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW',
                  'ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE TIES')
        for frame in frames:
            with self.subTest(frame=frame), self.assertRaisesRegex(ValueError, 'whole-partition'):
                module.render('SELECT FIRST_VALUE(v IGNORE NULLS) OVER (PARTITION BY k ORDER BY t '+frame+') FROM events', 'feldera')

    def test_dbsp_sql_uses_supported_json_function_and_quoted_reserved_columns(self):
        actual = module.render("SELECT date, json_extract_string(j, '$.CustomerPath'), x + INTERVAL (-n) DAY FROM events", 'dbspnet')
        self.assertIn('"date"', actual)
        self.assertIn('JSON_EXTRACT_STRING("j",', actual)
        self.assertNotIn('->>', actual)
        self.assertIn("INTERVAL '1' DAY", actual)
        self.assertNotIn('INTERVAL (', actual)

    def test_dbsp_constant_lowering_preserves_rows_with_empty_or_nonempty_sources(self):
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE __xt_unit(one INTEGER); INSERT INTO __xt_unit VALUES (1)')
            c.execute('CREATE TABLE events(value INTEGER)')
            query = "SELECT 'default' AS label UNION ALL SELECT CAST(value AS VARCHAR) FROM events"
            for insert in (False, True):
                if insert:
                    c.execute('INSERT INTO events VALUES (2), (NULL), (2)')
                expected = c.execute(query).fetchall()
                actual = c.execute(module.render(query, 'dbspnet')).fetchall()
                self.assertCountEqual(actual, expected)

    def test_dbsp_values_and_constant_cohorts_preserve_bag_semantics(self):
        with module.duckdb.connect() as c:
            c.execute('CREATE TABLE __xt_unit(one INTEGER); INSERT INTO __xt_unit VALUES (1)')
            c.execute('CREATE TABLE events(value INTEGER); INSERT INTO events VALUES (1), (2), (2), (NULL)')
            queries = ["SELECT * FROM (VALUES (1, 'a'), (1, 'a'), (NULL, 'b')) AS v(n, label)"]
            for cohort in ('all', 'hot', 'cold'):
                queries.append(f"SELECT value FROM events WHERE ('{cohort}' = 'all' OR value IN (SELECT value FROM events WHERE value = 2))")
            for query in queries:
                self.assertCountEqual(c.execute(module.render(query, 'dbspnet')).fetchall(),
                                      c.execute(query).fetchall())

    def test_all_models_are_outputs_and_compile_failure_cannot_drop_models(self):
        manifest={'schemas':{'events':{'_source_row_id':'VARCHAR'}},'source_configuration':{'report_date':'2002-12-31','history_months':12},'_query_cohort':'all'}
        spec=module.program(manifest,Path('/data/processed/dbspnet/extended-tpcds'),'dbspnet')
        self.assertEqual(len(spec['outputs']),96)
        self.assertEqual(len(spec['program']),98)
        self.assertTrue(all(o['mode']=='truncate' for o in spec['outputs']))
        # TRY_CAST remains explicit: unsupported syntax must fail at compile,
        # not acquire throwing-cast semantics from a dialect conversion.
        self.assertIn('TRY_CAST','\n'.join(spec['program']))


if __name__=='__main__':unittest.main()
