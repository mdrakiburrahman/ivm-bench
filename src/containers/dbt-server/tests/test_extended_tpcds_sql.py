import importlib.util
import json
import os
from pathlib import Path
import tempfile
import sys
sys.path.insert(0, str(Path(__file__).parents[1]))
os.environ['EXTENDED_TPCDS_WORKLOAD_DIR'] = str(Path(__file__).parents[3]/'workloads/extended-tpcds')
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('extended_tpcds_sql_tested', Path(__file__).parents[1]/'services/extended_tpcds_sql.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ExtendedTPCDSSqlTest(unittest.TestCase):
    def test_relation_rewrite_preserves_ctes_literals_and_aliases(self):
        sql = "WITH s AS (SELECT a FROM source) SELECT s.a, 'source' AS label FROM s JOIN model m ON s.a=m.a"
        actual = module.translate(sql, {'source':'cat.xt.source', 'model':'cat.xt.model', 's':'cat.xt.wrong'})
        import sqlglot
        from sqlglot import exp
        tables = [t.sql(dialect='spark') for t in sqlglot.parse_one(actual,read='spark').find_all(exp.Table)]
        self.assertIn('`cat`.`xt`.`source`',tables)
        self.assertIn('`cat`.`xt`.`model` AS m',tables)
        self.assertIn('s',tables)
        self.assertNotIn('wrong',actual)
        self.assertIn("'source' AS label",actual)

    def test_local_spark_connection_uses_existing_adapter_profile(self):
        from services import dbt_runner
        project_root = Path(__file__).parents[1]/'dbt-projects'
        with patch.object(dbt_runner,'PROJECTS_DIR',str(project_root)), patch('dbt.adapters.fabricspark.connections.FabricSparkConnectionManager.open') as open_connection:
            handle=open_connection.return_value.handle
            handle.cursor.return_value.description = None
            connection = module.Connection('spark',Path('/plan'),{'plan_id':'p'},{'statement_timeout_s':123})
            credentials=open_connection.call_args.args[0].credentials
            self.assertEqual(credentials.livy_url,'http://spark:8998')
            self.assertEqual(credentials.statement_timeout,123)
            handle.cursor.return_value.execute.assert_any_call("SET TIME ZONE 'UTC'")
            self.assertEqual(connection.execute('SELECT 1'),[])
            connection.close()
            handle.close.assert_called_once()

    def test_fabric_profile_is_parsed_before_rendering_quoted_environment_json(self):
        from services import dbt_runner
        root = Path(__file__).parents[1]/'dbt-projects'
        with patch.object(dbt_runner, 'PROJECTS_DIR', str(root)):
            for engine in ('fabric-jvm-35', 'fabric-openivm-jvm-35'):
                with self.subTest(engine=engine):
                    values = dbt_runner.load_profile(engine, {'FABRIC_ENVIRONMENT_ID': 'env-id',
                                                              'FABRIC_LAKEHOUSE_ID': 'lake-id'})
                    self.assertEqual(values['spark_config']['conf']['spark.fabric.environmentDetails'],
                                     '{"id": "env-id"}')
                    self.assertEqual(values['lakehouseid'], 'lake-id')
                    self.assertIsInstance(values['threads'], int)

    def test_feldera_profile_retains_native_resource_types(self):
        from services import dbt_runner
        root = Path(__file__).parents[1]/'dbt-projects'
        with patch.object(dbt_runner, 'PROJECTS_DIR', str(root)):
            for limit in ('0', '8192'):
                values = dbt_runner.load_profile('feldera', {'FELDERA_MAX_RSS_MB': limit})
                self.assertEqual(values['max_rss_mb'], int(limit))
                self.assertIsInstance(values['workers'], int)
                self.assertIsInstance(values['timeout'], int)
                self.assertIs(values['dev_tweaks']['adaptive_joins'], True)

    def test_parquet_timestamps_use_utc_ltz_without_changing_non_timestamp_fields(self):
        connection = module.Connection.__new__(module.Connection)
        connection.remote_root = '/plan'
        connection.schemas = {'events': {'_source_row_id': 'VARCHAR', 'ts': 'TIMESTAMP', 'day': 'DATE'}}
        actual = connection.parquet({'path': 'initial/events.parquet'}, 'events')
        self.assertIn('CAST(`ts` AS TIMESTAMP_LTZ) AS `ts`', actual)
        self.assertIn('`_source_row_id`', actual)
        self.assertIn('`day`', actual)
        self.assertNotIn('CAST(`day`', actual)
        self.assertIn('parquet.`/plan/initial/events.parquet`', actual)

    def test_timestamp_and_dynamic_interval_translation(self):
        sql = "SELECT TRY_STRPTIME(d,'%Y%m%d'), DATE '2002-12-31' + INTERVAL (-(365)) DAY, d + INTERVAL (-lag.n) DAY"
        actual = module.translate(sql)
        self.assertIn("TRY_TO_TIMESTAMP(d, 'yyyyMMdd')",actual)
        self.assertIn("INTERVAL '-365' DAY",actual)
        self.assertIn("(-lag.n) * INTERVAL '1' DAY",actual)
        self.assertNotIn('TRY_STRPTIME',actual)

    def test_timestamp_casts_match_utc_sources_and_survive_compiler_remapping(self):
        from services.databricks_openivm import remap_sql
        sql = "SELECT CAST(TRY_STRPTIME(d, '%Y%m%d') AS TIMESTAMP) AS ts FROM events"
        actual = module.translate(sql)
        self.assertIn('AS TIMESTAMP)', actual)
        compiled = remap_sql(actual, {}, for_compiler=True)
        self.assertNotIn('TIMESTAMP_NTZ', compiled)

    def test_fabric_uses_schema_enabled_lakehouse_and_qualified_namespace(self):
        from services import dbt_runner, fabric
        project_root = Path(__file__).parents[1]/'dbt-projects'
        def resolved(env):
            env.update(FABRIC_LAKEHOUSE_NAME='compute_lake', FABRIC_LAKEHOUSE_ID='11111111-1111-1111-1111-111111111111',
                       FABRIC_ENVIRONMENT_ID='22222222-2222-2222-2222-222222222222')
        for engine in ('fabric-jvm-35', 'fabric-openivm-jvm-35'):
            with self.subTest(engine=engine), \
                 patch.object(dbt_runner, 'PROJECTS_DIR', str(project_root)), \
                 patch.object(dbt_runner, '_inject_fabric_resolved', side_effect=resolved), \
                 patch.object(fabric, 'provision_run') as provision, \
                 patch.object(fabric, 'resolve_cache_lakehouse', return_value='cache'), \
                 patch.object(fabric, '_azcopy'), \
                 patch.object(fabric, 'onelake_abfss', side_effect=lambda path, lakehouse_id=None: 'abfss://'+(lakehouse_id or 'compute')+'/'+path), \
                 patch('dbt.adapters.fabricspark.connections.FabricSparkConnectionManager.open') as open_connection:
                open_connection.return_value.handle.cursor.return_value.description = None
                c = module.Connection(engine, Path('/plan'), {'plan_id':'p'},
                                      {'statement_timeout_s':123}, initialize=True)
                provision.assert_called_once_with(openivm='openivm' in engine, enable_schemas=True)
                self.assertEqual(c.catalog, 'compute_lake')
                self.assertEqual(open_connection.call_args.args[0].credentials.schema, 'dbo')
                self.assertTrue(c.namespace.startswith('compute_lake.xt_p_'))
                calls = c.handle.cursor.return_value.execute
                calls.assert_any_call('CREATE SCHEMA '+module.identifier(c.namespace))
                if 'openivm' in engine:
                    calls.assert_any_call('SET spark.openivm.managedTablesRoot=abfss://compute/Tables/'+c.schema)
                    calls.assert_any_call('SET spark.openivm.catalogPreservesColumnCase=true')
                else:
                    self.assertFalse(any('managedTablesRoot' in c.args[0] for c in calls.call_args_list))

    def test_statement_closes_owned_cursor_on_success_and_error_but_keeps_livy_session(self):
        for borrowed, failure in ((False, False), (False, True), (True, False)):
            with self.subTest(borrowed=borrowed, failure=failure):
                c = module.Connection.__new__(module.Connection)
                c.engine = 'databricks-enzyme'
                c.statements = []
                c.handle = Mock()
                cursor = c.handle if borrowed else Mock()
                c.handle.cursor.return_value = cursor
                cursor.description = [('n',)]
                cursor.fetchall.return_value = [[1]]
                if failure:
                    cursor.execute.side_effect = ValueError('SQL failed')
                    with self.assertLogs(level='ERROR'), self.assertRaisesRegex(ValueError, 'SQL failed'):
                        c.execute('SELECT 1')
                else:
                    self.assertEqual(c.execute('SELECT 1'), [[1]])
                if borrowed:
                    c.handle.close.assert_not_called()
                else:
                    cursor.close.assert_called_once()
                    c.handle.close.assert_not_called()

    def test_replay_normalizes_guard_counts_and_still_rejects_mismatches(self):
        from services.extended_tpcds import mutations
        for before_count, duplicate_count in (('0','0'), ('1','0'), ('0','1')):
            with self.subTest(before=before_count, duplicates=duplicate_count), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                images = []
                for name in ('before', 'after'):
                    path = root/(name+'.parquet')
                    path.write_bytes(name.encode())
                    images.append({'path':path.name, 'sha256':mutations.file_checksum(path)})
                manifest = {'schemas':{'events':{}}, 'batches':[{'operations':[{
                    'kind':'update', 'table':'events', 'before':images[0], 'after':images[1]}]}]}
                c = Mock()
                c.parquet.return_value = '(SELECT 1 AS _source_row_id)'
                c.execute.side_effect = [[[before_count]], [], [[duplicate_count]]]
                if before_count != '0':
                    with self.assertRaisesRegex(ValueError, 'before-image mismatch'):
                        module.replay(c, manifest, root, 2)
                    self.assertEqual(c.execute.call_count, 1)
                elif duplicate_count != '0':
                    with self.assertRaisesRegex(ValueError, 'duplicate source IDs'):
                        module.replay(c, manifest, root, 2)
                else:
                    module.replay(c, manifest, root, 2)
                    self.assertTrue(c.execute.call_args_list[1].args[0].startswith('MERGE INTO'))

    def test_validation_normalizes_numeric_strings_without_hiding_nonzero_counts(self):
        for difference in ('0', '2', 0):
            with self.subTest(difference=difference), tempfile.TemporaryDirectory() as folder:
                root = Path(folder)
                manifest = root/'manifest.json'
                manifest.write_text(json.dumps({'plan_id':'p'}))
                (root/'sql-checkpoint.json').write_text(json.dumps({
                    'dirty':False, 'batch_num':1, 'plan_id':'p', 'namespace':'lake.xt'}))
                c = Mock(namespace='lake.xt')
                c.execute.side_effect = lambda sql: [['v','int']] if sql.startswith('DESCRIBE ') else (
                    [[difference]] if sql.startswith('SELECT ') else [])
                with patch.object(module, 'Connection', return_value=c), patch.object(module, 'replay'), \
                     patch.object(module, 'queries', return_value={'m':'SELECT 1 AS v'}), patch.object(module, 'build'):
                    result = module.validate_batch('fabric-jvm-35', 1, manifest, root, {})
                self.assertEqual(result['differences'], {'m':int(difference)})
                self.assertEqual(result['status'], 'passed' if int(difference)==0 else 'failed')

    def test_baseline_refresh_preserves_table_identity_and_enables_cdf(self):
        c = Mock(engine='spark')
        state = {}
        module.build(c, {'model':'SELECT 1 AS a'},state, initial=True)
        self.assertIn('delta.enableChangeDataFeed',c.execute.call_args.args[0])
        module.build(c, {'model':'SELECT 2 AS a'},state)
        self.assertEqual(c.execute.call_args.args[0],'INSERT OVERWRITE `model` SELECT 2 AS a')
        self.assertEqual(state['materializations']['model']['classification'],'full')

    def test_enzyme_and_spark_openivm_use_their_own_materialization_commands(self):
        for engine in ('spark-openivm','fabric-openivm-jvm-35','databricks-enzyme'):
            c=Mock(engine=engine, namespace='cat.xt')
            module.build(c,{'m':'SELECT 1'},{},initial=True)
            create=c.execute.call_args.args[0]
            self.assertIn('CREATE MATERIALIZED VIEW',create)
            self.assertEqual('REFRESH POLICY AUTO' in create,engine=='databricks-enzyme')
            if engine=='databricks-enzyme':
                self.assertIn('CREATE MATERIALIZED VIEW `cat`.`xt`.`m`', create)
            module.build(c,{'m':'SELECT 1'},{})
            self.assertEqual(c.execute.call_args.args[0], 'REFRESH MATERIALIZED VIEW '+('`cat`.`xt`.`m`' if engine=='databricks-enzyme' else '`m`'))

    def test_databricks_openivm_cannot_use_enzyme_materialization(self):
        with self.assertRaisesRegex(ValueError,'compiled SQL'):
            module.build(Mock(engine='databricks-openivm', namespace='cat.xt'),{'m':'SELECT 1'},{},initial=True)

    def test_failed_build_keeps_original_error_and_catalog_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            manifest = root/'manifest.json'
            manifest.write_text(json.dumps({'plan_id':'p'}))
            connection = Mock(engine='databricks-enzyme', namespace='cat.xt', statements=[])
            diagnose = module.Connection.diagnose_failure
            connection.diagnose_failure.side_effect = lambda: diagnose(connection)
            connection.execute.return_value = [['xt', 'stg_customer_policy_history', False]]
            failure = RuntimeError('missing staging relation')
            with patch.object(module, 'Connection', return_value=connection), \
                 patch.object(module, 'queries', return_value={'m':'SELECT 1'}), \
                 patch.object(module, 'replay'), patch.object(module, 'build', side_effect=failure), \
                 self.assertLogs(level='ERROR') as logs:
                with self.assertRaises(RuntimeError) as caught:
                    module.run_batch('databricks-enzyme', 1, manifest, root, {})
            self.assertIs(caught.exception, failure)
            connection.execute.assert_any_call('SHOW TABLES IN `cat`.`xt`')
            self.assertIn('system.access.audit', connection.execute.call_args.args[0])
            self.assertIn('stg_customer_policy_history', '\n'.join(logs.output))
            self.assertTrue(json.loads((root/'sql-checkpoint.json').read_text())['dirty'])
            connection.close.assert_called_once()

    def test_denied_audit_access_does_not_replace_failure_diagnostics(self):
        c = module.Connection.__new__(module.Connection)
        c.engine, c.catalog, c.schema = 'databricks-enzyme', 'cat', 'xt'
        c.execute = Mock(side_effect=[[['xt','m',False]], PermissionError('audit denied')])
        with self.assertLogs(level='ERROR') as logs:
            c.diagnose_failure()
        self.assertEqual(c.execute.call_count, 2)
        self.assertIn('audit denied', '\n'.join(logs.output))

    def test_dirty_or_replayed_batch_is_rejected_before_connecting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); manifest=root/'manifest.json';manifest.write_text(json.dumps({'plan_id':'p'}))
            checkpoint=root/'sql-checkpoint.json'
            for state in ({'batch_num':1,'dirty':False},{'batch_num':0,'dirty':True}):
                checkpoint.write_text(json.dumps({**state,'plan_id':'p','options':{}}))
                with patch.object(module,'Connection') as connection:
                    with self.assertRaisesRegex(ValueError,'checkpoint'):
                        module.run_batch('spark',1,manifest,root,{})
                    connection.assert_not_called()

    def test_all_model_queries_translate_to_spark(self):
        import sqlglot
        root=Path(__file__).parents[3]/'workloads/extended-tpcds'
        count=0
        for p in (root/'models').glob('*.sql'):
            sql=p.read_text().replace('__REPORT_DATE__','2002-12-31').replace('__history_months__','12').replace('__COHORT__','all')
            with self.subTest(model=p.stem):
                translated=module.translate(sql)
                sqlglot.parse_one(translated,read='spark')
            count+=1
        self.assertEqual(count,96)


if __name__=='__main__':unittest.main()
