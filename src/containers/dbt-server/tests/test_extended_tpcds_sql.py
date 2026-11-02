import importlib.util
import json
from pathlib import Path
import tempfile
import sys
sys.path.insert(0, str(Path(__file__).parents[1]))
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
            c=Mock(engine=engine)
            module.build(c,{'m':'SELECT 1'},{},initial=True)
            create=c.execute.call_args.args[0]
            self.assertIn('CREATE MATERIALIZED VIEW',create)
            self.assertEqual('REFRESH POLICY AUTO' in create,engine=='databricks-enzyme')
            module.build(c,{'m':'SELECT 1'},{})
            self.assertEqual(c.execute.call_args.args[0],'REFRESH MATERIALIZED VIEW `m`')

    def test_databricks_openivm_cannot_use_enzyme_materialization(self):
        with self.assertRaisesRegex(ValueError,'compiled SQL'):
            module.build(Mock(engine='databricks-openivm'),{'m':'SELECT 1'},{},initial=True)

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
