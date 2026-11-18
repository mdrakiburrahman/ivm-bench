"""Profiling uses live workload sessions and exports only the current batch."""
import csv
import io
import os
import sys
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0,str(Path(__file__).parents[1]))
from services.extended_tpcds_profile import BatchProfile, STEP_ENGINES
from services.spark_openivm_profile import _PROFILE_COLS


def step(refresh_id, view='m', duration=12):
    return dict(zip(_PROFILE_COLS,[refresh_id,view,'2026-11-18',0,'command_span',duration,'detail']))


class BatchProfileTest(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch.dict(os.environ,{'OPENIVM_PROFILE_REFRESH':'1'}))

    def test_native_and_livy_exports_exclude_previous_refreshes_and_other_namespaces(self):
        for engine in STEP_ENGINES:
            with self.subTest(engine=engine):
                c=Mock(namespace='workload')
                rows=[step('old')]
                def execute(sql):
                    if sql.startswith('SET '):return []
                    return Mock(rows=rows) if engine=='duckdb-openivm' else [list(r.values()) for r in rows]
                c.execute.side_effect=execute
                profile=BatchProfile(engine,2,['m'],c)
                rows.extend([step('new'),step('foreign','other.reference' if engine=='duckdb-openivm' else 'other.m')])
                data=profile.finish({'m':0.2})
                self.assertEqual(data['status'],'ok')
                self.assertEqual(data['scope'],'batch')
                exported=list(csv.DictReader(io.StringIO(data['csv']['profile'])))
                self.assertEqual([r['refresh_id'] for r in exported],['new'])
                self.assertEqual(exported[0]['exported_after_batch'],'2')
                self.assertEqual(c.execute.call_args_list[0].args[0],
                                 'SET '+('openivm_profile_refresh=true' if engine=='duckdb-openivm'
                                         else 'spark.openivm.profile.refresh=true'))

    def test_first_batch_needs_no_existing_profile_table_and_reports_missing_models(self):
        c=Mock()
        c.execute.return_value.rows=[step('create')]
        profile=BatchProfile('duckdb-openivm',1,['m','unchanged'],c)
        self.assertEqual(c.execute.call_count,1)
        result=profile.finish({'m':0.1,'unchanged':0.1})
        self.assertEqual(result['status'],'partial')
        self.assertEqual(result['models_without_steps'],['unchanged'])

    def test_enabled_profile_cannot_silently_export_empty_or_truncated_results(self):
        for rows in ([],[[None]*7]*1000):
            with self.subTest(rows=len(rows)):
                c=Mock();c.execute.return_value=rows
                profile=BatchProfile('spark-openivm',1,['m'],c)
                with self.assertLogs(level='ERROR'):
                    result=profile.finish({'m':0.1})
                self.assertEqual(result['status'],'failed')
                self.assertIn('error',result)

    def test_disabled_profiling_does_not_submit_observer_queries(self):
        engines=(*STEP_ENGINES,'duckdb','spark','fabric-jvm-35','databricks-openivm','databricks-enzyme','feldera','dbspnet')
        with patch.dict(os.environ,{'OPENIVM_PROFILE_REFRESH':'0'}):
            for engine in engines:
                c=Mock()
                profile=BatchProfile(engine,2,['m'],c)
                self.assertEqual(profile.finish({'m':None})['status'],'disabled')
                c.execute.assert_not_called()

    def test_plain_baselines_explicitly_retain_command_times_without_claiming_step_profiles(self):
        for engine in ('duckdb','spark','fabric-jvm-35'):
            result=BatchProfile(engine,1,['m'],Mock()).finish({'m':1.25})
            self.assertEqual(result['status'],'not_supported')
            self.assertEqual(result['model_command_s'],{'m':1.25})

    def test_databricks_collects_exact_query_ids_and_scopes_enzyme_to_workload_schema(self):
        for engine in ('databricks-openivm','databricks-enzyme'):
            c=Mock(schema='isolated')
            profile=BatchProfile(engine,2,['m'],c)
            statements=[{'query_id':'current','sql':'REFRESH MATERIALIZED VIEW m'}]
            updates=[{'creation_time':profile.started_at_ms-1,'event_count':1,'update_id':'old'},
                     {'creation_time':profile.started_at_ms+1,'event_count':2,'update_id':'current'}]
            events={'pipeline_count':1,'pipelines':[{'updates':updates,'pipeline_level_events':['old']}],
                    'event_count':3,'update_count':2}
            with patch('services.databricks_openivm.statement_metrics',return_value={'status':'ok'}) as metrics, \
                 patch('services.databricks_enzyme_pipeline_events.collect_pipeline_events',return_value=events) as collect:
                result=profile.finish({'m':1.0},statements=statements)
            metrics.assert_called_once_with(statements,2)
            self.assertEqual(result['kind'],'query_history')
            if engine=='databricks-enzyme':
                collect.assert_called_once_with(2,schemas=['isolated'],connection=c.handle)
                self.assertEqual(result['pipeline_events']['update_count'],1)
                self.assertEqual(result['pipeline_events']['event_count'],2)
            else:collect.assert_not_called()

    def test_pipeline_stats_are_retained_as_cumulative_metrics_not_sql_step_times(self):
        for engine in ('feldera','dbspnet'):
            with patch('services.'+('feldera_client' if engine=='feldera' else 'dbspnet_client')+'.get_stats',
                       return_value={'ticks':42}):
                result=BatchProfile(engine,3,['m']).finish({'m':None})
            self.assertEqual(result['status'],'ok')
            self.assertEqual(result['metrics'],{'ticks':42})
            self.assertIn('cumulative',result['metrics_scope'])
            self.assertIsNone(result['model_command_s']['m'])
