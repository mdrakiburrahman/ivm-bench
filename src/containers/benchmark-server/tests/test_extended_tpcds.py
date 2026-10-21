import csv
import io
import json
import os
import sys
import struct
import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from models.config import BenchmarkConfig, ExtendedTPCDSOptions, EngineConfig, ResourceAllocation, workload_cache_key
from models.experiments import parse_experiments_json
from services.engine_runner import EngineRunner, OpenIvmValidationError
from services.orchestrator import Orchestrator
from services.oat_runner import generate_results_csv, disk_cleanup_after_experiment
from handlers import oat_chart


class ExtendedTPCDSConfigTests(unittest.TestCase):
    def experiment(self, **overrides):
        return parse_experiments_json(json.dumps({'experiments':[{'workload':'extended-tpcds','scale_factor':.01,**overrides}]}))[0]

    def test_fractional_scale_and_default_engines(self):
        e = self.experiment()
        self.assertEqual(e.scale_factor,.01)
        self.assertEqual(e.engines,['duckdb','duckdb-openivm'])
        self.assertEqual(e.to_dict()['extended_tpcds']['profile'],'native')
        self.assertEqual(json.loads(e.to_compose_env()['EXTENDED_TPCDS_OPTIONS'])['mode'],'mixed')
        original = parse_experiments_json('{"experiments":[{"scale_factor":3}]}')[0]
        self.assertEqual(original.workload,'tpc-di')
        self.assertIs(type(original.scale_factor),int)

    def test_days_batch_count_and_zero_insert_percentage(self):
        e = self.experiment(extended_tpcds={'mode':'days','days':6})
        self.assertEqual(e.batch_count,7)
        self.assertEqual(e.batch_2_pct,'0')
        self.assertEqual(e.batch_3_pct,'0')

    def test_pure_update_defaults_and_invalid_mixed_percentages(self):
        e=self.experiment(extended_tpcds={'mode':'updates'})
        self.assertEqual(e.batch_2_pct,'0')
        self.assertEqual(e.batch_2_update_pct,'1')
        with self.assertRaisesRegex(ValueError,'percentages'):
            self.experiment(batch_2_update_pct='60',batch_2_delete_pct='60')

    def test_repeated_batches_reuse_batch_three_settings(self):
        c = BenchmarkConfig(workload='extended-tpcds',scale_factor=.01,engines=['duckdb'],
                            extended_tpcds=ExtendedTPCDSOptions(batches=5),batch_3_pct='7',batch_3_update_pct='3')
        self.assertEqual(c.extended_tpcds.steps(c)[-1],{'insert_pct':'7','update_pct':'3','delete_pct':'0'})
        self.assertEqual(len(c.extended_tpcds.steps(c)),4)

    def test_cache_identity_changes_for_every_workload_binding(self):
        keys=[]
        for changes in [{},{'profile':'calibrated'},{'mode':'days'},{'seed':31},{'query_cohort':'hot'}, {'mutation_cohort':'cold'}]:
            c=BenchmarkConfig(workload='extended-tpcds',scale_factor=.01,engines=['duckdb'],extended_tpcds=ExtendedTPCDSOptions(**changes),batch_2_pct='0',batch_3_pct='0')
            keys.append(workload_cache_key(c))
        self.assertEqual(len(set(keys)),len(keys))

    def test_rejects_unsupported_engine_and_invalid_scale(self):
        with self.assertRaisesRegex(ValueError,'supports'):
            self.experiment(engines=['spark'])
        for sf in [0,-1,float('nan'),float('inf')]:
            with self.assertRaises(ValueError):self.experiment(scale_factor=sf)
        with self.assertRaisesRegex(ValueError,'integer'):
            BenchmarkConfig(scale_factor=.01)

    def test_csv_includes_all_daily_batches_and_classification(self):
        e=self.experiment(extended_tpcds={'mode':'days','days':4},engines=['duckdb-openivm'])
        state={'experiments':[{'inputs':e.to_dict(),'engines':{'duckdb-openivm':{'batches':[{'batch_num':5,'status':'completed','extra':{'plan_id':'plan','classification_counts':{'incremental':18,'full':78}}}]}}}]}
        rows=list(csv.DictReader(io.StringIO(generate_results_csv(state))))
        self.assertEqual(len(rows),5)
        self.assertEqual(rows[-1]['batch_num'],'5')
        self.assertEqual(rows[-1]['workload'],'extended-tpcds')
        self.assertEqual(rows[-1]['plan_id'],'plan')
        self.assertEqual(rows[-1]['incremental_models'],'18')
        self.assertEqual(rows[-1]['full_refresh_models'],'78')

    def test_runner_sizes_batches_from_workload(self):
        c=BenchmarkConfig(workload='extended-tpcds',scale_factor=.01,engines=['duckdb'],extended_tpcds=ExtendedTPCDSOptions(mode='days',days=4),batch_2_pct='0',batch_3_pct='0')
        resources=ResourceAllocation(2,4)
        e=EngineConfig('duckdb','docker/docker-compose.benchmark.duckdb.yml',5002,None,resources,resources)
        with patch('services.engine_runner.DockerManager'):
            runner=EngineRunner(c,e,lambda _:None)
        self.assertEqual(len(runner.result.batches),5)

    def test_daily_reports_include_late_batches_and_duckdb_comparison(self):
        inputs = self.experiment(extended_tpcds={'mode':'days','days':4})
        exp = {'status':'completed', 'inputs':inputs.to_dict(), 'engines':{
            engine:{'batches':[{'batch_num':i,'duration_s':duration} for i in range(1,6)]}
            for engine,duration in [('duckdb',10),('duckdb-openivm',5)]}}
        columns = oat_chart._aggregate_columns([exp],inputs.engines)
        self.assertIn('duckdb-openivm.b5',[column.key for column in columns])
        with patch.object(oat_chart,'_load_oat_state',return_value={'experiments':[exp]}):
            report = oat_chart.generate_oat_results_md('test')
        self.assertIn('duckdb-openivm b5',report)
        self.assertIn('b5 ratio',report)
        self.assertIn('duckdb-openivm/duckdb',report)
        self.assertEqual(oat_chart._batch_ratio(exp,5),.5)

    def test_per_model_comparison_reads_both_engine_pairs(self):
        for workload,engines,sf in [('extended-tpcds',('duckdb','duckdb-openivm'),.01),
                                    ('tpc-di',('spark','spark-openivm'),1)]:
            with self.subTest(workload=workload), tempfile.TemporaryDirectory() as folder:
                inputs = self.experiment(workload=workload,engines=list(engines),scale_factor=sf)
                exp = {'status':'completed','inputs':inputs.to_dict(),'dbt_run_files':{}}
                for engine,duration in zip(engines,(4,2)):
                    path = Path(folder)/f'run-{engine}-batch2.json'
                    path.write_text(json.dumps({'nodes':[{'unique_id':'model.test.m','name':'m',
                        'resource_type':'model','execution_time_s':duration}]}))
                    exp['dbt_run_files'][engine]=[str(path)]
                values,names = oat_chart._ratio_value(exp,2,folder)
                self.assertEqual(values,{'model.test.m':-1.0})
                self.assertEqual(names,{'model.test.m':'m'})
                self.assertEqual(oat_chart._break_even_rows([exp],folder),[['m',sf,'0.50']])

    def test_per_model_plot_exports_one_and_five_batches(self):
        heights = []
        for count in (1,5):
            with self.subTest(batches=count), tempfile.TemporaryDirectory() as folder:
                options = {'batches':1} if count==1 else {'mode':'days','days':4}
                inputs = self.experiment(extended_tpcds=options)
                exp = {'status':'completed','inputs':inputs.to_dict(),'dbt_run_files':{}}
                for engine,duration in [('duckdb',4),('duckdb-openivm',2)]:
                    paths = []
                    for batch in range(1,count+1):
                        path = Path(folder)/f'run-{engine}-batch{batch}.json'
                        path.write_text(json.dumps({'nodes':[{'unique_id':'model.test.m','name':'m',
                            'resource_type':'model','execution_time_s':duration}]}))
                        paths.append(str(path))
                    exp['dbt_run_files'][engine] = paths
                with patch.object(oat_chart,'_load_oat_state',return_value={'experiments':[exp]}):
                    png = oat_chart.generate_oat_per_model_png('test',folder)
                self.assertEqual(png[:8],b'\x89PNG\r\n\x1a\n')
                heights.append(struct.unpack('>II',png[16:24])[1])
        self.assertGreater(heights[1],heights[0])

    def test_unrelated_benchmark_modes_are_rejected(self):
        for flag in ('compiler_bench','cost_model_bench'):
            with self.subTest(flag=flag), self.assertRaisesRegex(ValueError,'cannot be combined'):
                self.experiment(feature_flags={flag:True})

    def test_preparation_restores_workload_after_build_failure(self):
        orchestrator = object.__new__(Orchestrator)
        orchestrator._config = BenchmarkConfig()
        original_engines = list(orchestrator._config.engines)
        for name in ('_teardown_existing','_clean_mount','_pre_create_dirs'):
            setattr(orchestrator,name,Mock())
        orchestrator._phase1_prep = Mock(side_effect=RuntimeError('build failed'))
        with self.assertRaisesRegex(RuntimeError,'build failed'):
            orchestrator._phase0_one_time_prep([self.experiment()])
        self.assertEqual(orchestrator._config.workload,'tpc-di')
        self.assertEqual(orchestrator._config.engines,original_engines)
        self.assertTrue(orchestrator._phase1_prep.call_args.kwargs['extended_tpcds'])

    def test_preparation_container_is_stopped_after_failure(self):
        orchestrator = object.__new__(Orchestrator)
        orchestrator._config = BenchmarkConfig(workload='extended-tpcds',engines=['duckdb'])
        orchestrator._heartbeat = Mock(return_value=nullcontext())
        with patch('services.orchestrator.DockerManager') as manager:
            manager.return_value.up.side_effect = RuntimeError('preparation timed out')
            with self.assertRaisesRegex(RuntimeError,'preparation timed out'):
                orchestrator._run_datagen()
            manager.return_value.down.assert_called_once_with()

    def test_failure_metadata_survives_database_cleanup(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder)/'mount/results/0.01/duckdb-openivm/extended-tpcds'
            source.mkdir(parents=True)
            (source/'batch2.json').write_text('{"status":"failed","error":"refresh failed"}')
            (source/'materializations.json').write_text('{"m":{"classification":"incremental"}}')
            (source/'state.duckdb').write_bytes(b'database')
            archive = Path(folder)/'mount/oat-state/run/forensics'
            disk_cleanup_after_experiment(folder,.01,['duckdb-openivm'],emit=lambda _:None,
                                          failure_archive_dir=str(archive))
            saved = archive/'duckdb-openivm-extended-tpcds'
            self.assertEqual(json.loads((saved/'batch2.json').read_text())['status'],'failed')
            self.assertTrue((saved/'materializations.json').is_file())
            self.assertFalse((saved/'state.duckdb').exists())
            self.assertFalse(source.exists())

    def test_engine_failure_response_retains_its_reason(self):
        runner=object.__new__(EngineRunner)
        runner._engine=Mock(name='engine');runner._engine.name='duckdb-openivm'
        from models.result import EngineResult
        runner._result=EngineResult('duckdb-openivm')
        runner._dbt_url='http://local'
        for name in ['_persist_batch_result','_start_cpu_measurement','_finish_cpu_measurement','_ensure_cpu_measurement_status']:
            setattr(runner,name,Mock())
        response=Mock(status_code=500);response.json.return_value={'status':'failed','error':'Binder Error: wrong target column'}
        with patch('services.engine_runner.requests.post',return_value=response):
            with self.assertRaisesRegex(RuntimeError,'wrong target column'):
                runner._run_extended_tpcds_batch(1)
        self.assertEqual(runner._result.batches[0].status,'failed')
        self.assertIn('wrong target column',runner._result.batches[0].error)

    def test_correctness_failure_is_fatal_and_keeps_classification(self):
        runner=object.__new__(EngineRunner)
        runner._engine=Mock();runner._engine.name='duckdb-openivm'
        from models.result import EngineResult
        runner._result=EngineResult('duckdb-openivm')
        runner._dbt_url='http://local'
        for name in ['_persist_batch_result','_start_cpu_measurement','_finish_cpu_measurement','_ensure_cpu_measurement_status']:
            setattr(runner,name,Mock())
        execute=Mock(status_code=200)
        execute.json.return_value={'duration_s':1.0,'plan_id':'p','classification_counts':{'incremental':96}}
        validate=Mock(status_code=422)
        validate.json.return_value={'status':'failed','models_checked':96,'differences':{'m':3}}
        with patch.dict(os.environ,{'OPENIVM_VALIDATE':'1'}),patch('services.engine_runner.requests.post',side_effect=[execute,validate]):
            with self.assertRaises(OpenIvmValidationError):runner._run_extended_tpcds_batch(1)
        batch=runner._result.batches[0]
        self.assertEqual(batch.status,'failed')
        self.assertEqual(batch.extra['classification_counts'],{'incremental':96})
        self.assertEqual(batch.extra['engine_result']['validation']['differences'],{'m':3})


if __name__=='__main__':unittest.main()
