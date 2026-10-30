import csv
import io
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from models.config import BenchmarkConfig, ExtendedTPCDSOptions, EngineConfig, ResourceAllocation, workload_cache_key
from models.experiments import parse_experiments_json
from services.engine_runner import EngineRunner, OpenIvmValidationError
from services.oat_runner import generate_results_csv


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
        self.assertEqual(original.workload,'standard')
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


    def test_serial_extended_workload_stops_on_first_engine_failure(self):
        from services.orchestrator import Orchestrator
        from models.result import BenchmarkResult, EngineResult
        from types import SimpleNamespace
        orchestrator = Orchestrator.__new__(Orchestrator)
        orchestrator._config = BenchmarkConfig(workload='extended-tpcds',scale_factor=.01,engines=['duckdb','duckdb-openivm'])
        orchestrator._result = BenchmarkResult()
        orchestrator._benchmark_id = None
        orchestrator.emit = Mock()
        failed = EngineResult(engine='duckdb',status='failed',error='source load failed')
        with patch('services.orchestrator.EngineRunner') as runner:
            runner.return_value.run.return_value = failed
            with self.assertRaisesRegex(RuntimeError,'remaining engines were not started'):
                orchestrator._run_engines_serial({'duckdb':Mock(),'duckdb-openivm':Mock()})
            self.assertEqual(runner.call_count,1)
        self.assertIs(orchestrator._result.engines['duckdb'],failed)


if __name__=='__main__':unittest.main()
