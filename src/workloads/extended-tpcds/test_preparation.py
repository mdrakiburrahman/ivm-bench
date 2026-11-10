"""Preparation constrains every phase and validates complete source populations."""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import prepare_experiment


class PreparationTests(unittest.TestCase):
    def test_limits_and_validation_reach_each_preparation_route(self):
        limits = {'memory_limit':'2GB','threads':3,'max_temp_directory_size':'1GB'}
        for mode,initial in [('mixed','100'),('insertion','50'),('days','100')]:
            options = {'profile':'native','seed':17,'report_date':'2002-12-31',
                       'history_months':12,'mode':mode,'days':3,'mutation_cohort':'all'}
            with self.subTest(mode=mode,initial=initial), tempfile.TemporaryDirectory() as folder, \
                 patch.dict(os.environ, {'DUCKDB_MEM_LIMIT':'2GB','DUCKDB_THREADS':'3',
                                         'DUCKDB_MAX_TEMP_DIRECTORY_SIZE':'1GB'}), \
                 patch.object(prepare_experiment.duckdb,'connect') as connect, \
                 patch.object(prepare_experiment.generate,'prepare',return_value={}) as generate, \
                 patch.object(prepare_experiment.generate,'statistics',return_value={'extension_rows':{}}), \
                 patch.object(prepare_experiment,'source_storage',return_value={}), \
                 patch.object(prepare_experiment.mutations,'sources',return_value=[]), \
                 patch.object(prepare_experiment.mutations,'plan',return_value={'plan_id':'p','batches':[]}) as plan:
                prepare_experiment.prepare(folder,.01,options,initial,[])
                self.assertEqual(connect.call_count,3)
                for call in connect.call_args_list:
                    self.assertEqual(call.kwargs['config'],limits)
                    self.assertTrue(call.args, "preparation tables must be disk-backed to permit eviction")
                    self.assertNotEqual(call.args[0], ':memory:')
                deferred = mode!='days' and initial=='100'
                self.assertEqual(generate.call_args.kwargs['validate_source_ids'],not deferred)
                self.assertEqual(plan.call_args.kwargs['expected_initial_rows'],{} if deferred else None)


    def test_source_storage_measures_all_initial_tables_not_refreshes(self):
        with tempfile.TemporaryDirectory() as folder, prepare_experiment.duckdb.connect() as connection:
            root = Path(folder)
            image = prepare_experiment.mutations.export_rows(connection,
                "SELECT n, repeat('abcdefgh', 100) AS payload FROM range(1000) t(n)", root/'initial.parquet', root)
            (root/'refresh.parquet').write_bytes(b'not part of the initial snapshot')
            result = prepare_experiment.source_storage(connection, root, {'initial':{'example':image}})
            self.assertEqual(result['rows'],1000)
            self.assertEqual(result['parquet_bytes'],(root/'initial.parquet').stat().st_size)
            self.assertGreater(result['parquet_uncompressed_bytes'],result['parquet_bytes'])
            self.assertEqual(result['tables']['example']['rows'],1000)

    def test_source_size_regenerates_whole_dataset_then_reuses_only_matching_cache(self):
        options = {'mode':'insertion', 'source_size_gb':25}
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)/'output'
            def generate(directory, sf, *args):
                (directory/'plan').mkdir(parents=True)
                (directory/'plan'/'manifest.json').write_text(json.dumps({'base_sf':sf}))
                return {'configuration':{'base_scale_factor':sf}, 'source_storage':{
                    'parquet_uncompressed_bytes':int(100_000_000 + sf*12_000_000_000)}}
            with patch.object(prepare_experiment,'_prepare',side_effect=generate) as prepare:
                result = prepare_experiment.prepare(root,25,options,'100',[])
                self.assertGreaterEqual(prepare.call_count,2)
                self.assertEqual(prepare.call_args_list[1].args[1],2)
                measured = result['source_storage']['parquet_uncompressed_bytes']
                self.assertLessEqual(abs(measured/25_000_000_000-1),0.10)
                actual_sf = json.loads((root/'plan'/'manifest.json').read_text())['base_sf']
                self.assertEqual(actual_sf,result['configuration']['base_scale_factor'])
                self.assertEqual(result['recipe']['scale_factor'],25)
                self.assertEqual(len(list(root.iterdir())),2)
                prepare.reset_mock()
                self.assertEqual(prepare_experiment.prepare(root,25,options,'100',[]),result)
                prepare.assert_not_called()
                with self.assertRaisesRegex(ValueError,'different recipe'):
                    prepare_experiment.prepare(root,25,{**options,'source_size_gb':20},'100',[])

    def test_unreachable_size_never_publishes_a_plan(self):
        def generate(directory, *args):
            directory.mkdir()
            return {'source_storage':{'parquet_uncompressed_bytes':100}}
        with tempfile.TemporaryDirectory() as folder, patch.object(prepare_experiment,'_prepare',side_effect=generate) as prepare:
            with self.assertRaisesRegex(ValueError,'did not converge'):
                prepare_experiment.prepare(folder,25,{'mode':'insertion','source_size_gb':25},'100',[])
            self.assertEqual(prepare.call_count,4)
            self.assertFalse((Path(folder)/'plan').exists())
            self.assertFalse((Path(folder)/'preparation.json').exists())

    def test_source_target_rejects_invalid_or_partial_snapshot_before_generation(self):
        for target in (0,-1,float('nan'),float('inf'),True,'25'):
            with self.subTest(target=target), patch.object(prepare_experiment,'_prepare') as prepare:
                with self.assertRaisesRegex(ValueError,'positive and finite'):
                    prepare_experiment.prepare('unused',25,{'source_size_gb':target},'100',[])
                prepare.assert_not_called()
        for mode,initial in [('insertion','50'),('days','100')]:
            with self.subTest(mode=mode), self.assertRaisesRegex(ValueError,'complete initial'):
                prepare_experiment.prepare('unused',25,{'mode':mode,'source_size_gb':25},initial,[])


if __name__ == '__main__':
    unittest.main()
