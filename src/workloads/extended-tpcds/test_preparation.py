"""Preparation constrains every phase and validates complete source populations."""

import os
import tempfile
import unittest
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


if __name__ == '__main__':
    unittest.main()
