"""Preparation must constrain every DuckDB phase, before engine timing starts."""

import os
import tempfile
import unittest
from unittest.mock import patch

import prepare_experiment


class PreparationTests(unittest.TestCase):
    def test_limits_reach_generation_extension_and_mutation_planning(self):
        options = {'profile':'native','seed':17,'report_date':'2002-12-31',
                   'history_months':12,'mode':'mixed','days':3,'mutation_cohort':'all'}
        limits = {'memory_limit':'2GB','threads':3,'max_temp_directory_size':'1GB'}
        with tempfile.TemporaryDirectory() as folder, \
             patch.dict(os.environ, {'DUCKDB_MEM_LIMIT':'2GB','DUCKDB_THREADS':'3',
                                     'DUCKDB_MAX_TEMP_DIRECTORY_SIZE':'1GB'}), \
             patch.object(prepare_experiment.duckdb,'connect') as connect, \
             patch.object(prepare_experiment.generate,'prepare',return_value={}), \
             patch.object(prepare_experiment.generate,'statistics',return_value={}), \
             patch.object(prepare_experiment.mutations,'sources',return_value=[]), \
             patch.object(prepare_experiment.mutations,'plan',return_value={'plan_id':'p','batches':[]}):
            prepare_experiment.prepare(folder,.01,options,'100',[])
            self.assertEqual(connect.call_count,3)
            for call in connect.call_args_list:
                self.assertEqual(call.kwargs['config'],limits)


if __name__ == '__main__':
    unittest.main()
