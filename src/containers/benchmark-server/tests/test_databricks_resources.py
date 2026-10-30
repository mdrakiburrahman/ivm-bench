"""Both Databricks engines must receive the same local orchestration budget."""

import sys
import unittest
from pathlib import Path

SERVER = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER))

from models.config import BenchmarkConfig
from services.resource_calc import compute_engine_configs


class DatabricksResourcesTest(unittest.TestCase):
    def test_local_client_and_loader_allocations_match_in_serial_and_parallel(self):
        for parallel, cpus, memory in ((False, 32, 251), (True, 16, 125)):
            with self.subTest(parallel=parallel):
                config = BenchmarkConfig(engines=["databricks-enzyme", "databricks-openivm"],
                                         parallel=parallel, host_cores=32, host_memory_gb=251)
                engines = compute_engine_configs(config)
                enzyme, openivm = engines["databricks-enzyme"], engines["databricks-openivm"]
                self.assertIsNone(openivm.main_service)
                self.assertEqual(openivm.dbt_server_resources, enzyme.dbt_server_resources)
                self.assertEqual(openivm.main_resources, enzyme.main_resources)
                oi_env, en_env = openivm.env_dict(), enzyme.env_dict()
                self.assertEqual(oi_env["DBT_SERVER_CPUS"], en_env["DATABRICKS_ENZYME_CPUS"])
                self.assertEqual(oi_env["DBT_SERVER_MEM"], en_env["DATABRICKS_ENZYME_MEM"])
                self.assertEqual(oi_env["DBT_SERVER_CPUS"], str(cpus))
                self.assertEqual(oi_env["DBT_SERVER_MEM"], f"{memory}g")


if __name__ == "__main__":
    unittest.main()
