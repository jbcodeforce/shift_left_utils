"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.statement_mgr import *
import shift_left.core.compute_pool_mgr as cpm
from shift_left.core.utils.ccloud_client import ConfluentCloudClient

class TestPoolManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
    # ---- Compute pool apis ------------------- 

    def test_verify_pool_state(self):
        """
        Given the compute pool id in the test config filr, get information about the pool using cloud client
        """
        config = get_config()
        client = ConfluentCloudClient(config)
        result = cpm._verify_compute_pool_provisioned(client, config['flink']['compute_pool_id'],
                                                      config.get('confluent_cloud').get('environment_id'))
        assert result == True

    def test_get_compute_pool_list(self):
        config = get_config()
        pool_list = cpm.get_compute_pool_list(config.get('confluent_cloud').get('environment_id'))
        self.assertGreater(len(pool_list.pools), 0)
        first_pool = pool_list.pools[0]
        assert getattr(first_pool, 'name', None)
        assert getattr(first_pool, 'env_id', None) == config.get('confluent_cloud').get('environment_id')
        # Optionally, print as dicts for readability
        print(json.dumps([p.model_dump() for p in pool_list.pools], indent=2))

    def test_validate_a_pool(self):
        config = get_config()
        result = cpm.is_pool_valid(config['flink']['compute_pool_id'])
        assert result


if __name__ == '__main__':
    unittest.main()