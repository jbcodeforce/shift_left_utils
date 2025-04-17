
"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import sys
import os
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-all.yaml")


from shift_left.core.utils.app_config import get_config
from shift_left.core.flink_compute_pool_model import *
from shift_left.core.compute_pool_mgr import get_compute_pool_list

class TestComputePoolMgr(unittest.TestCase):

   
    def test_compute_pool_list(self):
        config = get_config()
        cpl = get_compute_pool_list(config.get('confluent_cloud').get('environment_id'), 
                                    region= config.get('confluent_cloud').get('region'))
        print(cpl.model_dump_json(indent=3))
        assert cpl.pools is not None
        assert len(cpl.pools) > 0

if __name__ == '__main__':
    unittest.main()