
"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import sys
import os
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] =  str(pathlib.Path(__file__).parent.parent /  "data/flink-project/pipelines")
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_compute_pool_model import *
import shift_left.core.compute_pool_mgr as compute_mgr


class TestComputePoolMgr(unittest.TestCase):

   
    def test_1_compute_pool_list(self):
        config = get_config()
        print(f"test_1_compute_pool_list: should get at least one compute pool in {config.get('confluent_cloud').get('environment_id')}")
        cpl = compute_mgr.get_compute_pool_list(config.get('confluent_cloud').get('environment_id'), 
                                    region= config.get('confluent_cloud').get('region'))
        print(cpl.model_dump_json(indent=3))
        assert cpl.pools is not None
        assert len(cpl.pools) > 0
        assert cpl.pools[0].env_id == config.get('confluent_cloud').get('environment_id')
        assert cpl.pools[0].region == config.get('confluent_cloud').get('region')
        assert cpl.pools[0].status_phase == "PROVISIONED"
        assert cpl.pools[0].current_cfu >= 0
        assert cpl.pools[0].max_cfu > 0
    

    def test_2_search_for_matching_compute_pools(self):
        config = get_config()
        print("test_2_search_for_matching_compute_pools: should get dev-p1-fct-order")
        cpl = compute_mgr.search_for_matching_compute_pools("p1-fct-order")
        assert cpl is not None
        assert len(cpl) > 0
        assert cpl[0].name == "dev-p1-fct-order"
        assert cpl[0].env_id == config.get('confluent_cloud').get('environment_id')
        assert cpl[0].region == config.get('confluent_cloud').get('region')
        assert cpl[0].status_phase == "PROVISIONED"
        assert cpl[0].current_cfu >= 0
        assert cpl[0].max_cfu > 0

    def test_2_1_get_compute_pool_with_id(self):
        print("test_2_1_get_compute_pool_with_id")
        cpl = compute_mgr.get_compute_pool_list()
        compute_pool_id = "lfcp-d3n9zz"
        compute_pool = compute_mgr.get_compute_pool_with_id(cpl, compute_pool_id)
        assert compute_pool is not None
        assert compute_pool.id == compute_pool_id
        assert compute_pool.name == "dev-p1-fct-order"


    def test_3_create_existing_compute_pool(self):
        try:    
            print("test_3_create_existing_compute_pool should generate an error as the compute pool already exists")
            compute_pool_id, compute_pool_name = compute_mgr.create_compute_pool("p1-fct-order")
        except Exception as e:
            print(f"The error is expected as the compute pool already exists")
            print(e)
            assert True
    
    def test_4_test_pool_validation_and_delete(self):
        try:    
            print("test_4_test_pool_validation and delete the compute pool")
            compute_pool_list = compute_mgr.search_for_matching_compute_pools("p1-test-table")
            assert len(compute_pool_list) > 0
            compute_pool_id = compute_pool_list[0].id
            compute_pool_name = compute_pool_list[0].name
            assert compute_pool_id is not None
            assert compute_pool_name is not None
            assert compute_mgr.is_pool_valid(compute_pool_id)
            compute_mgr.delete_compute_pool(compute_pool_id)
        except Exception as e:
            print(e)
            assert True


if __name__ == '__main__':
    unittest.main()