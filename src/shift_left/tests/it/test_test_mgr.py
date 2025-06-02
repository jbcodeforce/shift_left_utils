"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import pathlib
from shift_left.core.utils.app_config import shift_left_dir
import os
from unittest.mock import ANY
from shift_left.core.utils.app_config import get_config
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")



import  shift_left.core.test_mgr as test_mgr 


class TestTestManager(unittest.TestCase):
    
    def test_execute_one_test(self):
        print("test_execute_one_test")
        table_name= "p1_fct_order"
        compute_pool_id = "compute_pool_id"
        test_case_name = "test_case_1"
        result = test_mgr.execute_one_test(table_name, test_case_name, compute_pool_id)
        assert result


    def test_get_topic_list(self):
        table_exist = test_mgr._table_exists("products")
        print(f"table_exist: {table_exist}")
        assert test_mgr._topic_list
        assert table_exist
        table_exist = test_mgr._table_exists("p1_fct_order")
        print(f"table_exist: {table_exist}")
        assert not table_exist


    def test_delete_test_artifacts(self):
        config = get_config()
        table_name = "aqem_dim_role"
        compute_pool_id = config["flink"]["compute_pool_id"]
        test_mgr.delete_test_artifacts(table_name=table_name, 
                                       compute_pool_id=compute_pool_id,
                                       test_suite_result=None)
        
if __name__ == '__main__':
    unittest.main()