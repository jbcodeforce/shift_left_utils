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
    
    def test_1_execute_one_test(self):
        print("test_execute_one_test")
        table_name= "c360_dim_users"
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
        test_case_name = "test_c360_dim_users_1"
        result = test_mgr.execute_one_or_all_tests(table_name, test_case_name, compute_pool_id)
        assert result


    def test_2_get_topic_list(self):
        table_exist = test_mgr._table_exists("products")
        print(f"table_exist: {table_exist}")
        assert table_exist == False
        assert test_mgr._topic_list_cache
        assert test_mgr._topic_list_cache.topic_list
        print(f"test_mgr._topic_list: {test_mgr._topic_list_cache.topic_list}")
        table_exist = test_mgr._table_exists("c360_dim_users")
        print(f"table_exist: {table_exist}")
        assert table_exist

    def test_3_execute_validation_tests(self):
        print("test_execute_one_test")
        table_name= "c360_dim_users"
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
        test_case_name = "test_c360_dim_users_1"
        result = test_mgr.execute_validation_tests(table_name, test_case_name, compute_pool_id)
        assert result

    def test_4_delete_test_artifacts(self):
        config = get_config()
        table_name = "c360_dim_users"
        compute_pool_id = config["flink"]["compute_pool_id"]
        test_mgr.delete_test_artifacts(table_name=table_name, 
                                       compute_pool_id=compute_pool_id,
                                       test_suite_result=None)
        
if __name__ == '__main__':
    unittest.main()