"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import pathlib
from shift_left.core.utils.app_config import shift_left_dir
import os
import shutil
from unittest.mock import ANY
from shift_left.core.utils.app_config import get_config
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
from typer.testing import CliRunner
from shift_left.cli import app


import  shift_left.core.test_mgr as test_mgr 


class IntegrationTestTestManager(unittest.TestCase):
    
    def _test_init_unit_tests(self):
        """
        Test creating tests files for a table using the init unit tests command
        """
        working_dir=os.getenv("PIPELINES") + "/intermediates/p2/a/tests"
        if os.path.exists(working_dir):
            shutil.rmtree(working_dir)

        runner = CliRunner()
        result = runner.invoke(app, ["table", "init-unit-tests", "a", "--nb-test-cases", "1"])
        assert result.exit_code == 0
        assert os.path.exists(working_dir)
        assert os.path.exists(os.getenv("PIPELINES") + "/intermediates/p2/a/tests/test_definitions.yaml")
        assert os.path.exists(os.getenv("PIPELINES") + "/intermediates/p2/a/tests/ddl_src_x.sql")
        assert os.path.exists(os.getenv("PIPELINES") + "/intermediates/p2/a/tests/insert_src_x_1.sql")
        assert os.path.exists(os.getenv("PIPELINES") + "/intermediates/p2/a/tests/validate_a_1.sql")

    def test_1_execute_one_test_for_c360_dim_users(self):
        print("test_run one_test from tuned unit tests of existing table")
        """
        should get c360_dim_groups_jb,  src_c360_users_jb, c360_dim_users_jb as foundation tables
        """
        table_name= "c360_dim_users"
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
        test_case_name = "test_c360_dim_users_1"
        result = test_mgr.execute_one_or_all_tests(table_name=table_name, 
                                                test_case_name=test_case_name, 
                                                compute_pool_id=compute_pool_id,
                                                run_validation=False)
        assert result
        assert len(result.test_results) == 1
        print(f"test_result: {result.model_dump_json(indent=2)}")
        assert len(result.foundation_statements) == 4  #( 3 DDLs + DML)
        test_result = result.test_results[test_case_name]
        assert test_result
     
        assert len(test_result.statements) == 2   # the inserts. 
        assert test_result.result == "insertion done"



    def _test_run_from_automatic_created_unit_tests(self):
        runner = CliRunner()
        result = runner.invoke(app, ["table", "run-test-suite", "a"])
        assert result.exit_code == 0
        assert "Unit tests execution" in result.stdout

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
        print(f"test_result: {result.model_dump_json(indent=2)}")

    def test_4_delete_test_artifacts(self):
        config = get_config()
        table_name = "c360_dim_users"
        compute_pool_id = config["flink"]["compute_pool_id"]
        test_mgr.delete_test_artifacts(table_name=table_name, 
                                       compute_pool_id=compute_pool_id)
        
if __name__ == '__main__':
    unittest.main()