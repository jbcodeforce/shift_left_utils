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


def get_env_for_cli():
    """Get all environment variables that should be passed to CliRunner."""
    # Start with current environment
    env = dict(os.environ)
    return env

class IntegrationTestForUnitTestManager(unittest.TestCase):


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



    def test_run_dim_users_tests(self):
        runner = CliRunner()
        result = runner.invoke(app, ['table', 'run-unit-tests', 'c360_dim_users', '--test-case-name', 'test_c360_dim_users_1'], env=get_env_for_cli())

        assert result.exit_code == 0
        assert "Unit tests execution" in result.stdout
        print(result)


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
