"""
Copyright 2024-2025 Confluent, Inc.
Scenario to do end to end unit tests.   
"""

import unittest
from unittest.mock import patch, MagicMock
import os
import pytest
import pathlib
from datetime import datetime
import json
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] =  str(pathlib.Path(__file__).parent.parent.parent /  "data/flink-project/pipelines")
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import build_inventory
from shift_left.core.utils.app_config import reset_all_caches
import shift_left.core.test_mgr as test_mgr
from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementResult,
    StatementInfo, 
    StatementListCache, 
    Status, 
    Spec, 
    Data, 
    OpRow, 
    Metadata)  



class TestTestMgrScenario(unittest.TestCase):
    """
    Scenario to do end to end unit tests.
    """
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"
        reset_all_caches() # Reset all caches to ensure test isolation
        build_inventory(os.getenv("PIPELINES"))

    def setUp(self):
        """
        Set up the test environment
        """
        self._ddls_executed = {}

    def _mock_table_exists(self, table_name):
        """
        Mock the _table_exists(table_name) function to return True if the table name is in the _ddls_executed dictionary
        """
        if table_name not in self._ddls_executed.keys():
            return False
        value = self._ddls_executed[table_name]
        self._ddls_executed[table_name] = True  # mock the table will exist after the tet execution
        return value


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_happy_path_scenario(self, mock_post_flink_statement, 
                mock_table_exists,
                mock_get_statement_info,
                mock_get_statement_results,
                mock_get_statement
                ):
        """
        Taking the fact table fct_order, we want to create the unit tests, run the foundation and inserts, then run the validation.
        Verify the test has _ut as a based but the sql content is modified on the fly by using the app.post_fix_unit_test variable.
        """
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})

        def _mock_statement_info(statement_name):
            """
            Mock the statement_mgr.get_statement_info(statement_name) function to return None
            to enforce execution of the statement
            """
            print(f"mock_statement_info: {statement_name}")
            return None

        def _mock_get_statement(statement_name: str):
            print(f"mock_get_statement: {statement_name} returns None")  
            
            return None
    
        def _mock_get_statement_results(statement_name: str):
            print(f"mock_get_statement_results: {statement_name}")
            result = StatementResult(api_version="1.0", kind="StatementResult",  
                         results=Data(data=[OpRow(op=0, row=['PASS'])]))
            return result

        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_get_statement.side_effect = _mock_get_statement
        mock_get_statement_results.side_effect = _mock_get_statement_results
        
        table_name = "dim_users"
        print("Test happy path scenario\n--- Step 0: reset the testcases")
        test_mgr.delete_test_artifacts(table_name=table_name)
        where_to_find=os.getenv("PIPELINES") + "/dimensions/users/dim_users/tests/test_definitions.yaml"
        print(f"Where to find: {where_to_find}")
        self.assertTrue(os.path.exists(where_to_find))
        print("Test happy path scenario\n--- Step 2: run the foundation")
        test_mgr.execute_one_or_all_tests(table_name=table_name, test_case_name="test_dim_users_1", compute_pool_id=None, run_validation=False)
        print("Test happy path scenario\n--- Step 3: run the validation")
        test_mgr.execute_validation_tests(table_name=table_name, test_case_name="test_dim_users_1", compute_pool_id=None)

if __name__ == "__main__":
    unittest.main()