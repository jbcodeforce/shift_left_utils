"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import pathlib
import os
from unittest.mock import ANY

os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../../data/flink-project/pipelines")
from shift_left.core.flink_statement_model import Statement


from shift_left.core.test_mgr import (
    _load_test_suite_definition, 
    SLTestDefinition, 
    SLTestCase, 
    SLTestData, 
    Foundation, 
    execute_one_test,
    _run_foundations,
    _change_table_names_for_test_in_sql_content
)


class TestTestManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data" 
    
    def test_execute_one_test(self):
        print("test_execute_one_test")
        table_folder= os.getenv("PIPELINES") + "/facts/p1/fct_order"
        compute_pool_id = "compute_pool_id"
        test_case_name = "test_case_1"

        result = execute_one_test(table_folder, test_case_name)
        assert result, f"Test case '{test_case_name}' executed successfully"

if __name__ == '__main__':
    unittest.main()