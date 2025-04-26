"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import pathlib
import os
from unittest.mock import ANY

os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config-all.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../../data/flink-project/pipelines")
from shift_left.core.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.test_mgr as test_mgr



class TestTestManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data" 
    
    def test_create_tests_structure(self):
        print("test_load_test_definition_for_fact_table")
        os.environ["CONFIG_FILE"] =  os.getenv("HOME") +  "/.shift_left/config-stage-flink.yaml"
        os.environ["PIPELINES"] = "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"

        table_name= "aqem_dim_tag"
        test_mgr.init_unit_test_for_table(table_name)

    




if __name__ == '__main__':
    unittest.main()