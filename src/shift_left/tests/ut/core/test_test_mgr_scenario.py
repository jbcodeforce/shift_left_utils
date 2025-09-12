"""
Copyright 2024-2025 Confluent, Inc.
Scenario to do end to end unit tests.   
"""

import unittest
from unittest.mock import patch, MagicMock
import os
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
        pass

    def test_happy_path_scenario(self):
        """
        Taking the fact table fct_order, we want to create the unit tests, run the foundation and inserts, then run the validation.
        Verify the test has _ut as a based but the sql content is modified on the fly by using the app.post_fix_unit_test variable.
        """
        print("Test happy path scenario\n--- Step 1: init testcases")
        table_name = "dim_users_users"
        test_mgr.init_unit_test_for_table(table_name, create_csv=False, nb_test_cases=1, use_ai=False)
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/test_definitions.yaml"))
       

if __name__ == "__main__":
    unittest.main()