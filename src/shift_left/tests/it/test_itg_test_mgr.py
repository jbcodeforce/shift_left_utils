"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for Integration Test Manager functionality.
"""
import os
import pathlib
import shutil
import unittest
from unittest.mock import patch, mock_open, MagicMock, call
import shift_left.core.statement_mgr as sm
from shift_left.core.utils.file_search import (
    get_or_build_inventory,
    from_pipeline_to_absolute,
)
# Set up environment variables before importing the module under test
#os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.integration_test_mgr import (
    init_integration_tests,
    IntegrationTestSuite,
    IntegrationTestScenario,
    IntegrationTestData,
    INTEGRATION_TEST_FOLDER,
    CONFIGURED_POST_FIX_INTEGRATION_TEST,
    INTEGRATION_TEST_DEFINITION_FILE,
    _find_source_tables_for_sink,
    _create_integration_test_definition,
    _create_synthetic_data_files,
    _create_validation_query_templates
)
from shift_left.core.utils.file_search import FlinkTableReference
from shift_left.core.utils.app_config import reset_all_caches
import shift_left.core.pipeline_mgr as pm
from shift_left.core.utils.sql_parser import SQLparser
class TestIntegrationTestManager(unittest.TestCase):
    """
    Validate that init_integration_test_for_pipeline creates a tests folder structure
    with insert statements for source tables and validation SQLs for relevant intermediates
    and the sink tables.
    """

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"

    def _assert_files_exist(self, base_dir: str, file_names: list[str]) -> None:
        print(f"base_dir: {base_dir}")
        for fn in file_names:
            self.assertTrue(pathlib.Path(base_dir + "/" + fn).exists(), f"Expected file missing: {(base_dir + "/" + fn)}")

    def _cleanup_tests_dir(self, product_name: str, table_name: str) -> None:
        tests_dir = pathlib.Path(self.inventory_path) / "tests" / product_name / table_name
        if tests_dir.exists():
            shutil.rmtree(tests_dir)

    def setUp(self):
        """Set up test environment and reset caches."""
        
        # Sample test data
        self.test_pipeline_path = os.getenv("PIPELINES")
        self.test_sink_table = "fct_user_per_group"
        self.test_product_name = "c360"
        

    def test_show_flink_table_structure(self):
        """Test showing flink table structure."""
        sql_content = sm.show_flink_table_structure("raw_users")
        parser = SQLparser()
        print(sql_content)
        column_defs = parser.build_column_metadata_from_sql_content(sql_content)
        
        assert sql_content
        assert "CREATE TABLE" in sql_content
        assert column_defs
        assert len(column_defs) == 8
        print(column_defs)
        assert column_defs['user_id'] == {'name': 'user_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert column_defs['tenant_id'] == {'name': 'tenant_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert column_defs['user_email'] == {'name': 'user_email', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert column_defs['user_name'] == {'name': 'user_name', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert column_defs['group_id'] == {'name': 'group_id', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert column_defs['created_date'] == {'name': 'created_date', 'type': 'STRING', 'nullable': True, 'primary_key': False}
        assert column_defs['is_active'] == {'name': 'is_active', 'type': 'BOOLEAN', 'nullable': True, 'primary_key': False}

if __name__ == '__main__':
    unittest.main()
