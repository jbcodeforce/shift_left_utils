"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for Integration Test Manager functionality.
"""
import os
import pathlib
import shutil
import unittest
from unittest.mock import patch, mock_open, MagicMock, call
from datetime import datetime, timezone
from shift_left.core.utils.file_search import (
    get_or_build_inventory,
)
# Set up environment variables before importing the module under test
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
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


class TestIntegrationTestManager(unittest.TestCase):
    """
    Validate that init_integration_test_for_pipeline creates a tests folder structure
    with insert statements for source tables and validation SQLs for relevant intermediates
    and the sink tables.
    """

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"

    def _assert_files_exist(self, base_dir: pathlib.Path, file_names: list[str]) -> None:
        for fn in file_names:
            self.assertTrue((base_dir / fn).exists(), f"Expected file missing: {(base_dir / fn)}")

    def _cleanup_tests_dir(self, product_name: str, table_name: str) -> None:
        tests_dir = pathlib.Path(self.inventory_path) / "tests" / product_name / table_name
        if tests_dir.exists():
            shutil.rmtree(tests_dir)

    def setUp(self):
        """Set up test environment and reset caches."""
        
        # Sample test data
        self.test_pipeline_path = os.getenv("PIPELINES")
        self.test_sink_table = "fct_user_per_group"
        self.test_product_name = "users"
        

    def test_init_integration_tests_success_with_project_path(self):
        """Test successful initialization of integration tests with provided project path."""
        with patch('shift_left.core.integration_test_mgr._find_source_tables_for_sink') as mock_find_sources:
            mock_find_sources.return_value = ["src_test_source"]
            # Execute
            itg_test_def = init_integration_tests(self.test_sink_table, self.test_pipeline_path)
            assert itg_test_def is not None
            assert itg_test_def.sink_test_path is not None
            expected_path = os.path.join(self.test_pipeline_path, "..", INTEGRATION_TEST_FOLDER, self.test_product_name, self.test_sink_table)
            self.assertEqual(itg_test_def.sink_test_path, expected_path)
            assert itg_test_def.product_name is not None
            assert itg_test_def.sink_table is not None
            assert itg_test_def.scenarios is not None
            assert len(itg_test_def.scenarios) == 1
            assert itg_test_def.scenarios[0].name is not None
            assert itg_test_def.scenarios[0].sink_table is not None
            assert itg_test_def.scenarios[0].source_data is not None
            assert itg_test_def.scenarios[0].validation_queries is not None
            assert itg_test_def.scenarios[0].measure_latency is not None
            assert itg_test_def.scenarios[0].source_data[0].table_name is not None
            assert itg_test_def.scenarios[0].source_data[0].file_name is not None
            assert itg_test_def.scenarios[0].source_data[0].file_type is not None
            assert itg_test_def.scenarios[0].validation_queries[0].table_name is not None
            assert itg_test_def.scenarios[0].validation_queries[0].file_name is not None
            assert itg_test_def.scenarios[0].validation_queries[0].file_type is not None
            
        
    def test_init_integration_tests_success_with_env_var(self):
        """Test successful initialization using PIPELINES environment variable."""
        with patch('shift_left.core.integration_test_mgr._find_source_tables_for_sink') as mock_find_sources:
            mock_find_sources.return_value = ["src_test_source"]      
            expected_path = os.path.join(self.test_pipeline_path, "..", INTEGRATION_TEST_FOLDER, self.test_product_name, self.test_sink_table)
               
            # Execute (no project_path provided, should use env var)
            itg_test_def = init_integration_tests(self.test_sink_table)
            assert itg_test_def is not None
            self.assertEqual(itg_test_def.sink_test_path, expected_path)

    def test_init_integration_tests_no_project_path_or_env(self):
        """Test error when no project path provided and no PIPELINES env var."""
        with patch.dict(os.environ, {}, clear=True):
            # Restore CONFIG_FILE for the test
            os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
            
            with self.assertRaises(ValueError) as context:
                init_integration_tests(self.test_sink_table)
            
            self.assertIn("Project path must be provided", str(context.exception))

    def test_init_integration_tests_table_not_found(self):
        """Test error when sink table not found in inventory."""
        with self.assertRaises(ValueError) as context:
            init_integration_tests("non_existent_table", self.test_pipeline_path)
        
        self.assertIn("Sink table 'non_existent_table' not found in inventory", str(context.exception))


    def test_find_source_tables_for_sink(self):
        """Test finding source tables for a sink table."""
        inventory = get_or_build_inventory(self.test_pipeline_path, self.test_pipeline_path, False)
        result = _find_source_tables_for_sink(self.test_sink_table, inventory, self.test_pipeline_path)
        
        # Should find both source tables
        self.assertCountEqual(result, ["src_users_users", "src_users_groups"])

   
    def _test_create_synthetic_data_files(self):
        """Test creation of synthetic data files."""
        test_path = "/test/path"
        source_tables = ["src_table1", "src_table2"]
        
        with patch('builtins.open', mock_open()) as mock_file, \
             patch('uuid.uuid4') as mock_uuid, \
             patch('shift_left.core.integration_test_mgr.datetime') as mock_datetime:
            
            # Setup mocks
            mock_uuid.return_value = MagicMock()
            mock_uuid.return_value.__str__ = lambda: "test-uuid-123"
            mock_now = MagicMock()
            mock_now.isoformat.return_value = "2024-01-01T00:00:00"
            mock_datetime.now.return_value = mock_now
            
            _create_synthetic_data_files(test_path, source_tables)
            
            # Verify file creation calls
            expected_files = [
                os.path.join(test_path, "insert_src_table1_scenario_1.sql"),
                os.path.join(test_path, "insert_src_table2_scenario_1.sql")
            ]
            
            for expected_file in expected_files:
                mock_file.assert_any_call(expected_file, 'w')
            
            # Verify content written
            self.assertEqual(mock_file.call_count, 2)

    def test_create_validation_query_templates(self):
        """Test creation of validation query templates."""
        test_path = "/test/path"
        
        with patch('builtins.open', mock_open()) as mock_file:
            _create_validation_query_templates(test_path, self.test_sink_table)
            
            expected_file = os.path.join(test_path, f"validate_{self.test_sink_table}_scenario_1.sql")
            mock_file.assert_called_once_with(expected_file, 'w')


if __name__ == '__main__':
    unittest.main()
