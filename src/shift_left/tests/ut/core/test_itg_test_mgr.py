"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for Integration Test Manager functionality.
"""
import os
import pathlib
import shutil
import unittest
from unittest.mock import patch, mock_open, MagicMock, call
from shift_left.core.utils.file_search import (
    get_or_build_inventory
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
    _create_validation_query_templates,
    _create_ddl_file_for_raw_tables
)
from shift_left.core.utils.file_search import FlinkTableReference
from shift_left.core.utils.app_config import reset_all_caches
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
        self.base_ddl_content = """CREATE TABLE j9r-env.j9r-kafka.raw_users (
    user_id STRING,
    group_id STRING,
    tenant_id STRING,
    created_date STRING,
    is_active BOOLEAN
) DISTRIBUTED BY HASH(user_id) INTO 1 BUCKETS
WITH (
    'changelog.mode' = 'append'
    )"""

        
    def test_find_source_tables_for_sink(self):
        """Test finding source tables for a sink table."""
        inventory = get_or_build_inventory(self.test_pipeline_path, self.test_pipeline_path, False)
        result = _find_source_tables_for_sink(self.test_sink_table, inventory, self.test_pipeline_path)
        self.assertCountEqual(result, ["raw_users", "raw_tenants","raw_groups"])


    def test_init_integration_tests_success_with_project_path(self):
        """
        Test if the test definition has all the expected fields.
        And files are created under the integration tests path.
        """
        with patch('shift_left.core.integration_test_mgr.statement_mgr.show_flink_table_structure') as mock_show_structure:
            # Configure mock to return different content based on table name
            def mock_structure_side_effect(table_name):
                return self.base_ddl_content.replace('raw_users', table_name)
            
            mock_show_structure.side_effect = mock_structure_side_effect

            itg_test_def = init_integration_tests(self.test_sink_table, self.test_pipeline_path)
            assert itg_test_def is not None
            assert itg_test_def.sink_test_path is not None
            expected_path = os.path.join(self.test_pipeline_path, "..", INTEGRATION_TEST_FOLDER, self.test_product_name, self.test_sink_table)
            self.assertEqual(itg_test_def.sink_test_path, expected_path)
            assert itg_test_def.product_name == self.test_product_name
            assert itg_test_def.sink_table == self.test_sink_table
            assert itg_test_def.scenarios is not None
            assert len(itg_test_def.scenarios) == 1
            assert itg_test_def.scenarios[0].name is not None
            assert itg_test_def.scenarios[0].source_data is not None
            assert len(itg_test_def.scenarios[0].source_data) == 3
            for source_data in itg_test_def.scenarios[0].source_data:
                assert source_data.table_name in ["raw_groups", "raw_users", "raw_tenants"]
                assert source_data.file_name in ["./insert_raw_groups_scenario_1.sql", "./insert_raw_users_scenario_1.sql", "./insert_raw_tenants_scenario_1.sql"]
            assert len(itg_test_def.scenarios[0].validation_queries) == 1
            assert len(itg_test_def.foundations) == 3
            for foundation in itg_test_def.foundations:
                assert foundation.table_name in ["raw_groups", "raw_users", "raw_tenants"]
                assert foundation.ddl_for_test in ["./ddl.raw_groups.sql", "./ddl.raw_users.sql", "./ddl.raw_tenants.sql"]
            for validation_query in itg_test_def.scenarios[0].validation_queries:
                assert validation_query.table_name == self.test_sink_table
                assert validation_query.file_name == "./validate_fct_user_per_group_scenario_1.sql"
            self._assert_files_exist(expected_path, ["ddl.raw_groups.sql", "ddl.raw_users.sql", "ddl.raw_tenants.sql", "insert_raw_groups_scenario_1.sql", "insert_raw_users_scenario_1.sql", "insert_raw_tenants_scenario_1.sql", "validate_fct_user_per_group_scenario_1.sql"])


    @patch('shift_left.core.integration_test_mgr.get_or_build_inventory')
    @patch('shift_left.core.integration_test_mgr.statement_mgr.show_flink_table_structure')
    @patch('shift_left.core.integration_test_mgr._find_source_tables_for_sink')
    @patch('builtins.open', new_callable=mock_open) 
    def test_init_integration_tests_success_with_env_var(self, mock_file,mock_find_sources, mock_show_structure, mock_get_inventory):
        """Test successful initialization using PIPELINES environment variable."""
        mock_get_inventory.return_value = {"src_test_source": {"table_name": "src_test_source", "product_name": "c360", "table_type": "source"}, "fct_user_per_group": {"table_name": "fct_user_per_group", "product_name": "c360", "table_type": "fact"}}
        mock_find_sources.return_value = ["src_test_source"]      
        mock_show_structure.return_value = self.base_ddl_content.replace('raw_users', "src_test_source")
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

    def test_create_synthetic_data_files(self):
        """Test creation of synthetic data files."""
        test_path = "/test/path"
        sql_contents = {"src_table1": "create table src_table1 (id int, name string, headers map<string, string> metadata)", 
                        "src_table2": "create table src_table2 (id int, name string, tenant_id string,  headers map<string, string> metadata)"}
        
        with patch('builtins.open', mock_open()) as mock_file, \
             patch('shift_left.core.integration_test_mgr.datetime') as mock_datetime:
            
            # Setup mocks
            mock_now = MagicMock()
            mock_now.isoformat.return_value = "2024-01-01T00:00:00"
            mock_datetime.now.return_value = mock_now
            
            _create_synthetic_data_files(test_path, sql_contents)
            
            # Verify file creation calls
            expected_files = [
                os.path.join(test_path, "insert_src_table1_scenario_1.sql"),
                os.path.join(test_path, "insert_src_table2_scenario_1.sql")
            ]
            handle = mock_file()
            write_calls = handle.write.call_args_list
            
            # Check that the content was written with the postfix applied
            self.assertEqual(len(write_calls), len(sql_contents))
            for write_call in write_calls:
                written_content = write_call[0][0]
                self.assertIn(CONFIGURED_POST_FIX_INTEGRATION_TEST, written_content)
                print(written_content)            
            

    def _test_create_validation_query_templates(self):
        """Test creation of validation query templates."""
        test_path = "/test/path"
        
        with patch('builtins.open', mock_open()) as mock_file:
            _create_validation_query_templates(test_path, self.test_sink_table)
            
            expected_file = os.path.join(test_path, f"validate_{self.test_sink_table}_scenario_1.sql")
            mock_file.assert_called_once_with(expected_file, 'w')

    def test_create_ddl_file_for_raw_tables(self):
        """Test creation of DDL files for raw tables with mocked show_flink_table_structure."""
        test_sink_path = "/test/path"
        source_tables = ["raw_users", "raw_groups"]
        
        # Mock SQL content that will be returned by show_flink_table_structure
       
        
        with patch('shift_left.core.integration_test_mgr.statement_mgr.show_flink_table_structure') as mock_show_structure, \
             patch('builtins.open', mock_open()) as mock_file:
            
            # Configure mock to return different content based on table name
            def mock_structure_side_effect(table_name):
                return self.base_ddl_content.replace('raw_users', table_name)
            
            mock_show_structure.side_effect = mock_structure_side_effect
            
            # Execute the function under test
            _create_ddl_file_for_raw_tables(test_sink_path, source_tables)
            
            # Verify show_flink_table_structure was called for each source table
            self.assertEqual(mock_show_structure.call_count, len(source_tables))
            mock_show_structure.assert_any_call('raw_users')
            mock_show_structure.assert_any_call('raw_groups')
            
            # Verify files were created with correct names
            expected_calls = [
                call(os.path.join(test_sink_path, 'ddl.raw_users.sql'), 'w'),
                call(os.path.join(test_sink_path, 'ddl.raw_groups.sql'), 'w')
            ]
            mock_file.assert_has_calls(expected_calls, any_order=True)
            
            # Verify that the file write operations included the modified table names
            handle = mock_file()
            write_calls = handle.write.call_args_list
            
            # Check that the content was written with the postfix applied
            self.assertEqual(len(write_calls), len(source_tables))
            for write_call in write_calls:
                written_content = write_call[0][0]  # First argument of write call
                self.assertIn(CONFIGURED_POST_FIX_INTEGRATION_TEST, written_content)
                print(written_content)
                # Check that the table names were replaced correctly in the CREATE TABLE statement
                # The function should replace "raw_users" with "raw_users_it" (or similar postfix)
                if 'raw_users' + CONFIGURED_POST_FIX_INTEGRATION_TEST in written_content:
                    # Verify that "CREATE TABLE j9r-env.j9r-kafka.raw_users_it" appears (not just "raw_users")
                    self.assertIn(f'CREATE TABLE j9r-env.j9r-kafka.raw_users{CONFIGURED_POST_FIX_INTEGRATION_TEST}', written_content)
                elif 'raw_groups' + CONFIGURED_POST_FIX_INTEGRATION_TEST in written_content:
                    # Verify that "CREATE TABLE j9r-env.j9r-kafka.raw_groups_it" appears (not just "raw_groups")  
                    self.assertIn(f'CREATE TABLE j9r-env.j9r-kafka.raw_groups{CONFIGURED_POST_FIX_INTEGRATION_TEST}', written_content)
                self.assertIn('headers MAP<STRING, STRING> METADATA', written_content)

if __name__ == '__main__':
    unittest.main()
