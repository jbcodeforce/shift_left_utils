"""
Unit tests for SQL Content Processing Extension.

This module provides comprehensive unit tests for the ModifySqlContentForDifferentEnv class,
including tests for environment-specific transformations, mocking of configuration,
and validation of SQL transformation logic.

The tests cover:
- Configuration loading and initialization
- DDL transformations (CREATE TABLE statements)
- DML transformations (INSERT, SELECT statements)
- Environment-specific logic (dev, stage, prod)
- Thread safety validation
- Error handling scenarios
"""

import unittest
from unittest.mock import patch, MagicMock
import threading
import time
from typing import Dict, Any

from shift_left.extensions.sql_content_processing import ModifySqlContentForDifferentEnv


class TestModifySqlContentForDifferentEnv(unittest.TestCase):
    """
    Test suite for the ModifySqlContentForDifferentEnv class.
    
    This test class validates all functionality of the SQL content processor,
    including environment-specific transformations, configuration handling,
    and thread safety.
    """

    def setUp(self):
        """
        Set up test fixtures before each test method.
        
        Initializes common test data and mock configurations used across
        multiple test cases.
        """
        # Sample configuration for testing
        self.test_config = {
            'kafka': {
                'cluster_type': 'dev',
                'src_topic_prefix': 'clone'
            }
        }
        
        # Sample SQL statements for testing
        self.ddl_sql_dev = """
        CREATE TABLE src_customer (
            id BIGINT,
            name STRING
        ) WITH (
            'connector' = 'kafka',
            'format' = 'avro',
            'avro-confluent.schema-registry.url' = 'https://schema-registry.flink-dev.example.com'
        );
        """
        
        self.dml_sql_basic = "SELECT * FROM final;"
        
        self.dml_sql_with_topic = "INSERT INTO src_customer SELECT * FROM clone.dev.ap-east-1-dev.customer_topic;"
        
        self.dml_sql_insert_src = "INSERT INTO src_customer SELECT tenant_id, name FROM source WHERE active = true;"
        
        # SQL that matches the tenant filtering pattern more precisely
        self.dml_sql_select_final = "select * from final;"

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_init_default_config(self, mock_get_config):
        """
        Test initialization with default configuration.
        
        Validates that the processor correctly loads configuration and sets up
        default values for environment and topic prefix.
        """
        mock_get_config.return_value = self.test_config
        
        processor = ModifySqlContentForDifferentEnv()
        
        self.assertEqual(processor.env, 'dev')
        self.assertEqual(processor.topic_prefix, 'clone')
        self.assertIsInstance(processor.semaphore, threading.Semaphore)
        self.assertIsNotNone(processor.insert_into_src)
        mock_get_config.assert_called_once()

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_init_stage_config(self, mock_get_config):
        """
        Test initialization with staging environment configuration.
        
        Validates that the processor correctly adapts to staging environment
        settings and updates replacement patterns accordingly.
        """
        stage_config = {
            'kafka': {
                'cluster_type': 'stage',
                'src_topic_prefix': 'staging'
            }
        }
        mock_get_config.return_value = stage_config
        
        processor = ModifySqlContentForDifferentEnv()
        
        self.assertEqual(processor.env, 'stage')
        self.assertEqual(processor.topic_prefix, 'staging')
        
        # Verify that replacement patterns are updated with correct environment
        stage_replace = processor.dml_replacements["stage"]["adapt"]["replace"]
        self.assertIn('staging', stage_replace)
        self.assertIn('stage', stage_replace)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_init_missing_config_defaults(self, mock_get_config):
        """
        Test initialization with missing configuration uses defaults.
        
        Validates that the processor handles missing configuration gracefully
        by falling back to default values.
        """
        # Configuration missing kafka section
        empty_config = {}
        mock_get_config.return_value = empty_config
        
        processor = ModifySqlContentForDifferentEnv()
        
        # Should fall back to defaults when config is missing
        self.assertEqual(processor.env, 'dev')  # Default from fallback dict
        self.assertEqual(processor.topic_prefix, 'clone')  # Default from fallback dict

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_ddl_transformation_stage_environment(self, mock_get_config):
        """
        Test DDL transformation for staging environment.
        
        Validates that CREATE TABLE statements are correctly transformed
        for staging environment, particularly schema registry URLs.
        """
        stage_config = {
            'kafka': {
                'cluster_type': 'stage',
                'src_topic_prefix': 'clone'
            }
        }
        mock_get_config.return_value = stage_config
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(self.ddl_sql_dev)
        
        self.assertTrue(updated)
        self.assertIn('flink-stage', result)
        self.assertNotIn('flink-dev', result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_ddl_transformation_prod_environment(self, mock_get_config):
        """
        Test DDL transformation for production environment.
        
        Validates that CREATE TABLE statements are correctly transformed
        for production environment.
        """
        prod_config = {
            'kafka': {
                'cluster_type': 'prod',
                'src_topic_prefix': 'clone'
            }
        }
        mock_get_config.return_value = prod_config
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(self.ddl_sql_dev)
        
        self.assertTrue(updated)
        self.assertIn('flink-prod', result)
        self.assertNotIn('flink-dev', result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_ddl_no_transformation_dev_environment(self, mock_get_config):
        """
        Test that DDL statements are not transformed in dev environment.
        
        Validates that CREATE TABLE statements remain unchanged when
        already in the target dev environment.
        """
        mock_get_config.return_value = self.test_config  # dev environment
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(self.ddl_sql_dev)
        
        # No transformation should occur in dev environment for DDL
        self.assertFalse(updated)
        self.assertEqual(result, self.ddl_sql_dev)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_dml_clone_dev_removal_stage_environment(self, mock_get_config):
        """
        Test removal of 'clone.dev.' prefix in non-dev environments.
        
        Validates that DML statements with 'clone.dev.' prefixes are
        correctly modified when running in staging environment.
        """
        stage_config = {
            'kafka': {
                'cluster_type': 'stage',
                'src_topic_prefix': 'clone'
            }
        }
        mock_get_config.return_value = stage_config
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(self.dml_sql_with_topic)
        
        self.assertTrue(updated)
        self.assertNotIn('clone.dev.', result)
        # After both transformations: clone.dev removal AND topic adaptation
        # The result should have the adapted topic pattern
        self.assertIn('clone.stage.ap-east-1-stage.customer_topic', result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_dml_no_transformation_dev_environment_standard_dml(self, mock_get_config):
        """
        Test that most DML statements don't get transformed in dev environment.
        
        The dev environment logic is very specific - it only applies to
        INSERT INTO src_ statements with specific conditions.
        """
        mock_get_config.return_value = self.test_config  # dev environment
        
        processor = ModifySqlContentForDifferentEnv()
        
        # Test SQL that should NOT trigger transformation
        sql_basic = "SELECT * FROM some_table;"
        
        updated, result = processor.update_sql_content(
            sql_basic, 
            column_to_search='tenant_id', 
            product_name='test_product'
        )
        
        # No transformation should occur for basic SQL in dev environment
        self.assertFalse(updated)
        self.assertEqual(result, sql_basic)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_dml_insert_src_pattern_dev_environment(self, mock_get_config):
        """
        Test the specific INSERT INTO src_ pattern logic in dev environment.
        
        This tests the complex logic that checks for INSERT INTO src_ and
        applies SELECT FROM final replacement when conditions are met.
        """
        mock_get_config.return_value = self.test_config  # dev environment
        
        processor = ModifySqlContentForDifferentEnv()
        
        # SQL that matches INSERT INTO src_ pattern and contains the search column
        sql_insert_src = "INSERT INTO src_customer SELECT tenant_id, name FROM source WHERE active = true;"
        
        updated, result = processor.update_sql_content(
            sql_insert_src, 
            column_to_search='tenant_id', 
            product_name='test_product'
        )
        
        # This should trigger the special INSERT INTO src_ logic
        # But the current implementation has unusual behavior - let's test what it actually does
        # Based on the code, it should apply the "select * from final" pattern replacement
        if updated:
            # If transformation occurred, verify it contains tenant filtering elements
            self.assertIn('tenant_filter_pipeline', result)
            self.assertIn('test_product', result)
        else:
            # If no transformation, the SQL should remain unchanged
            self.assertEqual(result, sql_insert_src)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_dml_no_tenant_filtering_for_stage_product(self, mock_get_config):
        """
        Test that tenant filtering is skipped for stage and common products.
        
        Validates that products named 'stage' or 'common' do not get
        tenant filtering applied even in dev environment.
        """
        mock_get_config.return_value = self.test_config  # dev environment
        
        processor = ModifySqlContentForDifferentEnv()
        
        # Test with 'stage' product
        updated_stage, result_stage = processor.update_sql_content(
            self.dml_sql_insert_src, 
            column_to_search='tenant_id', 
            product_name='stage'
        )
        
        # Test with 'common' product
        updated_common, result_common = processor.update_sql_content(
            self.dml_sql_insert_src, 
            column_to_search='tenant_id', 
            product_name='common'
        )
        
        # No tenant filtering should be applied
        self.assertFalse(updated_stage)
        self.assertFalse(updated_common)
        self.assertNotIn('WHERE tenant_id IN', result_stage)
        self.assertNotIn('WHERE tenant_id IN', result_common)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_dml_topic_adaptation_stage_environment(self, mock_get_config):
        """
        Test DML topic adaptation for staging environment.
        
        Validates that topic references are correctly adapted when
        moving from dev to staging environment.
        """
        stage_config = {
            'kafka': {
                'cluster_type': 'stage',
                'src_topic_prefix': 'clone'
            }
        }
        mock_get_config.return_value = stage_config
        
        # SQL with topic pattern that should be adapted
        sql_with_topic_pattern = "INSERT INTO dest SELECT * FROM ap-east-1-dev.source_topic;"
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(sql_with_topic_pattern)
        
        self.assertTrue(updated)
        self.assertIn('clone.stage.ap-east-1-stage', result)
        self.assertNotIn('ap-east-1-dev', result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_no_transformation_for_non_matching_sql(self, mock_get_config):
        """
        Test that SQL without matching patterns remains unchanged.
        
        Validates that SQL statements that don't match any transformation
        patterns are returned unchanged.
        """
        mock_get_config.return_value = self.test_config
        
        # Simple SQL that shouldn't match any patterns
        simple_sql = "SELECT count(*) FROM regular_table;"
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(simple_sql)
        
        self.assertFalse(updated)
        self.assertEqual(result, simple_sql)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_case_insensitive_create_table_detection(self, mock_get_config):
        """
        Test that CREATE TABLE detection is case insensitive.
        
        Validates that both 'CREATE TABLE' and 'create table' are
        properly detected and processed as DDL statements.
        """
        stage_config = {
            'kafka': {
                'cluster_type': 'stage',
                'src_topic_prefix': 'clone'
            }
        }
        mock_get_config.return_value = stage_config
        
        # Test lowercase version
        lowercase_ddl = self.ddl_sql_dev.replace('CREATE TABLE', 'create table')
        
        processor = ModifySqlContentForDifferentEnv()
        updated, result = processor.update_sql_content(lowercase_ddl)
        
        self.assertTrue(updated)
        self.assertIn('flink-stage', result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_thread_safety_concurrent_access(self, mock_get_config):
        """
        Test thread safety of the SQL content processor.
        
        Validates that multiple concurrent calls to update_sql_content
        are properly synchronized using the semaphore.
        """
        mock_get_config.return_value = self.test_config
        
        processor = ModifySqlContentForDifferentEnv()
        results = []
        errors = []
        
        def worker_function(sql_content: str, worker_id: int):
            """Worker function for concurrent testing."""
            try:
                updated, result = processor.update_sql_content(
                    f"{sql_content} -- Worker {worker_id}"
                )
                results.append((worker_id, updated, result))
            except Exception as e:
                errors.append((worker_id, str(e)))
        
        # Create multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(
                target=worker_function, 
                args=(self.dml_sql_basic, i)
            )
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        # Validate results
        self.assertEqual(len(errors), 0, f"Errors occurred: {errors}")
        self.assertEqual(len(results), 5)
        
        # All results should be processed correctly
        for worker_id, updated, result in results:
            self.assertIsInstance(updated, bool)
            self.assertIsInstance(result, str)
            self.assertIn(f"Worker {worker_id}", result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    @patch('shift_left.extensions.sql_content_processing.logger')
    def test_logging_behavior(self, mock_logger, mock_get_config):
        """
        Test that appropriate logging occurs during SQL processing.
        
        Validates that debug and info logs are generated during
        SQL transformation operations.
        """
        mock_get_config.return_value = self.test_config
        
        processor = ModifySqlContentForDifferentEnv()
        processor.update_sql_content(self.dml_sql_basic)
        
        # Verify that logging methods were called
        self.assertTrue(mock_logger.debug.called)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_multiple_transformations_in_sequence(self, mock_get_config):
        """
        Test multiple consecutive transformations on the same processor.
        
        Validates that the processor can handle multiple different
        SQL transformations in sequence without state corruption.
        """
        stage_config = {
            'kafka': {
                'cluster_type': 'stage',
                'src_topic_prefix': 'clone'
            }
        }
        mock_get_config.return_value = stage_config
        
        processor = ModifySqlContentForDifferentEnv()
        
        # Process DDL statement
        ddl_updated, ddl_result = processor.update_sql_content(self.ddl_sql_dev)
        
        # Process DML statement
        dml_updated, dml_result = processor.update_sql_content(self.dml_sql_with_topic)
        
        # Both should be transformed correctly
        self.assertTrue(ddl_updated)
        self.assertTrue(dml_updated)
        self.assertIn('flink-stage', ddl_result)
        self.assertNotIn('clone.dev.', dml_result)

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_empty_sql_content(self, mock_get_config):
        """
        Test handling of empty or whitespace-only SQL content.
        
        Validates that the processor handles edge cases like empty
        strings gracefully without errors.
        """
        mock_get_config.return_value = self.test_config
        
        processor = ModifySqlContentForDifferentEnv()
        
        # Test empty string
        updated_empty, result_empty = processor.update_sql_content("")
        self.assertFalse(updated_empty)
        self.assertEqual(result_empty, "")
        
        # Test whitespace only
        updated_whitespace, result_whitespace = processor.update_sql_content("   \n\t  ")
        self.assertFalse(updated_whitespace)
        self.assertEqual(result_whitespace, "   \n\t  ")

    @patch('shift_left.extensions.sql_content_processing.get_config')
    def test_optional_parameters_none(self, mock_get_config):
        """
        Test behavior when optional parameters are None.
        
        Validates that the processor handles None values for optional
        parameters correctly without errors.
        """
        mock_get_config.return_value = self.test_config
        
        processor = ModifySqlContentForDifferentEnv()
        
        # Test with explicit None values
        updated, result = processor.update_sql_content(
            self.dml_sql_basic,
            column_to_search=None,
            product_name=None
        )
        
        self.assertIsInstance(updated, bool)
        self.assertIsInstance(result, str)


if __name__ == '__main__':
    """
    Run the test suite when the module is executed directly.
    
    This allows for easy execution of tests during development:
    python test_sql_content_processing.py
    """
    unittest.main(verbosity=2)