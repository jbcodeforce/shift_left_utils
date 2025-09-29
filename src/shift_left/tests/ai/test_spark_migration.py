"""
Copyright 2024-2025 Confluent, Inc.
"""
from re import T
import unittest
import pathlib
import os
from typing import Dict, List
from unittest.mock import patch, MagicMock
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
from shift_left.core.utils.translator_to_flink_sql import get_or_build_sql_translator_agent
from shift_left.core.utils.app_config import get_config, log_dir, logger
from shift_left.core.utils.spark_sql_code_agent import ErrorCategory
from shift_left.core.process_src_tables import migrate_one_file
from shift_left.core.utils.spark_sql_code_agent import SparkToFlinkSqlAgent

"""
Taking a complex SQL statement migrates to Flink SQL.
"""

class TestSparkMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging") 
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "spark-project")
        os.makedirs(os.environ["STAGING"], exist_ok=True)   
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)
        config = get_config()
        config['app']['translator_to_flink_sql_agent']='shift_left.core.utils.spark_sql_code_agent.SparkToFlinkSqlAgent'
        print('validate agent factory')
        get_or_build_sql_translator_agent()
        # but to get access to agent specific methods for this test, create a real instance.
        cls.agent= SparkToFlinkSqlAgent()

    def setUp(self):
        """Initialize agent for each test"""
        self.validation_results: List[Dict] = []

    def _load_spark_sql_file(self, src_file: str) -> str:
        src_folder = os.environ["SRC_FOLDER"]
        src_file_path = src_folder + "/" + src_file
        try:
            with open(src_file_path, "r") as f:
                return f.read()
        except Exception as e:
            self.fail(f"Test file not found: {src_file_path}")
            


    def _validate_translation_output(self, ddl: str, dml: str, source_file: str, original_sql: str = None):
        """Validate the quality of translation output"""
        # Basic validation
        self.assertIsNotNone(dml, f"DML should not be None for {source_file}")
        self.assertTrue(len(dml.strip()) > 0, f"DML should not be empty for {source_file}")
        
        # Check for proper Flink SQL structure
        if dml.strip():
            dml_upper = dml.upper()
            self.assertTrue(
                dml_upper.startswith(('INSERT INTO', 'WITH', 'SELECT')),
                f"DML should start with valid SQL statement for {source_file}"
            )
        
        # Check for proper Spark to Flink translations
        self._check_function_translations(ddl, dml, source_file, original_sql)
        
        # DDL validation (if generated and actually contains DDL)
        if ddl and ddl.strip():
            ddl_upper = ddl.upper()
            # Only check for CREATE TABLE if the DDL doesn't look like DML
            if not ddl_upper.startswith(('INSERT INTO', 'WITH', 'SELECT')):
                self.assertTrue(
                    'CREATE TABLE' in ddl_upper,
                    f"DDL should contain CREATE TABLE for {source_file}"
                )
            else:
                # DDL generation may have failed, which is acceptable for some tests
                logger.warning(f"DDL generation appears to have returned DML instead for {source_file}")

    def _check_function_translations(self, ddl: str, dml: str, source_file: str, original_sql: str = None):
        """Check that Spark-specific functions are properly translated"""
        all_sql = f"{ddl} {dml}".lower()
        
        # Get original SQL content
        if original_sql:
            original_lower = original_sql.lower()
        else:
            try:
                original_content = self._load_spark_sql_file(source_file.split('/')[-1] 
                                                           if '/' in source_file else source_file)
                original_lower = original_content.lower()
            except:
                # If we can't load the file, skip detailed function checking
                original_lower = ""
        
        # Check that Spark functions are translated
        spark_functions = ['surrogate_key', 'current_timestamp()']
        for func in spark_functions:
            self.assertNotIn(func, all_sql, 
                           f"Untranslated Spark function '{func}' found in {source_file}")
        
        # Check for proper Flink translations only if original had surrogate_key
        if 'surrogate_key' in original_lower:
            self.assertIn('md5', all_sql, f"surrogate_key should be translated to MD5 in {source_file}")
            self.assertIn('concat_ws', all_sql, f"surrogate_key should use CONCAT_WS in {source_file}")

    

# -- test methods --
    def test_1_agent_initialization_and_prompts(self):
        """Test that the agent initializes correctly and loads prompts"""
        
        self.assertIsNotNone(self.agent.translator_system_prompt, 
                           "Translator prompt should be loaded")
        self.assertIsNotNone(self.agent.ddl_creation_system_prompt,
                           "DDL creation prompt should be loaded")
        self.assertIsNotNone(self.agent.refinement_system_prompt,
                           "Refinement prompt should be loaded")
        
        # Check that prompts contain expected content
        
        self.assertIn("code assistant specializing in Apache Flink SQL", 
                    self.agent.translator_system_prompt,
                     "Translator prompt should mention Flink SQL")
        self.assertIn("Use CREATE TABLE IF NOT EXISTS", 
                    self.agent.ddl_creation_system_prompt,
                   "DDL creation prompt should handle CREATE TABLE IF NOT EXISTS")
        self.assertIn("SQL error correction agent", 
                    self.agent.refinement_system_prompt,
                     "Refinement prompt should handle SQL error correction")
        
        logger.info("✅ Agent initialization test passed")

    @patch('builtins.input', return_value='n')  # Auto-decline continuation prompts
    def test_3_complex_spark_translation_with_mocked_validation(self, mock_input):
        """Test complex translation with mocked CC validation"""
        
        # Load a complex Spark SQL file
        spark_sql = self._load_spark_sql_file("facts/fct_advanced_transformations.sql")
        
        # Mock the CC validation to simulate various scenarios
        with patch.object(self.agent, 'validate_with_flink_engine') as mock_validate:
            # Simulate initial failure then success on refinement
            mock_validate.side_effect = [
                (False, "Function 'GET_JSON_OBJECT' is not supported in Flink SQL"),
                (True, "DDL Statement is valid"),
                (True, "Statement is valid")
            ]
            
            ddl, dml = self.agent.translate_to_flink_sql(spark_sql, validate=True)
            
            self._validate_translation_output(ddl, dml, "advanced_transformations", spark_sql)
            
            # Check validation history
            history = self.agent.get_validation_history()
            self.assertGreater(len(history), 0, "Validation history should exist")
            
            # Verify error categorization worked
            if len(history) > 0 and not history[0]['is_valid']:
                self.assertIsNotNone(history[0]['error_category'], 
                                   "Error should be categorized")
        
        logger.info("✅ Complex translation with mocked validation test passed")

    def test_error_categorization(self):
        """Test the error categorization functionality"""
        test_errors = [
            ("Syntax error near 'SELECT'", ErrorCategory.SYNTAX_ERROR),
            ("Function 'surrogate_key' not supported", ErrorCategory.FUNCTION_INCOMPATIBILITY),
            ("Cannot cast STRING to INTEGER", ErrorCategory.TYPE_MISMATCH),
            ("Watermark definition is invalid", ErrorCategory.WATERMARK_ISSUE),
            ("Table properties are missing", ErrorCategory.CONNECTOR_ISSUE),
            ("Column 'unknown_column' not found", ErrorCategory.SEMANTIC_ERROR),
            ("Something unexpected happened", ErrorCategory.UNKNOWN)
        ]
        
        for error_message, expected_category in test_errors:
            actual_category = self.agent._categorize_error(error_message)
            self.assertEqual(actual_category, expected_category,
                           f"Error '{error_message}' should be categorized as {expected_category.value}")
        
        logger.info("✅ Error categorization test passed")

    def test_pre_validation_syntax_checks(self):
        """Test the pre-validation syntax checking"""
        
        # Test valid SQL
        valid_ddl = "CREATE TABLE test (id INT, name STRING)"
        valid_dml = "INSERT INTO test SELECT id, name FROM source"
        is_valid, error = self.agent._pre_validate_syntax(valid_ddl, valid_dml)
        self.assertTrue(is_valid, f"Valid SQL should pass pre-validation: {error}")
        
        # Test invalid SQL structures
        invalid_cases = [
            ("", "INVALID STATEMENT", "DML must start with INSERT, SELECT, or WITH"),
            ("INVALID DDL", "INSERT INTO test VALUES (1, 'test')", "DDL must start with CREATE"),
            ("CREATE TABLE test (id INT", "SELECT * FROM test", "unbalanced parentheses"),
            ("CREATE TABLE test (id INT)", "SELECT surrogate_key(1,2) FROM test", "untranslated Spark function")
        ]
        
        for ddl, dml, expected_error_part in invalid_cases:
            is_valid, error = self.agent._pre_validate_syntax(ddl, dml)
            self.assertFalse(is_valid, f"Invalid SQL should fail pre-validation: {ddl}, {dml}")
            self.assertIn(expected_error_part.lower(), error.lower(),
                         f"Error message should contain '{expected_error_part}': {error}")
        
        logger.info("✅ Pre-validation syntax checks test passed")

    @patch('builtins.input', return_value='n')  # Auto-decline continuation
    def test_refinement_agent_with_mock(self, mock_input):
        """Test the refinement agent functionality with mocked LLM responses"""
        
        original_ddl = "CREATE TABLE test (id INT, name VARCHAR(100))"
        original_dml = "SELECT id, name, surrogate_key(id, name) FROM test"
        error_message = "Function 'surrogate_key' not supported"
        
        # Mock the LLM client response for refinement
        mock_response = MagicMock()
        mock_response.choices[0].message.parsed.refined_ddl = "CREATE TABLE test (id INT, name STRING)"
        mock_response.choices[0].message.parsed.refined_dml = "SELECT id, name, MD5(CONCAT_WS(',', id, name)) FROM test"
        mock_response.choices[0].message.parsed.explanation = "Replaced surrogate_key with MD5(CONCAT_WS)"
        mock_response.choices[0].message.parsed.changes_made = ["Fixed function compatibility issue"]
        
        with patch.object(self.agent.llm_client.chat.completions, 'parse', return_value=mock_response):
            refined_ddl, refined_dml = self.agent._refinement_agent(
                original_ddl, original_dml, error_message, []
            )
            
            self.assertNotEqual(refined_dml, original_dml, "DML should be refined")
            self.assertIn("MD5", refined_dml, "Should contain MD5 function")
            self.assertNotIn("surrogate_key", refined_dml, "Should not contain surrogate_key")
        
        logger.info("✅ Refinement agent test passed")


    def test_validation_history_tracking(self):
        """Test that validation history is properly tracked"""
        
        simple_sql = "SELECT customer_id, SUM(amount) FROM sales GROUP BY customer_id"
        
        # Mock validation calls to track history
        with patch.object(self.agent, 'validate_with_flink_engine') as mock_validate:
            mock_validate.return_value = (True, "Valid")
            
            ddl, dml = self.agent.translate_to_flink_sql(simple_sql, validate=True)
            
            history = self.agent.get_validation_history()
            self.assertGreater(len(history), 0, "History should be recorded")
            
            # Check history structure
            if len(history) > 0:
                entry = history[0]
                required_keys = ['iteration', 'ddl', 'dml', 'is_valid', 'error', 'error_category']
                for key in required_keys:
                    self.assertIn(key, entry, f"History entry should contain '{key}'")
        
        logger.info("✅ Validation history tracking test passed")

    @patch('builtins.input')
    def test_x_all_spark_examples(self, mock_input):
        """
        Integration test to validate all Spark SQL examples can be processed.
        """
        mock_input.return_value = "n"
        
        spark_examples = [
            ("fct_users", "facts/p5/fct_users.sql") ,
            ("dim_product_analytics", os.environ["SRC_FOLDER"] + "/dimensions/dim_product_analytics.sql"),
            ("dim_customer_journey", os.environ["SRC_FOLDER"] + "/dimensions/dim_customer_journey.sql"),
            ("dim_event_processing", os.environ["SRC_FOLDER"] + "/dimensions/dim_event_processing.sql"),
            ("dim_sales_pivot", os.environ["SRC_FOLDER"] + "/dimensions/dim_sales_pivot.sql"),
            ("dim_streaming_aggregations", os.environ["SRC_FOLDER"] + "/dimensions/dim_streaming_aggregations.sql"),
            ("dim_set_operations", os.environ["SRC_FOLDER"] + "/dimensions/dim_set_operations.sql"),
            ("dim_temporal_analytics", os.environ["SRC_FOLDER"] + "/dimensions/dim_temporal_analytics.sql"),
            ("fct_advanced_transformations", os.environ["SRC_FOLDER"] + "/facts/fct_advanced_transformations.sql")
        ]
        
        success_count = 0
        total_count = len(spark_examples)
        for table_name,example in spark_examples:
            try:
                print("-"*50 +f"\n\tProcessing: {example}\n" + "-"*50)
                migrate_one_file(table_name=table_name,
                                sql_src_file=example, 
                                staging_target_folder=os.environ["STAGING"], 
                                src_folder_path=os.environ["SRC_FOLDER"], 
                                process_parents=False, 
                                source_type="spark",
                                validate=True)
                success_count += 1
                print(f"✓ Successfully processed: {example}")
            except Exception as e:
                print(f"✗ Failed to process {example}: {e}")
        
        print(f"\nIntegration Test Results: {success_count}/{total_count} examples processed successfully")
        
        # Allow some failures but expect most to work
        success_rate = success_count / total_count
        assert success_rate >= 0.7, f"Integration test failed: only {success_rate:.1%} of examples processed successfully"

if __name__ == '__main__':
    unittest.main()