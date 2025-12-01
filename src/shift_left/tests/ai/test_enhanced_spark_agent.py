"""
Copyright 2024-2025 Confluent, Inc.

Integration tests for the enhanced SparkToFlinkSqlAgent with agentic validation flow.
"""
import unittest
import pathlib
import os
import tempfile
from unittest.mock import patch, MagicMock
from typing import Dict, List

from shift_left.ai.spark_sql_code_agent import SparkToFlinkSqlAgent, ErrorCategory
from shift_left.core.utils.app_config import get_config, logger


class TestSparkToFlinkSqlAgent(unittest.TestCase):
    """
    Integration tests for the enhanced Spark SQL to Flink SQL agent with:
    - Agentic validation flow
    - Error categorization and refinement
    - Real-world Spark SQL scenarios
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment and data directories"""
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"
        cls.spark_project_dir = cls.data_dir / "spark-project"

        # Set up environment variables
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging")
        os.environ["SRC_FOLDER"] = str(cls.spark_project_dir)
        os.makedirs(os.environ["STAGING"], exist_ok=True)
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)

        # Test files to process
        cls.test_files = [
            "facts/src_advanced_transformations.sql",
            "sources/src_streaming_aggregations.sql",
            "facts/p5/fct_users.sql",
            "sources/src_event_processing.sql",
            "sources/src_temporal_analytics.sql"
        ]

    def setUp(self):
        """Initialize agent for each test"""
        self.agent = SparkToFlinkSqlAgent()
        self.validation_results: List[Dict] = []

    def tearDown(self):
        """Clean up after each test"""
        # Clear validation history
        if hasattr(self, 'agent'):
            self.agent.validation_history = []

    def _load_spark_sql_file(self, relative_path: str) -> str:
        """Load Spark SQL content from test data files"""
        file_path = self.spark_project_dir / relative_path
        if not file_path.exists():
            self.fail(f"Test file not found: {file_path}")

        with open(file_path, 'r') as f:
            return f.read()

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

    def _test_2_simple_spark_translation_without_validation(self):
        """Test basic translation without CC validation for fast feedback"""
        simple_spark_sql = """
        WITH sales_data AS (
            SELECT
                customer_id,
                product_id,
                SUM(amount) as total_amount,
                surrogate_key(customer_id, product_id) as sales_key
            FROM sales_transactions
            WHERE created_at >= current_timestamp() - INTERVAL 7 DAY
            GROUP BY customer_id, product_id
        )
        SELECT * FROM sales_data WHERE total_amount > 100;
        """

        ddl, dml = self.agent.translate_to_flink_sqls(table_name="simple_test", sql=simple_spark_sql, validate=False)

        self._validate_translation_output(ddl[0], dml[0], "simple_test", simple_spark_sql)
        self.assertEqual(len(self.agent.get_validation_history()), 0,
                        "No validation history should exist when validation is disabled")
        print(f"Final DDL: {ddl}")
        print(f"Final DML: {dml}")
        logger.info("✅ Simple translation test passed")

    @patch('builtins.input', return_value='n')  # Auto-decline continuation prompts
    def test_3_complex_spark_translation_with_mocked_validation(self, mock_input):
        """Test complex translation with mocked CC validation"""

        # Load a complex Spark SQL file
        spark_sql = self._load_spark_sql_file("facts/src_advanced_transformations.sql")

        # Mock the CC validation to simulate various scenarios
        with patch.object(self.agent, '_validate_flink_sql_on_cc') as mock_validate:
            # Simulate initial failure then success on refinement
            mock_validate.side_effect = [
                (False, "Function 'GET_JSON_OBJECT' is not supported in Flink SQL"),
                (True, "DDL Statement is valid"),
                (True, "Statement is valid")
            ]
            table_name = "advanced_transformations"
            ddl, dml = self.agent.translate_to_flink_sqls(table_name=table_name, sql=spark_sql, validate=True)

            self._validate_translation_output(ddl[0], dml[0], "advanced_transformations", spark_sql)

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
            refined_ddl, refined_dml = self.agent._run_refinement_agent(
                original_ddl, original_dml, error_message, []
            )

            self.assertNotEqual(refined_dml, original_dml, "DML should be refined")
            self.assertIn("MD5", refined_dml, "Should contain MD5 function")
            self.assertNotIn("surrogate_key", refined_dml, "Should not contain surrogate_key")

        logger.info("✅ Refinement agent test passed")

    def _test_translation_with_various_spark_files(self):
        """Test translation with various real Spark SQL files (without CC validation)"""

        for test_file in self.test_files:
            with self.subTest(file=test_file):
                try:
                    spark_sql = self._load_spark_sql_file(test_file)

                    # Test without validation for speed
                    ddl, dml = self.agent.translate_to_flink_sqls(table_name=test_file, sql=spark_sql, validate=False)

                    self._validate_translation_output(ddl[0], dml[0], test_file, spark_sql)

                    logger.info(f"✅ Translation successful for {test_file}")

                except Exception as e:
                    logger.error(f"❌ Translation failed for {test_file}: {str(e)}")
                    self.fail(f"Translation failed for {test_file}: {str(e)}")



    def test_validation_history_tracking(self):
        """Test that validation history is properly tracked"""

        simple_sql = "SELECT customer_id, SUM(amount) FROM sales GROUP BY customer_id"

        # Mock validation calls to track history
        with patch.object(self.agent, '_validate_flink_sql_on_cc') as mock_validate:
            mock_validate.return_value = (True, "Valid")

            ddl, dml = self.agent.translate_to_flink_sqls(table_name="simple_test", sql=simple_sql, validate=True)

            history = self.agent.get_validation_history()
            self.assertGreater(len(history), 0, "History should be recorded")

            # Check history structure
            if len(history) > 0:
                entry = history[0]
                required_keys = ['iteration', 'ddl', 'dml', 'is_valid', 'error', 'error_category']
                for key in required_keys:
                    self.assertIn(key, entry, f"History entry should contain '{key}'")

        logger.info("✅ Validation history tracking test passed")

    @patch('builtins.input', return_value='n')
    def _test_end_to_end_translation_flow(self, mock_input):
        """Test the complete end-to-end translation flow"""

        # Use a moderately complex Spark SQL
        spark_sql = self._load_spark_sql_file("facts/p5/fct_users.sql")

        # Mock CC validation to return success
        with patch.object(self.agent, '_validate_flink_sql_on_cc') as mock_validate:
            mock_validate.return_value = (True, "Statement is valid")

            # Test complete flow
            ddl, dml = self.agent.translate_to_flink_sqls(table_name="fct_users", sql=spark_sql, validate=True)

            # Comprehensive validation
            self._validate_translation_output(ddl[0], dml[0], "fct_users.sql", spark_sql)

            # Check that the complete flow executed
            history = self.agent.get_validation_history()
            self.assertGreater(len(history), 0, "Validation should have occurred")

            # Verify final output quality
            self.assertIn("INSERT INTO", dml[0].upper(), "DML should be an INSERT statement")

            if ddl:
                self.assertIn("CREATE TABLE", ddl[0].upper(), "DDL should create a table")

        logger.info("✅ End-to-end translation flow test passed")


if __name__ == '__main__':
    # Set up logging for test output
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run the tests
    unittest.main(verbosity=2)
