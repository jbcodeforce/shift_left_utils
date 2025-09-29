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

from shift_left.core.utils.spark_sql_code_agent import SparkToFlinkSqlAgent, ErrorCategory
from shift_left.core.utils.app_config import get_config, logger


class TestSparkToFlinkSqlAgent(unittest.TestCase):
    """
    Integration tests for the enhanced Spark SQL to Flink SQL agent with:
    - Agentic validation flow
    - Error categorization and refinement
    - Real-world Spark SQL scenarios
    """


    def tearDown(self):
        """Clean up after each test"""
        # Clear validation history
        if hasattr(self, 'agent'):
            self.agent.validation_history = []


    def test_2_simple_spark_translation_without_validation(self):
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
        
        ddl, dml = self.agent.translate_to_flink_sql(simple_spark_sql, validate=False)
        
        self._validate_translation_output(ddl, dml, "simple_test", simple_spark_sql)
        self.assertEqual(len(self.agent.get_validation_history()), 0, 
                        "No validation history should exist when validation is disabled")
        print(f"Final DDL: {ddl}")
        print(f"Final DML: {dml}")
        logger.info("✅ Simple translation test passed")

   
    def _test_translation_with_various_spark_files(self):
        """Test translation with various real Spark SQL files (without CC validation)"""
        
        for test_file in self.test_files:
            with self.subTest(file=test_file):
                try:
                    spark_sql = self._load_spark_sql_file(test_file)
                    
                    # Test without validation for speed
                    ddl, dml = self.agent.translate_to_flink_sql(spark_sql, validate=False)
                    
                    self._validate_translation_output(ddl, dml, test_file, spark_sql)
                    
                    logger.info(f"✅ Translation successful for {test_file}")
                    
                except Exception as e:
                    logger.error(f"❌ Translation failed for {test_file}: {str(e)}")
                    self.fail(f"Translation failed for {test_file}: {str(e)}")

 

   

    @patch('builtins.input', return_value='n')
    def _test_end_to_end_translation_flow(self, mock_input):
        """Test the complete end-to-end translation flow"""
        
        # Use a moderately complex Spark SQL
        spark_sql = self._load_spark_sql_file("facts/p5/fct_users.sql")
        
        # Mock CC validation to return success
        with patch.object(self.agent, '_validate_flink_sql_on_cc') as mock_validate:
            mock_validate.return_value = (True, "Statement is valid")
            
            # Test complete flow
            ddl, dml = self.agent.translate_to_flink_sql(spark_sql, validate=True)
            
            # Comprehensive validation
            self._validate_translation_output(ddl, dml, "fct_users.sql", spark_sql)
            
            # Check that the complete flow executed
            history = self.agent.get_validation_history()
            self.assertGreater(len(history), 0, "Validation should have occurred")
            
            # Verify final output quality
            self.assertIn("INSERT INTO", dml.upper(), "DML should be an INSERT statement")
            
            if ddl:
                self.assertIn("CREATE TABLE", ddl.upper(), "DDL should create a table")
        
        logger.info("✅ End-to-end translation flow test passed")

