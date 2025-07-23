
"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import pathlib
import os
# Set up test environment
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")

from shift_left.core.utils.ksql_code_agent import KsqlToFlinkSqlAgent, _iterate_on_validation


class TestKsqlCodeAgent(unittest.TestCase):
    """Unit test suite for KsqlToFlinkSqlAgent._iterate_on_validation function."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.agent = KsqlToFlinkSqlAgent()
        self.test_sql = "SELECT * FROM test_table"
        self.refined_sql = "SELECT id, name FROM test_table"
        self.error_message = "Column 'invalid_column' does not exist"
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_successful_validation_first_try(self, mock_print, mock_input):
        """Test successful validation on the first attempt."""
        # Mock successful validation
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        mock_input.return_value = "y"
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Assertions
        self.assertEqual(result_sql, self.test_sql)
        self.assertTrue(is_validated)
        self.agent._validate_flink_sql_on_cc.assert_called_once_with(self.test_sql)
        mock_input.assert_called_once()

    @patch('builtins.input')
    @patch('builtins.print')
    def test_validation_fails_then_succeeds_after_refinement(self, mock_print, mock_input):
        """Test validation fails first, succeeds after refinement."""
        # Mock validation: fails first, succeeds second
        self.agent._validate_flink_sql_on_cc = MagicMock(side_effect=[
            (False, self.error_message),  # First call fails
            (True, "")                    # Second call succeeds
        ])
        self.agent._refinement_agent = MagicMock(return_value=self.refined_sql)
        mock_input.return_value = "y"
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Assertions
        self.assertEqual(result_sql, self.refined_sql)
        self.assertTrue(is_validated)
        self.assertEqual(self.agent._validate_flink_sql_on_cc.call_count, 2)
        self.agent._refinement_agent.assert_called_once_with(
            self.test_sql, 
            "[]", 
            self.error_message
        )

    @patch('builtins.input')
    @patch('builtins.print')
    def test_validation_fails_max_iterations(self, mock_print, mock_input):
        """Test validation fails for maximum iterations (3 attempts)."""
        # Mock validation to always fail
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(False, self.error_message))
        # Mock refinement to return different SQL each time
        self.agent._refinement_agent = MagicMock(side_effect=[
            "SELECT refined_1 FROM test_table",
            "SELECT refined_2 FROM test_table",
            "SELECT refined_3 FROM test_table"
        ])
        mock_input.return_value = "y"
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Assertions
        self.assertFalse(is_validated)
        self.assertEqual(self.agent._validate_flink_sql_on_cc.call_count, 3)
        self.assertEqual(self.agent._refinement_agent.call_count, 3)
        # Should return the last refined SQL
        self.assertEqual(result_sql, "SELECT refined_3 FROM test_table")
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_user_stops_early_after_first_failure(self, mock_print, mock_input):
        """Test user chooses to stop after first validation failure."""
        # Mock validation to fail
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(False, self.error_message))
        self.agent._refinement_agent = MagicMock(return_value=self.refined_sql)
        mock_input.return_value = "n"  # User chooses to stop
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Assertions
        self.assertEqual(result_sql, self.refined_sql)
        self.assertFalse(is_validated)
        self.agent._validate_flink_sql_on_cc.assert_called_once_with(self.test_sql)
        self.agent._refinement_agent.assert_called_once()
        mock_input.assert_called_once()
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_user_stops_early_after_second_failure(self, mock_print, mock_input):
        """Test user chooses to stop after second validation failure."""
        # Mock validation to always fail
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(False, self.error_message))
        self.agent._refinement_agent = MagicMock(side_effect=[
            "SELECT refined_1 FROM test_table",
            "SELECT refined_2 FROM test_table"
        ])
        # User continues first time, stops second time
        mock_input.side_effect = ["y", "n"]
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Assertions
        self.assertEqual(result_sql, "SELECT refined_2 FROM test_table")
        self.assertFalse(is_validated)
        self.assertEqual(self.agent._validate_flink_sql_on_cc.call_count, 2)
        self.assertEqual(self.agent._refinement_agent.call_count, 2)
        self.assertEqual(mock_input.call_count, 2)
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_user_stops_immediately_on_success(self, mock_print, mock_input):
        """Test user chooses to stop even when validation succeeds."""
        # Mock successful validation
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        mock_input.return_value = "n"  # User chooses to stop
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Assertions
        self.assertEqual(result_sql, self.test_sql)
        self.assertTrue(is_validated)
        self.agent._validate_flink_sql_on_cc.assert_called_once_with(self.test_sql)
        mock_input.assert_called_once()
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_agent_history_tracking(self, mock_print, mock_input):
        """Test that agent history is properly tracked through iterations."""
        # Mock validation to fail twice, succeed third time
        self.agent._validate_flink_sql_on_cc = MagicMock(side_effect=[
            (False, "error1"),
            (False, "error2"), 
            (True, "")
        ])
        self.agent._refinement_agent = MagicMock(side_effect=[
            "SELECT refined_1 FROM test_table",
            "SELECT refined_2 FROM test_table"
        ])
        mock_input.return_value = "y"
        
        result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
        
        # Verify refinement agent was called with proper history
        calls = self.agent._refinement_agent.call_args_list
        
        # First call should have empty history
        self.assertEqual(calls[0][0][1], "[]")
        
        # Second call should have history with first refinement
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT refined_1 FROM test_table'}]"
        self.assertEqual(calls[1][0][1], expected_history)
        
        self.assertTrue(is_validated)
        self.assertEqual(result_sql, "SELECT refined_2 FROM test_table")
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_edge_case_empty_sql(self, mock_print, mock_input):
        """Test behavior with empty SQL string."""
        empty_sql = ""
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        mock_input.return_value = "y"
        
        result_sql, is_validated = _iterate_on_validation(self.agent, empty_sql)
        
        self.assertEqual(result_sql, empty_sql)
        self.assertTrue(is_validated)
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_edge_case_various_user_inputs(self, mock_print, mock_input):
        """Test various user input responses."""
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        
        # Test different inputs that should stop execution
        for user_input in ["n", "N", "no", "quit", "", "anything_else"]:
            with self.subTest(user_input=user_input):
                mock_input.return_value = user_input
                result_sql, is_validated = _iterate_on_validation(self.agent, self.test_sql)
                self.assertEqual(result_sql, self.test_sql)
                self.assertTrue(is_validated)

    
    # ===============================================================
    # Tests for translate_from_ksql_to_flink_sql() function
    # ===============================================================
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_without_validation(self, mock_print, mock_input):
        """Test basic translation without validation (validate=False)."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DML_SQL", "DDL_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DML", "UPDATED_DDL"))
        
        ksql_input = "CREATE STREAM test AS SELECT * FROM source"
        
        result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql(ksql_input, validate=False)
        
        # Assertions
        self.assertEqual(result_dml, "UPDATED_DML")
        self.assertEqual(result_ddl, "UPDATED_DDL")
        
        # Verify method calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")
        
        # Verify no validation-related methods are called
        mock_input.assert_not_called()
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_with_validation_user_declines(self, mock_print, mock_input):
        """Test translation with validation=True but user chooses not to continue."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DML_SQL", "DDL_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DML", "UPDATED_DDL"))
        
        # User chooses not to continue validation
        mock_input.return_value = "n"
        
        ksql_input = "CREATE STREAM test AS SELECT * FROM source"
        
        result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql(ksql_input, validate=True)
        
        # Assertions
        self.assertEqual(result_dml, "UPDATED_DML")
        self.assertEqual(result_ddl, "UPDATED_DDL")
        
        # Verify method calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")
        mock_input.assert_called_once()
        
    
    @patch('shift_left.core.utils.ksql_code_agent._iterate_on_validation')
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_with_validation_full_success(self, mock_print, mock_input, mock_iterate_validation):
        """Test translation with validation where both DDL and DML validate successfully."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DML_SQL", "DDL_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DML", "UPDATED_DDL"))
        self.agent._process_semantic_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        
        # User chooses to continue validation
        mock_input.return_value = "y"
        
        # Mock successful validation for both DDL and DML
        mock_iterate_validation.side_effect = [
            ("VALIDATED_DDL", True),   # DDL validation succeeds
            ("VALIDATED_DML", True)    # DML validation succeeds
        ]
        
        ksql_input = "CREATE STREAM test AS SELECT * FROM source"
        
        result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql(ksql_input, validate=True)
        
        # Assertions
        self.assertEqual(result_dml, "SEMANTIC_VALIDATED_DML")
        self.assertEqual(result_ddl, "SEMANTIC_VALIDATED_DDL")
        
        # Verify method calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")
        mock_input.assert_called_once()
        
        # Verify _iterate_on_validation was called for both DDL and DML
        self.assertEqual(mock_iterate_validation.call_count, 2)
        mock_iterate_validation.assert_any_call(self.agent, "UPDATED_DDL")
        mock_iterate_validation.assert_any_call(self.agent, "UPDATED_DML")
        
        # Verify semantic validation was called for both
        self.assertEqual(self.agent._process_semantic_validation.call_count, 2)
        self.agent._process_semantic_validation.assert_any_call("VALIDATED_DDL")
        self.agent._process_semantic_validation.assert_any_call("VALIDATED_DML")
    
    @patch('shift_left.core.utils.ksql_code_agent._iterate_on_validation')
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_with_validation_ddl_success_dml_fail(self, mock_print, mock_input, mock_iterate_validation):
        """Test translation with validation where DDL succeeds but DML fails validation."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DML_SQL", "DDL_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DML", "UPDATED_DDL"))
        self.agent._process_semantic_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        
        # User chooses to continue validation
        mock_input.return_value = "y"
        
        # Mock validation: DDL succeeds, DML fails
        mock_iterate_validation.side_effect = [
            ("VALIDATED_DDL", True),   # DDL validation succeeds
            ("FAILED_DML", False)      # DML validation fails
        ]
        
        ksql_input = "CREATE STREAM test AS SELECT * FROM source"
        
        result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql(ksql_input, validate=True)
        
        # Assertions
        self.assertEqual(result_dml, "FAILED_DML")  # DML not semantically processed due to failure
        self.assertEqual(result_ddl, "SEMANTIC_VALIDATED_DDL")  # DDL was semantically processed
        
        # Verify method calls
        self.assertEqual(mock_iterate_validation.call_count, 2)
        # Verify semantic validation was called only for DDL (since DML failed)
        self.agent._process_semantic_validation.assert_called_once_with("VALIDATED_DDL")
    
    @patch('shift_left.core.utils.ksql_code_agent._iterate_on_validation')
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_with_validation_ddl_fails(self, mock_print, mock_input, mock_iterate_validation):
        """Test translation with validation where DDL validation fails."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DML_SQL", "DDL_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DML", "UPDATED_DDL"))
        self.agent._process_semantic_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        
        # User chooses to continue validation
        mock_input.return_value = "y"
        
        # Mock DDL validation failure
        mock_iterate_validation.return_value = ("FAILED_DDL", False)
        
        ksql_input = "CREATE STREAM test AS SELECT * FROM source"
        
        result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql(ksql_input, validate=True)
        
        # Assertions
        self.assertEqual(result_dml, "UPDATED_DML")  # Original DML returned
        self.assertEqual(result_ddl, "FAILED_DDL")   # Failed DDL returned
        
        # Verify method calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")
        
        # Verify _iterate_on_validation was called only once (for DDL)
        mock_iterate_validation.assert_called_once_with(self.agent, "UPDATED_DDL")
        
        # Verify no semantic validation occurred
        self.agent._process_semantic_validation.assert_not_called()
    
    
    
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_edge_cases(self, mock_print, mock_input):
        """Test edge cases for translate_from_ksql_to_flink_sql."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("", ""))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("", ""))
        
        # Test with empty KSQL input
        result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql("", validate=False)
        self.assertEqual(result_dml, "")
        self.assertEqual(result_ddl, "")
        
        # Test with various user inputs for validation prompt
        for user_input in ["n", "N", "no", "quit", "", "anything_not_y"]:
            with self.subTest(user_input=user_input):
                mock_input.return_value = user_input
                result_dml, result_ddl = self.agent.translate_from_ksql_to_flink_sql("TEST", validate=True)
                self.assertEqual(result_dml, "")
                self.assertEqual(result_ddl, "")
    
    @patch('shift_left.core.utils.ksql_code_agent._iterate_on_validation')
    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_method_call_order(self, mock_print, mock_input, mock_iterate_validation):
        """Test that methods are called in the correct order during translation."""
        # Mock the agent methods with side effects to track call order
        call_order = []
        
        def translator_side_effect(ksql):
            call_order.append("translator_agent")
            return ("DML", "DDL")
        
        def mandatory_validation_side_effect(ddl, dml):
            call_order.append("mandatory_validation_agent")
            return ("UPDATED_DML", "UPDATED_DDL")
        
        def semantic_validation_side_effect(sql):
            call_order.append(f"semantic_validation_{sql}")
            return f"SEMANTIC_{sql}"
        
        self.agent._translator_agent = MagicMock(side_effect=translator_side_effect)
        self.agent._mandatory_validation_agent = MagicMock(side_effect=mandatory_validation_side_effect)
        self.agent._process_semantic_validation = MagicMock(side_effect=semantic_validation_side_effect)
        
        # User continues with validation
        mock_input.return_value = "y"
        mock_iterate_validation.side_effect = [
            ("VALIDATED_DDL", True),
            ("VALIDATED_DML", True)
        ]
        
        self.agent.translate_from_ksql_to_flink_sql("TEST", validate=True)
        
        # Verify correct call order
        expected_order = [
            "translator_agent",
            "mandatory_validation_agent",
            "semantic_validation_VALIDATED_DDL",
            "semantic_validation_VALIDATED_DML"
        ]
        self.assertEqual(call_order, expected_order)


if __name__ == '__main__':
    unittest.main()