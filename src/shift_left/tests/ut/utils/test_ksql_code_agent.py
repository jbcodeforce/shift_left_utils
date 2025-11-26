
"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock, mock_open, call
import pathlib
import os
import tempfile
import shutil
from pydantic import ValidationError

# Set up test environment
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")

from shift_left.ai.ksql_code_agent import (
    KsqlToFlinkSqlAgent,
    KsqlTableDetection,
    KsqlFlinkSql,
    FlinkSql,
    FlinkSqlForRefinement,
    _snapshot_ddl_dml
)


class TestKsqlCodeAgent(unittest.TestCase):
    """Unit test suite for KsqlToFlinkSqlAgent._iterate_on_validation function."""

    def setUp(self):
        """Set up test fixtures."""
        self.agent = KsqlToFlinkSqlAgent()
        self.test_sql = "SELECT * FROM test_table"
        self.refined_sql = "SELECT id, name FROM test_table"
        self.error_message = "Column 'invalid_column' does not exist"

        # Create a temporary directory for file operations
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up after tests."""
        # Clean up temporary directory
        if hasattr(self, 'temp_dir') and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_clean_ksql_input(self):
        """
        Test the _clean_ksql_input function to ensure it properly removes
        DROP TABLE statements and comment lines starting with '--'
        """
        # Create an instance of the agent for testing
        agent = KsqlToFlinkSqlAgent()

        # Test case 1: Simple DROP TABLE removal
        ksql_input = """
DROP TABLE IF EXISTS old_table;
CREATE TABLE new_table (
    id INT,
    name STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'my-topic'
);
"""
        expected_output = """
CREATE TABLE new_table (
id INT,
name STRING
) WITH (
'connector' = 'kafka',
'topic' = 'my-topic'
);
"""
        result = agent._clean_ksql_input(ksql_input)
        print(f"result: {result}")
        self.assertEqual(result, expected_output)

        # Test case 2: Comment lines removal
        ksql_input = """
-- This is a comment
CREATE TABLE test_table (
    id INT,
    -- Another comment
    name STRING
) WITH (
    'connector' = 'kafka'
);
-- Final comment
"""
        expected_output = """
CREATE TABLE test_table (
id INT,
name STRING
) WITH (
'connector' = 'kafka'
);
"""
        result = agent._clean_ksql_input(ksql_input)
        self.assertEqual(result, expected_output)

        # Test case 3: Mixed DROP TABLE and comments (case insensitive)
        ksql_input = """
-- Header comment
drop table if exists temp_table;
DROP TABLE another_table;
CREATE STREAM my_stream (
    -- Field comment
    event_id STRING,
    timestamp BIGINT
) WITH (
    'kafka.topic' = 'events'
);
-- End comment
"""
        expected_output = """
CREATE STREAM my_stream (
event_id STRING,
timestamp BIGINT
) WITH (
'kafka.topic' = 'events'
);
"""
        result = agent._clean_ksql_input(ksql_input)
        self.assertEqual(result, expected_output)

        # Test case 4: No changes needed
        ksql_input = """
CREATE TABLE clean_table (
    id INT,
    data STRING
) WITH (
    'connector' = 'kafka'
);
"""
        expected_output = """
CREATE TABLE clean_table (
id INT,
data STRING
) WITH (
'connector' = 'kafka'
);
"""
        result = agent._clean_ksql_input(ksql_input)
        self.assertEqual(result, expected_output)

        # Test case 5: Empty and whitespace handling
        ksql_input = """

CREATE TABLE spaced_table (
    id INT
);
"""
        expected_output = """

CREATE TABLE spaced_table (
id INT
);
"""
        result = agent._clean_ksql_input(ksql_input)
        self.assertEqual(result, expected_output)

        # Test case 6: DROP STREAM removal
        ksql_input = """
DROP STREAM old_stream;
drop stream another_stream;
CREATE STREAM new_stream (id INT) WITH ('kafka.topic' = 'test');
"""
        expected_output = """
CREATE STREAM new_stream (id INT) WITH ('kafka.topic' = 'test');
"""
        result = agent._clean_ksql_input(ksql_input)
        self.assertEqual(result, expected_output)


    def test_load_prompts_success(self):
        """Test successful loading of prompt files."""
        # Test the method
        agent = KsqlToFlinkSqlAgent()
        agent._load_prompts()
        assert agent.translator_system_prompt
        assert agent.refinement_system_prompt
        assert agent.mandatory_validation_system_prompt
        assert agent.table_detection_system_prompt


    @patch('shift_left.core.utils.ksql_code_agent.importlib.resources.files')
    def test_load_prompts_file_error(self, mock_files):
        """Test error handling when prompt files cannot be loaded."""
        # Mock file access to raise an exception
        mock_files.return_value.joinpath.return_value.open.side_effect = FileNotFoundError("File not found")

        # Should raise the exception when agent is created (calls _load_prompts in __init__)
        with self.assertRaises(FileNotFoundError):
            agent = KsqlToFlinkSqlAgent()

    def test_table_detection_agent_single_table(self):
        """Test table detection agent with single table input."""
        # Mock LLM response for single table
        mock_detection = KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE TABLE single (id INT)"],
            description="Single table detected"
        )

        mock_message = MagicMock()
        mock_message.parsed = mock_detection
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.table_detection_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        ksql = "CREATE TABLE single (id INT)"
        result = self.agent._table_detection_agent(ksql)

        # Assertions
        self.assertFalse(result.has_multiple_tables)
        self.assertEqual(result.description, "Single table detected")
        self.agent.llm_client.chat.completions.parse.assert_called_once()

    def test_table_detection_agent_multiple_tables(self):
        """Test table detection agent with multiple tables input."""
        # Mock LLM response for multiple tables
        mock_detection = KsqlTableDetection(
            has_multiple_tables=True,
            table_statements=[
                "CREATE TABLE table1 (id INT)",
                "CREATE TABLE table2 (name STRING)"
            ],
            description="Multiple tables detected"
        )

        mock_message = MagicMock()
        mock_message.parsed = mock_detection
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.table_detection_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        ksql = "CREATE TABLE table1 (id INT); CREATE TABLE table2 (name STRING)"
        result = self.agent._table_detection_agent(ksql)

        # Assertions
        self.assertTrue(result.has_multiple_tables)
        self.assertEqual(len(result.table_statements), 2)
        self.assertEqual(result.description, "Multiple tables detected")

    def test_table_detection_agent_parsing_error(self):
        """Test table detection agent when LLM response parsing fails."""
        # Mock LLM response with parsing failure
        mock_message = MagicMock()
        mock_message.parsed = None
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.table_detection_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        ksql = "CREATE TABLE test (id INT)"
        result = self.agent._table_detection_agent(ksql)

        # Should return fallback response
        self.assertFalse(result.has_multiple_tables)
        self.assertEqual(result.table_statements, [ksql])
        self.assertIn("Error in detection", result.description)

    def test_translator_agent_success(self):
        """Test successful translation by translator agent."""
        # Mock LLM response
        mock_translation = KsqlFlinkSql(
            ksql_input="CREATE STREAM test AS SELECT * FROM source",
            flink_ddl_output="CREATE TABLE test_ddl (id INT)",
            flink_dml_output="INSERT INTO test SELECT * FROM source"
        )

        mock_message = MagicMock()
        mock_message.parsed = mock_translation
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.translator_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        with patch('builtins.print'):  # Suppress print output
            ddl, dml = self.agent._translator_agent("CREATE STREAM test AS SELECT * FROM source")

        # Assertions
        self.assertEqual(ddl, "CREATE TABLE test_ddl (id INT)")
        self.assertEqual(dml, "INSERT INTO test SELECT * FROM source")
        self.agent.llm_client.chat.completions.parse.assert_called_once()

    def test_translator_agent_parsing_error(self):
        """Test translator agent when LLM response parsing fails."""
        # Mock LLM response with parsing failure
        mock_message = MagicMock()
        mock_message.parsed = None
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.translator_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        with patch('builtins.print'):  # Suppress print output
            ddl, dml = self.agent._translator_agent("CREATE STREAM test AS SELECT * FROM source")

        # Should return empty strings
        self.assertEqual(ddl, "")
        self.assertEqual(dml, "")

    def test_mandatory_validation_agent_success(self):
        """Test successful validation by mandatory validation agent."""
        # Mock LLM response
        mock_validation = FlinkSql(
            ddl_sql_input="CREATE TABLE test (id INT)",
            dml_sql_input="INSERT INTO test VALUES (1)",
            flink_ddl_output="CREATE TABLE test (id INT) WITH ('connector' = 'kafka')",
            flink_dml_output="INSERT INTO test SELECT * FROM source"
        )

        mock_message = MagicMock()
        mock_message.parsed = mock_validation
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.mandatory_validation_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        ddl, dml = self.agent._mandatory_validation_agent(
            "CREATE TABLE test (id INT)",
            "INSERT INTO test VALUES (1)"
        )

        # Assertions
        self.assertEqual(ddl, "CREATE TABLE test (id INT) WITH ('connector' = 'kafka')")
        self.assertEqual(dml, "INSERT INTO test SELECT * FROM source")

    def test_mandatory_validation_agent_parsing_error(self):
        """Test mandatory validation agent when LLM response parsing fails."""
        # Mock LLM response with parsing failure
        mock_message = MagicMock()
        mock_message.parsed = None
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.mandatory_validation_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        ddl, dml = self.agent._mandatory_validation_agent("DDL", "DML")

        # Should return empty strings
        self.assertEqual(ddl, "")
        self.assertEqual(dml, "")

    def test_refinement_agent_success(self):
        """Test successful refinement by refinement agent."""
        # Mock LLM response
        mock_refinement = FlinkSqlForRefinement(
            sql_input="SELECT * FROM invalid_table",
            error_message="Table does not exist",
            flink_output="SELECT * FROM valid_table"
        )

        mock_message = MagicMock()
        mock_message.parsed = mock_refinement
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.refinement_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        result = self.agent._refinement_agent(
            "SELECT * FROM invalid_table",
            "[{'agent': 'translator', 'sql': 'original'}]",
            "Table does not exist"
        )

        # Assertions
        self.assertEqual(result, "SELECT * FROM valid_table")

    def test_refinement_agent_parsing_error(self):
        """Test refinement agent when LLM response parsing fails."""
        # Mock LLM response with parsing failure
        mock_message = MagicMock()
        mock_message.parsed = None
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_response = MagicMock()
        mock_response.choices = [mock_choice]

        self.agent.llm_client = MagicMock()
        self.agent.llm_client.chat.completions.parse.return_value = mock_response
        self.agent.refinement_system_prompt = "test prompt"
        self.agent.model_name = "test-model"

        result = self.agent._refinement_agent("SQL", "history", "error")

        # Should return empty string
        self.assertEqual(result, "")

    def test_process_semantic_validation(self):
        """Test semantic validation processing (currently a passthrough)."""
        test_sql = "SELECT * FROM test_table"
        result = self.agent._process_semantic_validation(test_sql)

        # Currently just returns the input unchanged
        self.assertEqual(result, test_sql)

    @patch('shift_left.core.utils.ksql_code_agent.shift_left_dir', '/tmp/test')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.join')
    def test_snapshot_ddl_dml_success(self, mock_join, mock_file_open):
        """Test successful snapshot of DDL and DML to files."""
        # Setup mocks
        mock_join.side_effect = lambda *args: '/'.join(args)

        ddl = "CREATE TABLE test (id INT)"
        dml = "INSERT INTO test VALUES (1)"
        table_name = "test_table"

        result_ddl, result_dml = _snapshot_ddl_dml(table_name, ddl, dml)

        # Assertions
        self.assertEqual(result_ddl, ddl)
        self.assertEqual(result_dml, dml)

        # Verify file operations
        self.assertEqual(mock_file_open.call_count, 2)
        mock_file_open.assert_any_call('/tmp/test/ddl.test_table.sql', 'w')
        mock_file_open.assert_any_call('/tmp/test/dml.test_table.sql', 'w')

        # Verify file writes
        handle = mock_file_open.return_value
        handle.write.assert_any_call(ddl)
        handle.write.assert_any_call(dml)

    @patch('builtins.input')
    @patch('builtins.print')
    @patch('shift_left.core.utils.ksql_code_agent._snapshot_ddl_dml')
    def test_translate_multiple_tables_success(self, mock_snapshot, mock_print, mock_input):
        """Test translation with multiple tables detected."""
        # Mock table detection to return multiple tables
        mock_detection = KsqlTableDetection(
            has_multiple_tables=True,
            table_statements=[
                "CREATE TABLE table1 (id INT)",
                "CREATE TABLE table2 (name STRING)"
            ],
            description="Found 2 tables"
        )

        # Mock all agent methods
        self.agent._table_detection_agent = MagicMock(return_value=mock_detection)
        self.agent._translator_agent = MagicMock(side_effect=[
            ("DDL1", "DML1"),
            ("DDL2", "DML2")
        ])
        self.agent._mandatory_validation_agent = MagicMock(side_effect=[
            ("VALIDATED_DDL1", "VALIDATED_DML1"),
            ("VALIDATED_DDL2", "VALIDATED_DML2")
        ])

        ksql_input = "CREATE TABLE table1 (id INT); CREATE TABLE table2 (name STRING)"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls(
            "test_tables", ksql_input, validate=False
        )

        # Assertions
        self.assertEqual(len(result_ddl), 2)
        self.assertEqual(len(result_dml), 2)
        self.assertEqual(result_ddl, ["VALIDATED_DDL1", "VALIDATED_DDL2"])
        self.assertEqual(result_dml, ["VALIDATED_DML1", "VALIDATED_DML2"])

        # Verify each table was processed
        self.assertEqual(self.agent._translator_agent.call_count, 2)
        self.assertEqual(self.agent._mandatory_validation_agent.call_count, 2)

        # Verify snapshot calls
        self.assertEqual(mock_snapshot.call_count, 4)  # 2 calls per table (before and after validation)

    @patch('builtins.input')
    @patch('builtins.print')
    @patch('shift_left.core.utils.ksql_code_agent._snapshot_ddl_dml')
    def test_translate_multiple_tables_with_empty_results(self, mock_snapshot, mock_print, mock_input):
        """Test translation with multiple tables where some return empty results."""
        # Mock table detection to return multiple tables
        mock_detection = KsqlTableDetection(
            has_multiple_tables=True,
            table_statements=[
                "CREATE TABLE table1 (id INT)",
                "CREATE TABLE table2 (name STRING)"
            ],
            description="Found 2 tables"
        )

        # Mock agent methods with one empty result
        self.agent._table_detection_agent = MagicMock(return_value=mock_detection)
        self.agent._translator_agent = MagicMock(side_effect=[
            ("DDL1", "DML1"),
            ("", "")  # Empty result for second table
        ])
        self.agent._mandatory_validation_agent = MagicMock(side_effect=[
            ("VALIDATED_DDL1", "VALIDATED_DML1"),
            ("", "")
        ])

        ksql_input = "CREATE TABLE table1 (id INT); CREATE TABLE table2 (name STRING)"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls(
            "test_tables", ksql_input, validate=False
        )

        # Assertions - only non-empty results should be included
        self.assertEqual(len(result_ddl), 1)
        self.assertEqual(len(result_dml), 1)
        self.assertEqual(result_ddl, ["VALIDATED_DDL1"])
        self.assertEqual(result_dml, ["VALIDATED_DML1"])

    def test_pydantic_model_ksql_flink_sql_validation(self):
        """Test KsqlFlinkSql model validation."""
        # Valid model
        valid_model = KsqlFlinkSql(
            ksql_input="CREATE STREAM test AS SELECT * FROM source",
            flink_ddl_output="CREATE TABLE test (id INT)",
            flink_dml_output="INSERT INTO test SELECT * FROM source"
        )

        self.assertEqual(valid_model.ksql_input, "CREATE STREAM test AS SELECT * FROM source")
        self.assertEqual(valid_model.flink_ddl_output, "CREATE TABLE test (id INT)")
        self.assertEqual(valid_model.flink_dml_output, "INSERT INTO test SELECT * FROM source")

        # Test with missing required fields - should raise ValidationError
        with self.assertRaises(ValidationError):
            KsqlFlinkSql(flink_ddl_output="test", flink_dml_output="test")  # Missing required ksql_input field

    def test_pydantic_model_ksql_table_detection_validation(self):
        """Test KsqlTableDetection model validation."""
        # Valid model
        valid_model = KsqlTableDetection(
            has_multiple_tables=True,
            table_statements=["CREATE TABLE t1", "CREATE TABLE t2"],
            description="Multiple tables found"
        )

        self.assertTrue(valid_model.has_multiple_tables)
        self.assertEqual(len(valid_model.table_statements), 2)
        self.assertEqual(valid_model.description, "Multiple tables found")

        # Test with empty list (valid but different from multi-table scenario)
        empty_model = KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=[],
            description="No tables"
        )
        self.assertFalse(empty_model.has_multiple_tables)
        self.assertEqual(len(empty_model.table_statements), 0)

    def test_pydantic_model_flink_sql_validation(self):
        """Test FlinkSql model validation."""
        # Valid model
        valid_model = FlinkSql(
            ddl_sql_input="CREATE TABLE test (id INT)",
            dml_sql_input="INSERT INTO test VALUES (1)",
            flink_ddl_output="CREATE TABLE test (id INT) WITH ('connector' = 'kafka')",
            flink_dml_output="INSERT INTO test SELECT * FROM source"
        )

        self.assertEqual(valid_model.ddl_sql_input, "CREATE TABLE test (id INT)")
        self.assertEqual(valid_model.flink_ddl_output, "CREATE TABLE test (id INT) WITH ('connector' = 'kafka')")
        self.assertEqual(valid_model.dml_sql_input, "INSERT INTO test VALUES (1)")
        self.assertEqual(valid_model.flink_dml_output, "INSERT INTO test SELECT * FROM source")

    def test_pydantic_model_flink_sql_for_refinement_validation(self):
        """Test FlinkSqlForRefinement model validation."""
        # Valid model
        valid_model = FlinkSqlForRefinement(
            sql_input="SELECT * FROM invalid_table",
            error_message="Table does not exist",
            flink_output="SELECT * FROM valid_table"
        )

        self.assertEqual(valid_model.sql_input, "SELECT * FROM invalid_table")
        self.assertEqual(valid_model.error_message, "Table does not exist")
        self.assertEqual(valid_model.flink_output, "SELECT * FROM valid_table")

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_error_in_table_detection(self, mock_print, mock_input):
        """Test error handling when table detection fails."""
        # Mock table detection to raise an exception
        self.agent._table_detection_agent = MagicMock(side_effect=Exception("LLM connection error"))

        ksql_input = "CREATE TABLE test (id INT)"

        with self.assertRaises(Exception) as context:
            self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=False)

        self.assertIn("LLM connection error", str(context.exception))

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_error_in_translator_agent(self, mock_print, mock_input):
        """Test error handling when translator agent fails."""
        # Mock successful table detection but failing translator
        mock_detection = KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE TABLE test (id INT)"],
            description="Single table"
        )

        self.agent._table_detection_agent = MagicMock(return_value=mock_detection)
        self.agent._translator_agent = MagicMock(side_effect=Exception("Translation error"))

        ksql_input = "CREATE TABLE test (id INT)"

        with self.assertRaises(Exception) as context:
            self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=False)

        self.assertIn("Translation error", str(context.exception))

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_error_in_mandatory_validation(self, mock_print, mock_input):
        """Test error handling when mandatory validation fails."""
        # Mock successful table detection and translation but failing validation
        mock_detection = KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE TABLE test (id INT)"],
            description="Single table"
        )

        self.agent._table_detection_agent = MagicMock(return_value=mock_detection)
        self.agent._translator_agent = MagicMock(return_value=("DDL", "DML"))
        self.agent._mandatory_validation_agent = MagicMock(side_effect=Exception("Validation error"))

        ksql_input = "CREATE TABLE test (id INT)"

        with self.assertRaises(Exception) as context:
            self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=False)

        self.assertIn("Validation error", str(context.exception))

    @patch('shift_left.core.utils.ksql_code_agent.shift_left_dir', '/tmp/nonexistent')
    @patch('builtins.open', side_effect=PermissionError("Permission denied"))
    def test_snapshot_ddl_dml_permission_error(self, mock_open):
        """Test snapshot function with file permission error."""
        ddl = "CREATE TABLE test (id INT)"
        dml = "INSERT INTO test VALUES (1)"
        table_name = "test_table"

        with self.assertRaises(PermissionError):
            _snapshot_ddl_dml(table_name, ddl, dml)

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_empty_ksql_input(self, mock_print, mock_input):
        """Test translation with empty KSQL input."""
        # Mock table detection for empty input
        mock_detection = KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=[""],
            description="Empty input"
        )

        self.agent._table_detection_agent = MagicMock(return_value=mock_detection)
        self.agent._translator_agent = MagicMock(return_value=("", ""))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("", ""))

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", "", validate=False)

        self.assertEqual(result_ddl, [""])
        self.assertEqual(result_dml, [""])

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_with_whitespace_only_input(self, mock_print, mock_input):
        """Test translation with whitespace-only KSQL input."""
        whitespace_input = "   \n\t  \n  "

        # Mock table detection
        mock_detection = KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=[whitespace_input],
            description="Whitespace input"
        )

        self.agent._table_detection_agent = MagicMock(return_value=mock_detection)
        self.agent._translator_agent = MagicMock(return_value=("", ""))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("", ""))

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", whitespace_input, validate=False)

        self.assertEqual(result_ddl, [""])
        self.assertEqual(result_dml, [""])

    @patch('builtins.input')
    def test_successful_validation_first_try(self, mock_input):
        """Test successful validation on the first attempt."""
        # Mock successful validation
        self.agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        mock_input.return_value = "y"

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

        # Assertions
        self.assertEqual(result_sql, self.test_sql)
        self.assertTrue(is_validated)
        self.agent._validate_flink_sql_on_cc.assert_called_once_with(self.test_sql)


    @patch('builtins.input')
    def test_validation_fails_then_succeeds_after_refinement(self, mock_input):
        """Test validation fails first, succeeds after refinement."""
        # Mock validation: fails first, succeeds second
        self.agent._validate_flink_sql_on_cc = MagicMock(side_effect=[
            (False, self.error_message),  # First call fails
            (True, "")                    # Second call succeeds
        ])
        self.agent._refinement_agent = MagicMock(return_value=self.refined_sql)
        mock_input.return_value = "y"

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

        # Assertions
        self.assertEqual(result_sql, self.refined_sql)
        self.assertTrue(is_validated)
        self.assertEqual(self.agent._validate_flink_sql_on_cc.call_count, 2)
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT * FROM test_table'}]"
        self.agent._refinement_agent.assert_called_once_with(
            self.test_sql,
            expected_history,
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

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

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

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

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

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

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

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

        # Assertions
        self.assertEqual(result_sql, self.test_sql)
        self.assertTrue(is_validated)
        self.agent._validate_flink_sql_on_cc.assert_called_once_with(self.test_sql)

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

        result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)

        # Verify refinement agent was called with proper history
        calls = self.agent._refinement_agent.call_args_list

        # First call should have history with first refinement
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT * FROM test_table'}]"
        self.assertEqual(calls[0][0][1], expected_history)
        # second call should have history with second refinement
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT * FROM test_table'}, {'agent': 'refinement', 'sql': 'SELECT refined_1 FROM test_table'}]"
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

        result_sql, is_validated = self.agent._iterate_on_validation(empty_sql)

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
                result_sql, is_validated = self.agent._iterate_on_validation(self.test_sql)
                self.assertEqual(result_sql, self.test_sql)
                self.assertTrue(is_validated)


    # ===============================================================
    # Tests for translate_to_flink_sqls() function
    # ===============================================================

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_without_validation(self, mock_print, mock_input):
        """Test basic translation without validation (validate=False)."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        self.agent._table_detection_agent = MagicMock(return_value=KsqlTableDetection(has_multiple_tables=False, table_statements=[], description=""))

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=False)

        # Assertions
        self.assertEqual(result_dml, ["UPDATED_DML"])
        self.assertEqual(result_ddl, ["UPDATED_DDL"])

        # Verify method calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")

        # Verify no validation-related methods are called
        mock_input.assert_not_called()

    @patch('builtins.input')
    def test_translate_with_validation_user_declines(self, mock_input):
        """Test translation with validation=True but user chooses not to continue."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        self.agent._table_detection_agent = MagicMock(return_value=KsqlTableDetection(has_multiple_tables=False, table_statements=[], description=""))
        # User chooses not to continue validation
        mock_input.return_value = "n"

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

        # Assertions
        self.assertEqual(result_dml, ["UPDATED_DML"])
        self.assertEqual(result_ddl, ["UPDATED_DDL"])

        # Verify method calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")
        mock_input.assert_called_once()



    @patch('builtins.input')
    def test_translate_with_validation_full_success(self, mock_input):
        """Test translation with validation where both DDL and DML validate successfully."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        self.agent._process_semantic_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        self.agent._table_detection_agent = MagicMock(return_value=KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE STREAM test AS SELECT * FROM source"],
            description="Single table"
        ))
        # User chooses to continue validation
        mock_input.return_value = "y"

        # Mock successful validation for both DDL and DML
        self.agent._iterate_on_validation = MagicMock(side_effect=[
            ("VALIDATED_DDL", True),   # DDL validation succeeds
            ("VALIDATED_DML", True)    # DML validation succeeds
        ])
        # Create a mock manager to track call order
        mock_manager = MagicMock()
        mock_manager.attach_mock(self.agent._table_detection_agent, 'table_detection_agent')
        mock_manager.attach_mock(self.agent._translator_agent, 'translator_agent')
        mock_manager.attach_mock(self.agent._mandatory_validation_agent, 'mandatory_validation_agent')
        mock_manager.attach_mock(self.agent._iterate_on_validation, 'iterate_on_validation')
        mock_manager.attach_mock(self.agent._process_semantic_validation, 'process_semantic_validation')

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

        # Assertions
        self.assertEqual(result_ddl, ["SEMANTIC_VALIDATED_DDL"])  # DDL first
        self.assertEqual(result_dml, ["SEMANTIC_VALIDATED_DML"])  # DML second

        # Verify the order of method calls
        expected_calls = [
            call.table_detection_agent(ksql_input),
            call.translator_agent(ksql_input),
            call.mandatory_validation_agent("DDL_SQL", "DML_SQL"),
            call.iterate_on_validation("UPDATED_DDL"),
            call.process_semantic_validation("VALIDATED_DDL"),
            call.iterate_on_validation("UPDATED_DML"),
            call.process_semantic_validation("VALIDATED_DML")
        ]
        print(mock_manager.mock_calls)
        # Verify the exact order of calls
        self.assertEqual(mock_manager.mock_calls, expected_calls)

        # Verify user interaction
        mock_input.assert_called_once()

    @patch('builtins.input')
    def test_translate_with_validation_ddl_success_dml_fail(self, mock_input):
        """Test translation with validation where DDL succeeds but DML fails validation."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        self.agent._process_semantic_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        self.agent._table_detection_agent = MagicMock(return_value=KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE STREAM test AS SELECT * FROM source"],
            description="Single table"
        ))
        # User chooses to continue validation
        mock_input.return_value = "y"

        # Mock validation: DDL succeeds, DML fails
        self.agent._iterate_on_validation = MagicMock(side_effect=[
            ("VALIDATED_DDL", True),   # DDL validation succeeds
            ("FAILED_DML", False)      # DML validation fails
        ])

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

        # Assertions
        self.assertEqual(result_ddl, ["SEMANTIC_VALIDATED_DDL"])  # DDL was semantically processed
        self.assertEqual(result_dml, ["FAILED_DML"])  # DML not semantically processed due to failure

        # Verify method calls
        self.assertEqual(self.agent._iterate_on_validation.call_count, 2)
        # Verify semantic validation was called only for DDL (since DML failed)
        self.agent._process_semantic_validation.assert_called_once_with("VALIDATED_DDL")

        # Verify the order of calls
        self.agent._translator_agent.assert_called_once_with(ksql_input)
        self.agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")

    @patch('builtins.input')
    def test_translate_with_validation_ddl_fails(self, mock_input):
        """Test translation with validation where DDL validation fails."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        self.agent._process_semantic_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        self.agent._table_detection_agent = MagicMock(return_value=KsqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE STREAM test AS SELECT * FROM source"],
            description="Single table"
        ))
        # User chooses to continue validation
        mock_input.return_value = "y"

        # Mock DDL validation failure
        self.agent._iterate_on_validation = MagicMock(return_value=("FAILED_DDL", False))
        # Create a mock manager to track call order
        mock_manager = MagicMock()
        mock_manager.attach_mock(self.agent._table_detection_agent, 'table_detection_agent')
        mock_manager.attach_mock(self.agent._translator_agent, 'translator_agent')
        mock_manager.attach_mock(self.agent._mandatory_validation_agent, 'mandatory_validation_agent')
        mock_manager.attach_mock(self.agent._iterate_on_validation, 'iterate_on_validation')

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

        # Assertions
        self.assertEqual(result_ddl, ["FAILED_DDL"])   # Failed DDL returned
        self.assertEqual(result_dml, ["UPDATED_DML"])  # Original DML returned since DDL failed

        # Verify the order of method calls
        expected_calls = [
            call.table_detection_agent(ksql_input),
            call.translator_agent(ksql_input),
            call.mandatory_validation_agent("DDL_SQL", "DML_SQL"),
            call.iterate_on_validation("UPDATED_DDL")  # Only DDL validation attempted
        ]


        # Verify the exact order of calls
        self.assertEqual(mock_manager.mock_calls, expected_calls)

        # Verify no semantic validation occurred since DDL failed
        self.agent._process_semantic_validation.assert_not_called()

        # Verify user interaction
        mock_input.assert_called_once()



    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_edge_cases(self, mock_print, mock_input):
        """Test edge cases for translate_to_flink_sqls."""
        # Mock the agent methods
        self.agent._translator_agent = MagicMock(return_value=("", ""))
        self.agent._mandatory_validation_agent = MagicMock(return_value=("", ""))
        self.agent._table_detection_agent = MagicMock(return_value=KsqlTableDetection(has_multiple_tables=False, table_statements=[], description=""))

        # Test with empty KSQL input
        result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", "", validate=False)
        self.assertEqual(result_dml, [''])
        self.assertEqual(result_ddl, [''])

        # Test with various user inputs for validation prompt
        for user_input in ["n", "N", "no", "quit", "", "anything_not_y"]:
            with self.subTest(user_input=user_input):
                mock_input.return_value = user_input
                result_ddl, result_dml = self.agent.translate_to_flink_sqls("test_table", "TEST", validate=True)
                self.assertEqual(result_dml, [''])
                self.assertEqual(result_ddl, [''])

    @patch('builtins.input')
    def test_translate_method_call_order(self, mock_input):
        """Test that methods are called in the correct order during translation."""
        # Mock the agent methods with side effects to track call order
        call_order = []

        def translator_side_effect(ksql):
            call_order.append("translator_agent")
            return ("DDL_SQL", "DML_SQL")

        def mandatory_validation_side_effect(ddl, dml):
            call_order.append("mandatory_validation_agent")
            return ("UPDATED_DDL", "UPDATED_DML")

        def semantic_validation_side_effect(sql):
            call_order.append(f"semantic_validation_{sql}")
            return f"SEMANTIC_{sql}"

        def table_detection_side_effect(ksql):
            call_order.append("table_detection_agent")
            return KsqlTableDetection(has_multiple_tables=False, table_statements=["TEST"], description="Single table")

        self.agent._translator_agent = MagicMock(side_effect=translator_side_effect)
        self.agent._mandatory_validation_agent = MagicMock(side_effect=mandatory_validation_side_effect)
        self.agent._process_semantic_validation = MagicMock(side_effect=semantic_validation_side_effect)
        self.agent._table_detection_agent = MagicMock(side_effect=table_detection_side_effect)
        # User continues with validation
        mock_input.return_value = "y"
        self.agent._iterate_on_validation = MagicMock(side_effect=[
            ("VALIDATED_DDL", True),
            ("VALIDATED_DML", True)
        ])

        self.agent.translate_to_flink_sqls("test_table", "TEST", validate=True)

        # Verify correct call order
        expected_order = [
            "table_detection_agent",
            "translator_agent",
            "mandatory_validation_agent",
            "semantic_validation_VALIDATED_DDL",
            "semantic_validation_VALIDATED_DML"
        ]
        self.assertEqual(call_order, expected_order)


if __name__ == '__main__':
    unittest.main()
