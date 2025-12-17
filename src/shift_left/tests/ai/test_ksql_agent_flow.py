
import unittest
from unittest.mock import patch, mock_open, MagicMock, call
import pathlib
import os, sys
import shutil

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
from shift_left.ai.agent_factory import AgentFactory
from shift_left.ai.spark_sql_code_agent import SparkToFlinkSqlAgent
from shift_left.ai.ksql_code_agent import KsqlToFlinkSqlAgent, SqlTableDetection
from shift_left.ai.process_src_tables import migrate_one_file
from ai.utilities import compare_files_unordered
data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory

MULTIPLE_TABLE_KSQL = """
-- Drop old table
DROP TABLE IF EXISTS old_table;
CREATE STREAM all_publications (bookid BIGINT KEY,
                                author VARCHAR,
                                title VARCHAR)
WITH (KAFKA_TOPIC='publication_events'
            PARTITIONS=1,
            VALUE_FORMAT='JSON');
CREATE STREAM george_martin WITH (KAFKA_TOPIC='george_martin_books') AS
SELECT *
    FROM all_publications
    WHERE author = 'George R. R. Martin';
"""

SIMPLE_STREAM_KSQL = """
CREATE STREAM acting_events_drama AS
    SELECT name, title
    FROM acting_events
    WHERE genre='drama';
"""

MATCHING_FSQL_DDL = """
CREATE TABLE IF NOT EXISTS acting_events_drama (
    name STRING,
    title STRING,
    PRIMARY KEY (name) NOT ENFORCED
) DISTRIBUTED BY HASH(name) INTO 1 BUCKETS WITH (
    'changelog.mode' = 'append',
    'value.format' = 'json-registry',
    'key.avro-registry.schema-context' = '.flink-dev',
    'value.avro-registry.schema-context' = '.flink-dev',
    'scan.bounded.mode' = 'unbounded',
    'value.fields-include' = 'all',
    'scan.startup.mode' = 'earliest-offset'
);
"""

MATCHING_FSQL_DML = """
INSERT INTO acting_events_drama
SELECT name, title
FROM acting_events
WHERE genre='drama';
"""

class TestAgentFlow(unittest.TestCase):
    """
    Test the agent flow for ksql migration. UT all functions in the agent flow by isolation.
    """
    @classmethod
    def setUpClass(cls):
        cls.src_folder = str(data_dir / "ksql-project/sources")
        cls.staging = str(data_dir / "flink-project/staging/ut")
        cls.file_reference_dir = str(data_dir / "ksql-project/flink-references")
        cls.product_name = "basic"

    def test_agent_factory_success(self):
        agent = AgentFactory().get_or_build_sql_translator_agent("spark")
        self.assertIsInstance(agent, SparkToFlinkSqlAgent)
        agent = AgentFactory().get_or_build_sql_translator_agent("ksql")
        self.assertIsInstance(agent, KsqlToFlinkSqlAgent)

    def test_agent_factory_failure(self):
        with self.assertRaises(ValueError):
            AgentFactory().get_or_build_sql_translator_agent("invalid")


    def test_load_prompts(self):
        """
        Test the load_prompts function.
        """
        agent = KsqlToFlinkSqlAgent()
        agent._load_prompts()
        assert agent.translator_system_prompt
        assert agent.refinement_system_prompt
        assert agent.mandatory_validation_system_prompt
        assert agent.table_detection_system_prompt


    def test_clean_sql_input(self):
        """
        Test the _clean_sql_input function to ensure it properly removes
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
        result = agent._clean_sql_input(ksql_input)
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
        result = agent._clean_sql_input(ksql_input)
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
        result = agent._clean_sql_input(ksql_input)
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
        result = agent._clean_sql_input(ksql_input)
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
        result = agent._clean_sql_input(ksql_input)
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
        result = agent._clean_sql_input(ksql_input)
        self.assertEqual(result, expected_output)





    @patch('shift_left.ai.translator_to_flink_sql.shift_left_dir', '/tmp/test')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.join')
    def test_snapshot_ddl_dml_success(self, mock_join, mock_file_open):
        """Test successful snapshot of DDL and DML to files."""
        # Setup mocks
        mock_join.side_effect = lambda *args: '/'.join(args)

        ddl = "CREATE TABLE test (id INT)"
        dml = "INSERT INTO test VALUES (1)"
        table_name = "test_table"
        agent = KsqlToFlinkSqlAgent()
        agent._snapshot_ddl_dml(table_name, ddl, dml)

        # Verify file operations
        self.assertEqual(mock_file_open.call_count, 2)
        mock_file_open.assert_any_call('/tmp/test/ddl.test_table.sql', 'w')
        mock_file_open.assert_any_call('/tmp/test/dml.test_table.sql', 'w')

        # Verify file writes
        handle = mock_file_open.return_value
        handle.write.assert_any_call(ddl)
        handle.write.assert_any_call(dml)


    def test_table_detection_agent_multiple_tables(self):
        """
        Test the table detection agent for multiple tables.
        """
        agent = KsqlToFlinkSqlAgent()
        ksql = MULTIPLE_TABLE_KSQL
        table_detection = agent._detect_multitable_with_agent(ksql)
        assert isinstance(table_detection, SqlTableDetection)
        assert table_detection.has_multiple_tables == True
        assert len(table_detection.table_statements) == 2

    def test_table_detection_agent_single_table(self):
        """
        Test the table detection agent for single table.
        """
        agent = KsqlToFlinkSqlAgent()
        ksql = SIMPLE_STREAM_KSQL
        table_detection = agent._detect_multitable_with_agent(ksql)
        assert isinstance(table_detection, SqlTableDetection)
        assert table_detection.has_multiple_tables == False
        assert len(table_detection.table_statements) == 1

    def test_simple_ksql_translator_agent(self):
        """
        Test the translator agent.
        """
        agent = KsqlToFlinkSqlAgent()
        ksql = SIMPLE_STREAM_KSQL
        ddl_sql, dml_sql = agent._do_translation_with_agent(ksql)
        print(ddl_sql)
        print(dml_sql)
        assert "json-registry" in ddl_sql
        assert "earliest-offset" in ddl_sql
        assert "name STRING" in ddl_sql
        assert "INSERT INTO acting_events_drama" in dml_sql
        assert "SELECT name, title" in dml_sql
        assert "FROM acting_events" in dml_sql
        assert "WHERE genre" in dml_sql

    def test_simple_ksql_mandatory_validation_agent(self):
        """
        Test the mandatory validation agent.
        """
        agent = KsqlToFlinkSqlAgent()
        ddl_sql = MATCHING_FSQL_DDL
        ddl_sql = ddl_sql.replace("IF NOT EXISTS", "")
        dml_sql = MATCHING_FSQL_DML
        ddl_sql_output, dml_sql_output = agent._mandatory_validation_agent(ddl_sql, dml_sql)
        print(ddl_sql_output)
        print(dml_sql_output)
        for line in ["'changelog.mode' = 'append'",
                     "'value.format' = 'avro-registry'",
                     "'key.avro-registry.schema-context' = '.flink-dev'",
                     "'value.avro-registry.schema-context' = '.flink-dev'",
                     "'scan.bounded.mode' = 'unbounded'",
                     "'value.fields-include' = 'all'",
                     "'scan.startup.mode' = 'earliest-offset'"]:
            assert line in ddl_sql_output
        assert MATCHING_FSQL_DML[1:-1] == dml_sql_output # remove the first and last \n

    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_empty_ksql_input(self, mock_print, mock_input):
        """Test translation with empty KSQL input."""
        # Mock table detection for empty input
        mock_detection = SqlTableDetection(
            has_multiple_tables=False,
            table_statements=[""],
            description="Empty input"
        )
        agent = KsqlToFlinkSqlAgent()
        agent._detect_multitable_with_agent = MagicMock(return_value=mock_detection)
        agent._do_translation_with_agent = MagicMock(return_value=("", ""))
        agent._mandatory_validation_agent = MagicMock(return_value=("", ""))

        result_ddl, result_dml = agent.translate_to_flink_sqls("test_table", "", validate=False)

        self.assertEqual(result_ddl, [""])
        self.assertEqual(result_dml, [""])


    @patch('builtins.input')
    @patch('builtins.print')
    def test_translate_with_whitespace_only_input(self, mock_print, mock_input):
        """Test translation with whitespace-only KSQL input."""
        whitespace_input = "   \n\t  \n  "

        # Mock table detection
        mock_detection = SqlTableDetection(
            has_multiple_tables=False,
            table_statements=[whitespace_input],
            description="Whitespace input"
        )
        agent = KsqlToFlinkSqlAgent()
        agent._detect_multitable_with_agent = MagicMock(return_value=mock_detection)
        agent._do_translation_with_agent = MagicMock(return_value=("", ""))
        agent._mandatory_validation_agent = MagicMock(return_value=("", ""))

        result_ddl, result_dml = agent.translate_to_flink_sqls("test_table", whitespace_input, validate=False)
        self.assertEqual(result_ddl, [""])
        self.assertEqual(result_dml, [""])

    @patch('builtins.input')
    def test_successful_validation_first_try(self, mock_input):
        """Test successful validation on the first attempt."""
        # Mock successful validation
        agent = KsqlToFlinkSqlAgent()
        test_sql = "SELECT * FROM test_table"
        agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        mock_input.return_value = "y"

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Assertions
        self.assertEqual(result_sql, test_sql)
        self.assertTrue(is_validated)
        agent._validate_flink_sql_on_cc.assert_called_once_with(test_sql)

    @patch('builtins.input')
    def test_validation_fails_then_succeeds_after_refinement(self, mock_input):
        """Test validation fails first, succeeds after refinement."""
        # Mock validation: fails first, succeeds second
        agent = KsqlToFlinkSqlAgent()
        test_sql = "SELECT * FROM test_table"
        error_message = "Column 'invalid_column' does not exist"
        refined_sql = "SELECT id, name FROM test_table"
        agent._validate_flink_sql_on_cc = MagicMock(side_effect=[
            (False, error_message),  # First call fails
            (True, "")                    # Second call succeeds
        ])
        agent._refinement_agent = MagicMock(return_value=refined_sql)
        mock_input.return_value = "y"

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Assertions
        self.assertEqual(result_sql, refined_sql)
        self.assertTrue(is_validated)
        self.assertEqual(agent._validate_flink_sql_on_cc.call_count, 2)
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT * FROM test_table'}]"
        agent._refinement_agent.assert_called_once_with(
            test_sql,
            expected_history,
            error_message
        )

    @patch('builtins.input')
    @patch('builtins.print')
    def test_validation_fails_max_iterations(self, mock_print, mock_input):
        """Test validation fails for maximum iterations (3 attempts)."""
        # Mock validation to always fail
        agent = KsqlToFlinkSqlAgent()
        test_sql = "SELECT * FROM test_table"
        error_message = "Column 'invalid_column' does not exist"
        refined_sql = "SELECT id, name FROM test_table"
        agent._validate_flink_sql_on_cc = MagicMock(return_value=(False, error_message))
        # Mock refinement to return different SQL each time
        agent._refinement_agent = MagicMock(side_effect=[
            "SELECT refined_1 FROM test_table",
            "SELECT refined_2 FROM test_table",
            "SELECT refined_3 FROM test_table"
        ])
        mock_input.return_value = "y"

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Assertions
        self.assertFalse(is_validated)
        self.assertEqual(agent._validate_flink_sql_on_cc.call_count, 3)
        self.assertEqual(agent._refinement_agent.call_count, 3)
        # Should return the last refined SQL
        self.assertEqual(result_sql, "SELECT refined_3 FROM test_table")

    @patch('builtins.input')
    @patch('builtins.print')
    def test_user_stops_early_after_first_failure(self, mock_print, mock_input):
        """Test user chooses to stop after first validation failure."""
        # Mock validation to fail
        agent = KsqlToFlinkSqlAgent()
        test_sql = "SELECT * FROM test_table"
        error_message = "Column 'invalid_column' does not exist"
        refined_sql = "SELECT id, name FROM test_table"
        agent._validate_flink_sql_on_cc = MagicMock(return_value=(False, error_message))
        agent._refinement_agent = MagicMock(return_value=refined_sql)
        mock_input.return_value = "n"  # User chooses to stop

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Assertions
        self.assertEqual(result_sql, refined_sql)
        self.assertFalse(is_validated)
        agent._validate_flink_sql_on_cc.assert_called_once_with(test_sql)
        agent._refinement_agent.assert_called_once()
        mock_input.assert_called_once()

    @patch('builtins.input')
    @patch('builtins.print')
    def test_user_stops_early_after_second_failure(self, mock_print, mock_input):
        """Test user chooses to stop after second validation failure."""
        # Mock validation to always fail
        agent = KsqlToFlinkSqlAgent()
        test_sql = "SELECT * FROM test_table"
        error_message = "Column 'invalid_column' does not exist"
        refined_sql = "SELECT id, name FROM test_table"
        agent._validate_flink_sql_on_cc = MagicMock(return_value=(False, error_message))
        agent._refinement_agent = MagicMock(side_effect=[
            "SELECT refined_1 FROM test_table",
            "SELECT refined_2 FROM test_table"
        ])
        # User continues first time, stops second time
        mock_input.side_effect = ["y", "n"]

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Assertions
        self.assertEqual(result_sql, "SELECT refined_2 FROM test_table")
        self.assertFalse(is_validated)
        self.assertEqual(agent._validate_flink_sql_on_cc.call_count, 2)
        self.assertEqual(agent._refinement_agent.call_count, 2)
        self.assertEqual(mock_input.call_count, 2)

    @patch('builtins.input')
    @patch('builtins.print')
    def test_user_stops_immediately_on_success(self, mock_print, mock_input):
        """Test user chooses to stop even when validation succeeds."""
        # Mock successful validation
        agent = KsqlToFlinkSqlAgent()
        test_sql = "SELECT * FROM test_table"
        agent._validate_flink_sql_on_cc = MagicMock(return_value=(True, ""))
        mock_input.return_value = "n"  # User chooses to stop

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Assertions
        self.assertEqual(result_sql, test_sql)
        self.assertTrue(is_validated)
        agent._validate_flink_sql_on_cc.assert_called_once_with(test_sql)


    @patch('builtins.input')
    @patch('builtins.print')
    def test_agent_history_tracking(self, mock_print, mock_input):
        """Test that agent history is properly tracked through iterations."""
        # Mock validation to fail twice, succeed third time
        agent = KsqlToFlinkSqlAgent()
        agent._validate_flink_sql_on_cc = MagicMock(side_effect=[
            (False, "error1"),
            (False, "error2"),
            (True, "")
        ])
        agent._refinement_agent = MagicMock(side_effect=[
            "SELECT refined_1 FROM test_table",
            "SELECT refined_2 FROM test_table"
        ])
        mock_input.return_value = "y"
        test_sql = "SELECT * FROM test_table"

        result_sql, is_validated = agent._iterate_on_validation(test_sql)

        # Verify refinement agent was called with proper history
        calls = agent._refinement_agent.call_args_list

        # First call should have history with first refinement
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT * FROM test_table'}]"
        self.assertEqual(calls[0][0][1], expected_history)
        # second call should have history with second refinement
        expected_history = "[{'agent': 'refinement', 'sql': 'SELECT * FROM test_table'}, {'agent': 'refinement', 'sql': 'SELECT refined_1 FROM test_table'}]"
        self.assertEqual(calls[1][0][1], expected_history)

        self.assertTrue(is_validated)
        self.assertEqual(result_sql, "SELECT refined_2 FROM test_table")



    @patch('builtins.input')
    def test_translate_with_validation_full_success(self, mock_input):
        """Test translation with validation where both DDL and DML validate successfully."""
        # Mock the agent methods
        agent = KsqlToFlinkSqlAgent()
        agent._do_translation_with_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        agent._process_syntax_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        agent._detect_multitable_with_agent = MagicMock(return_value=SqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE STREAM test AS SELECT * FROM source"],
            description="Single table"
        ))
        # User chooses to continue validation
        mock_input.return_value = "y"

        # Mock successful validation for both DDL and DML
        agent._iterate_on_validation = MagicMock(side_effect=[
            ("VALIDATED_DDL", True),   # DDL validation succeeds
            ("VALIDATED_DML", True)    # DML validation succeeds
        ])
        # Create a mock manager to track call order
        mock_manager = MagicMock()
        mock_manager.attach_mock(agent._detect_multitable_with_agent, 'table_detection_agent')
        mock_manager.attach_mock(agent._do_translation_with_agent, 'translator_agent')
        mock_manager.attach_mock(agent._mandatory_validation_agent, 'mandatory_validation_agent')
        mock_manager.attach_mock(agent._iterate_on_validation, 'iterate_on_validation')
        mock_manager.attach_mock(agent._process_syntax_validation, 'process_semantic_validation')

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

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
        agent = KsqlToFlinkSqlAgent()
        agent._do_translation_with_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        agent._process_syntax_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        agent._detect_multitable_with_agent = MagicMock(return_value=SqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE STREAM test AS SELECT * FROM source"],
            description="Single table"
        ))
        # User chooses to continue validation
        mock_input.return_value = "y"
        # Mock validation: DDL succeeds, DML fails
        agent._iterate_on_validation = MagicMock(side_effect=[
            ("VALIDATED_DDL", True),   # DDL validation succeeds
            ("FAILED_DML", False)      # DML validation fails
        ])

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

        # Assertions
        self.assertEqual(result_ddl, ["SEMANTIC_VALIDATED_DDL"])  # DDL was semantically processed
        self.assertEqual(result_dml, ["FAILED_DML"])  # DML not semantically processed due to failure

        # Verify method calls
        self.assertEqual(agent._iterate_on_validation.call_count, 2)
        # Verify semantic validation was called only for DDL (since DML failed)
        agent._process_syntax_validation.assert_called_once_with("VALIDATED_DDL")

        # Verify the order of calls
        agent._do_translation_with_agent.assert_called_once_with(ksql_input)
        agent._mandatory_validation_agent.assert_called_once_with("DDL_SQL", "DML_SQL")

    @patch('builtins.input')
    def test_translate_with_validation_ddl_fails(self, mock_input):
        """Test translation with validation where DDL validation fails."""
        # Mock the agent methods
        agent = KsqlToFlinkSqlAgent()
        agent._do_translation_with_agent = MagicMock(return_value=("DDL_SQL", "DML_SQL"))
        agent._mandatory_validation_agent = MagicMock(return_value=("UPDATED_DDL", "UPDATED_DML"))
        agent._process_syntax_validation = MagicMock(side_effect=lambda x: f"SEMANTIC_{x}")
        agent._detect_multitable_with_agent = MagicMock(return_value=SqlTableDetection(
            has_multiple_tables=False,
            table_statements=["CREATE STREAM test AS SELECT * FROM source"],
            description="Single table"
        ))
        # User chooses to continue validation
        mock_input.return_value = "y"

        # Mock DDL validation failure
        agent._iterate_on_validation = MagicMock(return_value=("FAILED_DDL", False))
        # Create a mock manager to track call order
        mock_manager = MagicMock()
        mock_manager.attach_mock(agent._detect_multitable_with_agent, 'table_detection_agent')
        mock_manager.attach_mock(agent._do_translation_with_agent, 'translator_agent')
        mock_manager.attach_mock(agent._mandatory_validation_agent, 'mandatory_validation_agent')
        mock_manager.attach_mock(agent._iterate_on_validation, 'iterate_on_validation')

        ksql_input = "CREATE STREAM test AS SELECT * FROM source"

        result_ddl, result_dml = agent.translate_to_flink_sqls("test_table", ksql_input, validate=True)

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
        agent._process_syntax_validation.assert_not_called()

        # Verify user interaction
        mock_input.assert_called_once()



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
            return SqlTableDetection(has_multiple_tables=False, table_statements=["TEST"], description="Single table")

        agent = KsqlToFlinkSqlAgent()
        agent._do_translation_with_agent = MagicMock(side_effect=translator_side_effect)
        agent._mandatory_validation_agent = MagicMock(side_effect=mandatory_validation_side_effect)
        agent._process_syntax_validation = MagicMock(side_effect=semantic_validation_side_effect)
        agent._detect_multitable_with_agent = MagicMock(side_effect=table_detection_side_effect)
        # User continues with validation
        mock_input.return_value = "y"
        agent._iterate_on_validation = MagicMock(side_effect=[
            ("VALIDATED_DDL", True),
            ("VALIDATED_DML", True)
        ])

        agent.translate_to_flink_sqls("test_table", "TEST", validate=True)

        # Verify correct call order
        expected_order = [
            "table_detection_agent",
            "translator_agent",
            "mandatory_validation_agent",
            "semantic_validation_VALIDATED_DDL",
            "semantic_validation_VALIDATED_DML"
        ]
        self.assertEqual(call_order, expected_order)


    @patch('builtins.input')
    def test_flow_for_ksql_basic_table_no_validation(self, mock_input):
        """
        Test a basic table ksql create table migration.
        The table BASIC_TABLE_STREAM will be used to other tables.
        """
        mock_input.return_value = "y"
        migrate_one_file(table_name="BASIC_TABLE_STREAM",
                        sql_src_file=self.src_folder + "/ddl-basic-table.ksql",
                        staging_target_folder=self.staging,
                        product_name=self.product_name,
                        source_type="ksql",
                        validate=False)
        assert os.path.exists(self.staging + "/"+ self.product_name + "/basic_table_stream")
        assert os.path.exists(self.staging + "/"+ self.product_name + "/basic_table_stream/sql-scripts/ddl.basic_table_stream.sql")
        assert os.path.exists(self.staging + "/"+ self.product_name + "/basic_table_stream/sql-scripts/dml.basic_table_stream.sql")
        reference_file = self.file_reference_dir + "/basic_table_stream/sql-scripts/ddl.basic_table_stream.sql"
        created_file = self.staging + "/" + self.product_name + "/basic_table_stream/sql-scripts/ddl.basic_table_stream.sql"
        result = compare_files_unordered(reference_file, created_file, allow_extra_lines=True)
        print(f"\n\nresult: {result}")
        assert result['all_reference_lines_present']
        assert result['match_percentage'] == 100
        shutil.rmtree(self.staging, ignore_errors=True)
        os.makedirs(self.staging, exist_ok=True)

if __name__ == "__main__":
    unittest.main()
