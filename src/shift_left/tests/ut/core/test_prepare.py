"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for the prepare_tables_from_sql_file function in deployment_mgr.py
"""
import unittest
import tempfile
import os
from unittest.mock import patch, MagicMock, mock_open, call
from datetime import datetime
import pathlib

# Set environment variables before importing
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.deployment_mgr import prepare_tables_from_sql_file
from shift_left.core.models.flink_statement_model import Statement, Status, Spec, Metadata
from ut.core.BaseUT import BaseUT

COMPUTE_POOL_ID = "lfcp-121"
class TestPrepareTablesFromSqlFile(BaseUT):
    """Test suite for the prepare_tables_from_sql_file function."""

    def setUp(self):
        """Set up test environment before each test."""
        super().setUp()
        self.sample_sql_content = "CREATE TABLE test_table (id INT);\nALTER TABLE test_table ADD COLUMN name STRING;\n-- This is a comment\nDROP TABLE old_table;"

    def _create_mock_statement_with_status(self, status_phase: str) -> Statement:
        """Create a mock Statement object with specific status."""
        status = Status(phase=status_phase, detail="test-detail")
        spec = Spec(
            compute_pool_id=COMPUTE_POOL_ID,
            principal="test-principal",
            statement="test-sql",
            properties={"sql.current-catalog": "default", "sql.current-database": "default"},
            stopped=False
        )
        metadata = Metadata(
            created_at=datetime.now().isoformat(),
            resource_version="1",
            self="https://test-url",
            uid="test-uid"
        )
        return Statement(
            name="test-statement",
            status=status,
            environment_id="test-env-123",
            spec=spec,
            metadata=metadata
        )

    @patch('shift_left.core.deployment_mgr.logger')
    @patch('shift_left.core.deployment_mgr.time.sleep')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_or_build_sql_content_transformer')
    @patch('shift_left.core.deployment_mgr.get_config')
    @patch('shift_left.core.deployment_mgr.datetime')
    @patch('builtins.open', new_callable=mock_open)
    @patch('builtins.print')
    def test_prepare_tables_basic_functionality(
        self,
        mock_print,
        mock_file_open,
        mock_datetime,
        mock_get_config,
        mock_get_transformer,
        mock_post_statement,
        mock_get_statement,
        mock_delete_statement,
        mock_sleep,
        mock_logger
    ):
        """Test basic functionality with valid SQL content."""
        # Setup mocks
        mock_config = {'flink': {'compute_pool_id': COMPUTE_POOL_ID}}
        mock_get_config.return_value = mock_config

        mock_datetime.now.return_value.strftime.return_value = "20240420101502"

        mock_transformer = MagicMock()
        mock_transformer.update_sql_content.side_effect = [
            ("", "CREATE TABLE test_table (id INT);"),
            ("", "ALTER TABLE test_table ADD COLUMN name STRING;"),
            ("", "DROP TABLE old_table;")
        ]
        mock_get_transformer.return_value = mock_transformer

        # Mock file content
        sql_lines = [
            "CREATE TABLE test_table (id INT);\n",
            "ALTER TABLE test_table ADD COLUMN name STRING;\n",
            "-- This is a comment\n",
            "DROP TABLE old_table;\n"
        ]
        mock_file_open.return_value.readlines = MagicMock(return_value=sql_lines)
        mock_file_open.return_value.__iter__ = MagicMock(return_value=iter(sql_lines))

        # Mock statements that complete immediately
        completed_statement = self._create_mock_statement_with_status("COMPLETED")
        mock_post_statement.return_value = completed_statement

        # Execute the function
        prepare_tables_from_sql_file("test.sql", COMPUTE_POOL_ID)

        # Verify file was opened
        mock_file_open.assert_called_once_with("test.sql", "r")

        # Verify transformer was called for non-comment lines only (3 times)
        self.assertEqual(mock_transformer.update_sql_content.call_count, 3)

        # Verify statements were posted (3 times, skipping comment)
        self.assertEqual(mock_post_statement.call_count, 3)

        # Verify expected statement names
        expected_calls = [
            call(COMPUTE_POOL_ID, "prepare-table-20240420101502-0", "CREATE TABLE test_table (id INT);"),
            call(COMPUTE_POOL_ID, "prepare-table-20240420101502-1", "ALTER TABLE test_table ADD COLUMN name STRING;"),
            call(COMPUTE_POOL_ID, "prepare-table-20240420101502-2", "DROP TABLE old_table;")
        ]
        mock_post_statement.assert_has_calls(expected_calls)

        # Verify statements were deleted
        self.assertEqual(mock_delete_statement.call_count, 3)
        delete_calls = [
            call("prepare-table-20240420101502-0"),
            call("prepare-table-20240420101502-1"),
            call("prepare-table-20240420101502-2")
        ]
        mock_delete_statement.assert_has_calls(delete_calls)

        # Verify print was called for transformed SQL
        self.assertEqual(mock_print.call_count, 3)


    @patch('shift_left.core.deployment_mgr.logger')
    @patch('shift_left.core.deployment_mgr.time.sleep')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_or_build_sql_content_transformer')
    @patch('shift_left.core.deployment_mgr.get_config')
    @patch('shift_left.core.deployment_mgr.datetime')
    @patch('builtins.open', new_callable=mock_open)
    def test_prepare_tables_statement_waits_for_completion(
        self,
        mock_file_open,
        mock_datetime,
        mock_get_config,
        mock_get_transformer,
        mock_post_statement,
        mock_get_statement,
        mock_delete_statement,
        mock_sleep,
        mock_logger
    ):
        """Test that function waits for statement completion."""
        # Setup mocks
        mock_config = {'flink': {'compute_pool_id': COMPUTE_POOL_ID}}
        mock_get_config.return_value = mock_config

        mock_datetime.now.return_value.strftime.return_value = "20240420101502"

        mock_transformer = MagicMock()
        mock_transformer.update_sql_content.return_value = ("", "CREATE TABLE test (id INT);")
        mock_get_transformer.return_value = mock_transformer

        sql_lines = ["CREATE TABLE test (id INT);\n"]
        mock_file_open.return_value.__iter__ = MagicMock(return_value=iter(sql_lines))

        # Mock statement that starts running, then completes
        pending_statement = self._create_mock_statement_with_status("PENDING")
        completed_statement = self._create_mock_statement_with_status("COMPLETED")

        mock_post_statement.return_value = pending_statement
        mock_get_statement.side_effect = [pending_statement, completed_statement]

        # Execute the function
        prepare_tables_from_sql_file("test.sql", COMPUTE_POOL_ID)

        # Verify sleep was called (waiting for completion)
        mock_sleep.assert_called_with(2)

        # Verify get_statement was called to check status
        expected_get_calls = [
            call("prepare-table-20240420101502-0"),
            call("prepare-table-20240420101502-0")
        ]
        mock_get_statement.assert_has_calls(expected_get_calls)

        # Verify logger was called
        mock_logger.info.assert_called()

    @patch('shift_left.core.deployment_mgr.logger')
    @patch('shift_left.core.deployment_mgr.time.sleep')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_or_build_sql_content_transformer')
    @patch('shift_left.core.deployment_mgr.get_config')
    @patch('shift_left.core.deployment_mgr.datetime')
    @patch('builtins.open', new_callable=mock_open)
    def test_prepare_tables_statement_fails(
        self,
        mock_file_open,
        mock_datetime,
        mock_get_config,
        mock_get_transformer,
        mock_post_statement,
        mock_get_statement,
        mock_delete_statement,
        mock_sleep,
        mock_logger
    ):
        """Test behavior when statement fails."""
        # Setup mocks
        mock_config = {'flink': {'compute_pool_id': COMPUTE_POOL_ID}}
        mock_get_config.return_value = mock_config

        mock_datetime.now.return_value.strftime.return_value = "20240420101502"

        mock_transformer = MagicMock()
        mock_transformer.update_sql_content.return_value = ("", "INVALID SQL STATEMENT;")
        mock_get_transformer.return_value = mock_transformer

        sql_lines = ["INVALID SQL STATEMENT;\n"]
        mock_file_open.return_value.__iter__ = MagicMock(return_value=iter(sql_lines))

        # Mock statement that fails
        failed_statement = self._create_mock_statement_with_status("FAILED")
        mock_post_statement.return_value = failed_statement

        # Execute the function
        prepare_tables_from_sql_file("test.sql", COMPUTE_POOL_ID)

        # Verify statement was still deleted even after failure
        mock_delete_statement.assert_called_once_with("prepare-table-20240420101502-0")

        # Should not call sleep since statement immediately failed
        mock_sleep.assert_not_called()


    @patch('shift_left.core.deployment_mgr.statement_mgr.get_or_build_sql_content_transformer')
    @patch('shift_left.core.deployment_mgr.get_config')
    @patch('shift_left.core.deployment_mgr.datetime')
    @patch('builtins.open', new_callable=mock_open)
    def test_prepare_tables_empty_file(
        self,
        mock_file_open,
        mock_datetime,
        mock_get_config,
        mock_get_transformer
    ):
        """Test with empty file."""
        # Setup mocks
        mock_config = {'flink': {'compute_pool_id': COMPUTE_POOL_ID}}
        mock_get_config.return_value = mock_config

        mock_datetime.now.return_value.strftime.return_value = "20240420101502"

        mock_transformer = MagicMock()
        mock_get_transformer.return_value = mock_transformer

        # Empty file
        sql_lines = []
        mock_file_open.return_value.__iter__ = MagicMock(return_value=iter(sql_lines))

        # Execute the function
        prepare_tables_from_sql_file("test.sql", COMPUTE_POOL_ID)

        # Verify transformer was never called for empty file
        mock_transformer.update_sql_content.assert_not_called()

    def test_prepare_tables_file_not_found(self):
        """Test behavior when file doesn't exist."""
        with self.assertRaises(FileNotFoundError):
            prepare_tables_from_sql_file("non_existent_file.sql", COMPUTE_POOL_ID)

    @patch('shift_left.core.deployment_mgr.logger')
    @patch('shift_left.core.deployment_mgr.time.sleep')
    @patch('shift_left.core.deployment_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_or_build_sql_content_transformer')
    @patch('shift_left.core.deployment_mgr.get_config')
    @patch('shift_left.core.deployment_mgr.datetime')
    @patch('builtins.open', new_callable=mock_open)
    def test_prepare_tables_mixed_content(
        self,
        mock_file_open,
        mock_datetime,
        mock_get_config,
        mock_get_transformer,
        mock_post_statement,
        mock_get_statement,
        mock_delete_statement,
        mock_sleep,
        mock_logger
    ):
        """Test file with mixed SQL statements and comments."""
        # Setup mocks
        mock_config = {'flink': {'compute_pool_id': COMPUTE_POOL_ID}}
        mock_get_config.return_value = mock_config

        mock_datetime.now.return_value.strftime.return_value = "20240420101502"

        mock_transformer = MagicMock()
        mock_transformer.update_sql_content.side_effect = [
            ("", "CREATE TABLE table1 (id INT);"),
            ("", ""),  # Empty line produces empty SQL
            ("", "CREATE TABLE table2 (name STRING);")
        ]
        mock_get_transformer.return_value = mock_transformer

        # Mixed content: SQL, comment, empty line, SQL, comment
        sql_lines = [
            "CREATE TABLE table1 (id INT);\n",
            "-- Comment about table1\n",
            "\n",  # Empty line
            "CREATE TABLE table2 (name STRING);\n",
            "   -- Another comment\n"
        ]
        mock_file_open.return_value.__iter__ = MagicMock(return_value=iter(sql_lines))

        completed_statement = self._create_mock_statement_with_status("COMPLETED")
        mock_post_statement.return_value = completed_statement

        # Execute the function
        prepare_tables_from_sql_file("test.sql", COMPUTE_POOL_ID)

        # Should process 3 lines: 2 SQL statements + 1 empty line
        # (empty line will be passed to transformer but comments are skipped)
        self.assertEqual(mock_transformer.update_sql_content.call_count, 3)

        # Should post 3 statements (including empty SQL from empty line)
        # The function posts whatever the transformer returns, even if it's empty
        self.assertEqual(mock_post_statement.call_count, 3)

    @patch('shift_left.core.deployment_mgr.get_config')
    def test_prepare_tables_no_compute_pool_config(self, mock_get_config):
        """Test behavior when no compute pool ID in config."""
        # Mock config without flink section
        mock_get_config.return_value = {}

        with self.assertRaises(Exception):
            prepare_tables_from_sql_file("test.sql", COMPUTE_POOL_ID)


if __name__ == '__main__':
    unittest.main()
