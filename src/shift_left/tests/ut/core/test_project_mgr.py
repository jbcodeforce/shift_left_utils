"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import pathlib
import unittest
from unittest.mock import patch, MagicMock

TEST_PIPELINES_DIR = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["SL_CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = TEST_PIPELINES_DIR

import shift_left.core.project_manager as pm
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm
from shift_left.core.models.flink_statement_model import Statement, Spec, Status


class TestProjectManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        tm.get_or_create_inventory(os.getenv("PIPELINES", TEST_PIPELINES_DIR))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES", TEST_PIPELINES_DIR))
        pipemgr.build_all_pipeline_definitions(os.getenv("PIPELINES", TEST_PIPELINES_DIR))

    @patch("shift_left.core.project_manager.statement_mgr.get_statement")
    def test_assess_flink_statement_state_with_statement_mgr_mock(self, mock_get_statement):
        """Test _assess_flink_statement_state with mocked statement_mgr.get_statement."""
        table_name = "sl_c360_fct_user_per_group"
        file_path = "pipelines/facts/c360/fct_user_per_group/sql-scripts/dml.c360_fct_user_per_group.sql"
        sql_content = "INSERT INTO sl_c360_fct_user_per_group SELECT * FROM source_table;"

        mock_statement = MagicMock(spec=Statement)
        mock_statement.spec = MagicMock(spec=Spec)
        mock_statement.spec.statement = sql_content
        mock_statement.status = MagicMock(spec=Status)
        mock_statement.status.phase = "RUNNING"
        mock_get_statement.return_value = mock_statement

        same_sql, running = pm._assess_flink_statement_state(table_name, file_path, sql_content)

        self.assertTrue(same_sql, "SQL content should match")
        self.assertTrue(running, "Statement should be running")
        mock_get_statement.assert_called_once_with("dev-usw2-c360-dml-sl-c360-fct-user-per-group")

        mock_get_statement.reset_mock()
        mock_statement.spec.statement = "DIFFERENT SQL CONTENT"
        mock_statement.status.phase = "STOPPED"

        same_sql, running = pm._assess_flink_statement_state(table_name, file_path, sql_content)

        self.assertFalse(same_sql, "SQL content should not match")
        self.assertFalse(running, "Statement should not be running")
        mock_get_statement.assert_called_once_with("dev-usw2-c360-dml-sl-c360-fct-user-per-group")

        mock_get_statement.reset_mock()
        mock_get_statement.return_value = None

        same_sql, running = pm._assess_flink_statement_state(table_name, file_path, sql_content)

        self.assertTrue(same_sql, "Should return True when statement not found")
        self.assertFalse(running, "Should return False when statement not found")
        mock_get_statement.assert_called_once_with("dev-usw2-c360-dml-sl-c360-fct-user-per-group")


if __name__ == "__main__":
    unittest.main()
