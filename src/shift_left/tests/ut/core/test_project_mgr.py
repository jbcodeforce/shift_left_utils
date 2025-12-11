"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil

import pathlib
from unittest.mock import patch, mock_open, MagicMock, call
from datetime import datetime, timezone, timedelta
import subprocess
TEST_PIPELINES_DIR = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = TEST_PIPELINES_DIR
from shift_left.core.utils.app_config import get_config
import shift_left.core.project_manager as pm
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm
from shift_left.core.models.flink_statement_model import Statement, Spec, Status

class TestProjectManager(unittest.TestCase):
    data_dir = ""

    @classmethod
    def setUpClass(cls):
        cls.data_dir = str(Path(__file__).parent / "../tmp")  # Path to the tmp directory
        tm.get_or_create_inventory(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        pipemgr.build_all_pipeline_definitions( os.getenv("PIPELINES",TEST_PIPELINES_DIR))

    def test_create_data_product_project(self):
        try:
            pm.build_project_structure("test_data_project", self.data_dir, pm.DATA_PRODUCT_PROJECT_TYPE)
            assert os.path.exists(os.path.join(self.data_dir, "test_data_project"))
            assert os.path.exists(os.path.join(self.data_dir, "test_data_project/pipelines"))
            shutil.rmtree(self.data_dir)
        except Exception as e:
            self.fail()

    def test_1_create_data_kimball_project(self):
        try:
            pm.build_project_structure("test_data_kimball_project",self.data_dir, pm.KIMBALL_PROJECT_TYPE)
            assert os.path.exists(os.path.join( self.data_dir, "test_data_kimball_project"))
            assert os.path.exists(os.path.join( self.data_dir, "test_data_kimball_project/pipelines"))
            assert os.path.exists(os.path.join( self.data_dir, "test_data_kimball_project/pipelines/intermediates"))
            shutil.rmtree(self.data_dir)
        except Exception as e:
            self.fail()

    def test_report_table_cross_products(self):
        print("test_report_table_cross_products: list the tables used in other products")
        tm.get_or_create_inventory(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        pipemgr.build_all_pipeline_definitions( os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        result = pm.report_table_cross_products(os.getenv("PIPELINES",TEST_PIPELINES_DIR))
        assert result
        assert len(result) == 2
        assert "src_table_1" in result
        assert "src_common_tenant" in result

    @patch('builtins.open', new_callable=mock_open)
    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('shift_left.core.project_manager.datetime.datetime')
    @patch('shift_left.core.project_manager._assess_flink_statement_state')
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    @patch('shift_left.core.project_manager.os.path.exists')
    def test_list_modified_files_with_mocked_sql_parsing(self, mock_path_exists,
                                                         mock_sql_parser_class,
                                                         mock_assess_state,
                                                         mock_datetime_class,
                                                         mock_chdir,
                                                         mock_getcwd,
                                                         mock_subprocess,
                                                         mock_file):
        """Test that list_modified_files calls SQL parser to extract table names from file contents"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        mock_datetime_class.now.return_value = datetime(2024, 1, 15, tzinfo=timezone.utc)
        mock_path_exists.return_value = True  # All files exist

        # Mock the assessment function
        mock_assess_state.return_value = (False, True)  # same_sql=False, running=True

        # Mock SQL parser instance and methods
        mock_parser_instance = MagicMock()
        mock_sql_parser_class.return_value = mock_parser_instance
        mock_parser_instance.extract_table_name_from_insert_into_statement.side_effect = [
            "user_accounts",  # First file returns this table name
            "order_history"   # Second file returns this table name
        ]

        # Mock file content for SQL parsing - provide realistic SQL content
        mock_file.side_effect = [
            mock_open(read_data="INSERT INTO user_accounts SELECT * FROM source_table;").return_value,
            mock_open(read_data="INSERT INTO order_history SELECT * FROM order_source;").return_value,
            mock_open(read_data='{"description": "test", "file_list": []}').return_value  # Output file write
        ]

        # Mock git commands
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="main\n", returncode=0),
            # git log --name-only --since=2024-01-14
            MagicMock(stdout="pipelines/data_product/file1.sql\npipelines/data_product/file2.sql\npipelines/data_product/file3.py\n", returncode=0),
         ]

        # Call the function
        result = pm.list_modified_files(
            project_path="/test/project",
            branch_name="main",
            since="",  # Will use yesterday's date
            file_filter=".sql",
            output_file="modified_files.txt"
        )

        # Verify return type
        self.assertIsInstance(result, pm.ModifiedFilesResult)

        # Verify description content
        self.assertIn("Modified files in branch:'main'", result.description)
        self.assertIn("Filter applied: .sql", result.description)
        self.assertIn("Total files: 2", result.description)

        # Verify file list
        self.assertEqual(len(result.file_list), 2)

        # Check second file - should have extracted table name via SQL parser
        for file in result.file_list:
            self.assertIn(file.table_name, ["order_history", "user_accounts"])  # From mocked SQL parser
            self.assertIn(file.file_modified_url, ["/test/project/pipelines/data_product/file1.sql", "/test/project/pipelines/data_product/file2.sql"])
            self.assertEqual(file.same_sql_content, False)  # From mock assessment
            self.assertEqual(file.running, True)   # From mock assessment


        # Verify directory changes
        mock_chdir.assert_has_calls([
            call("/test/project"),
            call("/original/path")
        ])

        # Verify git commands
        expected_calls = [
            call(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True),
            call(["git", "log", "--name-only", "--since=2024-01-14", '--pretty=format:'], capture_output=True, text=True, check=True),
        ]
        mock_subprocess.assert_has_calls(expected_calls)

        # Verify assessment function was called for each file
        self.assertEqual(mock_assess_state.call_count, 2)

        # Verify file operations - should read SQL files from absolute paths and write output
        file_calls = mock_file.call_args_list
        print("DEBUG: All file call args:", file_calls)
        self.assertTrue(any('/test/project/pipelines/data_product/file1.sql' in str(call) for call in file_calls),
                       "Should have read pipelines/data_product/file1.sql from absolute path")
        self.assertTrue(any('/test/project/pipelines/data_product/file2.sql' in str(call) for call in file_calls),
                       "Should have read pipelines/data_product/file2.sql from absolute path")
        self.assertTrue(any('modified_files.txt' in str(call) for call in file_calls),
                       "Should have written to output file")

    @patch('shift_left.core.project_manager.datetime.datetime')
    @patch('shift_left.core.project_manager.SQLparser')
    @patch('shift_left.core.project_manager._assess_flink_statement_state')
    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('shift_left.core.project_manager.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_branch_switch(self,
                    mock_file,
                    mock_path_exists,
                    mock_chdir,
                    mock_getcwd,
                    mock_subprocess,
                    mock_assess_state,
                    mock_sql_parser_class,
                    mock_datetime_class):
        """Test branch switching functionality"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        mock_datetime_class.now.return_value = datetime(2025, 9, 15, 10, 30, 0, tzinfo=timezone.utc)
        mock_path_exists.return_value = True  # All files exist

        # Mock SQL parser instance and methods
        mock_parser_instance = MagicMock()
        mock_sql_parser_class.return_value = mock_parser_instance
        mock_parser_instance.extract_table_name_from_insert_into_statement.side_effect = [
            "test1_table",  # First file table name
            "test2_table"   # Second file table name
        ]

        # Mock file content reads and output file write
        mock_file.side_effect = [
            mock_open(read_data="INSERT INTO test1_table SELECT * FROM source1;").return_value,
            mock_open(read_data="INSERT INTO test2_table SELECT * FROM source2;").return_value,
            mock_open(read_data='{"description": "test", "file_list": []}').return_value  # Output file write
        ]

        # Mock git commands - current branch is different
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="feature-branch\n", returncode=0),
            # git checkout main
            MagicMock(stdout="", returncode=0),
            # git log --name-only --since=2025-09-01
            MagicMock(stdout="pipelines/data_product/test1.sql\npipelines/data_product/test2.sql\n", returncode=0),
        ]

        # Mock assessment state for both files: not same sql, and running
        mock_assess_state.return_value = (False, True)

        # Call the function
        result = pm.list_modified_files(
            project_path=".",
            branch_name="main",
            since="2025-09-01",
            file_filter=".sql",
            output_file="output.txt"
        )

        # Verify git checkout was called
        expected_calls = [
            call(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True),
            call(["git", "checkout", "main"], capture_output=True, text=True, check=True),
            call(["git", "log", "--name-only", "--since=2025-09-01", '--pretty=format:'], capture_output=True, text=True, check=True)
        ]
        mock_subprocess.assert_has_calls(expected_calls)

        # Verify result structure
        self.assertIsInstance(result, pm.ModifiedFilesResult)
        print(f"DEBUG: result: {result}")
        self.assertEqual(len(result.file_list), 2)

        # Verify branch information in description
        self.assertIn("Modified files in branch:'feature-branch'", result.description)  # Should show current branch

        # Verify file details
        file1 = result.file_list.pop()
        self.assertTrue(file1.table_name in ["test1_table", "test2_table"])
        self.assertIn(file1.file_modified_url, ["./pipelines/data_product/test1.sql", "./pipelines/data_product/test2.sql"])
        self.assertEqual(file1.same_sql_content, False)
        self.assertEqual(file1.running, True)

        file2 = result.file_list.pop()
        self.assertTrue(file2.table_name in ["test1_table", "test2_table"])
        self.assertIn(file2.file_modified_url, ["./pipelines/data_product/test2.sql", "./pipelines/data_product/test1.sql"])
        self.assertEqual(file2.same_sql_content, False)
        self.assertEqual(file2.running, True)

        # Verify SQL parser was used for both files
        self.assertEqual(mock_parser_instance.extract_table_name_from_insert_into_statement.call_count, 2)

        # Verify assessment function was called for each file
        self.assertEqual(mock_assess_state.call_count, 2)

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('shift_left.core.project_manager.from_pipeline_to_absolute')
    @patch('shift_left.core.project_manager._assess_flink_statement_state')
    @patch('shift_left.core.project_manager.os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_file_filtering(self, mock_file, mock_path_exists, mock_assess_state, mock_from_pipeline, mock_chdir, mock_getcwd, mock_subprocess):
        """Test file filtering functionality"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        mock_path_exists.return_value = True  # All files exist
        # Mock the path conversion function
        mock_from_pipeline.side_effect = [
            "/original/path/pipelines/data_product/file1.sql",
            "/original/path/pipelines/data_product/file3.SQL",
            "/original/path/pipelines/data_product/script.sql"
        ]

        # Mock the assessment function
        mock_assess_state.return_value = (False, True)

        # Mock file content with INSERT statements to extract table names
        mock_file.side_effect = [
            mock_open(read_data="INSERT INTO file1 SELECT * FROM source1;").return_value,
            mock_open(read_data="INSERT INTO file3 SELECT * FROM source2;").return_value,
            mock_open(read_data="INSERT INTO script SELECT * FROM source3;").return_value,
            mock_open(read_data='{"description": "test", "file_list": []}').return_value  # Output file write
        ]

        # Mock git commands with various file types
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="main\n", returncode=0),
            # git log with mixed file types
            MagicMock(stdout="pipelines/data_product/file1.sql\npipelines/data_product/file2.py\npipelines/data_product/file3.SQL\ntest.java\npipelines/data_product/script.sql\n", returncode=0),
        ]

        # Call the function with .sql filter
        result = pm.list_modified_files(
            project_path=".",
            branch_name="main",
            since="2025-09-01",
            file_filter=".sql",
            output_file="sql_files.txt"
        )

        # Verify return type and filtering
        self.assertIsInstance(result, pm.ModifiedFilesResult)
        self.assertEqual(len(result.file_list), 3)  # Only SQL files

        # Verify SQL files are included (case insensitive)
        file_urls = [f.file_modified_url for f in result.file_list]
        self.assertIn("./pipelines/data_product/file1.sql", file_urls)
        self.assertIn("./pipelines/data_product/file3.SQL", file_urls)  # Case insensitive
        self.assertIn("./pipelines/data_product/script.sql", file_urls)

        # Verify non-SQL files are not included
        self.assertNotIn("./pipelines/data_product/file2.py", file_urls)
        self.assertNotIn("pipelines/data_product/test.java", file_urls)

        # Verify table names are extracted correctly
        table_names = [f.table_name for f in result.file_list]
        self.assertIn("file1", table_names)
        self.assertIn("file3", table_names)
        self.assertIn("script", table_names)

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    def test_list_modified_files_git_error(self, mock_chdir, mock_getcwd, mock_subprocess):
        """Test error handling for git command failures"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"

        # Mock git command failure
        mock_subprocess.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["git", "rev-parse", "--abbrev-ref", "HEAD"],
            stderr="fatal: not a git repository"
        )

        # Call should raise exception
        with self.assertRaises(subprocess.CalledProcessError):
            pm.list_modified_files(
                project_path="/test/project",
                branch_name="main",
                since="2024-01-10",
                file_filter=".sql",
                output_file="output.txt"
            )

        # Verify we still restore original directory
        mock_chdir.assert_has_calls([
            call("/test/project"),
            call("/original/path")
        ])

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_empty_results(self, mock_file, mock_chdir, mock_getcwd, mock_subprocess):
        """Test handling of empty git results"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"

        # Mock git commands with no results
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="main\n", returncode=0),
            # git log with no files
            MagicMock(stdout="", returncode=0),
            # date command
            MagicMock(stdout="Mon Jan 15 10:30:00 UTC 2024\n", returncode=0)
        ]

        # Call the function
        result = pm.list_modified_files(
            project_path=".",
            branch_name="main",
            since="2024-01-10",
            file_filter=".sql",
            output_file="empty_results.txt"
        )

        # Verify return type and empty results
        self.assertIsInstance(result, pm.ModifiedFilesResult)
        self.assertEqual(len(result.file_list), 0)

        # Verify description contains correct information
        self.assertIn("Modified files in branch:'main'", result.description)
        self.assertIn("Total files: 0", result.description)
        self.assertIn("Filter applied: .sql", result.description)

        # Verify file operations still work with empty results
        mock_file.assert_called_once()

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('shift_left.core.project_manager.from_pipeline_to_absolute')
    @patch('shift_left.core.project_manager._assess_flink_statement_state')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_absolute_output_path(self,
                            mock_file,
                            mock_assess_state,
                            mock_from_pipeline,
                            mock_chdir,
                            mock_getcwd,
                            mock_subprocess):
        """Test handling of absolute output file paths"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"

        # Mock the path conversion and assessment functions
        mock_from_pipeline.return_value = "/original/path/test.sql"
        mock_assess_state.return_value = (False, True)

        # Mock file content
        mock_file.side_effect = [
            mock_open(read_data="INSERT INTO test SELECT * FROM source;").return_value,
            mock_open(read_data='{"description": "test", "file_list": []}').return_value
        ]

        # Mock git commands
        mock_subprocess.side_effect = [
            MagicMock(stdout="main\n", returncode=0),
            MagicMock(stdout="test.sql\n", returncode=0),
            MagicMock(stdout="Mon Jan 15 10:30:00 UTC 2024\n", returncode=0)
        ]

        # Call with absolute path
        absolute_output = "/absolute/path/to/output.txt"
        pm.list_modified_files(
            project_path=".",
            branch_name="main",
            since="2024-01-10",
            file_filter=".sql",
            output_file=absolute_output
        )

        # Verify absolute path is used directly and the function completed
        self.assertTrue(mock_file.call_count >= 1)

    @patch('shift_left.core.project_manager.statement_mgr.get_statement')
    def test_assess_flink_statement_state_with_statement_mgr_mock(self, mock_get_statement):
        """Test _assess_flink_statement_state function with mocked statement_mgr.get_statement"""

        # Setup mock data
        table_name = "fct_user_per_group"
        file_path = "pipelines/facts/c360/fct_user_per_group/sql-scripts/dml.fct_user_per_group.sql"
        sql_content = "INSERT INTO fct_user_per_group SELECT * FROM source_table;"

        # Test Case 1: Statement found and running
        mock_statement = MagicMock(spec=Statement)
        mock_statement.spec = MagicMock(spec=Spec)
        mock_statement.spec.statement = sql_content
        mock_statement.status = MagicMock(spec=Status)
        mock_statement.status.phase = "RUNNING"
        mock_get_statement.return_value = mock_statement

        same_sql, running = pm._assess_flink_statement_state(table_name, file_path, sql_content)

        # Verify the result
        self.assertTrue(same_sql, "SQL content should match")
        self.assertTrue(running, "Statement should be running")
        mock_get_statement.assert_called_once_with("dev-usw2-c360-dml-fct-user-per-group")

        # Test Case 2: Statement found but not running and different SQL
        mock_get_statement.reset_mock()
        mock_statement.spec.statement = "DIFFERENT SQL CONTENT"
        mock_statement.status.phase = "STOPPED"

        same_sql, running = pm._assess_flink_statement_state(table_name, file_path, sql_content)

        self.assertFalse(same_sql, "SQL content should not match")
        self.assertFalse(running, "Statement should not be running")
        mock_get_statement.assert_called_once_with("dev-usw2-c360-dml-fct-user-per-group")

        # Test Case 3: Statement not found
        mock_get_statement.reset_mock()
        mock_get_statement.return_value = None

        same_sql, running = pm._assess_flink_statement_state(table_name, file_path, sql_content)

        self.assertTrue(same_sql, "Should return True when statement not found")
        self.assertFalse(running, "Should return False when statement not found")
        mock_get_statement.assert_called_once_with("dev-usw2-c360-dml-fct-user-per-group")


    def test_isolate_data_product(self):
        """Test isolate_data_product function"""
        tgt_folder = os.getenv("PIPELINES",TEST_PIPELINES_DIR) + "/../c360_isolated"

        pm.isolate_data_product("c360", os.getenv("PIPELINES",TEST_PIPELINES_DIR), tgt_folder)
        assert os.path.exists(tgt_folder)
        assert os.path.exists(tgt_folder + "/pipelines/facts/c360/fct_user_per_group")
        assert os.path.exists(tgt_folder + "/pipelines/facts/c360/fct_user_per_group/sql-scripts")
        assert os.path.exists(tgt_folder + "/pipelines/facts/c360/fct_user_per_group/sql-scripts/dml.c360_fct_user_per_group.sql")

if __name__ == '__main__':
    unittest.main()
