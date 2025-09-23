"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from pathlib import Path
import shutil
import shift_left.core.project_manager as pm
import pathlib
from unittest.mock import patch, mock_open, MagicMock, call
from datetime import datetime, timezone, timedelta
import subprocess
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
from shift_left.core.utils.app_config import get_config
import shift_left.core.pipeline_mgr as pipemgr
import shift_left.core.table_mgr as tm

class TestProjectManager(unittest.TestCase):
    data_dir = ""

    @classmethod
    def setUpClass(cls):
        cls.data_dir = str(Path(__file__).parent / "../tmp")  # Path to the tmp directory

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
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
        pipemgr.delete_all_metada_files(os.getenv("PIPELINES"))
        pipemgr.build_all_pipeline_definitions( os.getenv("PIPELINES"))
        result = pm.report_table_cross_products(os.getenv("PIPELINES"))
        assert result
        assert len(result) == 2
        assert "src_table_1" in result
        assert "src_common_tenant" in result

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('builtins.open', new_callable=mock_open)
    @patch('shift_left.core.project_manager.datetime.datetime')
    def test_list_modified_files_basic_functionality(self, mock_datetime_class, mock_file, mock_chdir, mock_getcwd, mock_subprocess):
        """Test basic functionality of list_modified_files"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        mock_datetime_class.now.return_value = datetime(2024, 1, 15, tzinfo=timezone.utc)
        
        # Mock git commands
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="main\n", returncode=0),
            # git log --name-only --since=2024-01-14
            MagicMock(stdout="file1.sql\nfile2.sql\nfile3.py\n", returncode=0),
            # date command
            MagicMock(stdout="Mon Jan 15 10:30:00 UTC 2024\n", returncode=0)
        ]
        
        # Call the function
        result = pm.list_modified_files(
            project_path="/test/project",
            branch_name="main", 
            date_filter="",  # Will use yesterday's date
            file_filter=".sql",
            output_file="modified_files.txt"
        )
        
        # Verify return type
        self.assertIsInstance(result, pm.ModifiedFilesResult)
        
        # Verify description content
        self.assertIn("Modified files in branch 'main'", result.description)
        self.assertIn("Filter applied: .sql", result.description)
        self.assertIn("Generated on: Mon Jan 15 10:30:00 UTC 2024", result.description)
        self.assertIn("Total files: 2", result.description)
        
        # Verify file list
        self.assertEqual(len(result.file_list), 2)
        
        # Check first file
        file1 = result.file_list[0]
        self.assertIsInstance(file1, pm.ModifiedFileInfo)
        self.assertEqual(file1.table_name, "file1")
        self.assertEqual(file1.file_modified_url, "file1.sql")
        
        # Check second file
        file2 = result.file_list[1]
        self.assertEqual(file2.table_name, "file2")
        self.assertEqual(file2.file_modified_url, "file2.sql")
        
        # Verify directory changes
        mock_chdir.assert_has_calls([
            call("/test/project"),
            call("/original/path")
        ])
        
        # Verify git commands
        expected_calls = [
            call(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True),
            call(["git", "log", "--name-only", "--since=2024-01-14", '--pretty=format:'], capture_output=True, text=True, check=True),
            call(['date'], capture_output=True, text=True)
        ]
        mock_subprocess.assert_has_calls(expected_calls)
        
        # Verify file operations (backward compatibility) - now writes JSON
        mock_file.assert_called_once_with(Path("/original/path/modified_files.txt"), 'w')
        
        # Verify JSON content was written
        handle = mock_file.return_value.__enter__.return_value
        write_calls = handle.write.call_args_list
        self.assertEqual(len(write_calls), 1)  # Should be one JSON write
        
        # Parse the written JSON to verify structure
        import json
        written_json = write_calls[0][0][0]
        parsed_data = json.loads(written_json)
        
        self.assertIn("description", parsed_data)
        self.assertIn("file_list", parsed_data)
        self.assertEqual(len(parsed_data["file_list"]), 2)

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_branch_switch(self, mock_file, mock_chdir, mock_getcwd, mock_subprocess):
        """Test branch switching functionality"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        
        # Mock git commands - current branch is different
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="feature-branch\n", returncode=0),
            # git checkout main
            MagicMock(stdout="", returncode=0),
            # git log --name-only --since=2024-01-10
            MagicMock(stdout="test1.sql\ntest2.sql\n", returncode=0),
            # date command
            MagicMock(stdout="Mon Jan 15 10:30:00 UTC 2024\n", returncode=0)
        ]
        
        # Call the function
        pm.list_modified_files(
            project_path=".",
            branch_name="main",
            date_filter="2024-01-10",
            file_filter=".sql",
            output_file="output.txt"
        )
        
        # Verify git checkout was called
        expected_calls = [
            call(["git", "rev-parse", "--abbrev-ref", "HEAD"], capture_output=True, text=True, check=True),
            call(["git", "checkout", "main"], capture_output=True, text=True, check=True),
            call(["git", "log", "--name-only", "--since=2024-01-10", '--pretty=format:'], capture_output=True, text=True, check=True),
            call(['date'], capture_output=True, text=True)
        ]
        mock_subprocess.assert_has_calls(expected_calls)

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_file_filtering(self, mock_file, mock_chdir, mock_getcwd, mock_subprocess):
        """Test file filtering functionality"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        
        # Mock git commands with various file types
        mock_subprocess.side_effect = [
            # git rev-parse --abbrev-ref HEAD
            MagicMock(stdout="main\n", returncode=0),
            # git log with mixed file types
            MagicMock(stdout="file1.sql\nfile2.py\nfile3.SQL\ntest.java\nscript.sql\n", returncode=0),
            # date command
            MagicMock(stdout="Mon Jan 15 10:30:00 UTC 2024\n", returncode=0)
        ]
        
        # Call the function with .sql filter
        result = pm.list_modified_files(
            project_path=".",
            branch_name="main",
            date_filter="2024-01-10",
            file_filter=".sql",
            output_file="sql_files.txt"
        )
        
        # Verify return type and filtering
        self.assertIsInstance(result, pm.ModifiedFilesResult)
        self.assertEqual(len(result.file_list), 3)  # Only SQL files
        
        # Verify SQL files are included (case insensitive)
        file_urls = [f.file_modified_url for f in result.file_list]
        self.assertIn("file1.sql", file_urls)
        self.assertIn("file3.SQL", file_urls)  # Case insensitive
        self.assertIn("script.sql", file_urls)
        
        # Verify non-SQL files are not included
        self.assertNotIn("file2.py", file_urls)
        self.assertNotIn("test.java", file_urls)
        
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
                date_filter="2024-01-10",
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
            date_filter="2024-01-10",
            file_filter=".sql",
            output_file="empty_results.txt"
        )
        
        # Verify return type and empty results
        self.assertIsInstance(result, pm.ModifiedFilesResult)
        self.assertEqual(len(result.file_list), 0)
        
        # Verify description contains correct information
        self.assertIn("Modified files in branch 'main'", result.description)
        self.assertIn("Total files: 0", result.description)
        self.assertIn("Filter applied: .sql", result.description)
        
        # Verify file operations still work with empty results
        mock_file.assert_called_once()

    @patch('shift_left.core.project_manager.subprocess.run')
    @patch('shift_left.core.project_manager.os.getcwd')
    @patch('shift_left.core.project_manager.os.chdir')
    @patch('builtins.open', new_callable=mock_open)
    def test_list_modified_files_absolute_output_path(self, mock_file, mock_chdir, mock_getcwd, mock_subprocess):
        """Test handling of absolute output file paths"""
        # Setup mocks
        mock_getcwd.return_value = "/original/path"
        
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
            date_filter="2024-01-10",
            file_filter=".sql",
            output_file=absolute_output
        )
        
        # Verify absolute path is used directly
        mock_file.assert_called_once_with(Path(absolute_output), 'w')

    def test_extract_table_name_from_path(self):
        """Test table name extraction from various file paths"""
        # Test basic SQL file
        self.assertEqual(pm._extract_table_name_from_path("table1.sql"), "table1")
        
        # Test file with DDL prefix
        self.assertEqual(pm._extract_table_name_from_path("ddl.user_table.sql"), "user_table")
        
        # Test file with DML prefix
        self.assertEqual(pm._extract_table_name_from_path("dml.user_table.sql"), "user_table")
        
        # Test pipeline directory structure
        self.assertEqual(pm._extract_table_name_from_path("pipelines/user_table/ddl.sql"), "user_table")
        
        # Test complex path
        self.assertEqual(pm._extract_table_name_from_path("project/pipelines/facts/user_facts/dml.sql"), "user_facts")
        
        # Test file without extension
        self.assertEqual(pm._extract_table_name_from_path("pipelines/customer_dim/metadata"), "customer_dim")
        
if __name__ == '__main__':
    unittest.main()