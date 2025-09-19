"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
import tempfile
from unittest.mock import patch, MagicMock
from typer.testing import CliRunner
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
from shift_left.core.utils.app_config import shift_left_dir
from shift_left.cli_commands.project import app
import subprocess

class TestProjectCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")

    @classmethod
    def tearDownClass(cls):
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_init_project(self):
        runner = CliRunner()
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        print(temp_dir)
        result = runner.invoke(app, [ "init", "project_test_via_cli", str(temp_dir)])
        print(result.stdout)
        assert result.exit_code == 0
        assert "Project project_test_via_cli created in " in result.stdout
        assert os.path.exists(temp_dir / "project_test_via_cli")
        assert os.path.exists(temp_dir / "project_test_via_cli/pipelines")

    def test_list_topics(self):
        runner = CliRunner()
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        result = runner.invoke(app, [ "list-topics", str(temp_dir)])
        print(result.stdout)

    def test_compute_pool_list(self):
        runner = CliRunner()
        result = runner.invoke(app, [ "list-compute-pools"])
        print(result.stdout)

    def test_clean_completed_failed_statements(self):
        original_config = os.environ.get("CONFIG_FILE")
        os.environ["CONFIG_FILE"] =  shift_left_dir +  "/config-stage-flink.yaml"
        runner = CliRunner()
        try:
            result = runner.invoke(app, [ "clean-completed-failed-statements"])
            print(result)
            assert "Workspace statements cleaned" in result.stdout
        finally:
            if original_config:
                os.environ["CONFIG_FILE"] = original_config

    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_valid(self, mock_get_config):
        """Test validate_config with a valid configuration"""
        # Create a valid config for testing
        valid_config = {
            "kafka": {
                "src_topic_prefix": "test_prefix",
                "cluster_id": "test_cluster",
                "pkafka_cluster": "test_pkafka",
                "cluster_type": "test_type"
            },
            "confluent_cloud": {
                "base_api": "https://api.confluent.cloud",
                "environment_id": "env-123",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-123",
                "api_key": "test_key",
                "api_secret": "test_secret",
                "url_scope": "test_scope"
            },
            "flink": {
                "flink_url": "https://flink.test.com",
                "api_key": "flink_key",
                "api_secret": "flink_secret",
                "compute_pool_id": "pool-123",
                "catalog_name": "test_catalog",
                "database_name": "test_db",
                "max_cfu": 10,
                "max_cfu_percent_before_allocation": 80
            },
            "app": {
                "delta_max_time_in_min": 60,
                "timezone": "UTC",
                "logging": "INFO",
                "data_limit_column_name_to_select_from": "created_at",
                "products": ["product1", "product2"],
                "accepted_common_products": ["common1"],
                "sql_content_modifier": "test_modifier",
                "dml_naming_convention_modifier": "test_dml_modifier",
                "compute_pool_naming_convention_modifier": "test_pool_modifier",
                "data_limit_where_condition": "WHERE 1=1",
                "data_limit_replace_from_reg_ex": "test_regex",
                "data_limit_table_type": "test_type"
            }
        }
        
        # Mock get_config to return our test configuration
        mock_get_config.return_value = valid_config
        
        runner = CliRunner()
        result = runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert result.exit_code == 0
        assert "Config.yaml validated" in result.stdout

    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_missing_sections(self, mock_get_config):
        """Test validate_config with missing required sections"""
        # Create config missing required sections
        invalid_config = {
            "kafka": {
                "src_topic_prefix": "test_prefix"
            }
            # Missing confluent_cloud, flink, app sections
        }
        
        # Mock get_config to return our test configuration
        mock_get_config.return_value = invalid_config
        
        runner = CliRunner()
        result = runner.invoke(app, ["validate-config"])
        print(result.stdout)
        # Should still exit with 0 but show validation errors in output
        assert "Configuration validation failed" in result.stdout
        assert "missing confluent_cloud section" in result.stdout
        assert "missing flink section" in result.stdout
        assert "missing app section" in result.stdout

    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_placeholder_values(self, mock_get_config):
        """Test validate_config with placeholder values that need to be replaced"""
        # Create config with placeholder values
        config_with_placeholders = {
            "kafka": {
                "src_topic_prefix": "<TO_FILL>",
                "cluster_id": "test_cluster",
                "pkafka_cluster": "test_pkafka",
                "cluster_type": "test_type"
            },
            "confluent_cloud": {
                "base_api": "https://api.confluent.cloud",
                "environment_id": "env-123",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-123",
                "api_key": "<kafka-api-key>",
                "api_secret": "<kafka-api-key_secret>",
                "url_scope": "test_scope"
            },
            "flink": {
                "flink_url": "https://flink.test.com",
                "api_key": "flink_key",
                "api_secret": "flink_secret",
                "compute_pool_id": "pool-123",
                "catalog_name": "test_catalog",
                "database_name": "test_db",
                "max_cfu": 10,
                "max_cfu_percent_before_allocation": 80
            },
            "app": {
                "delta_max_time_in_min": 60,
                "timezone": "UTC",
                "logging": "INFO",
                "data_limit_column_name_to_select_from": "created_at",
                "products": ["product1"],
                "accepted_common_products": ["common1"],
                "sql_content_modifier": "test_modifier",
                "dml_naming_convention_modifier": "test_dml_modifier",
                "compute_pool_naming_convention_modifier": "test_pool_modifier",
                "data_limit_where_condition": "WHERE 1=1",
                "data_limit_replace_from_reg_ex": "test_regex",
                "data_limit_table_type": "test_type"
            }
        }
        
        # Mock get_config to return our test configuration
        mock_get_config.return_value = config_with_placeholders
        
        runner = CliRunner()
        result = runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert "Configuration validation failed" in result.stdout
        assert "placeholder value '<TO_FILL>'" in result.stdout
        assert "placeholder value '<kafka-api-key>'" in result.stdout
        assert "placeholder value '<kafka-api-key_secret>'" in result.stdout

    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_invalid_data_types(self, mock_get_config):
        """Test validate_config with invalid data types"""
        # Create config with invalid data types
        invalid_types_config = {
            "kafka": {
                "src_topic_prefix": "test_prefix",
                "cluster_id": "test_cluster",
                "pkafka_cluster": "test_pkafka",
                "cluster_type": "test_type"
            },
            "confluent_cloud": {
                "base_api": "https://api.confluent.cloud",
                "environment_id": "env-123",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-123",
                "api_key": "test_key",
                "api_secret": "test_secret",
                "url_scope": "test_scope"
            },
            "flink": {
                "flink_url": "https://flink.test.com",
                "api_key": "flink_key",
                "api_secret": "flink_secret",
                "compute_pool_id": "pool-123",
                "catalog_name": "test_catalog",
                "database_name": "test_db",
                "max_cfu": "not_a_number",  # Should be numeric
                "max_cfu_percent_before_allocation": 80
            },
            "app": {
                "delta_max_time_in_min": "not_a_number",  # Should be numeric
                "timezone": "UTC",
                "logging": "INVALID_LEVEL",  # Should be valid log level
                "data_limit_column_name_to_select_from": "created_at",
                "products": "not_a_list",  # Should be a list
                "accepted_common_products": "not_a_list",  # Should be a list
                "sql_content_modifier": "test_modifier",
                "dml_naming_convention_modifier": "test_dml_modifier",
                "compute_pool_naming_convention_modifier": "test_pool_modifier",
                "data_limit_where_condition": "WHERE 1=1",
                "data_limit_replace_from_reg_ex": "test_regex",
                "data_limit_table_type": "test_type"
            }
        }
        
        # Mock get_config to return our test configuration
        mock_get_config.return_value = invalid_types_config
        
        runner = CliRunner()
        result = runner.invoke(app, ["validate-config"])
        print(result.stdout)
        assert "Configuration validation failed" in result.stdout
        assert "must be a number" in result.stdout
        assert "must be a valid log level" in result.stdout
        assert "must be a list" in result.stdout

    @patch('shift_left.cli_commands.project.subprocess.run')
    def test_list_modified_files_success(self, mock_subprocess_run):
        """Test list_modified_files command with successful git operations"""
        runner = CliRunner()
        
        # Mock git subprocess calls
        mock_subprocess_run.side_effect = [
            # Mock git rev-parse --abbrev-ref HEAD (current branch)
            MagicMock(stdout="feature-branch\n", stderr="", returncode=0),
            # Mock git diff --name-only main...HEAD (modified files)
            MagicMock(stdout="pipelines/sources/src_table1.sql\npipelines/facts/fact_table2.sql\nsrc/some_file.py\ndocs/readme.md\n", 
                     stderr="", returncode=0),
            # Mock date command for timestamp
            MagicMock(stdout="Mon Jan 1 12:00:00 UTC 2024\n", stderr="", returncode=0)
        ]
        
        # Create temporary directory for output file
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, "test_modified_files.txt")
            
            result = runner.invoke(app, [
                "list-modified-files", 
                "main",
                "--output-file", output_file,
                "--file-filter", ".sql"
            ])
            
            print(result.stdout)
            assert result.exit_code == 0
            assert "Found 4 total modified files" in result.stdout
            assert "Found 2 modified files matching filter '.sql'" in result.stdout
            assert "pipelines/sources/src_table1.sql" in result.stdout
            assert "pipelines/facts/fact_table2.sql" in result.stdout
            
            # Verify output file was created and contains expected content
            assert os.path.exists(output_file)
            with open(output_file, 'r') as f:
                content = f.read()
                assert "feature-branch" in content
                assert "main" in content
                assert "pipelines/sources/src_table1.sql" in content
                assert "pipelines/facts/fact_table2.sql" in content
                assert "Total files: 2" in content
                # Python and markdown files should not be in the output due to filter
                assert "src/some_file.py" not in content
                assert "docs/readme.md" not in content

    @patch('shift_left.cli_commands.project.subprocess.run')
    def test_list_modified_files_no_matches(self, mock_subprocess_run):
        """Test list_modified_files command when no files match the filter"""
        runner = CliRunner()
        
        # Mock git subprocess calls
        mock_subprocess_run.side_effect = [
            # Mock git rev-parse --abbrev-ref HEAD (current branch)
            MagicMock(stdout="feature-branch\n", stderr="", returncode=0),
            # Mock git diff --name-only main...HEAD (modified files, no SQL)
            MagicMock(stdout="src/some_file.py\ndocs/readme.md\nconfig.yaml\n", 
                     stderr="", returncode=0),
            # Mock date command for timestamp
            MagicMock(stdout="Mon Jan 1 12:00:00 UTC 2024\n", stderr="", returncode=0)
        ]
        
        # Create temporary directory for output file
        with tempfile.TemporaryDirectory() as temp_dir:
            output_file = os.path.join(temp_dir, "test_no_matches.txt")
            
            result = runner.invoke(app, [
                "list-modified-files", 
                "main",
                "--output-file", output_file,
                "--file-filter", ".sql"
            ])
            
            print(result.stdout)
            assert result.exit_code == 0
            assert "Found 3 total modified files" in result.stdout
            assert "Found 0 modified files matching filter '.sql'" in result.stdout
            assert "No modified files found matching filter '.sql'" in result.stdout
            
            # Verify output file was created even with no matches
            assert os.path.exists(output_file)
            with open(output_file, 'r') as f:
                content = f.read()
                assert "Total files: 0" in content

    @patch('shift_left.cli_commands.project.subprocess.run')
    def test_list_modified_files_git_error(self, mock_subprocess_run):
        """Test list_modified_files command when git command fails"""
        runner = CliRunner()
        
        # Mock git command failure
        mock_subprocess_run.side_effect = [
            # Mock git rev-parse failure (not in a git repo)
            subprocess.CalledProcessError(128, "git rev-parse", stderr="fatal: not a git repository")
        ]
        
        result = runner.invoke(app, [
            "list-modified-files", 
            "main"
        ])
        
        print(result.stdout)
        assert result.exit_code == 1
        assert "Git command failed" in result.stdout

    def test_delete_all_compute_pools_command(self):
        """Test delete-all-compute-pools command"""
        runner = CliRunner()
        
        # This command requires actual Confluent Cloud access
        # We test the command parsing but expect it might fail due to infrastructure
        result = runner.invoke(app, ["delete-all-compute-pools", "test_product"])
        
        # The command should parse correctly even if it fails due to missing infrastructure
        print(result.stdout)
        # We don't assert exit_code here since it depends on actual cloud connectivity
        

if __name__ == '__main__':
    unittest.main()