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
import shift_left.core.pipeline_mgr as pm
import subprocess

class TestProjectCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES",""))

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



    @patch('shift_left.cli_commands.project.get_config')
    def test_validate_config_valid(self, mock_get_config):
        """Test validate_config with a valid configuration"""
        # Create a valid config for testing
        valid_config = {
            "kafka": {
                "bootstrap.servers": "test_bootstrap_servers:9092",
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
                 "bootstrap.servers": "test_bootstrap_servers:9092",
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

    @patch('shift_left.cli_commands.project.project_manager._assess_flink_statement_state')
    @patch('shift_left.cli_commands.project.subprocess.run')
    def test_list_modified_files_success(self, mock_subprocess_run, mock_assess_state):
        """Test list_modified_files command with successful git operations"""
        runner = CliRunner()

        # Mock git subprocess calls
        mock_subprocess_run.side_effect = [
            # Mock git rev-parse --abbrev-ref HEAD (current branch)
            MagicMock(stdout="feature-branch\n", stderr="", returncode=0),
            # Mock git checkout main)
            MagicMock(stdout="main\n", stderr="", returncode=0),
            # Mock git log --name-only --since=2024-01-01 --pretty=format:
            MagicMock(stdout="pipelines/sources/c360/src_users/sql-scripts/dml.src_c360_users.sql\npipelines/facts/c360/fct_user_per_group/sql-scripts/ddl.c360_fct_user_per_group.sql\nsrc/some_file.py\ndocs/readme.md\n",
                     stderr="", returncode=0)
        ]
        # for the 2 sqls return same sql and if running flag
        mock_assess_state.side_effect = [(False, True), (False, True)]
        # Create temporary directory for output file
        output_file = os.getenv("HOME",'~') + "/.shift_left/modified_flink_files.txt"

        result = runner.invoke(app, [
            "list-modified-files",
            "main",
            "--file-filter", ".sql"
        ])

        print(f"result.stdout: {result.stdout}")
        assert result.exit_code == 0
        assert "Found 4 total modified files" in result.stdout
        assert "Found 2 modified files matching filter '.sql'" in result.stdout

        # Verify output file was created and contains expected content
        assert os.path.exists(output_file)
        with open(output_file, 'r') as f:
            content = f.read()
            assert "feature-branch" in content
            assert "pipelines/sources/c360/src_users/sql-scripts/dml.src_c360_users.sql" in content
            assert "pipelines/facts/c360/fct_user_per_group/sql-scripts/ddl.c360_fct_user_per_group.sql" in content
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
            # Mock git checkout main)
            MagicMock(stdout="main\n", stderr="", returncode=0),
            # Mock git diff --name-only main...HEAD (modified files, no SQL)
            MagicMock(stdout="src/some_file.py\ndocs/readme.md\nconfig.yaml\n",
                     stderr="", returncode=0),
            # Mock date command for timestamp
            MagicMock(stdout="Mon Jan 1 12:00:00 UTC 2024\n", stderr="", returncode=0)
        ]

        # Create temporary directory for output file
        output_file = os.getenv("HOME",'~') + "/.shift_left/modified_flink_files_short.txt"

        result = runner.invoke(app, [
            "list-modified-files",
            "main",
            "--file-filter", ".sql"
        ])

        print(f"result.stdout: {result.stdout}")
        assert result.exit_code == 0
        assert "Total modified files: 0" in result.stdout
        assert "Found 3 total modified files" in result.stdout
        assert "Found 0 modified files matching filter '.sql'" in result.stdout
        # Verify output file was created even with no matches
        assert os.path.exists(output_file)
        with open(output_file, 'r') as f:
            content = f.read()
            assert "" in content

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


if __name__ == '__main__':
    unittest.main()
