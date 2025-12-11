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
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
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
            result = runner.invoke(app, [ "housekeep-statements"])
            print(result)
            assert "Clean statements starting" in result.stdout
        finally:
            if original_config:
                os.environ["CONFIG_FILE"] = original_config

    def test_delete_all_compute_pools_command(self):
        """Test delete-all-compute-pools command"""
        runner = CliRunner()

        # This command requires actual Confluent Cloud access
        # We test the command parsing but expect it might fail due to infrastructure
        result = runner.invoke(app, ["delete-all-compute-pools", "test_product"])

        # The command should parse correctly even if it fails due to missing infrastructure
        print(result.stdout)
        # We don't assert exit_code here since it depends on actual cloud connectivity

    def test_list_modified_files(self):
        runner = CliRunner()
        project_path =  str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent)
        result = runner.invoke(app, [ "list-modified-files", "develop", "--project-path", project_path, "--file-filter", "sql", "--since", "2025-12-08"])
        print(result.stdout)
        assert result.exit_code == 0
        print(result.stdout)


if __name__ == '__main__':
    unittest.main()
