"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
from typer.testing import CliRunner
from shift_left.core.utils.app_config import shift_left_dir
from shift_left.cli_commands.project import app

class TestProjectCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  shift_left_dir +  "/it-config.yaml"

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
        os.environ["CONFIG_FILE"] =  shift_left_dir +  "/config-stage-flink.yaml"
        runner = CliRunner()
        result = runner.invoke(app, [ "clean-completed-failed-statements"])
        print(result)
        assert "Workspace statements cleaned" in result.stdout
        

if __name__ == '__main__':
    unittest.main()