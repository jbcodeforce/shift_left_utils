"""
Copyright 2024-2025 Confluent, Inc.
"""
import pytest
import unittest
from typer.testing import CliRunner
from shift_left.cli_commands.pipeline import app
import os
from pathlib import Path
from shift_left.core.utils.app_config import shift_left_dir



class TestPipelineCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  shift_left_dir +  "/it-config.yaml"

    def test_report_command_success(self):
        """Test successful execution of the report command"""
       
        runner = CliRunner()
        result = runner.invoke(app, ['report', 'p1_fct_order'])
        assert result.exit_code == 0
        assert "p1_fct_order" in result.stdout
        assert "int_p1_table_1" in result.stdout

    def test_report_command_error(self):
        """Test error handling when pipeline data cannot be retrieved"""

        runner = CliRunner()
        result = runner.invoke(app, ['report', 'non_existent_table'])
        assert result.exit_code == 1
        assert "Table not found" in result.stdout


if __name__ == '__main__':
    unittest.main()