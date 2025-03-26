import pytest
import unittest
from typer.testing import CliRunner
from shift_left.cli_commands.pipeline import app
from shift_left.core.pipeline_mgr import walk_the_hierarchy_for_report_from_table
from unittest.mock import patch, MagicMock
import logging
import os
from pathlib import Path

from shift_left.cli_commands.project import app

@pytest.fixture(autouse=True)
def mock_environment(tmp_path):
    """Mock environment variables and configuration"""
    # Create a temporary config file
    config_file = tmp_path / "config.yaml"
    config_file.write_text("""
app:
  logging: INFO
    """)
    
    with patch.dict(os.environ, {
        'PIPELINES': '/path/to/pipelines',
        'CONFIG_FILE': str(config_file)
    }):
        yield

@pytest.fixture
def mock_pipeline_data():
    return {
        "table_name": "test_table",
        "type": "fact",
        "base_path": "pipelines/test_table",
        "ddl_path": "pipelines/test_table/sql-scripts/ddl_test_table.sql",
        "dml_path": "pipelines/test_table/sql-scripts/dml_test_table.sql",
        "compute_pool_id": "cnp",
        "parents": [
            {
                "table_name": "parent_table1",
                "base_path": "pipelines/parent_table1",
                "ddl_path": "pipelines/parent_table1/sql-scripts/ddl_parent_table1.sql",
                "dml_path": "pipelines/parent_table1/sql-scripts/dml_parent_table1.sql",
                "parents": []
            }
        ],
        "children": []
    }

class TestPipelineCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  str(Path(__file__).parent /  "config.yaml")

    def test_report_command_success(self, mock_pipeline_data):
        """Test successful execution of the report command"""
        with patch('shift_left.cli_commands.pipeline.walk_the_hierarchy_for_report_from_table') as mock_walk:
            mock_walk.return_value = mock_pipeline_data
            runner = CliRunner()
            result = runner.invoke(app, ['report', 'test_table'])
            assert result.exit_code == 0
            assert "test_table" in result.stdout
            assert "parent_table1" in result.stdout
            mock_walk.assert_called_once_with('test_table')

    def test_report_command_error(self, mock_pipeline_data):
        """Test error handling when pipeline data cannot be retrieved"""
        with patch('shift_left.cli_commands.pipeline.walk_the_hierarchy_for_report_from_table') as mock_walk:
            mock_walk.side_effect = Exception("Pipeline definition not found")
            runner = CliRunner()
            result = runner.invoke(app, ['report', 'non_existent_table'])
            
            assert result.exit_code == 1
            assert "Error: Pipeline definition not found" in result.stdout
            mock_walk.assert_called_once_with('non_existent_table')
