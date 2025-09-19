"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from typer.testing import CliRunner
import os
from pathlib import Path
os.environ["CONFIG_FILE"] = str(Path(__file__).parent.parent.parent / "config-ccloud.yaml")
from shift_left.core.utils.app_config import shift_left_dir
from shift_left.cli import app
from shift_left.core.utils.file_search import INVENTORY_FILE_NAME, PIPELINE_JSON_FILE_NAME


class TestHappyPathCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")

    def test_happy_path(self):
        runner = CliRunner()
        # 1 verify config is read and loaded
        result = runner.invoke(app, ['version'])
        assert result.exit_code == 0
        assert 'shift-left CLI version' in result.stdout
        print(f"Validate config is read and loaded: {result.stdout}")
        # 2 verify table inventory creation
        result = runner.invoke(app, ['table', 'build-inventory', os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert 'Table inventory created' in result.stdout 
        print(f"Validate table inventory creation")
        inventory_path = Path(os.getenv("PIPELINES")) / INVENTORY_FILE_NAME
        assert inventory_path.exists(), f"{INVENTORY_FILE_NAME} not found in {os.getenv('PIPELINES')}"
        # 3 delete all pipeline definitions
        result = runner.invoke(app, ['pipeline', 'delete-all-metadata', os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert 'Delete pipeline definitions from' in result.stdout
        print(f"Validate pipeline definitions deletion")
        pipeline_path = Path(os.getenv("PIPELINES")) / "facts" / "users" / "fct_user_per_group" / PIPELINE_JSON_FILE_NAME
        assert not pipeline_path.exists(), f"{PIPELINE_JSON_FILE_NAME} not found in {pipeline_path}"
        # 4 verify pipeline definitions for all tables
        result = runner.invoke(app, ['pipeline', 'build-all-metadata', os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert 'Build all pipeline definitions for all tables' in result.stdout
        print(f"Validate pipeline inventory creation")
        assert pipeline_path.exists(), f"{PIPELINE_JSON_FILE_NAME} found in {pipeline_path}"


if __name__ == '__main__':
    unittest.main()