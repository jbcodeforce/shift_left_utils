"""
Copyright 2024-2025 Confluent, Inc.
"""
from time import sleep
import unittest
from typer.testing import CliRunner
import os
from pathlib import Path
os.environ["CONFIG_FILE"] = str(Path(__file__).parent.parent.parent / "config-ccloud.yaml")
from shift_left.core.utils.app_config import shift_left_dir, get_config
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
        pipeline_path = Path(os.getenv("PIPELINES")) / "facts" / "c360" / "fct_user_per_group" / PIPELINE_JSON_FILE_NAME
        assert not pipeline_path.exists(), f"{PIPELINE_JSON_FILE_NAME} not found in {pipeline_path}"
        # 4 verify build allpipeline definitions for all tables
        result = runner.invoke(app, ['pipeline', 'build-all-metadata', os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert 'Build all pipeline definitions for all tables' in result.stdout
        print(f"Validate pipeline inventory creation")
        assert pipeline_path.exists(), f"{PIPELINE_JSON_FILE_NAME} found in {pipeline_path}"
        # 6 verify build execution plan pipeline
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
       
        result = runner.invoke(app, ['pipeline', 'build-execution-plan', os.getenv("PIPELINES"), '--table-name', 'fct_user_per_group', '--compute-pool-id', compute_pool_id])
        assert result.exit_code == 0
        print(f"{result.stdout}")
        # 7 verify deploy pipeline
        compute_pool_id = get_config()["flink"]["compute_pool_id"]
       
        result = runner.invoke(app, ['pipeline', 'deploy', os.getenv("PIPELINES"), '--table-name', 'fct_user_per_group', '--compute-pool-id', compute_pool_id])
        assert result.exit_code == 0
        assert 'Deploy pipeline for table' in result.stdout
        # 8 verify undeploy pipeline
        print("Waiting 5 seconds to ensure the pipeline is deployed")
        sleep(5)
        result = runner.invoke(app, ['pipeline', 'undeploy', os.getenv("PIPELINES"), '--table-name', 'fct_user_per_group', '--no-ack'])
        assert result.exit_code == 0
        assert 'Undeploy pipeline for table' in result.stdout
        print(f"{result.stdout}")
        # 9 verify report running statements
        result = runner.invoke(app, ['pipeline', 'report-running-statements', os.getenv("PIPELINES"), '--table-name', 'fct_user_per_group'])
        assert result.exit_code == 0
        assert 'Report running statements for table' in result.stdout
        print(f"{result.stdout}")   
        
if __name__ == '__main__':
    unittest.main()