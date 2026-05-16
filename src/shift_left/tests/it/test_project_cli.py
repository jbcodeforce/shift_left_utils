"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
from typer.testing import CliRunner
os.environ["SL_CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
from shift_left.cli_commands.project import app
from it.BaseIT import _run_integration_tests, _SKIP_MSG

_RUN_IT = _run_integration_tests()
@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
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


    def test_list_topics(self):
        runner = CliRunner()
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        result = runner.invoke(app, [ "list-topics", str(temp_dir)])
        print(result.stdout)
        assert result.exit_code == 0

    def test_list_environments(self):
        runner = CliRunner()
        result = runner.invoke(app, [ "list-environments"])
        print(result.stdout)
        assert result.exit_code == 0

    def test_list_compute_pools(self):
        print("test_5: using cli project list-compute-pools")
        runner = CliRunner()
        result = runner.invoke(app, ["list-compute-pools"])
        print(result)
        assert result.exit_code == 0
        print(result.stdout)

    def test_clean_completed_failed_statements(self):
        runner = CliRunner()
        try:
            result = runner.invoke(app, [ "housekeep-statements"])
            print(result)
            assert "Clean statements starting" in result.stdout
        except Exception as e:
            self.fail(f"should not have execption: {e}")


    def test_list_modified_files(self):
        runner = CliRunner()
        project_path =  str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent)
        result = runner.invoke(app, [ "list-modified-files", "develop", "--project-path", project_path, "--file-filter", "sql", "--since", "2025-12-08"])
        print(result.stdout)
        assert result.exit_code == 0
        print(result.stdout)


if __name__ == '__main__':
    unittest.main()
