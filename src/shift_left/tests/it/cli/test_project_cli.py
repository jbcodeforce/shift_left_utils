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
from it.BaseIT import _run_integration_tests, _SKIP_MSG, IntegrationTestCase


# for unit test integrity and preparations
from shift_left.core.table_mgr import get_or_create_inventory
from shift_left.core.pipeline_mgr import delete_all_metada_files, build_all_pipeline_definitions


_RUN_IT = _run_integration_tests()
@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestProjectCLI(IntegrationTestCase):
    """
    Focus on tests that need to go to CC control plane. init project for example
    can be tested in unit tests.
    """


    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        get_or_create_inventory(os.environ["PIPELINES"])
        delete_all_metada_files(os.environ["PIPELINES"])
        build_all_pipeline_definitions(os.environ["PIPELINES"])


    @classmethod
    def tearDownClass(cls):
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_list_topics(self):

        temp_dir = pathlib.Path(__file__).parent.parent.parent /  "../tmp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = temp_dir / "topic_list.txt"
        if os.path.exists(temp_file):
            os.remove(temp_file)
        result = self.runner.invoke(app, [ "list-topics", str(temp_dir)])
        print(result.stdout)
        assert result.exit_code == 0
        assert os.path.exists(temp_dir / "topic_list.txt")

    def test_list_environments(self):
        result = self.runner.invoke(app, [ "list-environments"])
        print(result.stdout)
        assert result.exit_code == 0

    def test_list_compute_pools(self):
        result = self.runner.invoke(app, ["list-compute-pools"])
        print(result)
        assert result.exit_code == 0
        print(result.stdout)

    def test_list_statements(self):
        result = self.runner.invoke(app, ["get-statement-list", os.environ["SL_FLINK_COMPUTE_POOL_ID"]])
        print(result)
        assert result.exit_code == 0
        print(result.stdout)

    def test_clean_completed_failed_statements(self):
        try:
            result = self.runner.invoke(app, [ "housekeep-statements"])
            print(result)
            assert "Clean statements starting" in result.stdout
        except Exception as e:
            self.fail(f"should not have execption: {e}")


    def test_list_modified_files(self):
        project_path =  str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent)
        result = self.runner.invoke(app, [ "list-modified-files", "develop", "--project-path", project_path, "--file-filter", "sql", "--since", "2025-12-08"])
        print(result.stdout)
        assert result.exit_code == 0
        print(result.stdout)

    def test_list_impacted_tables(self):
        project_path =  str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent)
        result = self.runner.invoke(app, [ "list-impacted-tables", "--project-path", project_path])
        print(result.stdout)
        assert result.exit_code == 0
        print(result.stdout)

    def test_assess_unused_tables(self):
        project_path =  str(pathlib.Path(__file__).parent.parent.parent.parent.parent.parent)
        result = self.runner.invoke(app, [ "assess-unused-tables"])
        print(result.stdout)
        assert result.exit_code == 0
        print(result.stdout)


if __name__ == '__main__':
    unittest.main()
