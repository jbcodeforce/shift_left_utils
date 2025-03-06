import unittest
import pathlib
import os
import shutil
from typer.testing import CliRunner

from shift_left.cli_commands.project import app

class TestTableCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "src-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent /  "config.yaml")

    @classmethod
    def tearDownClass(cls):
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_init_project(self):
        runner = CliRunner()
        temp_dir = pathlib.Path(__file__).parent /  "../tmp"
        print(temp_dir)
        result = runner.invoke(app, [ "project_test_via_cli",  str(temp_dir)])
        print(result.stdout)
        assert result.exit_code == 0
        assert "Project project_test_via_cli created in " in result.stdout
        assert os.path.exists(temp_dir / "project_test_via_cli")
        assert os.path.exists(temp_dir / "project_test_via_cli/pipelines")

    def test_list_topics(self):
        runner = CliRunner()
        result = runner.invoke(app, [ "list-topics"])
        print(result.stdout)
        

if __name__ == '__main__':
    unittest.main()