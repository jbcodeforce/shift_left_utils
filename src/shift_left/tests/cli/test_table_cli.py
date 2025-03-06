import unittest
import pathlib
from unittest.mock import patch
import os
import shutil
from typer.testing import CliRunner

from shift_left.cli_commands.table import app

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
        temp_dir = os.getenv("STAGING") + "/data_product_1"
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    def test_init_table(self):
        runner = CliRunner()
        result = runner.invoke(app, ["init", "src_table_5", os.getenv("STAGING") + "/data_product_1/sources"])
        assert result.exit_code == 0
        assert "table_5" in result.stdout
        assert os.path.exists( os.getenv("STAGING") + "/data_product_1/sources/table_5")
        assert os.path.exists( os.getenv("STAGING") + "/data_product_1/sources/table_5/Makefile")


    def test_build_inventory(self):
        runner = CliRunner()
        result = runner.invoke(app, ["build-inventory", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/inventory.json")

    def test_search_parents_of_table(self):
        runner = CliRunner()
        result = runner.invoke(app, ["search-source-dependencies", os.getenv("SRC_FOLDER") + "/facts/a.sql", os.getenv("SRC_FOLDER")])
        assert result.exit_code == 0
        print(result)

    def test_update_makefile(self):
        runner = CliRunner()
        result = runner.invoke(app, ["update-makefile", "src_table_2", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/sources/p1/src_table_2/Makefile")

if __name__ == '__main__':
    unittest.main()