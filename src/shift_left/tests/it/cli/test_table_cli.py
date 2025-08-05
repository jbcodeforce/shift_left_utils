"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
from typer.testing import CliRunner

from shift_left.cli_commands.table import app
from shift_left.core.utils.app_config import shift_left_dir

class TestTableCLI(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging")
        os.environ["CONFIG_FILE"] =  shift_left_dir +  "/it-config.yaml"

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
        assert os.path.exists( os.getenv("STAGING") + "/data_product_1/sources/src_table_5")
        assert os.path.exists( os.getenv("STAGING") + "/data_product_1/sources/src_table_5/Makefile")


    def test_build_inventory(self):
        runner = CliRunner()
        result = runner.invoke(app, ["build-inventory", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/inventory.json")

    def test_search_parents_of_table(self):
        runner = CliRunner()
        result = runner.invoke(app, ["search-source-dependencies", os.getenv("SRC_FOLDER") + "/facts/p5/fct_users.sql", os.getenv("SRC_FOLDER")])
        assert result.exit_code == 0
        print(result)

    def test_update_makefile(self):
        runner = CliRunner()
        result = runner.invoke(app, ["update-makefile", "src_p2_a", os.getenv("PIPELINES")])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/sources/p2/src_a/Makefile")

    def test_init_unit_tests(self):
        runner = CliRunner()
        result = runner.invoke(app, ["init-unit-tests", "a"])
        assert result.exit_code == 0
        assert os.path.exists(os.getenv("PIPELINES") + "/intermediates/p2/a/tests")
        assert os.path.exists(os.getenv("PIPELINES") + "/intermediates/p2/a/tests/test_definitions.yaml")

    def test_run_unit_tests(self):
        runner = CliRunner()
        result = runner.invoke(app, ["run-test-suite", "a"])
        assert result.exit_code == 0
        assert "Unit tests execution" in result.stdout

if __name__ == '__main__':
    unittest.main()