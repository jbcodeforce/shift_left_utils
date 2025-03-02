import pytest
from unittest.mock import patch
import os
from typer.testing import CliRunner

from shift_left.cli_commands.table import app

runner = CliRunner()

def test_init_table():

    result = runner.invoke(app, ["init", "src_table_5", "./tmp/project_test/pipelines/data_product_1/sources"])
    assert result.exit_code == 0
    assert "table_5" in result.stdout
    assert "table table_5" in result.stdout
    assert os.path.exists("./tmp/project_test/pipelines/data_product_1/sources/table_5")
    assert os.path.exists("./tmp/project_test/pipelines/data_product_1/sources/table_5/Makefile")


def test_build_inventory():

    result = runner.invoke(app, ["build_inventory",  "../../examples/flink-project/pipelines/"])
    assert result.exit_code == 0
    assert os.path.exists("./tmp/project_test/pipelines/inventory.json")
