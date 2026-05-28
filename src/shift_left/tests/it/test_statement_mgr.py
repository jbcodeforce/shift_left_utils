"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import json
import pathlib
os.environ["SL_CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")

from shift_left.cli import app
import  shift_left.core.statement_mgr as sm
from shift_left.core.models.flink_statement_model import FlinkStatementNode, StatementInfo
from shift_left.core.utils.app_config import get_config
from typer.testing import CliRunner


def get_env_for_cli():
    """Get all environment variables that should be passed to CliRunner."""
    # Start with current environment
    env = dict(os.environ)
    return env
class TestStatementManager(unittest.TestCase):

    data_dir = None

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        # Remove cached statement list file if it exists
        if os.path.exists(sm.STATEMENT_LIST_FILE):
            os.remove(sm.STATEMENT_LIST_FILE)
        cls.runner = CliRunner()
        cls.env = get_env_for_cli()
        cls.env["SL_CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
        cls.env["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        cls.runner.invoke(app, ['table', 'build-inventory'], env=cls.env)
        cls.runner.invoke(app, ['pipeline', 'delete-all-metadata'], env=cls.env)
        cls.runner.invoke(app, ['pipeline', 'build-all-metadata'], env=cls.env)


    # ---- Statement public apis related tests -------------------
    def test_1_deploy_one_seed_table(self):
        result = self.runner.invoke(app, ['pipeline', 'deploy', '--table-name', 'sl_raw_tenants'], env=self.env)
        print(result)



    def test_2_get_statement_list(self):
        l = sm.get_statement_list()
        assert l
        assert len(l) >= 1
        assert isinstance(l["dev-usw2-common-dml-raw-tenants"], StatementInfo)


    def test_3_delete_statement(self):
        statementInfo = sm.get_statement_info("dev-usw2-common-dml-sl-raw-tenants")
        assert statementInfo
        sm.delete_statement_if_exists("dev-usw2-common-dml-sl-raw-tenants")
        statement = sm.get_statement_info("dev-usw2-common-dml-sl-raw-tenants")
        assert statement == None

    def _test_4_execute_show_create_table(self):
        response = sm.show_flink_table_structure("sl_raw_tenants")
        assert response
        assert "CREATE TABLE" in response
        statement = sm.get_statement_info("show-sl_raw_tenants")
        assert statement != None

    def test_5_execute_drop_table(self):
        response = sm.drop_table("sl_raw_tenants")
        assert response
        print(response)


if __name__ == '__main__':
    unittest.main()
