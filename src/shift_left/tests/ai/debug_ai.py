from this import d
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json

data_dir=os.path.join(os.path.dirname(__file__),'..','data')
os.environ["CONFIG_FILE"] =  os.path.dirname(__file__) + "/../config-ccloud.yaml"

from shift_left.core.utils.app_config import get_config
from typer.testing import CliRunner
from shift_left.cli import app

class TestDebugIntegrationTests(unittest.TestCase):


    def _test_ksql_migration(self):
        os.environ["PIPELINES"] =  data_dir + "/ksql-project/flink-references"
        os.environ["STAGING"] =  data_dir + "/ksql-project/staging/ut"
        os.environ["SRC_FOLDER"] =  data_dir + "/ksql-project/sources"
        runner = CliRunner()
        result = runner.invoke(app, ['table', 'migrate', 'big_table', os.getenv('SRC_FOLDER','.') + '/ddl-bigger-file.ksql', os.getenv('STAGING'),'--source-type', 'ksql'])
        print(result.stdout)

    def test_spark_migration(self):
        os.environ["STAGING"] =  data_dir + "/flink-project/staging/ut"
        if not os.getenv('SRC_FOLDER'):
            os.environ["SRC_FOLDER"] =  data_dir + "/spark-project"
        runner = CliRunner()
        #result = runner.invoke(app, ['table', 'migrate', 'customer_journey', os.getenv('SRC_FOLDER','.') + '/sources/c360/src_customer_journey.sql', os.getenv('STAGING'),'--source-type', 'spark'])
        result = runner.invoke(app, ['table', 'migrate', 'aggregate_insight', os.getenv('SRC_FOLDER','.') + '/facts/fct_advanced_transformations.sql', os.getenv('STAGING'),'--source-type', 'spark'])
        print(result.stdout)


if __name__ == '__main__':
    unittest.main()