from this import d
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json

data_dir=os.path.join(os.path.dirname(__file__),'..','data')
os.environ["CONFIG_FILE"] =  data_dir + "/../config-ccloud.yaml"
os.environ["PIPELINES"] =  data_dir + "/ksql-project/flink-references"
os.environ["STAGING"] =  data_dir + "/ksql-project/staging/ut"
os.environ["SRC_FOLDER"] =  data_dir + "/ksql-project/sources"
from shift_left.core.utils.app_config import get_config
from typer.testing import CliRunner
from shift_left.cli import app

class TestDebugIntegrationTests(unittest.TestCase):


    def test_exec_plan(self):
        runner = CliRunner()
        result = runner.invoke(app, ['table', 'migrate', 'big_table', os.getenv('SRC_FOLDER','.') + '/ddl-bigger-file.ksql', os.getenv('STAGING'),'--source-type', 'ksql'])
        print(result.stdout)

if __name__ == '__main__':
    unittest.main()