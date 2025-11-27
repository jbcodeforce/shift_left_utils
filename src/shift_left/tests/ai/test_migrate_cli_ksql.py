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

"""
Test migration cli to test ksql migration
"""
class TestKsqlMigration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["SRC_FOLDER"] = str(data_dir / "ksql-project/sources")
        os.environ["STAGING"] = str(data_dir / "flink-project/staging/ut")
        os.environ["CONFIG_FILE"] =  str(data_dir / "config-ccloud.yaml")

    def test_migrate_basic_table(self):
        runner = CliRunner()
        try :
            result = runner.invoke(app, ['table', 'migrate', 'splitter', os.getenv('SRC_FOLDER','.') + '/splitter.ksql', '--source-type', 'ksql', '--product-name', 'basic'])
            assert result.exit_code == 0
        except Exception as e:
            print(e)
            assert False

if __name__ == '__main__':
    unittest.main()
