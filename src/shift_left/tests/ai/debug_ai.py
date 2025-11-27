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
from ai.utilities import compare_files_unordered
class TestDebugIntegrationTests(unittest.TestCase):


    def _test_ksql_migration(self):
        os.environ["PIPELINES"] =  data_dir + "/ksql-project/flink-references"
        os.environ["STAGING"] =  data_dir + "/ksql-project/staging/ut"
        os.environ["SRC_FOLDER"] =  data_dir + "/ksql-project/sources"
        runner = CliRunner()
        result = runner.invoke(app, ['table', 'migrate', 'splitter', os.getenv('SRC_FOLDER','.') + '/splitter.ksql', '--source-type', 'ksql', '--product-name', 'basic'])
        print(result.stdout)

    def _test_spark_migration(self):
        os.environ["STAGING"] =  data_dir + "/flink-project/staging/"
        if not os.getenv('SRC_FOLDER'):
            os.environ["SRC_FOLDER"] =  data_dir + "/spark-project"
        runner = CliRunner()
        result = runner.invoke(app, ['table', 'migrate', 'aggregate_insight', os.getenv('SRC_FOLDER','.') + '/facts/src_advanced_transformations.sql','--source-type', 'spark'])
        print(result.stdout)


    def _test_compare_files_unordered(self):
        reference_file = data_dir + "/ksql-project/flink-references/tutorial/all_songs/sql-scripts/ddl.all_songs.sql"
        created_file = data_dir + "/flink-project/staging/ut/tutorial/all_songs/sql-scripts/ddl.all_songs.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(result)
        assert result['all_reference_lines_present']
        assert result['match_percentage'] == 100
        reference_file = data_dir + "/ksql-project/flink-references/tutorial/all_songs/sql-scripts/ddl.all_songs_1.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(result)

if __name__ == '__main__':
    unittest.main()
