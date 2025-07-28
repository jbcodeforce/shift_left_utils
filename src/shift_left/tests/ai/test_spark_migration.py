"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
from unittest.mock import patch
from shift_left.core.utils.translator_to_flink_sql import get_or_build_sql_translator_agent
from shift_left.core.utils.app_config import get_config

"""
Taking a complex SQL statement migrates to Flink SQL.
"""

class TestDbtMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging") 
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "spark-project")
        os.makedirs(os.environ["STAGING"], exist_ok=True)   
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)

    def _process_one_spark_file(self, dbt_file: str):
        config = get_config()
        config['app']['translator_to_flink_sql_agent']='shift_left.core.utils.translator_to_flink_sql.DbtTranslatorToFlinkSqlAgent'
        src_folder = os.environ["SRC_FOLDER"]
        dbt_src_file = src_folder + "/" + dbt_file
        with open(dbt_src_file, "r") as f:
            dbt_content = f.read()
        translator_agent = get_or_build_sql_translator_agent()
        dml, ddl = translator_agent.translate_to_flink_sqls(dbt_file, dbt_content, validate=True)
        print(dml)
        print(ddl)
        assert dml is not None
        assert ddl is not None

    def setUp(self):
        pass

    def tearDown(self):
        pass

# -- test methods --
    @patch('builtins.input')
    def test_1_spark_basic_table(self, mock_input):
        """
        Test a basic table spark fact users table migration. 
        """
        dbt_file = "facts/p5/fct_users.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

if __name__ == '__main__':
    unittest.main()