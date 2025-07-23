"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
from typing import List
from shift_left.core.utils.translator_to_flink_sql import KsqlTranslatorToFlinkSqlAgent
from shift_left.core.process_src_tables import _process_ksql_sql_file
from unittest.mock import patch

class TestKsqlMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["CONFIG_FILE"] = str(cls.data_dir / "config-ccloud.yaml")
        os.environ["STAGING"] = str(cls.data_dir / "ksql-project/staging/ut")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "ksql-project/sources")
        os.makedirs(os.environ["STAGING"], exist_ok=True)   

    def setUp(self):
        print("Should generate a flink ddl file in the staging/ut folder")
        print("It may take some time....")

    def tearDown(self):
        pass

    # -- private methods for testing--
    def _list_ksql_files(self) -> List[str]:
        src_folder = os.environ["SRC_FOLDER"]
        return [f for f in os.listdir(src_folder) if f.endswith(".ksql")]

    
    # -- test methods --
    @patch('builtins.input')
    def test_ksql_table_declaration_migration(self, mock_input):
        ksql_src_file = "ddl-basic-table.ksql"
        mock_input.return_value = "n"
        ddl, dml = _process_ksql_sql_file(table_name="basic_table", 
            ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
        staging_target_folder=os.environ["STAGING"])
        assert os.path.exists(os.environ["STAGING"] + "/basic_table/sql-scripts/ddl.basic_table.sql")
        assert os.path.exists(os.environ["STAGING"] + "/basic_table/sql-scripts/dml.basic_table.sql")

    @patch('builtins.input')
    def test_ksql_table_with_latest_offset(self, mock_input):
        ksql_src_file = "ddl-latest-offset-table.ksql"
        mock_input.return_value = "n"
        ddl, dml = _process_ksql_sql_file(table_name="latest_offset", 
        ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
        staging_target_folder=os.environ["STAGING"])
        assert os.path.exists(os.environ["STAGING"] + "/latest_offset/sql-scripts/ddl.latest_offset.sql")
        assert os.path.exists(os.environ["STAGING"] + "/latest_offset/sql-scripts/dml.latest_offset.sql")

    def _test_ksql_filtering(self):
        ksql_src_file = "ddl-filtering.ksql"
        ddl, dml = _process_ksql_sql_file(table_name="filtering", 
                                        ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
                                        staging_target_folder=os.environ["STAGING"])
        assert ddl is not None
        assert dml is not None

    def _test_ksql_map_location_migration(self):
        print("test_ksql_map_location_migration")
        ksql_src_file = "ddl-map_substr.ksql"
        ddl, dml = _process_ksql_sql_file(table_name="map_location", 
                                        ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
                                        staging_target_folder=os.environ["STAGING"])
        assert ddl is not None
        assert dml is not None

    def _test_ksql_bigger_file(self):
        ksql_src_file = "ddl-bigger-file.ksql"
        ddl, dml = _process_ksql_sql_file(table_name="equipment", 
        ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
        staging_target_folder=os.environ["STAGING"])
        assert ddl is not None
        assert dml is not None

    def _test_ksql_g(self):
        ksql_src_file = "ddl-g.ksql"
        ddl, dml = _process_ksql_sql_file(table_name="g", 
        ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
        staging_target_folder=os.environ["STAGING"])
        assert ddl is not None
        assert dml is not None



if __name__ == '__main__':
    unittest.main()