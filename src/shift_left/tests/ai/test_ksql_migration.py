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
import shutil

class TestKsqlMigrations(unittest.TestCase):
    """
    Test the ksql migration to Flink SQLs.
    """

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["CONFIG_FILE"] = str(cls.data_dir / "config-ccloud.yaml")
        os.environ["STAGING"] = str(cls.data_dir / "ksql-project/staging/ut")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "ksql-project/sources")
        shutil.rmtree(os.environ["STAGING"], ignore_errors=True)
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
    def _test_1_basic_table(self, mock_input):
        """
        Test a basic table ksql create table migration. 
        The table BASIC_TABLE_STREAM will be used to other tables.
        """
        ksql_src_file = "ddl-basic-table.ksql"
        mock_input.return_value = "y"
        ddl, dml = _process_ksql_sql_file(table_name="BASIC_TABLE_STREAM", 
                                          ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
                                          staging_target_folder=os.environ["STAGING"]) 
        assert ddl is not None
        assert os.path.exists(os.environ["STAGING"] + "/basic_table_stream/sql-scripts/ddl.basic_table_stream.sql")
        assert os.path.exists(os.environ["STAGING"] + "/basic_table_stream/sql-scripts/dml.basic_table_stream.sql")

    @patch('builtins.input')
    def test_2_kpi_config_table_with_latest_offset(self, mock_input):
        ksql_src_file = "ddl-kpi-config-table.ksql"
        mock_input.return_value = "n"
        ddl, dml = _process_ksql_sql_file(table_name="KPI_CONFIG_TABLE", 
                ksql_src_file=os.environ["SRC_FOLDER"] + "/" + ksql_src_file, 
                staging_target_folder=os.environ["STAGING"])
        assert ddl is not None
        assert dml is not None
        assert os.path.exists(os.environ["STAGING"] + "/kpi_config_table/sql-scripts/ddl.kpi_config_table.sql")
        assert os.path.exists(os.environ["STAGING"] + "/kpi_config_table/sql-scripts/dml.kpi_config_table.sql")

    @patch('builtins.input')
    def test_3_ksql_filtering(self, mock_input):
        """
        Test a filtering ksql create table migration.
        The table FILTERING will be used to other tables.
        """
        mock_input.return_value = "n"
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