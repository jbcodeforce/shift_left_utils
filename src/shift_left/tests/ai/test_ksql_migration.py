"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch
import pathlib
import os
import json
from typing import List
import shutil
from unittest.mock import patch, MagicMock
data_dir = pathlib.Path(__file__).parent.parent / "data"
os.environ["CONFIG_FILE"] = str(data_dir.parent / "config-ccloud.yaml")
import shift_left.core.utils.app_config as app_config
from shift_left.ai.process_src_tables import migrate_one_file
from ai.utilities import compare_files_unordered
# Mock validate_config to avoid SystemExit
app_config.validate_config = MagicMock()

class TestKsqlMigrations(unittest.TestCase):
    """
    Test the ksql migration to Flink SQLs.
    """

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        cls.src_folder = str(cls.data_dir / "ksql-project/sources")
        cls.staging = str(cls.data_dir / "flink-project/staging/ut")
        cls.product_name = "basic"
        cls.file_reference_dir = str(cls.data_dir / "ksql-project/flink-references")
        os.environ["STAGING"] = cls.staging
        shutil.rmtree(os.environ["STAGING"], ignore_errors=True)
        os.makedirs(os.environ["STAGING"], exist_ok=True)

    def tearDown(self):
        pass

    # -- test methods --
    @patch('builtins.input')
    def test_ksql_splitting_tutorial(self, mock_input):
        """
        https://developer.confluent.io/confluent-tutorials/splitting/ksql/
        split it into substreams based on a field in each event
        """
        mock_input.return_value = "y"
        migrate_one_file(table_name="acting_events",
                        sql_src_file=self.src_folder + "/splitting_tutorial.ksql",
                        staging_target_folder=self.staging,
                        product_name="tutorial",
                        source_type="ksql",
                        validate=False)
        assert os.path.exists(self.staging + "/tutorial" + "/acting_events")
        assert os.path.exists(self.staging + "/tutorial" + "/acting_events/sql-scripts/ddl.acting_events.sql")
        assert os.path.exists(self.staging + "/tutorial" + "/acting_events/sql-scripts/dml.acting_events.sql")
        reference_file = self.file_reference_dir + "/tutorial/acting_events/sql-scripts/ddl.acting_events.sql"
        created_file = self.staging + "/tutorial" + "/acting_events/sql-scripts/ddl.acting_events.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(result)
        assert result['all_reference_lines_present']
        assert result['match_percentage'] == 100

    @patch('builtins.input')
    def test_ksql_merge_tutorial(self, mock_input):
        """
        https://developer.confluent.io/confluent-tutorials/merging/ksql/
        merge mutliple streams into one
        """
        mock_input.return_value = "y"
        migrate_one_file(table_name="all_songs",
                        sql_src_file=self.src_folder + "/merge_tutorial.ksql",
                        staging_target_folder=self.staging,
                        product_name="tutorial",
                        source_type="ksql",
                        validate=False)
        assert os.path.exists(self.staging + "/tutorial/all_songs")
        assert os.path.exists(self.staging + "/tutorial/all_songs/sql-scripts/ddl.all_songs.sql")
        assert os.path.exists(self.staging + "/tutorial/all_songs/sql-scripts/dml.all_songs.sql")

        reference_file = self.file_reference_dir + "/tutorial/all_songs/sql-scripts/ddl.all_songs.sql"
        created_file = self.staging + "/tutorial" + "/all_songs/sql-scripts/ddl.all_songs.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(result)
        assert result['all_reference_lines_present']
        assert result['match_percentage'] == 100


    @patch('builtins.input')
    def test_ksql_aggregation(self, mock_input):
        mock_input.return_value = "n"
        migrate_one_file(table_name="orders",
                sql_src_file= self.src_folder + "/aggregation.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)
        assert os.path.exists(self.staging + "/" + self.product_name + "/orders/sql-scripts/ddl.orders.sql")
        assert os.path.exists(self.staging + "/" + self.product_name + "/orders/sql-scripts/ddl.daily_spend.sql")
        assert os.path.exists(self.staging + "/" + self.product_name + "/orders/sql-scripts/dml.orders.sql")
        reference_file = self.file_reference_dir + "/aggregation/sql-scripts/ddl.orders.sql"
        created_file = self.staging + "/" + self.product_name + "/orders/sql-scripts/ddl.orders.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(f"-"*40)
        print(type(result))
        print(result)
        print(f"result: {json.dumps(result, indent=4)}")
        assert result['match_percentage'] >= 80
        reference_file = self.file_reference_dir + "/aggregation/sql-scripts/dml.aggregation.sql"
        created_file = self.staging + "/" + self.product_name + "/orders/sql-scripts/dml.orders.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(f"-"*40)
        print(result)
        print(f"dml result: {json.dumps(result, indent=4)}")
        assert result['match_percentage'] >= 70

    @patch('builtins.input')
    def test_ksql_bigger_file(self, mock_input):
        mock_input.return_value = "n"
        migrate_one_file(table_name="equipment",
                sql_src_file= self.src_folder + "/ddl-bigger-file.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)
        assert os.path.exists(self.staging + "/" + self.product_name + "/equipment/sql-scripts/ddl.equipment.sql")
        assert os.path.exists(self.staging + "/" + self.product_name + "/equipment/sql-scripts/dml.equipment.sql")
        dml_file_path = self.staging + "/" + self.product_name + "/equipment/sql-scripts/dml.equipment.sql"
        with open(dml_file_path, "r") as f:
            dml_content = f.read()
        assert "EQUIPMENT_STAGE_STREAM" in dml_content
        assert "ERRORS_TX" in dml_content
        assert "DISCARDS_RX" in dml_content


    @patch('builtins.input')
    def test_2_kpi_config_table_with_latest_offset(self, mock_input):
        mock_input.return_value = "n"
        migrate_one_file(table_name="KPI_CONFIG_TABLE",
                sql_src_file= self.src_folder + "/ddl-kpi-config-table.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)
        assert os.path.exists(self.staging + "/" + self.product_name + "/all_songs/sql-scripts/ddl.all_songs.sql")



    @patch('builtins.input')
    def test_ksql_filtering(self, mock_input):
        """
        Test a filtering ksql create table migration.
        The table FILTERING will be used to other tables.
        """
        mock_input.return_value = "n"
        migrate_one_file(table_name="filtering",
                sql_src_file= self.src_folder + "/ddl-filtering.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)
        assert os.path.exists(self.staging + "/" + self.product_name + "/filtering/sql-scripts/ddl.orders.sql")
        assert os.path.exists(self.staging + "/" + self.product_name + "/filtering/sql-scripts/dml.filtering.sql")
        reference_file = self.file_reference_dir + "/filtering/sql-scripts/ddl.filtered_orders.sql"
        created_file = self.staging + "/" + self.product_name + "/filtering/sql-scripts/ddl.filtered_orders.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(f"-"*40)
        print(result)
        print(f"dml result: {json.dumps(result, indent=4)}")
        assert result['match_percentage'] >= 70

    @patch('builtins.input')
    def test_11_w2_processing(self, mock_input):
        """
        Test a filtering ksql create table migration.
        The table FILTERING will be used to other tables.
        """
        mock_input.return_value = "n"
        migrate_one_file(table_name="w2_processing",
                sql_src_file= self.src_folder + "/w2_processing.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)


    def _test_ksql_map_location_migration(self):
        print("test_ksql_map_location_migration")
        migrate_one_file(table_name="map_location",
                sql_src_file= self.src_folder + "/ddl-map_substr.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)



    def test_ksql_g(self):
        ksql_src_file = "ddl-g.ksql"
        migrate_one_file(table_name="table_struct",
                sql_src_file= self.src_folder + "/ddl-g.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)


    def test_ksql_geolocation(self):
        ksql_src_file = "ddl-geo.ksql"
        migrate_one_file(table_name="geolocation",
                sql_src_file= self.src_folder + "/ddl-geo.ksql",
                staging_target_folder=self.staging,
                product_name=self.product_name,
                source_type="ksql",
                validate=False)

if __name__ == '__main__':
    unittest.main()
