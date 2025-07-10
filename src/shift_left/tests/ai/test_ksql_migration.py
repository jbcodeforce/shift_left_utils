"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
from typing import List
import sys
import pytest
from shift_left.core.utils.translator_to_flink_sql import KsqlTranslatorToFlinkSqlAgent

class TestKsqlMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["CONFIG_FILE"] = str(cls.data_dir / "config-ccloud.yaml")
        os.environ["STAGING"] = str(cls.data_dir / "ksql-project/staging")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "ksql-project/sources")
        os.makedirs(os.environ["STAGING"], exist_ok=True)   
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    # -- private methods for testing--
    def _list_ksql_files(self) -> List[str]:
        src_folder = os.environ["SRC_FOLDER"]
        return [f for f in os.listdir(src_folder) if f.endswith(".ksql")]

    def _process_one_ksql_file(self, table_name: str, ksql_file: str, validate: bool = False):
        src_folder = os.environ["SRC_FOLDER"]
        ksql_src_file = src_folder + "/" + ksql_file
        agent = KsqlTranslatorToFlinkSqlAgent()
        with open(ksql_src_file, "r") as f:
            ksql_content = f.read()
            flink_content, _ = agent.translate_to_flink_sqls(table_name, ksql_content, validate=validate)
            print(flink_content)
            with open(os.environ["STAGING"] + "/data_product/" + ksql_file.replace(".ksql", ".sql"), "w") as f:
                f.write(flink_content)
            return flink_content
    
    # -- test methods --
    def _test_ksql_table_declaration_migration(self):
        print("Should generate a flink ddl file in the staging folder")
        print("It may take some time....")
        ksql_src_file = "ddl-basic-table.ksql"
        content=self._process_one_ksql_file("basic_table", ksql_src_file, validate=True)
        assert content is not None
        assert 'create table if not exists (' in content
        assert ('kpiStatus string,' in content or 'kpistatus string,' in content)
        assert not "'topic' =" in content

    def test_ksql_table_with_latest_offset(self):
        print("Should generate a flink dml file in the staging folder")
        print("It may take some time....")
        ksql_src_file = "ddl-latest-offset-table.ksql"
        content=self._process_one_ksql_file("latest_offset", ksql_src_file, validate=True)
        assert content is not None

    def test_ksql_filtering(self):
        ksql_src_file = "ddl-filtering.ksql"
        content=self._process_one_ksql_file("filtering",ksql_src_file)
        assert content is not None

    def test_ksql_map_location_migration(self):
        ksql_src_file = "ddl-map_substr.ksql"
        content=self._process_one_ksql_file("map_location",ksql_src_file)
        assert content is not None

    def _test_ksql_bigger_file(self):
        ksql_src_file = "ddl-bigger-file.ksql"
        content=self._process_one_ksql_file("equipment",ksql_src_file)
        assert content is not None

    def _test_ksql_g(self):
        ksql_src_file = "ddl-g.ksql"
        content=self._process_one_ksql_file("",ksql_src_file)
        assert content is not None

    def _test_ksql_h(self):
        ksql_src_file = "ddl-h.ksql"
        content=self._process_one_ksql_file("",ksql_src_file)
        assert content is not None

    def _test_ksql_i(self):  
        ksql_src_file = "ddl-i.ksql"    
        content=self._process_one_ksql_file("",ksql_src_file)
        assert content is not None

if __name__ == '__main__':
    unittest.main()