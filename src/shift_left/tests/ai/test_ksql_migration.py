"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
from typing import List
import sys
import pytest
from shift_left.core.utils.ksql_code_agent import KsqlToFlinkSqlAgent

class TestKsqlMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "ksql-project")
        os.makedirs(os.environ["STAGING"], exist_ok=True)   
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def _list_ksql_files(self) -> List[str]:
        src_folder = os.environ["SRC_FOLDER"]
        return [f for f in os.listdir(src_folder) if f.endswith(".ksql")]

    def _process_one_ksql_file(self, ksql_file: str):
        src_folder = os.environ["SRC_FOLDER"]
        ksql_src_file = src_folder + "/" + ksql_file
        with open(ksql_src_file, "r") as f:
            ksql_content = f.read()
        agent = KsqlToFlinkSqlAgent()
        flink_content = agent.translate_from_ksql_to_flink_sql(ksql_content)
        print(flink_content)
        with open(os.environ["STAGING"] + "/data_product/" + ksql_file.replace(".ksql", ".sql"), "w") as f:
            f.write(flink_content)
        return flink_content
    def _test_ksql_table_declaration_migration(self):
        print("Should generate a flink ddl file in the staging folder")
        print("It may take some time....")
        ksql_src_file = "ddl-a.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def _test_ksql_table_with_from_group_by_migration(self):
        print("Should generate a flink dml file in the staging folder")
        print("It may take some time....")
        ksql_src_file = "ddl-b.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def test_ksql_stream_to_table_migration(self):
        ksql_src_file = "ddl-c.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def test_ksql_where_clause_migration(self):
        ksql_src_file = "ddl-d.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def test_ksql_e(self):
        ksql_src_file = "ddl-e.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None
    
    def test_ksql_f(self):
        ksql_src_file = "ddl-f.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def test_ksql_g(self):
        ksql_src_file = "ddl-g.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def test_ksql_h(self):
        ksql_src_file = "ddl-h.ksql"
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

    def test_ksql_i(self):  
        ksql_src_file = "ddl-i.ksql"    
        content=self._process_one_ksql_file(ksql_src_file)
        assert content is not None

if __name__ == '__main__':
    unittest.main()