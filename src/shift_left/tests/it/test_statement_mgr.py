"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")

import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
import  shift_left.core.statement_mgr as sm 


class TestStatementManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
    
    # ---- Statement related  apis tests ------------------- 
    def test_1_create_src_table(self):
        flink_statement_file_path = os.getenv("PIPELINES") + "/sources/p2/a/sql-scripts/ddl.src_a.sql"
        result = sm.deploy_flink_statement(flink_statement_file_path)
        assert result
        print(result)
    
    def test_1_wrong_statement_name(self):
        flink_statement_file_path = os.getenv("PIPELINES") + "/sources/p2/a/sql-scripts/ddl.src_a.sql"
        result = sm.deploy_flink_statement(flink_statement_file_path= flink_statement_file_path,
                                           statement_name = "ddl.src-a")
        assert result == None

    def test_2_search_non_existant_statement(self):
        statement_dict = sm.get_statement_list()
        assert statement_dict == None
        assert not statement_dict["dummy"]

    def test_2_get_statement_list(self):
        l = sm.get_statement_list()
        assert l
        assert len(l) >= 1


    def test_3_execute_show_create_table_then_delete_statement(self):
        config= get_config()
        sql_path = os.getenv("PIPELINES") + "/intermediates/p1/int_table_1/tests/show_create_table.sql"
        statement = sm.deploy_flink_statement(sql_path, None, "show-table", config)
        assert statement
        print(statement)
        statement_dict = sm.get_statement_list()
        assert statement_dict
        print(f"\n -- {statement_dict["show-table"]}")
        response = sm.delete_statement_if_exists("show_table")
        assert response


if __name__ == '__main__':
    unittest.main()