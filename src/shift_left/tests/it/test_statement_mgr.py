"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import json 
import pathlib
#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-all.yaml")
#os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")

import  shift_left.core.statement_mgr as sm 


class TestStatementManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        #

       
    
    # ---- Statement related  apis tests ------------------- 
    def _test_1_create_src_table(self):
        flink_statement_file_path = os.getenv("PIPELINES") + "/sources/p2/src_a/sql-scripts/ddl.src_p2_a.sql"
        result = sm.build_and_deploy_flink_statement_from_sql_content(flink_statement_file_path)
        assert result["status"] == "success"
        print(result)

    def _test_2_get_statement_list(self):
        l = sm.get_statement_list()
        assert l
        assert len(l) >= 1

    def _test_3_get_statement_info(self):
        statement_info = sm.get_statement_info("dev-ddl-src-p2-a")
        assert statement_info
        print(statement_info.model_dump_json(indent=3))
        statement_info = sm.get_statement_info("dev-dummy_statement")
        assert statement_info == None


    def _test_4_delete_statement(self):
        statementInfo = sm.get_statement_info("dev-ddl-src-p2-a")
        assert statementInfo
        sm.delete_statement_if_exists("dev-ddl-src-p2-a")
        statement = sm.get_statement_info("dev-ddl-src-p2-a")
        assert statement == None

    def _test_5_execute_show_create_table(self):
        response = sm.show_flink_table_structure("src_p2_a")
        assert response
        assert "CREATE TABLE" in response
        statement = sm.get_statement_info("show-src-p2-a")
        assert statement == None

    def test_6_execute_drop_table(self):
        response = sm.drop_table("src_p2_a")
        assert response


if __name__ == '__main__':
    unittest.main()