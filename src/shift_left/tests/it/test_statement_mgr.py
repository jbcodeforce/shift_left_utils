"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")

import  shift_left.core.statement_mgr as sm 
from shift_left.core.models.flink_statement_model import FlinkStatementNode, StatementInfo
from shift_left.core.utils.app_config import get_config

class TestStatementManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        # Remove cached statement list file if it exists
        if os.path.exists(sm.STATEMENT_LIST_FILE):
            os.remove(sm.STATEMENT_LIST_FILE)
        
    
    # ---- Statement public apis related tests ------------------- 
    def test_1_create_src_table(self):
        config = get_config()
        flink_statement_file_path = os.getenv("PIPELINES") + "/sources/p2/src_a/sql-scripts/ddl.src_p2_a.sql"
        node_to_process = FlinkStatementNode(
            table_name="fct_order",
            ddl_ref=flink_statement_file_path,
            ddl_statement_name="dev-ddl-src-p2-a",
            compute_pool_id=config['flink']['compute_pool_id'],
            product_name="p1"
        )
        result = sm.build_and_deploy_flink_statement_from_sql_content(node_to_process, flink_statement_file_path)
        print(result)
        assert result.status.phase == "COMPLETED"


    def test_2_get_statement_list(self):
        l = sm.get_statement_list()
        assert l
        assert len(l) >= 1
        assert isinstance(l["dev-ddl-src-p2-a"], StatementInfo)

    def test_3_get_statement_info(self):
        statement_info = sm.get_statement_info("dev-ddl-src-p2-a")
        assert statement_info
        print(statement_info.model_dump_json(indent=3))
        statement_info = sm.get_statement_info("dev-dummy_statement")
        assert statement_info == None


    def test_4_delete_statement(self):
        statementInfo = sm.get_statement_info("dev-ddl-src-p2-a")
        assert statementInfo
        sm.delete_statement_if_exists("dev-ddl-src-p2-a")
        statement = sm.get_statement_info("dev-ddl-src-p2-a")
        assert statement == None

    def test_5_execute_show_create_table(self):
        response = sm.show_flink_table_structure("src_p2_a")
        assert response
        assert "CREATE TABLE" in response
        statement = sm.get_statement_info("show-src-p2-a")
        assert statement != None

    def test_6_execute_drop_table(self):
        response = sm.drop_table("src_p2_a")
        assert response


if __name__ == '__main__':
    unittest.main()