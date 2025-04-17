"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock, call
import pathlib
import os
from unittest.mock import ANY

os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config-all.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../../data/flink-project/pipelines")
from shift_left.core.flink_statement_model import Statement
from shift_left.core.utils.app_config import get_config

from shift_left.core.test_mgr import (
    _load_test_suite_definition, 
    SLTestDefinition, 
    SLTestCase, 
    SLTestData, 
    Foundation, 
    execute_one_test,
    _create_test_tables,
    _run_foundations,
    _change_table_names_for_test_in_sql_content
)


class TestTestManager(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data" 
    
    def test_should_load_test_definition(self):
        print("test_should_load_test_definition")
        td1 = SLTestData(table_name= "tb1", sql_file_name="ftb1")
        o1 = SLTestData(table_name= "tbo1", sql_file_name="to1")
        tc1 = SLTestCase(name="tc1", inputs=[td1], outputs=[o1])
        fds = [Foundation(table_name="tb1", ddl_for_test="ddl-tb1")]
        ts = SLTestDefinition(foundations=fds, test_suite=[tc1])
        assert ts
        print(ts.model_dump_json(indent=3))

    def test_load_test_definition_for_fact_table(self):
        print("test_load_test_definition_for_fact_table")
        table_name= os.getenv("PIPELINES") + "/facts/p1/fct_order"
        test_suite_def = _load_test_suite_definition(table_name)
        assert test_suite_def
        print(test_suite_def.model_dump_json(indent=3))
        assert len(test_suite_def.test_suite) == 2
        assert len(test_suite_def.foundations) == 3
        assert test_suite_def.test_suite[0].name == "test_case_1"
        assert test_suite_def.test_suite[1].name == "test_case_2"
        assert test_suite_def.foundations[0].table_name == "int_table_1"
        assert test_suite_def.foundations[1].table_name == "int_table_2"
        assert test_suite_def.foundations[2].table_name == "fct_order"
        
        
    def test_change_table_name_for_test(self):
        print("test_change_table_name_for_test")
        sql_file_path= os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts/ddl.p1_fct_order.sql"
        sql_content = _change_table_names_for_test_in_sql_content(sql_file_path)
        print(sql_content)
        assert "p1_fct_order_ut" in sql_content
        sql_file_path= os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-scripts/dml.p1_fct_order.sql"
        sql_content = _change_table_names_for_test_in_sql_content(sql_file_path)
        print(sql_content)
        assert "p1_fct_order_ut" in sql_content
        assert "int_p1_table_2_ut" in sql_content
        assert "int_p1_table_1_ut" in sql_content

    @patch('shift_left.core.test_mgr.ConfluentCloudClient')
    def test_run_foundations_should_have_3_statmeents(self, MockConfluentCloudClient):
        print("test_create_test_tables")
        mock_client_instance = MockConfluentCloudClient.return_value
        mock_client_instance.post_flink_statement.return_value =  Statement(name= "ut-fct-order")

        mock_client_instance.get_statement_info.return_value = None
        
        table_folder= os.getenv("PIPELINES") + "/facts/p1/fct_order"
        compute_pool_id = "compute_pool_id"
        statement_names=[]
        _run_foundations(table_folder, compute_pool_id, statement_names)
        assert len(statement_names) == 3
        config = get_config()
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    
        calls = [ call(compute_pool_id, "int-table-1-ut", ANY, properties),
                 call(compute_pool_id, "int-table-2-ut", ANY, properties),
                 call(compute_pool_id, "fct-order-ut", ANY, properties)
                 ]
        mock_client_instance.post_flink_statement.assert_has_calls(calls)


    def _test_execute_one_test(self):
        print("test_execute_one_test")
        table_folder= os.getenv("PIPELINES") + "/facts/p1/fct_order"
        compute_pool_id = "compute_pool_id"
        test_case_name = "test_case_1"

        result = execute_one_test(table_folder, test_case_name)
        assert result, f"Test case '{test_case_name}' executed successfully"

if __name__ == '__main__':
    unittest.main()