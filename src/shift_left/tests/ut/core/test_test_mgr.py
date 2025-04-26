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
from shift_left.core.flink_statement_model import Statement, StatementResult, Data, OpRow

import shift_left.core.test_mgr as test_mgr

from shift_left.core.test_mgr import (
    SLTestDefinition, 
    SLTestCase, 
    SLTestData, 
    Foundation
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
        table_name= "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def
        print(test_suite_def.model_dump_json(indent=3))
        assert len(test_suite_def.test_suite) == 2
        assert len(test_suite_def.foundations) == 3
        assert test_suite_def.test_suite[0].name == "test_case_1"
        assert test_suite_def.test_suite[1].name == "test_case_2"
        assert test_suite_def.foundations[0].table_name == "int_table_1"
        assert test_suite_def.foundations[1].table_name == "int_table_2"
        assert test_suite_def.foundations[2].table_name == "fct_order"
        
    def test_create_tests_structure(self):
        print("test_load_test_definition_for_fact_table")
        table_name= "e"
        test_mgr.init_unit_test_for_table(table_name)
        assert os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests")
        assert os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/test_definitions.yaml")
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_def
        print(test_def.model_dump_json(indent=3))
        assert test_def.foundations[0].table_name == "c"


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_input_validations(self,
                                     mock_post_flink_statement, 
                                    mock_get_statement_info,
                                    mock_get_statement_results):

        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            return Statement(name= statement_name, status= {"phase": "RUNNING"})

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            mock_info = MagicMock(spec=Statement)
            return None
        
        def mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            if statement_name == "dev-p1-test-case-1-validate-ut":
                op_row = OpRow(op=0, row=["PASS"])
            else:
                op_row = OpRow(op=0, row=["FAIL"])
            data= Data(data= [op_row])
            result = StatementResult(results=data)
            return Statement(name= statement_name, status= {"phase": "COMPLETED"}, 
                             result= result)
        
        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.side_effect = mock_post_statement
        mock_get_statement_results.side_effect = mock_statement_results


        table_name= "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statements = test_mgr._execute_one_testcase(test_suite_def.test_suite[0], table_ref)
        assert len(statements) == 3
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_foundations(self, mock_post_flink_statement, mock_get_statement_info):

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            mock_info = MagicMock(spec=Statement)
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            else:
                return Statement(name=statement_name, status= {"phase": "UNKNOWN"})
        mock_get_statement_info.side_effect = mock_statement_info

        mock_post_flink_statement.return_value =  Statement(name= "dev-p1-int-table-1-ut", status= {"phase": "RUNNING"})


        table_name= "p1_fct_order"
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_def.foundations[0].table_name == "int_table_1"
        statements = test_mgr._execute_foundation_statements(test_def, table_ref)
        assert len(statements) == 3
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_main_dml(self, mock_post_flink_statement, mock_get_statement_info):

        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            return Statement(name= statement_name, status= {"phase": "RUNNING"})

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            mock_info = MagicMock(spec=Statement)
            return None

        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.side_effect = mock_post_statement


        table_name= "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statements = test_mgr._execute_statements_under_test(table_name, table_ref)
        assert len(statements) == 2
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_all_testcases(self,
                                     mock_post_flink_statement, 
                                    mock_get_statement_info,
                                    mock_get_statement_results):

        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            return Statement(name= statement_name, status= {"phase": "RUNNING"})

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            mock_info = MagicMock(spec=Statement)
            return None
        
        def mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            if statement_name == "dev-p1-test-case-1-validate-ut":
                op_row = OpRow(op=0, row=["PASS"])
            else:
                op_row = OpRow(op=0, row=["FAIL"])
            data= Data(data= [op_row])
            result = StatementResult(results=data)
            return Statement(name= statement_name, status= {"phase": "COMPLETED"}, 
                             result= result)
        
        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.side_effect = mock_post_statement
        mock_get_statement_results.side_effect = mock_statement_results


        table_name= "p1_fct_order"
        statements = test_mgr.execute_all_tests(table_name)
        assert len(statements) == 9
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")

if __name__ == '__main__':
    unittest.main()