"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import pathlib
import unittest
from unittest.mock import patch, MagicMock, ANY

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.test_mgr as test_mgr
from shift_left.core.utils.file_search import build_inventory
from shift_left.core.test_mgr import (
    SLTestDefinition,
    SLTestCase,
    SLTestData,
    Foundation
)


class TestTestManager(unittest.TestCase):
    """Test suite for test manager functionality."""

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"
        build_inventory(os.getenv("PIPELINES"))

    def test_should_load_test_definition(self):
        """Test loading of test definition."""
        td1 = SLTestData(table_name="tb1", sql_file_name="ftb1")
        o1 = SLTestData(table_name="tbo1", sql_file_name="to1")
        tc1 = SLTestCase(name="tc1", inputs=[td1], outputs=[o1])
        fds = [Foundation(table_name="tb1", ddl_for_test="ddl-tb1")]
        ts = SLTestDefinition(foundations=fds, test_suite=[tc1])
        
        self.assertTrue(ts)
        print(ts.model_dump_json(indent=3))

    def test_load_test_definition_for_fact_table(self):
        """Test loading test definition for fact table."""
        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        
        self.assertTrue(test_suite_def)
        print(test_suite_def.model_dump_json(indent=3))
        
        self.assertEqual(len(test_suite_def.test_suite), 2)
        self.assertEqual(len(test_suite_def.foundations), 3)
        self.assertEqual(test_suite_def.test_suite[0].name, "test_case_1")
        self.assertEqual(test_suite_def.test_suite[1].name, "test_case_2")
        self.assertEqual(test_suite_def.foundations[0].table_name, "int_table_1")
        self.assertEqual(test_suite_def.foundations[1].table_name, "int_table_2")
        self.assertEqual(test_suite_def.foundations[2].table_name, "fct_order")

    def test_create_tests_structure(self):
        """Test creation of test structure."""
        table_name = "e"
        test_mgr.init_unit_test_for_table(table_name)
        
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests"))
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/test_definitions.yaml"))
        
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        self.assertTrue(test_def)
        print(test_def.model_dump_json(indent=3))
        self.assertEqual(test_def.foundations[0].table_name, "c")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_input_validations(self, mock_post_flink_statement, mock_get_statement_info, mock_get_statement_results):
        """Test execution of input validations."""
        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return None

        def mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            op_row = OpRow(op=0, row=["PASS"]) if statement_name == "dev-p1-test-case-1-validate-ut" else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data)
            return Statement(name=statement_name, status={"phase": "COMPLETED"}, result=result)

        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.side_effect = mock_post_statement
        mock_get_statement_results.side_effect = mock_statement_results

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statements = test_mgr._execute_one_testcase(test_suite_def.test_suite[0], table_ref)
        
        self.assertEqual(len(statements), 3)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_foundations(self, mock_post_flink_statement, mock_get_statement_info):
        """Test execution of foundations."""
        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.return_value = Statement(name="dev-p1-int-table-1-ut", status={"phase": "RUNNING"})

        table_name = "p1_fct_order"
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        
        self.assertEqual(test_def.foundations[0].table_name, "int_table_1")
        statements = test_mgr._execute_foundation_statements(test_def, table_ref)
        
        self.assertEqual(len(statements), 3)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_main_dml(self, mock_post_flink_statement, mock_get_statement_info):
        """Test execution of main DML."""
        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return None

        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.side_effect = mock_post_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statements = test_mgr._execute_statements_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 2)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_all_testcases(self, mock_post_flink_statement, mock_get_statement_info, mock_get_statement_results):
        """Test execution of all test cases."""
        def mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        def mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return None

        def mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            op_row = OpRow(op=0, row=["PASS"]) if statement_name == "dev-p1-test-case-1-validate-ut" else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data)
            return Statement(name=statement_name, status={"phase": "COMPLETED"}, result=result)

        mock_get_statement_info.side_effect = mock_statement_info
        mock_post_flink_statement.side_effect = mock_post_statement
        mock_get_statement_results.side_effect = mock_statement_results

        table_name = "p1_fct_order"
        statements = test_mgr.execute_all_tests(table_name)
        
        self.assertEqual(len(statements), 9)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")

    def test_statement_with_quoted_table_name(self):
        table_name = "int_p3_user_role"
        test_definition, table_ref = test_mgr._load_test_suite_definition(table_name)
        tests_folder_path = os.path.join(os.getenv("PIPELINES"), "intermediates", "p3", "user_role", "tests")
        table_inventory = build_inventory(os.getenv("PIPELINES"))
        table_struct = test_mgr._process_foundation_ddl_from_test_definitions(test_definition, 
                                                               tests_folder_path, 
                                                               table_inventory)
        print(table_struct)
        for table in table_struct:
            cname, rows= test_mgr._build_data_sample(table_struct[table])
            print(cname)
            print(rows)

if __name__ == '__main__':
    unittest.main()