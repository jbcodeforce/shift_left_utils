"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import pathlib
import unittest
from unittest.mock import patch, MagicMock, ANY

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementResult, 
    Data, 
    OpRow, 
    StatementError,
    StatementInfo,
    ErrorData)
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

    def test_create_tests_structure(self):
        """Test creation of tests structure with templates & test definitions."""
        table_name = "e"
        test_mgr.init_unit_test_for_table(table_name)
        
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests"))
        self.assertTrue(os.path.exists(os.getenv("PIPELINES") + "/facts/p2/e/tests/test_definitions.yaml"))
        
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        self.assertTrue(test_def)
        print(test_def.model_dump_json(indent=3))
        self.assertEqual(test_def.foundations[0].table_name, "c")

    def test_validate_test_model(self):
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
        self.assertEqual(len(test_suite_def.foundations), 2)
        self.assertEqual(test_suite_def.test_suite[0].name, "test_case_1")
        self.assertEqual(test_suite_def.test_suite[1].name, "test_case_2")
        self.assertEqual(test_suite_def.foundations[0].table_name, "int_table_1")
        self.assertEqual(test_suite_def.foundations[1].table_name, "int_table_2")
       
    def test_statement_with_quoted_table_name(self):
        table_name = "int_p3_user_role"
        test_definition, table_ref = test_mgr._load_test_suite_definition(table_name)
        tests_folder_path = os.path.join(os.getenv("PIPELINES"), "intermediates", "p3", "user_role", "tests")
        table_inventory = build_inventory(os.getenv("PIPELINES"))
        table_struct = test_mgr._process_foundation_ddl_from_test_definitions(test_definition, 
                                                               tests_folder_path, 
                                                               table_inventory)
        assert table_struct
        cnames = []
        table_rows = {}
        for table in table_struct:
            print(table)
            cname, rows= test_mgr._build_data_sample(table_struct[table])
            cnames.append(cname)
            print(cname)
            print(rows)
            table_rows[table] = rows
        assert "id, name, description, created_at" in cnames[0]
        assert "user_id, tenant_id, role_id, status" in cnames[1]
        assert "role_id, role_name" in cnames[2]
        assert "('id_1', 'name_1', 'description_1', 'created_at_1')" in table_rows["src_p3_tenants"]
        assert "('user_id_2', 'tenant_id_2', 'role_id_2', 'status_2')" in table_rows["src_p3_users"]
        assert "('role_id_1', 'role_name_1')" in table_rows["src_p3_roles"]
        
      


    # ---------- test execution -------------

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_run_statment_under_test_happy_path(self, 
                                     mock_post_flink_statement, 
                                     mock_get_statement_info,
                                     mock_table_exists,
                                     mock_get_statement):
        """Test starting the statement under test: should create ddl and dml statements
        as table not exists
        """
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
        
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return None

        self._ddl_executed = False
        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            value = _ddl_executed
            _ddl_executed = True
            return value
        
        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")  
            return None

        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 2)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")
        assert "dev-ddl-p1-fct-order-ut" in statements[0].name
        assert "dev-dml-p1-fct-order-ut" in statements[1].name




    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_table_exists_run_dml_only(self, 
                                     mock_post_flink_statement, 
                                     mock_get_statement_info,
                                     mock_table_exists,
                                     mock_get_statement):
        """Test starting the statement under test: should not run ddl as table exists 
        but dml statements as statement is unknown
        """
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "dml" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            # return no statement found so tool can run it
            return None

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        
        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")  
            return None

        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 1)
        for statement in statements:
            assert isinstance(statement, Statement)
            print(f"statement: {statement.name} {statement.status}")
        assert "dev-dml-p1-fct-order-ut" in statements[0].name


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_do_not_run_statements_if_table_exists_and_dml_running(self, 
                                     mock_post_flink_statement, 
                                     mock_get_statement_info,
                                     mock_table_exists,
                                     mock_get_statement):
        """Table exists so no DDL execution, DLM already RUNNING so not restart it
        """
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "dml" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            # return no statement found so tool can run it

            return StatementInfo(name=statement_name, status_phase="RUNNING")

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        
        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")  
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        self.assertEqual(len(statements), 0)

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_foundations(self, 
                              mock_post_flink_statement, 
                              mock_get_statement_info, 
                              mock_table_exists,
                              mock_get_statement):
        """Test execution of foundations for the fact table that uses 2 input tables."""
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return Statement(name=statement_name, status={"phase": "UNKNOWN"})
        
        self._ddls_executed  = {'int_table_1': False, 'int_table_2': False}
        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            value = self._ddls_executed[table_name]
            self._ddls_executed[table_name] = True
            return value

        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")  
            return None
        
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
        
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        test_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        self.assertEqual(test_def.foundations[0].table_name, "int_table_1")
        statements = test_mgr._execute_foundation_statements(test_def, table_ref)
        self.assertEqual(len(statements), 2)
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")
        assert "dev-ddl-int-table-1-ut" in statements[0].name
        assert "dev-ddl-int-table-2-ut" in statements[1].name

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_one_testcase(self, 
                                mock_post_flink_statement, 
                                mock_get_statement_info, 
                                mock_get_statement_results,
                                mock_table_exists,
                                mock_get_statement):
        
        """Test the execution of one test case."""
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return Statement(name=statement_name, status={"phase": "UNKNOWN"})
        
        self._ddls_executed  = {'int_table_1': False, 'int_table_2': False, 'p1_fct_order': False}
        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name} returns {self._ddls_executed[table_name]}")
            value = self._ddls_executed[table_name]
            self._ddls_executed[table_name] = True
            return value

        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name} returns None")  
            return None
        
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name or "-ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            

        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return None

        def _mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            op_row = OpRow(op=0, row=["PASS"]) if statement_name == "dev-val-1-p1-fct-order-ut" else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data, 
                                     api_version="v1", 
                                     kind="StatementResult", 
                                     metadata=None)
            return result

        mock_get_statement_results.side_effect = _mock_statement_results
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        test_result = test_mgr.execute_one_test(table_name, "test_case_1")
        assert test_result
        self.assertEqual(len(test_result.statements), 3)
        self.assertEqual(len(test_result.foundation_statements), 4)
        assert test_result.result == "PASS\n"
        for statement in test_result.statements:
            print(f"statement: {statement.name} {statement.status}")
        print(test_result.model_dump_json(indent=2))



    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_test_suite(self, 
                        mock_post_flink_statement, 
                        mock_get_statement_info, 
                        mock_get_statement_results,
                        mock_table_exists,
                        mock_get_statement):
        
        """Test the execution of one test case."""
        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            if statement_name.startswith("dev-p1-ddl-int-table-1"):
                return None
            return Statement(name=statement_name, status={"phase": "UNKNOWN"})
        
        self._ddls_executed  = {'int_table_1': False, 'int_table_2': False, 'p1_fct_order': False}
        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name} returns {self._ddls_executed[table_name]}")
            value = self._ddls_executed[table_name]
            self._ddls_executed[table_name] = True
            return value

        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name} returns None")  
            return None
        
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name or "-ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            

        def _mock_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return None

        def _mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            op_row = OpRow(op=0, row=["PASS"]) if statement_name == "dev-val-1-p1-fct-order-ut" else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data, 
                                     api_version="v1", 
                                     kind="StatementResult", 
                                     metadata=None)
            return result

        mock_get_statement_results.side_effect = _mock_statement_results
        mock_get_statement_info.side_effect = _mock_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        table_name = "p1_fct_order"
        suite_result = test_mgr.execute_all_tests(table_name)
        assert suite_result
        assert len(suite_result.test_results) == 2
        assert len(suite_result.foundation_statements) == 4
        assert suite_result.test_results["test_case_1"].result == "PASS\n"
        assert suite_result.test_results["test_case_2"].result == "FAIL\n"
        print(suite_result.model_dump_json(indent=2))



if __name__ == '__main__':
    unittest.main()