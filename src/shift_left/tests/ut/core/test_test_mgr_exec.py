"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import pathlib
from typing import Tuple, Optional
import unittest
from unittest.mock import patch, ANY

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementResult, 
    Data, 
    OpRow, 
    StatementError,
    ErrorData)
import shift_left.core.test_mgr as test_mgr
from shift_left.core.utils.file_search import build_inventory
from shift_left.core.utils.app_config import reset_all_caches



class TestTestManager(unittest.TestCase):
    """Unit test suite for test manager functionality."""
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent.parent / "data"
        reset_all_caches() # Reset all caches to ensure test isolation
        build_inventory(os.getenv("PIPELINES"))
    
    def setUp(self):
        # Ensure proper test isolation by resetting caches and rebuilding inventory
        reset_all_caches()
        build_inventory(os.getenv("PIPELINES"))
        self._ddls_executed  = {'int_table_1_ut': False, 'int_table_2_ut': False, 'p1_fct_order_ut': False}
    
    # ---- Mock functions to be used in tests to avoid calling remote services ----
    def _mock_table_exists(self, table_name):
        """
        Mock the _table_exists(table_name) function to return True if the table name is in the _ddls_executed dictionary
        """
        print(f"mock_table_exists: {table_name} returns {self._ddls_executed[table_name]}")
        value = self._ddls_executed[table_name]
        self._ddls_executed[table_name] = True  # mock the table will exist after the tet execution
        return value

    def _mock_get_None_statement(self, statement_name):
        print(f"mock_get_statement: {statement_name} returns None")  
        return None
    
    def _mock_post_ddl_statement(self, compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "ddl" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            else:
                return Statement(name=statement_name, status={"phase": "RUNNING"})

    def _mock_post_dml_statement(self, compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            self._sql_content = sql_content
            if "dml" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

    def _mock_get_running_statement(self, statement_name):
            print(f"mock_get_statement: {statement_name}")  
            return Statement(name=statement_name, status={"phase": "RUNNING"})

    def _mock_load_sql_and_execute_statement(self, table_name, sql_path, prefix, compute_pool_id, fct, product_name, statements):
            print(f"\nmock_load_sql_and_execute_statement: {table_name} {sql_path} {prefix} {compute_pool_id} {fct} {product_name} {statements}\n")
            if statements is None:
                statements = set()
            else:
                statement_name = test_mgr._build_statement_name(table_name, prefix)
                statements.add(Statement(name=statement_name, status={"phase": "COMPLETED"}))
            return statements

    def _mock_transform_sql_content(self, sql_input, table_name) -> str:
            return sql_input

    def _mock_poll_response(self, statement_name) -> Tuple[str, Optional[StatementResult]]:
        print(f"mock_poll_response: {statement_name}")
        data = Data(data=[OpRow(op=0, row=["FAIL"]),OpRow(op=0, row=["PASS"])])

        return "PASS", StatementResult(results=data, api_version="v1", kind="StatementResult", metadata=None)

    # ---------------------------------------------------------------
    # Start by testing the lower level private functions related to execution.
    # the sequencing of the test methods follow the flow of executing a test via execute_one_or_all_tests.
    # ---------------------------------------------------------------

    @patch('shift_left.core.test_mgr._load_sql_and_execute_statement')
    def test_init_test_foundations(self, mock_load_sql_and_execute_statement):
        """Test _init_test_foundations() for a fact table with int_p1_table_2 and int_p1_table_1 as input tables.
        it covers getting into _execute_foundation_statements and _start_ddl_dml_for_flink_under_test.
        """

        mock_load_sql_and_execute_statement.side_effect = self._mock_load_sql_and_execute_statement
        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        assert table_ref is not None
        test_suite_def, table_ref, prefix, test_result = test_mgr._init_test_foundations(table_name, "test_pool", "test_case_1")
        assert test_suite_def is not None
        assert table_ref is not None
        assert prefix =="dev"
        assert test_result is not None
        assert len(test_result.foundation_statements) == 4 # 3 DDLs and one DML
        for statement in test_result.foundation_statements:
            assert statement.name in ["dev-ddl-int-table-1-ut", "dev-ddl-int-table-2-ut", "dev-ddl-p1-fct-order-ut", "dev-dml-p1-fct-order-ut"]


    @patch('shift_left.core.test_mgr._load_sql_and_execute_statement')
    def test_execute_inputs(self, mock_load_sql_and_execute_statement):
        """Test the execution of the input statements for a test case."""


        mock_load_sql_and_execute_statement.side_effect = self._mock_load_sql_and_execute_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        test_case = test_suite_def.test_suite[0]
        statements= test_mgr._execute_test_inputs(test_case, table_ref, "dev", "test_pool")
        assert len(statements) == 2
        for statement in statements:
            assert statement.name in ["dev-int-table-1-ut", "dev-int-table-2-ut"]

    @patch('shift_left.core.test_mgr._execute_flink_test_statement')
    def test_execute_inputs_cvs(self, mock_load_sql_and_execute_flink_test_statement):
        """Test the execution of the input statements using the csv path for a test case."""
        def _mock_load_sql_and_execute_flink_test_statement(sql_content, statement_name, product_name, compute_pool_id):
            print(f"mock_load_sql_and_execute_flink_test_statement: {sql_content} {statement_name} {product_name} {compute_pool_id}")
            return Statement(name=statement_name, status={"phase": "COMPLETED"}), True

        mock_load_sql_and_execute_flink_test_statement.side_effect = _mock_load_sql_and_execute_flink_test_statement

        table_name = "c360_dim_groups"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        test_case = test_suite_def.test_suite[1]
        statements= test_mgr._execute_test_inputs(test_case, table_ref, "dev", "test_pool")
        assert len(statements) == 2
        for statement in statements:
            print(f"statement: {statement.name} {statement.status}")
            assert statement.name in ["dev-src-common-tenant-ut", "dev-src-c360-groups-ut"]

    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    def test_exec_flink_test_statement_happy_path(self, mock_get_statement, mock_post_flink_statement):
        """
        when the statement is not found, it should be created and executed
        """


        table_name = "p1_fct_order"
        prefix = "dev"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statement_name = test_mgr._build_statement_name(table_name, prefix+"-dml")

        mock_get_statement.return_value = None
        mock_post_flink_statement.return_value = Statement(name=statement_name, status={"phase": "COMPLETED"})
        

        sql_content = test_mgr._read_and_treat_sql_content_for_ut(table_ref.dml_ref, self._mock_transform_sql_content, table_name)
        statement, is_new = test_mgr._execute_flink_test_statement(sql_content, statement_name, "test_pool", "test_product")
        assert statement is not None
        assert is_new
        assert statement.name == statement_name
        assert statement.status.phase == "COMPLETED"
        

    @patch('shift_left.core.test_mgr._poll_response')
    @patch('shift_left.core.test_mgr._load_sql_and_execute_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    def test_execute_validation(self, mock_delete_statement_if_exists, mock_load_sql_and_execute_statement, mock_poll_response):
        """Test the execution of the validation statements for a test case."""
        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        test_case = test_suite_def.test_suite[0]
        mock_delete_statement_if_exists.return_value = "deleted"
        mock_load_sql_and_execute_statement.side_effect = self._mock_load_sql_and_execute_statement
        mock_poll_response.side_effect = self._mock_poll_response
        statements, result_text, results = test_mgr._execute_test_validation(test_case, table_ref, 'dev-val', 'test_pool')
        assert len(statements) == 1
        assert statements.pop().name == "dev-val-p1-fct-order-ut"
        assert result_text == "PASS"
        assert results is not None
        print(f"results: {results.model_dump_json(indent=2)}")
        assert results.results.data[0].op == 0
        assert results.results.data[0].row == ["FAIL"]
        assert results.results.data[1].op == 0
        assert results.results.data[1].row == ["PASS"]


    @patch('shift_left.core.test_mgr._poll_response')
    @patch('shift_left.core.test_mgr._load_sql_and_execute_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    def test_execute_all_validations(self, mock_delete_statement_if_exists, mock_load_sql_and_execute_statement, mock_poll_response):
        """Test the execution of the validation statements for a test case."""
        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        test_case = test_suite_def.test_suite[0]
        mock_delete_statement_if_exists.return_value = "deleted"
        mock_load_sql_and_execute_statement.side_effect = self._mock_load_sql_and_execute_statement
        mock_poll_response.side_effect = self._mock_poll_response
        results = test_mgr.execute_validation_tests(table_name=table_name, 
                                                                            test_case_name="", 
                                                                            compute_pool_id="test_pool", 
                                                                            run_all=True)

        print(f"results: {results.model_dump_json(indent=2)}")
        assert len(results.test_results) == 2
        for result in results.test_results.values():
            assert result.result == "PASS"


    # NON HAPPY PATHS
    # -------------
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    def test_exec_flink_test_statement_withStatementNotFoundError(self, mock_get_statement, mock_post_flink_statement):
        """
        when the statement is in error, it should be created and executed
        """

        table_name = "p1_fct_order"
        prefix = "dev"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statement_name = test_mgr._build_statement_name(table_name, prefix + "-dml")

        se = StatementError(errors=[ErrorData(status="404", detail="Not found")])
        mock_get_statement.return_value = se
        mock_post_flink_statement.side_effect = self._mock_post_dml_statement
        

        sql_content = test_mgr._read_and_treat_sql_content_for_ut(table_ref.dml_ref, self._mock_transform_sql_content, table_name)
        statement, is_new = test_mgr._execute_flink_test_statement(sql_content, statement_name, "test_pool", "test_product")
        assert statement is not None
        assert is_new
        assert statement.name == statement_name
        assert statement.status.phase == "RUNNING"

    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    def test_exec_flink_test_statement_with_failed_statement(self, 
                    mock_get_statement, 
                    mock_post_flink_statement,
                    mock_delete_statement_if_exists):
        """
        when the statement is failedr, it should be created and executed but not new
        """

        table_name = "p1_fct_order"
        prefix = "dev"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statement_name = test_mgr._build_statement_name(table_name, prefix+"-dml")
        print(f"statement_name: {statement_name}")
        se = Statement(name=statement_name, status={"phase": "FAILED"})
        mock_get_statement.return_value = se
        mock_post_flink_statement.side_effect = self._mock_post_dml_statement
        mock_delete_statement_if_exists.return_value = "deleted"

        sql_content = test_mgr._read_and_treat_sql_content_for_ut(table_ref.dml_ref, self._mock_transform_sql_content, table_name)
        statement, is_new = test_mgr._execute_flink_test_statement(sql_content, statement_name, "test_pool", "test_product")
        assert statement is not None
        assert not is_new
        assert statement.name == statement_name
        assert statement.status.phase == "RUNNING"



    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    def test_exec_flink_test_statement_running_statement(self, 
                    mock_get_statement):
        """
        when the statement is in error, it should be created and executed
        post is not called
        """

        table_name = "p1_fct_order"
        prefix = "dev"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        statement_name = test_mgr._build_statement_name(table_name, prefix+"-dml")
        se = Statement(name=statement_name, status={"phase": "RUNNING"})
        mock_get_statement.return_value = se

        sql_content = test_mgr._read_and_treat_sql_content_for_ut(table_ref.dml_ref, self._mock_transform_sql_content, table_name)
        statement, is_new = test_mgr._execute_flink_test_statement(sql_content, statement_name, "test_pool", "test_product")
        assert statement is not None
        assert not is_new
        assert statement.name == statement_name
        assert statement.status.phase == "RUNNING"

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_run_statement_under_test_happy_path(self, 
                                     mock_post_flink_statement, 
                                     mock_table_exists,
                                     mock_get_statement):
        """Test should create ddl and dml statements as the table under tests 
        does not exist
        """

        
        mock_post_flink_statement.side_effect = self._mock_post_ddl_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 2)
        for statement in statements:
            print(f"UT: statement: {statement.name} {statement.status}")
            assert statement.name in ["dev-ddl-p1-fct-order-ut", "dev-dml-p1-fct-order-ut"]


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_table_exists_run_dml_only(self, 
                                     mock_post_flink_statement, 
                                     mock_table_exists,
                                     mock_get_statement):
        """Test starting the statement under test: should not run ddl as table exists 
        but dml statements as statement is unknown
        """

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        

        mock_post_flink_statement.side_effect = self._mock_post_dml_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        
        self.assertEqual(len(statements), 1)
        for statement in statements:
            assert isinstance(statement, Statement)
            print(f"statement: {statement.name} {statement.status}")
            assert statement.name in  ["dev-dml-p1-fct-order-ut"]


    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_do_not_run_statements_if_table_exists_and_dml_running(self, 
                                     mock_post_flink_statement, 
                                     mock_table_exists,
                                     mock_get_statement):
        """Table exists so no DDL execution, DLM already RUNNING so not restart it
        """
        self._sql_content = ""

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True

        mock_post_flink_statement.side_effect = self._mock_post_dml_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = self._mock_get_running_statement

        table_name = "p1_fct_order"
        test_suite_def, table_ref = test_mgr._load_test_suite_definition(table_name)
        assert test_suite_def is not None
        statements = test_mgr._start_ddl_dml_for_flink_under_test(table_name, table_ref)
        print(f"statements: {statements}")
        self.assertEqual(len(statements), 1)


    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_onte_test_case_execution(self, 
                        mock_post_flink_statement, 
                        mock_get_statement_results,
                        mock_table_exists,
                        mock_get_statement,
                        mock_delete_statement):
        
        """Test the execution of one test case, with processing of statement results."""

        self._sql_content = ""

        def _mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            op_row = OpRow(op=0, row=["PASS"]) if "val-1" in statement_name else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data, 
                                     api_version="v1", 
                                     kind="StatementResult", 
                                     metadata=None)
            return result

        mock_get_statement_results.side_effect = _mock_statement_results

        mock_post_flink_statement.side_effect = self._mock_post_ddl_statement
        mock_table_exists.side_effect = self._mock_table_exists
        mock_get_statement.side_effect = self._mock_get_None_statement
        mock_delete_statement.return_value = 'deleted'  # Mock delete operation

        table_name = "p1_fct_order"
        suite_result = test_mgr.execute_one_or_all_tests(test_case_name="", table_name=table_name, run_validation=True)
        assert suite_result
        assert len(suite_result.test_results) == 2
        assert len(suite_result.foundation_statements) == 4
        assert suite_result.test_results["test_case_1"].result == "PASS"
        assert suite_result.test_results["test_case_2"].result == "FAIL"
        print(suite_result.model_dump_json(indent=2))

    

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    def test_poll_response_success_first_try(self, mock_get_results, mock_get_statement):
        """Test _poll_response function when results are available on first try."""
        from shift_left.core.models.flink_statement_model import StatementResult, Data, OpRow, Statement
        
        # Mock get_statement to return a successful statement
        mock_statement = Statement(name="test_statement", status={"phase": "COMPLETED"})
        mock_get_statement.return_value = mock_statement
        
        # Mock successful response on first call
        op_row = OpRow(op=0, row=["PASS"])
        data = Data(data=[op_row])
        result = StatementResult(results=data, api_version="v1", kind="StatementResult", metadata=None)
        mock_get_results.return_value = result
        
        final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "PASS")
        self.assertEqual(statement_result, result)
        mock_get_results.assert_called_once_with("test_statement")

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.time.sleep')
    def test_poll_response_retry_logic(self, mock_sleep, mock_get_results, mock_get_statement):
        """Test _poll_response function retry logic with empty results."""
        from shift_left.core.models.flink_statement_model import StatementResult, Data, OpRow, Statement
        
        # Mock get_statement to return a successful statement
        mock_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = mock_statement
        
        # First few calls return empty results, last call returns data
        empty_result = StatementResult(results=Data(data=[]), api_version="v1", kind="StatementResult", metadata=None)
        op_row = OpRow(op=0, row=["PASS"])
        data = Data(data=[op_row])
        success_result = StatementResult(results=data, api_version="v1", kind="StatementResult", metadata=None)
        
        mock_get_results.side_effect = [empty_result, empty_result, success_result]
        
        final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "PASS")
        self.assertEqual(statement_result, success_result)
        self.assertEqual(mock_get_results.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)  # Sleep called for first 2 empty results

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    def test_poll_response_max_retries_exceeded(self, mock_get_results, mock_get_statement):
        """Test _poll_response function when max retries are exceeded."""
        from shift_left.core.models.flink_statement_model import StatementResult, Data, Statement
        
        # Mock get_statement to return a running statement
        mock_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = mock_statement
        
        # Always return empty results
        empty_result = StatementResult(results=Data(data=[]), api_version="v1", kind="StatementResult", metadata=None)
        mock_get_results.return_value = empty_result
        
        with patch('shift_left.core.test_mgr.time.sleep'):
            final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "FAIL")  # Default when no data
        # Should call get_results for max_retries - 1 times (range(1, 7) = 1,2,3,4,5,6)
        self.assertEqual(mock_get_results.call_count, 6)  # max_retries - 1

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    def test_poll_response_exception_handling(self, mock_get_results, mock_get_statement):
        """Test _poll_response function exception handling."""
        from shift_left.core.models.flink_statement_model import Statement
        
        # Mock get_statement to return a running statement
        mock_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = mock_statement
        
        # Mock exception on first call
        mock_get_results.side_effect = Exception("API Error")
        
        final_result, statement_result = test_mgr._poll_response("test_statement")
        
        self.assertEqual(final_result, "FAIL")
        self.assertIsNone(statement_result)

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    @patch('shift_left.core.test_mgr.statement_mgr.get_or_build_sql_content_transformer')
    def test_execute_flink_test_statement_new_statement(self, 
                                                         mock_transformer, 
                                                         mock_post_statement, 
                                                         mock_get_statement):
        """Test _execute_flink_test_statement when statement doesn't exist."""
        from shift_left.core.models.flink_statement_model import StatementError
        
        # Mock that statement doesn't exist
        mock_get_statement.return_value = StatementError(message="Not found")
        
        # Mock transformer
        mock_transformer_instance = mock_transformer.return_value
        mock_transformer_instance.update_sql_content.return_value = ("", "transformed_sql")
        
        # Mock post statement success
        expected_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_post_statement.return_value = expected_statement
        
        result, is_new = test_mgr._execute_flink_test_statement(
            sql_content="SELECT * FROM test",
            statement_name="test_statement",
            compute_pool_id="test_pool"
        )
        
        self.assertEqual(result, expected_statement)
        self.assertFalse(is_new)  # Should be not new as there is an error in the statement
        mock_post_statement.assert_called_once()
        mock_transformer_instance.update_sql_content.assert_called_once()

    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    def test_execute_flink_test_statement_existing_statement(self, mock_get_statement):
        """Test _execute_flink_test_statement when statement already exists."""
        # Mock that statement exists
        existing_statement = Statement(name="test_statement", status={"phase": "RUNNING"})
        mock_get_statement.return_value = existing_statement
        
        result, is_new = test_mgr._execute_flink_test_statement(
            sql_content="SELECT * FROM test", 
            statement_name="test_statement",
            compute_pool_id="test_pool"
        )
        
        self.assertEqual(result, existing_statement)
        self.assertFalse(is_new)  # Should not be new since it already exists

    def test_execute_one_or_all_tests_error_handling(self):
        """Test execute_one_or_all_tests function error handling."""
        with patch('shift_left.core.test_mgr._init_test_foundations') as mock_init:
            mock_init.side_effect = Exception("Foundation error")
            
            with self.assertRaises(Exception) as context:
                test_mgr.execute_one_or_all_tests("nonexistent_table", "test_case")
            
            self.assertIn("Foundation error", str(context.exception))

    def test_execute_one_or_all_tests_error_handling(self):
        """Test execute_one_or_all_tests function error handling."""
        with patch('shift_left.core.test_mgr._init_test_foundations') as mock_init:
            mock_init.side_effect = Exception("Foundation error")
            
            with self.assertRaises(Exception) as context:
                test_mgr.execute_one_or_all_tests("nonexistent_table")
            
            self.assertIn("Foundation error", str(context.exception))

    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr._read_and_treat_sql_content_for_ut')
    @patch('shift_left.core.test_mgr._execute_flink_test_statement')
    def test_load_sql_and_execute_statement_ddl_table_exists(self, 
                                                              mock_execute, 
                                                              mock_read_sql, 
                                                              mock_table_exists):
        """Test _load_sql_and_execute_statement when DDL table already exists."""
        mock_table_exists.return_value = True
        mock_read_sql.return_value = "CREATE TABLE test_table (id INT);"
        
        result = test_mgr._load_sql_and_execute_statement(
            table_name="test_table",
            sql_path="test.sql",
            prefix="dev-ddl"
        )
        
        # Should return [] when table exists and prefix is ddl
        self.assertEqual(result, set())
        mock_execute.assert_not_called()


     # --- to clean after this one ---
    @patch('shift_left.core.test_mgr.statement_mgr.delete_statement_if_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement')
    @patch('shift_left.core.test_mgr._table_exists')
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_results')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_exec_one_testcase(self, 
                                mock_post_flink_statement, 
                                mock_get_statement_results,
                                mock_table_exists,
                                mock_get_statement,
                                mock_delete_statement):
        
        """Test the execution of one test case for p1_fct_order: 
        - create the foundation statements
        - run insert statements
        - run validation statements
        - check the result
        """

        self._sql_content = ""

        def _mock_statement_results(statement_name):
            print(f"mock_statement_results: {statement_name}")
            # Return PASS for validation statements related to test_case_1, FAIL for others
            op_row = OpRow(op=0, row=["PASS"]) if "val-1" in statement_name else OpRow(op=0, row=["FAIL"])
            data = Data(data=[op_row])
            result = StatementResult(results=data, 
                                     api_version="v1", 
                                     kind="StatementResult", 
                                     metadata=None)
            return result

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name} -> NO")
            return False

        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")
            return None
        
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            if "ddl" in statement_name or "ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "COMPLETED"})
            elif "dml" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            elif "val" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})


        mock_get_statement_results.side_effect = _mock_statement_results
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement
        mock_delete_statement.return_value = "deleted"  # Mock delete operation

        table_name = "p1_fct_order"
        test_suite_result = test_mgr.execute_one_or_all_tests(table_name, "test_case_1", run_validation=True)
        assert test_suite_result
        print(test_suite_result.model_dump_json(indent=2))
        assert len(test_suite_result.test_results) == 1
        test_result = test_suite_result.test_results["test_case_1"]
        assert test_result
        self.assertEqual(len(test_result.statements), 3)
        self.assertEqual(len(test_suite_result.foundation_statements), 4)
        assert test_result.result == "PASS"
        for statement in test_result.statements:
            print(f"statement: {statement.name} {statement.status}")
        
if __name__ == '__main__':
    unittest.main()