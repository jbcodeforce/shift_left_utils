import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.test_mgr as test_mgr
from shift_left.core.utils.file_search import build_inventory

class TestDebugUnitTests(unittest.TestCase):

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