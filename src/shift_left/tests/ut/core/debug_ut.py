import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
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
    @patch('shift_left.core.test_mgr.statement_mgr.get_statement_info')
    @patch('shift_left.core.test_mgr.statement_mgr.post_flink_statement')
    def test_execute_csv_inputs(self, 
                                mock_post_flink_statement, 
                                mock_get_statement_info, 
                                mock_table_exists, 
                                mock_get_statement):
        
        def _mock_post_statement(compute_pool_id, statement_name, sql_content):
            print(f"mock_post_statement: {statement_name}")
            print(f"sql_content: {sql_content}")
            if "dml" in statement_name or "ins" in statement_name:
                return Statement(name=statement_name, status={"phase": "RUNNING"})
            else:
                return Statement(name=statement_name, status={"phase": "UNKNOWN"})

        def _mock_running_statement_info(statement_name):
            print(f"mock_statement_info: {statement_name}")
            return StatementInfo(name=statement_name, status_phase="RUNNING")

        def _mock_table_exists(table_name):
            print(f"mock_table_exists: {table_name}")
            return True
        
        def _mock_get_statement(statement_name):
            print(f"mock_get_statement: {statement_name}")
            if "ins" in statement_name:
                return None
            return Statement(name=statement_name, status={"phase": "RUNNING"})

        mock_get_statement_info.side_effect = _mock_running_statement_info
        mock_post_flink_statement.side_effect = _mock_post_statement
        mock_table_exists.side_effect = _mock_table_exists
        mock_get_statement.side_effect = _mock_get_statement

        pipeline_folder = os.getenv("PIPELINES")
        table_name = "int_p3_user_role"
        test_case_name = "test_int_p3_user_role_2"
        compute_pool_id = "dev_pool_id"
        test_suite_def, table_ref, prefix, test_result= test_mgr._init_test_foundation(table_name, 
                                                                                       test_case_name, 
                                                                                       compute_pool_id)
        print(f"test_suite_def: {test_suite_def.test_suite[1]}")
        test_case = test_suite_def.test_suite[1]
        
        statements = test_mgr._execute_test_inputs(test_case=test_case,
                                                      table_ref=table_ref,
                                                      prefix="dev-ins",
                                                      compute_pool_id=compute_pool_id)
        assert len(statements) == 3
        assert statements[0].name == "dev-ins-src-p3-roles-ut"
        assert statements[0].status["phase"] == "RUNNING"
        print(f"statement: {statements[0]}")
        

    

if __name__ == '__main__':
    unittest.main()