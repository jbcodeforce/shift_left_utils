import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
os.environ["PIPELINES"] =  "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.metric_mgr as metric_mgr
import shift_left.core.test_mgr as test_mgr
class TestDebugIntegrationTests(unittest.TestCase):

    def _test_get_total_message(self):
        config = get_config()
        print("test_get_total_messages")
        table_name = "src_aqem_recordconfiguration_form_element"
        compute_pool_id = config["flink"]["compute_pool_id"]
        nb_of_messages = metric_mgr.get_total_amount_of_messages(table_name, compute_pool_id)
        print(nb_of_messages)
        assert nb_of_messages >= 0

    def _test_running_one_sql_test_case(self):
        table_name = "aqem_dim_role"
        test_case="test_aqem_dim_role_1"
        test_result = test_mgr.execute_one_test(table_name=table_name, test_case_name=test_case)
        assert test_result is not None
        print(f"test_result: {test_result.model_dump_json(indent=3)}")

    def test_delete_test_artifacts(self):
        config = get_config()
        table_name = "aqem_dim_role"
        compute_pool_id = config["flink"]["compute_pool_id"]
        test_mgr.delete_test_artifacts(table_name=table_name, 
                                       compute_pool_id=compute_pool_id,
                                       test_suite_result=None)

if __name__ == '__main__':
    unittest.main()