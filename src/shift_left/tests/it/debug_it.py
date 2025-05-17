import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.metric_mgr as metric_mgr

class TestDebugIntegrationTests(unittest.TestCase):

    def test_get_total_message(self):
        config = get_config()
        print("test_get_total_messages")
        table_name = "src_aqem_recordconfiguration_form_element"
        compute_pool_id = config["flink"]["compute_pool_id"]
        nb_of_messages = metric_mgr.get_total_amount_of_messages(table_name, compute_pool_id)
        print(nb_of_messages)
        assert nb_of_messages >= 0

if __name__ == '__main__':
    unittest.main()