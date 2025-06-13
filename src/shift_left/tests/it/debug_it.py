import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
os.environ["PIPELINES"] =  "/Users/jerome/Code/customers/mc/data-platform-flink/pipelines"
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.metric_mgr as metric_mgr
import shift_left.core.test_mgr as test_mgr
from typer.testing import CliRunner
from shift_left.cli_commands.pipeline import app

class TestDebugIntegrationTests(unittest.TestCase):


    def test_exec_plan(self):
        runner = CliRunner()
        #result = runner.invoke(app, ['build-execution-plan', '--table-name', 'aqem_fct_event_action_item_assignee_user', '--force-ancestors', '--cross-product-deployment'])
        result = runner.invoke(app, ['build-execution-plan', '--table-name', 'src_tenant_metadata', '--may-start-descendants', '--cross-product-deployment'])
        print(result.stdout)

        

        
if __name__ == '__main__':
    unittest.main()