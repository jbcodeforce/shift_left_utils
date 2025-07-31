import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json
#os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-2b-flink.yaml"
#os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-dev.yaml"

os.environ["PIPELINES"] =  "/Users/jerome/Code/customers/att/staging"
#os.environ["STAGING"] =  "/Users/jerome/Code/customers/mc/data-platform-flink/staging"
#os.environ["SRC_FOLDER"] =  "/Users/jerome/Code/customers/mc/de-datawarehouse/models"
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import Statement, StatementInfo, StatementListCache, Spec, Status
import  shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.models.flink_statement_model import Statement, StatementResult, Data, OpRow
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.metric_mgr as metric_mgr
import shift_left.core.test_mgr as test_mgr
from typer.testing import CliRunner
from shift_left.cli import app

class TestDebugIntegrationTests(unittest.TestCase):


    def test_exec_plan(self):
        runner = CliRunner()
        #result = runner.invoke(app, ['pipeline', 'deploy', '--table-name', 'aqem_fct_event_action_item_assignee_user', '--force-ancestors', '--cross-product-deployment'])
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'src_qx_training_trainee', '--may-start-descendants', '--cross-product-deployment'])
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'int_qx_infocard_helper_get_full_result', '--may-start-descendants'])
        #result = runner.invoke(app, ['table', 'migrate', 'dim_training_course', os.getenv('SRC_FOLDER','.') + '/dimensions/qx/dim_training_course.sql', os.getenv('STAGING')])
        #result = runner.invoke(app, ['table', 'init-unit-tests', 'uvs_pm_nok_stage_73xx_json_stream'])
        result = runner.invoke(app, ['table', 'build-inventory'])
        print(result.stdout)

        

        
if __name__ == '__main__':
    unittest.main()