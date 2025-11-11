import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
import json

#os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
#data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
#os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
#os.environ["SRC_FOLDER"] = str(data_dir / "spark-project")

from shift_left.core.utils.app_config import get_config
import  shift_left.core.pipeline_mgr as pipeline_mgr
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.metric_mgr as metric_mgr
import shift_left.core.test_mgr as test_mgr
import shift_left.core.table_mgr as table_mgr
from typer.testing import CliRunner
from shift_left.cli import app

import shift_left.core.statement_mgr as sm
import shift_left.core.deployment_mgr as dm  

class TestDebugIntegrationTests(unittest.TestCase):


    def _test_at_cli_level(self):
        runner = CliRunner()
        #result = runner.invoke(app, ['pipeline', 'deploy', '--table-name', 'aqem_fct_event_action_item_assignee_user', '--force-ancestors', '--cross-product-deployment'])
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'src_qx_training_trainee', '--may-start-descendants', '--cross-product-deployment'])
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--product-name', 'qx'])
        #result = runner.invoke(app, ['table', 'migrate', 'dim_training_course', os.getenv('SRC_FOLDER','.') + '/dimensions/qx/dim_training_course.sql', os.getenv('STAGING')])
        #result = runner.invoke(app, ['table', 'init-unit-tests', 'aqem_fct_step_role_assignee_relation'])
        #result = runner.invoke(app, ['table', 'build-inventory'])
        #result = runner.invoke(app, ['pipeline', 'build-metadata', os.getenv('PIPELINES') + '/stage/stage_tenant_dimension/dim_event_action_item/sql-scripts/dml.aqem_dim_event_action_item.sql'])
        #result = runner.invoke(app, ['table', 'run-unit-tests', 'aqem_dim_event_element', '--test-case-name', 'test_aqem_dim_event_element_1'])
        #result = runner.invoke(app, ['pipeline', 'deploy', '--product-name', 'aqem', '--max-thread' , 10, '--pool-creation'])
        #result = runner.invoke(app, ['pipeline', 'undeploy', '--product-name', 'aqem', '--no-ack'])
        result = runner.invoke(app,['pipeline', 'build-all-metadata'])
        #result = runner.invoke(app, ['pipeline', 'prepare', os.getenv('PIPELINES') + '/alter_table_avro_dev.sql'])
        #result = runner.invoke(app, ['table', 'init-unit-tests',  '--nb-test-cases', '1', 'aqem_dim_event_element'])
        #result = runner.invoke(app, ['pipeline', 'analyze-pool-usage', '--directory', os.getenv('PIPELINES') + '/sources])
        print(result.stdout)

      

        
if __name__ == '__main__':
    unittest.main()