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


    def test_at_cli_level(self):
        runner = CliRunner()
        #result = runner.invoke(app, ['pipeline', 'deploy', '--table-name', 'aqem_fct_event_action_item_assignee_user', '--force-ancestors', '--cross-product-deployment'])
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'src_qx_training_trainee', '--may-start-descendants', '--cross-product-deployment'])
        #result = runner.invoke(app, ['pipeline', 'build-execution-plan', '--table-name', 'int_qx_infocard_helper_get_full_result'])
        #result = runner.invoke(app, ['table', 'migrate', 'dim_training_course', os.getenv('SRC_FOLDER','.') + '/dimensions/qx/dim_training_course.sql', os.getenv('STAGING')])
        #result = runner.invoke(app, ['table', 'init-unit-tests', 'aqem_fct_step_role_assignee_relation'])
        #result = runner.invoke(app, ['table', 'build-inventory'])
        #result = runner.invoke(app, ['pipeline', 'build-metadata', os.getenv('PIPELINES') + '/dimensions/aqem/dim_event_action_item/sql-scripts/dml.aqem_dim_event_action_item.sql'])
        #result = runner.invoke(app, ['table', 'run-unit-tests', 'aqem_dim_event_element', '--test-case-name', 'test_aqem_dim_event_element_1'])
        result = runner.invoke(app, ['pipeline', 'deploy', '--product-name', 'aqem', '--max-thread' , 10, '--pool-creation'])
        # result = runner.invoke(app, ['pipeline', 'undeploy', '--product-name', 'aqem', '--no-ack'])
        print(result.stdout)

      
    def _test_6_0_deploy_by_medals_src(self):
        """
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        config = get_config()
        for table in ["src_x", "src_y", "src_p2_a", "src_b"]:
            try:
                print(f"Dropping table {table}")
                #sm.drop_table(table)
                print(f"Table {table} dropped")
            except Exception as e:
                print(e)
        table_mgr.build_inventory(os.getenv("PIPELINES"))
        pipeline_mgr.build_all_pipeline_definitions(os.getenv("PIPELINES"))
        summary, execution_plan = dm.build_and_deploy_all_from_directory( directory=os.getenv("PIPELINES") + "/sources/p2",
                                               inventory_path=os.getenv("PIPELINES"), 
                                               compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               execute_plan=True,
                                               force_ancestors=False)

        
if __name__ == '__main__':
    unittest.main()