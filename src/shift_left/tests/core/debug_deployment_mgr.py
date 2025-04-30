"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import time
import pathlib
from unittest.mock import patch
#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-all.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
os.environ["CONFIG_FILE"] = "/Users/jerome/.shift_left/it-config.yaml"
from shift_left.core.pipeline_mgr import (
    read_pipeline_definition_from_file,
    FlinkTablePipelineDefinition
)
from shift_left.core.utils.file_search import (
        PIPELINE_JSON_FILE_NAME
)
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.flink_statement_model import *
import shift_left.core.deployment_mgr as dm
from typing import Union



class TestDeploymentManager(unittest.TestCase):
       
    def _test_deploy_pipeline_from_sink_table(self):
        """
        As a sink table, it needs to verify the parents are running. This is the first deployment
        so it will run ddl, then ddls of all parent recursively.
        As we deploy both DDL and DML, force does not need to be True
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        import shift_left.core.deployment_mgr as dm
        config = get_config()
        table_path="/facts/p1/fct_order/"
        result, summary=dm.deploy_pipeline_from_table("p1_fct_order", os.getenv("PIPELINES"), config.get('flink').get('compute_pool_id'), False, True)
        assert summary
        print(summary)
        assert result
        print(result.model_dump_json())


    def test_build_execution_plan(self):
        inventory_path= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        #table_path="/intermediates/aqem/tag_tag_dummy/"
        #table_path="/facts/aqem/fct_action_item_event/"
        #table_path="/views/aqem/mv_dim_element_event/"
        table_path="/sources/mx/src_data_capture/"
        inventory_path=  os.environ["PIPELINES"]
        table_path="/intermediates/p1/int_table_1/"
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(inventory_path + table_path + PIPELINE_JSON_FILE_NAME)
        config = get_config()
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                config.get('flink').get('compute_pool_id'), 
                                                                False, 
                                                                True,
                                                                start_time = datetime.now())
        
        dm.persist_execution_plan(execution_plan)
        print(dm.build_summary_from_execution_plan(execution_plan))
           
        
    @patch('shift_left.core.deployment_mgr._get_and_update_statement_info_for_node')
    def _test_build_execution_plan_for_intermediated_table_including_children(self, mock_get_status): 
        """
        From an intermediate like z, start parents up to running sources that are not already running.
        plan should have: src_x, x, y, z, d, src_b, b, f, c, p, e. As force childen is false no children will be restarted.
        """
        inventory_path= os.getenv("PIPELINES")
        def mock_status(statement_name):
            if (statement_name.startswith("dev-dml-src-y") or 
                statement_name.startswith("dev-dml-p")):
                return StatementInfo(status_phase="RUNNING", compute_pool_id="test-pool-123")  
            else:
                return StatementInfo(status_phase="UNKNOWN", compute_pool_id=None)

        mock_get_status.side_effect = mock_status
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(inventory_path + "/intermediates/p2/z/" + PIPELINE_JSON_FILE_NAME)
        
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, 
                                                                get_config()['flink']['compute_pool_id'], 
                                                                False, 
                                                                True, datetime.now())  
        print(dm.build_summary_from_execution_plan(execution_plan))

        assert len(execution_plan.nodes) == 11

        

if __name__ == '__main__':
    unittest.main()