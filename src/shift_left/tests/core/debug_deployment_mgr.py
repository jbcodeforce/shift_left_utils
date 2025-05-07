"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import time
import pathlib
from unittest.mock import patch
#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
import shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.utils.file_search import (
        PIPELINE_JSON_FILE_NAME
)
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.models.flink_statement_model import *
import shift_left.core.deployment_mgr as dm
from typing import Union



class TestDeploymentManager(unittest.TestCase):
       
    def _test_something(self):
        """
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        import shift_left.core.deployment_mgr as dm
        config = get_config()
        result = dm.deploy_all_from_directory(os.getenv("PIPELINES") + "/facts/p2",
                                               os.getenv("PIPELINES"), 
                                               config.get('flink').get('compute_pool_id'), 
                                               False, 
                                               True)
        assert result
        print(result.model_dump_json())


    def _test_build_execution_plan(self):
        os.environ["PIPELINES"]= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        #table_path="/intermediates/aqem/tag_tag_dummy/"
        #table_path="/facts/aqem/fct_action_item_event/"
        #table_path="/views/aqem/mv_dim_element_event/"
        #table_path="/sources/mx/src_data_capture/"
        inventory_path=  os.environ["PIPELINES"]
        #table_path="/intermediates/p1/int_table_1/"
        table_name="aqem_dim_event_action_item"
        pipeline_def=  pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
        config = get_config()
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def=pipeline_def, 
                                                                compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                                                dml_only=False, 
                                                                may_start_children=False,
                                                                force_sources=False,
                                                                start_time = datetime.now())
        
        dm.persist_execution_plan(execution_plan)
        print(dm.build_summary_from_execution_plan(execution_plan))
           

    def test_report_running_flink_statements_for_all_from_product(self):
        os.environ["PIPELINES"]= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        inventory_path=  os.environ["PIPELINES"]
        table_name="stage_tenant_dimension"
        pipeline_def=  pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
        node = pipeline_def.to_node()
        node = dm._assign_compute_pool_id_to_node(node,None)
        print(node)

        

if __name__ == '__main__':
    unittest.main()