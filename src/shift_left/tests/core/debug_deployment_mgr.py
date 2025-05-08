"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os

#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
#os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-dev.yaml"
#os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
os.environ["PIPELINES"] = "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
import shift_left.core.pipeline_mgr as pipeline_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import (
        PIPELINE_JSON_FILE_NAME
)
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.models.flink_statement_model import *
import shift_left.core.deployment_mgr as dm
from typing import Union



class DebugDeploymentManager(unittest.TestCase):
       
    def test_deplo(self):
        """
        """
        import shift_left.core.deployment_mgr as dm
        config = get_config()
        table_name = "int_qx_infocard_helper_get_full_result"
        result = dm.deploy_pipeline_from_table(table_name=table_name,
                                               inventory_path=os.getenv("PIPELINES"), 
                                               compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                               dml_only=False, 
                                               may_start_children=True,
                                               force_sources=False)
        assert result
        print(result.model_dump_json())


    def _test_build_execution_plan(self):
        os.environ["PIPELINES"]= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        inventory_path=  os.environ["PIPELINES"]
        table_name="src_master_template"
        pipeline_def=  pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
        config = get_config()
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def=pipeline_def, 
                                                                compute_pool_id="lfcp-gyjd2v", 
                                                                dml_only=False, 
                                                                may_start_children=False,
                                                                force_sources=False,
                                                                start_time = datetime.now())
        
        dm.persist_execution_plan(execution_plan)
        print(dm.build_summary_from_execution_plan(execution_plan, compute_pool_mgr.get_compute_pool_list()))
           

    def _test_report_running_flink_statements_for_all_from_product(self):
        os.environ["PIPELINES"]= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        inventory_path=  os.environ["PIPELINES"]
        table_name="stage_tenant_dimension"
        pipeline_def=  pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
        node = pipeline_def.to_node()
        node = dm._assign_compute_pool_id_to_node(node,None)
        print(node)

    def _test_get_topic_message_count(self):
        os.environ["CONFIG_FILE"] =  os.getenv("HOME") +  "/.shift_left/config-stage-flink.yaml"
        client = ConfluentCloudClient(get_config())
        topic_name = "src_aqem_tag_tag"
        message_count = client.get_topic_message_count(topic_name)
        print(f"Message count for {topic_name}: {message_count}")

if __name__ == '__main__':
    unittest.main()