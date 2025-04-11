
import unittest
import os
import pathlib
from collections import deque
#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["CONFIG_FILE"] = "/Users/jerome/Code/customers/master-control/data-platform-flink/config.yaml"
from shift_left.core.utils.file_search import get_or_build_source_file_inventory
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import (
    FlinkTableReference,
    PipelineReport,
    read_pipeline_definition_from_file,
    FlinkTablePipelineDefinition,
    PIPELINE_JSON_FILE_NAME
)
from shift_left.core.utils.file_search import (
        get_table_ref_from_inventory,
        get_or_build_inventory
)
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.flink_statement_model import *

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
import json
from typing import Union



class TestDeploymentManager(unittest.TestCase):
    
    
    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        #os.environ["CONFIG_FILE"] = str(data_dir / "../config.yaml")
        
        #tm.get_or_create_inventory(os.getenv("PIPELINES"))
        
       
    def _test_deploy_pipeline_from_sink_table(self):
        """
        As a sink table, it needs to verify the parents are running. This is the first deployment
        so it will run ddl, then ddls of all parent recursively.
        As we deploy both DDL and DML, force does not need to be True
        """
        table_name="fct_order"
        #table_name="int_table_1"
        inventory_path= os.getenv("PIPELINES")
        import shift_left.core.deployment_mgr as dm
        """
        result = dm.deploy_pipeline_from_table(table_name, 
                                               inventory_path, 
                                               config["flink"]["compute_pool_id"], 
                                               False, 
                                               False)
        """
        result = dm.full_pipeline_undeploy_from_table(table_name,inventory_path)
        assert result
        print(result.model_dump_json())


    def _test_creating_pipeline_definition(self):
        os.environ["PIPELINES"] = os.getcwd() + "/../../../data-platform-flink/pipelines"
        filename=os.getenv("PIPELINES") + "/intermediates/mx/int_mx_vaults/sql-scripts/dml.int_mx_vaults.sql"
        filename=os.getenv("PIPELINES") + "/dimensions/aqem/dim_event_action_item/sql-scripts/dml.aqem_dim_event_action_item.sql"
        
        report = pm.build_pipeline_definition_from_dml_content(filename, os.getenv("PIPELINES"))
        print(report)


    def _test_validate_loading_src_inventory(self):
        src_folder_path=os.getenv("HOME") + "/Code/customers/master-control/de-datawarehouse/models"
        all_files= get_or_build_source_file_inventory(src_folder_path)
        print(all_files)

    def _test_list_of_compute_pools(self):
        import shift_left.core.project_manager as pmm
        config = get_config()
        env_id=config['confluent_cloud']['environment_id']
        results = pmm.get_list_of_compute_pool(env_id)
        assert results
        assert len(results) > 0

    def _test_deploy_pipeline_from_src_table(self):
        #table_name="src_aqem_tag_tag"
        table_name="int_aqem_tag_tag_dummy"
        inventory_path= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
        os.environ["PIPELINES"]=inventory_path
        pool_id= get_config()["flink"]["compute_pool_id"]
        pool_id="lfcp-g78q9r"
        import shift_left.core.deployment_mgr as dm
        """
        result = dm.deploy_pipeline_from_table(table_name, 
                                               inventory_path, 
                                               pool_id,  
                                               True, 
                                               True)
        """
        #result = dm.full_pipeline_undeploy_from_table(table_name,inventory_path)
        table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
        table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
        pipeline_def: FlinkTablePipelineDefinition= pm.read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
      
        result = dm.build_execution_plan_from_any_table(pipeline_def=pipeline_def,
                                                        compute_pool_id=pool_id)
        assert result
        for statement in result:
            print(f"Run {statement.dml_statement} on {statement.compute_pool_id} run: {statement.to_run} restart: {statement.to_restart}")


    def test_build_execution_plan(self):
        os.environ["PIPELINES"] = os.getcwd() + "/../../../data-platform-flink/pipelines"
        inventory_path= os.getenv("PIPELINES")
        import shift_left.core.deployment_mgr as dm
        table_name="fct_order"
        pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(inventory_path + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        current_node= pipeline_def.to_node()
        current_node.compute_pool_id = get_config()['flink']['compute_pool_id']
        current_node.update_children = True
        current_node.dml_only= False
        graph = dm._build_table_graph(current_node)
        for node in graph:
            print(f"{node} -> {graph[node].dml_statement}")
        execution_plan = dm._build_execution_plan(graph, current_node)
        for node in execution_plan:
            assert node.table_name
            assert node.dml_ref
            assert node.ddl_ref
            assert node.compute_pool_id
            print(f"{node.table_name}  {node.existing_statement_info.status_phase}, {node.existing_statement_info.compute_pool_id}")
         
        #l = dm._execute_plan(execution_plan, get_config()['flink']['compute_pool_id'])
        #for statement in l:
        #    print(statement)

        

if __name__ == '__main__':
    unittest.main()