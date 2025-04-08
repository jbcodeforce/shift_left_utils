
import unittest
import os
import pathlib
from collections import deque
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["CONFIG_FILE"] = "/Users/jerome/Code/customers/master-control/data-platform-flink/config.yaml"
from shift_left.core.utils.file_search import get_or_build_source_file_inventory
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import (
    FlinkTableReference,
    PipelineReport,
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


    def _test_one_table(self):
        os.environ["PIPELINES"] = os.getcwd() + "/../../../data-platform-flink/pipelines"
        print(os.getenv("PIPELINES"))
        report = pm.build_pipeline_definition_from_table(os.getenv("PIPELINES") + "/intermediates/mx/int_mx_vaults/sql-scripts/dml.int_mx_vaults.sql", os.getenv("PIPELINES"))
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

    def test_deploy_pipeline_from_src_table(self):
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
        pipeline_def = dm._complement_pipeline_definition(pipeline_def,pool_id)
        queue=deque()
        result = dm._build_execution_plan(pipeline_def,queue)
        assert result
        print(result.model_dump_json(indent=3))


if __name__ == '__main__':
    unittest.main()