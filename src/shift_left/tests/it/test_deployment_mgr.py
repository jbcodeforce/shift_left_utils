"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from datetime import datetime
import pathlib
from shift_left.core.utils.app_config import shift_left_dir
os.environ["CONFIG_FILE"] =  shift_left_dir +"/it-config.yaml"
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.app_config import get_config
from  shift_left.core.flink_statement_model import *
import shift_left.core.statement_mgr as sm
import shift_left.core.deployment_mgr as dm
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition, FlinkStatementNode
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import get_ddl_dml_names_from_pipe_def

"""
This test suite is used to test the deployment manager with connection to Confluent Cloud.
"""
class TestDeploymentManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")

    def test_1_clean_things(self):
        for table in ["src_table_1", "src_p1_table_2", "src_p1_table_3", "int_p1_table_2", "int_p1_table_1", "p1_fct_order"]:
            try:
                print(f"Dropping table {table}")
                sm.drop_table(table)
                print(f"Table {table} dropped")
            except Exception as e:
                print(e)
 
    def test_2_build_execution_plan(self):  
        inventory_path= os.getenv("PIPELINES")
        pipeline_def: FlinkTablePipelineDefinition = dm.read_pipeline_definition_from_file(inventory_path + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        execution_plan = dm.build_execution_plan_from_any_table(pipeline_def, get_config()['flink']['compute_pool_id'], False, True, datetime.now())
        assert execution_plan
        for node in execution_plan.nodes:
            print(f"{node}\n\n\n")
        assert execution_plan.nodes[0].table_name == "src_table_1" or execution_plan.nodes[0].table_name == "src_table_2" or execution_plan.nodes[0].table_name == "src_table_3"
        for i in range(0, len(execution_plan.nodes)):
            assert execution_plan.nodes[i].to_run
        assert execution_plan.nodes[5].table_name == "fct_order"
    
       
    def test_3_src_table_deployment(src):
        """
        Given a source table with children, deploy the DDL and DML without the children.
        """
        config= get_config()
        inventory_path = os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table(table_name="src_table_1", 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=None, 
                                               dml_only=False, 
                                               may_start_children=False)
        assert result
        print(result)
        for statement in result.flink_statements_deployed:
            print(statement)


    def test_4_deploy_pipeline_from_sink_table(self):
        config = get_config()
        table_name="p1_fct_order"
        inventory_path= os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=config['flink']['compute_pool_id'], 
                                               dml_only=True, 
                                               may_start_children=False)
        assert result
        print(result)
        print("Validating running dml")
        result = dm.report_running_flink_statements(table_name, inventory_path)
        assert result
        print(result)
        


        
    def test_5_deploy_pipeline_from_int_table(self):
        config = get_config()
        table_name="int_p1_table_1"
        inventory_path= os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=config['flink']['compute_pool_id'], 
                                               dml_only=True, 
                                               may_start_children=True)
        assert result
        print(result)
        print("Validating running dml")
        result = dm.report_running_flink_statements(table_name, inventory_path)
        assert result
        print(result)

if __name__ == '__main__':
    unittest.main()