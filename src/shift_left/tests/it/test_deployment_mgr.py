"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
from datetime import datetime
import pathlib

os.environ["CONFIG_FILE"] =   str(pathlib.Path(__file__).parent.parent / "config-ccloud.yaml")
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.app_config import get_config
from  shift_left.core.models.flink_statement_model import *
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
    
       
    def test_3_deploy_src_table(self):
        """
        Given a source table with children, deploy the DDL and DML without the children.
        """
        inventory_path = os.getenv("PIPELINES")
        table_name = "src_table_1"
        summary,result, = dm.build_deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=None, 
                                               dml_only=False, 
                                               may_start_descendants=False,
                                               force_ancestors=False,
                                               execute_plan=True)
        assert result
        print(result)
        assert summary
        print(summary)
        for statement in result.flink_statements_deployed:
            print(f"statement: {statement.name} - {statement.status.phase} - {statement.spec.compute_pool_id}")
        report = dm.report_running_flink_statements_for_a_table(table_name)
        assert report
        print(report)


    def test_4_deploy_pipeline_from_sink_table(self):
        config = get_config()
        table_name="p1_fct_order"
        inventory_path= os.getenv("PIPELINES")
        result = dm.build_deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=config['flink']['compute_pool_id'], 
                                               dml_only=True, 
                                               may_start_descendants=False,
                                               force_ancestors=False)
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
        result = dm.build_deploy_pipeline_from_table(table_name=table_name, 
                                               inventory_path=inventory_path, 
                                               compute_pool_id=config['flink']['compute_pool_id'], 
                                               dml_only=True, 
                                               may_start_descendants=True,
                                               force_ancestors=False)
        assert result
        print(result)
        print("Validating running dml")
        result = dm.report_running_flink_statements(table_name, inventory_path)
        assert result
        print(result)

    def test_6_deploy_facts_tables(self):
        """
        """
        os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        import shift_left.core.deployment_mgr as dm
        config = get_config()
        result = dm.build_and_deploy_all_from_directory( directory=os.getenv("PIPELINES") + "/facts/p2",
                                               inventory_path=os.getenv("PIPELINES"), 
                                               compute_pool_id=config.get('flink').get('compute_pool_id'), 
                                               dml_only=False, 
                                               may_start_descendants=True,
                                               force_ancestors=False)
        assert result
        print(result.model_dump_json())

    def test_7_report_running_flink_statements_for_all_from_product(self):
        os.environ["PIPELINES"]= str(pathlib.Path(__file__).parent.parent / "data/flink-project/pipelines")
        result = dm.report_running_flink_statements_for_a_product('p1', os.getenv("PIPELINES"))
        assert result
        print(result)


if __name__ == '__main__':
    unittest.main()