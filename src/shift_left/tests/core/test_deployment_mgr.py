
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../data/flink-project/pipelines")
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.statement_mgr import *
import shift_left.core.deployment_mgr as dm
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import get_ddl_dml_names_from_pipe_def

class TestDeploymentManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
        

    def test_clean_things(self):
        for table in ["src_table_1", "src_table_2", "src_table_3", "int_table_1", "int_table_2", "fct_order"]:
            dm.drop_table(table)

 

    def test_src_table_deployment(src):
        """
        Given a source table with children, deploy the DDL and DML without the children.
        """
        config= get_config()
        inventory_path = os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table("src_table_1", inventory_path, config['flink']['compute_pool_id'], False, False)
        assert result
        print(result)


    def test_deploy_pipeline_from_sink_table(self):
        config = get_config()
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table(table_name, inventory_path, config["flink"]["compute_pool_id"], True, False)
        assert result
        print(result)
        print("Validating running dml")
        result = dm.report_running_flink_statements(table_name, inventory_path)
        assert result
        print(result)
        
        

if __name__ == '__main__':
    unittest.main()