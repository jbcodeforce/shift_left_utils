
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
    
import shift_left.core.pipeline_mgr as pm
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import (get_ddl_dml_names_from_table,
            get_table_ref_from_inventory,
            load_existing_inventory,
            FlinkTableReference
        )
from  shift_left.core.flink_statement_model import *
import shift_left.core.deployment_mgr as dm
from shift_left.core.utils.ccloud_client import ConfluentCloudClient

class TestDeploymentManager(unittest.TestCase):
    
    
    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "src-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
  
    def test_search_non_existant_statement(self):
        statement_dict = dm.search_existing_flink_statement("dummy")
        assert statement_dict == None

    def test_build_pool_spec(self):
        config = get_config()
        result = dm._build_compute_pool_spec("fct-order", config)
        assert result
        assert result['display_name'] == "cp-fct-order"
        print(result)

    def test_verify_pool_state(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        result = dm._verify_compute_pool_provisioned(client, config['flink']['compute_pool_id'])
        assert result
        print(result)

    def test_validate_a_pool(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        result = dm._validate_a_pool(client, config['flink']['compute_pool_id'])
        assert result

    def test_ddl_deployment(self):
        table_name="int_table_1"
        inventory_path= os.getenv("PIPELINES")
        pipeline_def = pm.walk_the_hierarchy_for_report_from_table(table_name, inventory_path )
        statement = dm.deploy_ddl_dml_statements(pipeline_def)
        assert statement
        print(statement)

    def test_drop_table(self):
        dm.drop_table("int_table_1")
        
    def test_deploy_flink_statement(self):
        config = get_config()
        insert_data_path = os.getenv("PIPELINES") + "/facts/p1/fct_order/tests/insert_int_table_1_1.sql"
        dm.deploy_flink_statement(insert_data_path, None, None, config)

    def test_deploy_pipeline_from_sink_table(self):
        config = get_config()
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        statement = dm.deploy_pipeline_from_table(table_name, inventory_path, config["flink"]["compute_pool_id"], True, False)

    
    def _test_delete_a_statement(self):
        response = dm._delete_flink_statement('dev-p1-ddl-fct-order')
        assert response
        print(response)
        response = dm.search_existing_flink_statement('dev-p1-ddl-fct-order')
        assert response == None






if __name__ == '__main__':
    unittest.main()