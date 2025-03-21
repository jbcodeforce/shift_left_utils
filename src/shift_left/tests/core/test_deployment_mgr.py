
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
       
  
    def test_search_statement(self):
        statement = dm.search_existing_flink_statement("workspace-2025-02-19-035912-12318e87-8cff-4a80-91e5-5a42b46c909d")
        assert statement
        print(statement.spec.statement)

    def test_search_non_existant_statement(self):
        statement_dict = dm.search_existing_flink_statement("dummy")
        assert statement_dict == None

    def test_deploy_sink_table(self):
        config = get_config()
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        pipeline_def = pm.walk_the_hierarchy_for_report_from_table(table_name, inventory_path )
       
        assert pipeline_def.ddl_path
        print(pipeline_def.ddl_path)
        ddl_name, dml_name = get_ddl_dml_names_from_table(table_name, config["kafka"]["cluster_type"], "p1")
        print(ddl_name)
        statement= dm._deploy_ddl_statements(inventory_path + '/../' + pipeline_def.ddl_path, ddl_name, config["flink"]["compute_pool_id"])
        assert statement.status.phase == 'COMPLETED'
    
    def test_delete_a_statement(self):
        response = dm._delete_flink_statement('dev-p1-ddl-fct-order')
        assert response
        print(response)
        response = dm.search_existing_flink_statement('dev-p1-ddl-fct-order')
        assert response == None

    def test_build_pool_spec(self):
        config = get_config()
        result = dm._build_compute_pool_spec("fct-order", config)
        assert result
        assert result['display_name'] == "cp-fct-order"
        print(result)

    def test_create_compute_pool(self):
        result = dm._create_compute_pool("fct-order")
        assert result
        print(result)

    def test_verify_pool_state(self):
        client = ConfluentCloudClient(get_config())
        result = dm._verify_compute_pool_provisioned(client, "lfcp-d3n9zz")
        assert result
        print(result)

    def test_validate_a_pool(self):
        result = dm._validate_a_pool("lfcp-d3n9zz")
        assert result



if __name__ == '__main__':
    unittest.main()