
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
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       


    # ---- Compute pool apis ------------------- 
    def test_build_pool_spec(self):
        config = get_config()
        result = dm._build_compute_pool_spec("fct-order", config)
        assert result
        assert result['display_name'] == "cp-fct-order"
        print(result)

    def test_verify_pool_state(self):
        """
        Given the compute pool id in the test config filr, get information about the pool using cloud client
        """
        config = get_config()
        client = ConfluentCloudClient(config)
        result = dm._verify_compute_pool_provisioned(client, config['flink']['compute_pool_id'])
        assert result == True

    def test_get_compute_pool_list(self):
        client = ConfluentCloudClient(get_config())
        config=get_config()
        pools = client.get_compute_pool_list(config.get('confluent_cloud').get('environment_id'))
        self.assertGreater(len(pools), 0)
        print(json.dumps(pools, indent=2))


    def test_validate_a_pool(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        result = dm._validate_a_pool(client, config['flink']['compute_pool_id'])
        assert result

    # ---- Statement related  apis tests ------------------- 

    def test_dml_ddl_names(self):
        pipe_def = pm.read_pipeline_definition_from_file( os.getenv("PIPELINES") + "/facts/p1/fct_order/" + PIPELINE_JSON_FILE_NAME)
        config = get_config()
        ddl, dml = dm._return_ddl_dml_names(pipe_def, config)
        assert ddl == "dev-p1-ddl-fct-order"
        assert dml == "dev-p1-dml-fct-order"
        
    def test_search_non_existant_statement(self):
        statement_dict = dm.search_existing_flink_statement("dummy")
        assert statement_dict == None

    def test_execute_show_create_table_then_delete_statement(self):
        config= get_config()
        sql_path = os.getenv("PIPELINES") + "/intermediates/p1/int_table_1/tests/show_create_table.sql"
        statement = dm.deploy_flink_statement(sql_path, None, "show-table", config)
        assert statement
        print(statement)
        statement_dict = dm.search_existing_flink_statement("show-table")
        assert statement_dict
        print(f"\n -- {statement_dict}")
        response = dm.delete_flink_statement("show_table")
        assert response

    def _test_clean_things(self):
        for table in ["src_table_1", "src_table_2", "src_table_3", "int_table_1", "int_table_2", "fct_order"]:
             dm.drop_table(table)
             dm.delete_flink_statement("drop-" + table.replace('_','-'))

    def test_table_structure(self):
        result = dm.get_table_structure("int_table_2")
        print(f"\n\n---- {result}")
        assert result
        assert "int_table_2" in result

    def test_src_table_deployment(src):
        """
        Given a source table with children, deploy the DDL and DML without the children.
        """
        config= get_config()
        inventory_path = os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table("src_table_1", inventory_path, config['flink']['compute_pool_id'], False, False)
        assert result
        print(result)

    def _test_ddl_deployment(self):
        table_name="int_table_1"
        inventory_path= os.getenv("PIPELINES")
        pipeline_def = pm.build_pipeline_report_from_table(table_name, inventory_path )
        statement = dm._process_table_deployment(pipeline_def)
        assert statement
        print(statement)

    def _test_drop_table(self):
        dm.drop_table("int_table_1")
    
    def _test_deploy_flink_statement_without_compute_pool(self):
        config = get_config()
        insert_data_path = os.getenv("PIPELINES") + "/facts/p1/fct_order/sql-queries/ddl.fct_order.sql"
        statement = dm.deploy_flink_statement(insert_data_path, None, None, config)
        print (statement)
        assert statement
        dm.drop_table("fct_order")

    def test_deploy_pipeline_from_sink_table(self):
        config = get_config()
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table(table_name, inventory_path, config["flink"]["compute_pool_id"], True, False)
        assert result
        print(result)







if __name__ == '__main__':
    unittest.main()