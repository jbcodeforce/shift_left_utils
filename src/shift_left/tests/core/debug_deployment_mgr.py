
import unittest
import os
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
    
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import (
    FlinkTableReference,
    PipelineReport
)
from shift_left.core.utils.file_search import (
        get_table_ref_from_inventory,
            load_existing_inventory
)
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.flink_statement_model import *
import shift_left.core.deployment_mgr as dm
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
import json
from typing import Union



class TestDeploymentManager(unittest.TestCase):
    
    
    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(data_dir / "dbt-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
    def _test_deploy_pipeline_from_sink_table(self):
        """
        As a sink table, it needs to verify the parents are running. This is the first deployment
        so it will run ddl, then ddls of all parent recursively.
        As we deploy both DDL and DML, force does not need to be True
        """
        config = get_config()
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        result = dm.deploy_pipeline_from_table(table_name, 
                                               inventory_path, 
                                               config["flink"]["compute_pool_id"], 
                                               False, 
                                               False)
        assert result
        print(result)
    
    def test_create_statement(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        statement_name="test-statement"
        sql_content = "show create table `examples`.`marketplace`.`clicks`;"
        properties = {'sql.current-catalog' : 'examples' , 'sql.current-database' : 'marketplace'}
        rep= client.delete_flink_statement(statement_name)
        statement = client.post_flink_statement(config['flink']['compute_pool_id'], statement_name, sql_content, properties, False)
        print(f"\n\n---- {statement}")
        statement = client.get_statement_info(statement_name)
        print(statement)
        status=client.delete_flink_statement(statement_name)
        print(f"\n--- {status}")

    def _test_create_report(self):
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        root_ref= pm.build_pipeline_report_from_table(table_name, inventory_path)
        assert root_ref
        print(root_ref.model_dump_json(indent=3))

 




if __name__ == '__main__':
    unittest.main()