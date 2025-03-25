
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
    
from shift_left.core.pipeline_mgr import (
    FlinkStatementHierarchy
)
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import (get_ddl_dml_names_from_table,
            extract_product_name
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
       
    def test_deploy_pipeline_from_sink_table(self):
        """
        As a sink table, it needs to verify the parents are running. This is the first deployment
        so it will run ddl, then ddls of all parent recursively.
        As we deploy both DDL and DML, force does not need to be True
        """
        config = get_config()
        table_name="fct_order"
        inventory_path= os.getenv("PIPELINES")
        pipeline_def: FlinkStatementHierarchy = dm.walk_the_hierarchy_for_report_from_table(table_name, inventory_path )
        product_name = extract_product_name(pipeline_def.path)
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_table(pipeline_def.table_name, 
                                                        config['kafka']['cluster_type'], 
                                                        product_name)
        statement = dm._deploy_parents_if_needed([(pipeline_def, ddl_statement_name, dml_statement_name)], pipeline_def, config['flink']['compute_pool_id'], config)
        print(statement)

if __name__ == '__main__':
    unittest.main()