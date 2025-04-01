
import unittest
import os
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
    
import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import (
    FlinkTableReference,
    PipelineReport,
    PIPELINE_JSON_FILE_NAME
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


    def _test_one_table(self):
        os.environ["PIPELINES"] = os.getcwd() + "/../../../data-platform-flink/pipelines"
        print(os.getenv("PIPELINES"))
        report = pm.build_pipeline_definition_from_table(os.getenv("PIPELINES") + "/intermediates/mx/int_mx_vaults/sql-scripts/dml.int_mx_vaults.sql", os.getenv("PIPELINES"))
        print(report)


if __name__ == '__main__':
    unittest.main()