"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import json 
import pathlib
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent / "../data/flink-project/pipelines")
import shift_left.core.pipeline_mgr as pm
import shift_left.core.table_mgr as tm
from shift_left.core.utils.app_config import get_config
from  shift_left.core.statement_mgr import *
import shift_left.core.compute_pool_mgr as cpm
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.file_search import (
    get_ddl_dml_names_from_pipe_def,
    PIPELINE_JSON_FILE_NAME
)

class TestPoolManager(unittest.TestCase):
    
    data_dir = None
    
    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent / "../data"  # Path to the data directory
        os.environ["PIPELINES"] = str(cls.data_dir / "flink-project/pipelines")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "dbt-project")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
       
    # ---- Compute pool apis ------------------- 
    def _test_build_pool_spec(self):
        config = get_config()
        result = cpm._build_compute_pool_spec("fct-order", config)
        assert result
        assert result['display_name'] == "cp-fct-order"
        print(result)

    def test_verify_pool_state(self):
        """
        Given the compute pool id in the test config filr, get information about the pool using cloud client
        """
        config = get_config()
        client = ConfluentCloudClient(config)
        result = cpm._verify_compute_pool_provisioned(client, config['flink']['compute_pool_id'],
                                                      config.get('confluent_cloud').get('environment_id'))
        assert result == True

    def test_get_compute_pool_list(self):
        config = get_config()
        pools = cpm.get_compute_pool_list(config.get('confluent_cloud').get('environment_id'))
        self.assertGreater(len(pools), 0)
        assert pools[0].get('name')
        assert pools[0].get('env_id') == config.get('confluent_cloud').get('environment_id')
        print(json.dumps(pools, indent=2))

    def test_validate_a_pool(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        result = cpm._validate_a_pool(client, config['flink']['compute_pool_id'], config.get('confluent_cloud').get('environment_id'))
        assert result


if __name__ == '__main__':
    unittest.main()