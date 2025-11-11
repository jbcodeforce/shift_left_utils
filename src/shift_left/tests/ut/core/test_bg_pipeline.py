"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import pathlib
from unittest.mock import patch
os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent.parent /  "config.yaml")

import shift_left.core.deployment_mgr as dm
import shift_left.core.table_mgr as tm
import shift_left.core.pipeline_mgr as pm
from ut.core.BaseUT import BaseUT


class TestBgPipeline(BaseUT):

    @classmethod
    def setUpClass(cls):
        data_dir = pathlib.Path(__file__).parent.parent.parent / "data"  # Path to the data directory
        os.environ["PIPELINES"] = str(data_dir / "flink-project/pipelines")
        tm.get_or_create_inventory(os.getenv("PIPELINES"))
        pm.delete_all_metada_files(os.getenv("PIPELINES"))
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_bg_one_table(self):
        table_list_name = ["fct_user_per_group"]
        result, report= dm.build_and_deploy_all_from_table_list(include_table_names=table_list_name,
                                                inventory_path=os.getenv("PIPELINES"),
                                                compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                                                dml_only=False,
                                                may_start_descendants=False,
                                                force_ancestors=False,
                                                cross_product_deployment=False,
                                                execute_plan=True,
                                                sequential=True,
                                                pool_creation=False,
                                                exclude_table_names=[],
                                                max_thread=1,
                                                version="_v2")
