"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch, MagicMock
import os
import pathlib
from datetime import datetime
import uuid
from typing import Tuple
import time
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
import shift_left.core.deployment_mgr as dm

from shift_left.core.models.flink_statement_model import (
    Statement,
    StatementInfo,
    Status,
    Spec,
    Metadata
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode
)

from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from ut.core.BaseUT import BaseUT

class TestDeploymentManager(BaseUT):
    """Test suite for the verifying the exclude table file functionality for deployment and build execution plan."""


    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES",""))

    def setUp(self) -> None:
        """Set up test case before each test."""
        self.config = get_config()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID_1
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES","")
        self.count = 0  # Initialize count as instance variable


    def _get_status(self, statement_name: str) -> StatementInfo:
            print(f"@@@@ get status {statement_name} -> UNKNOWN")
            return self._create_mock_get_statement_info(name=statement_name, status_phase="UNKNOWN")

    #  ----------- TESTS -----------

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def _test_filter_tables_from_exclude_table_file_for_product_deployment(self,
                mock_get_statement_list,
                mock_get_status,
                mock_get_compute_pool_list
        ) -> None:
        """
        given a leaf of the pipeline and a list of table to exclude,
        the function should make those table to not run and not restart.
        """

        mock_get_status.side_effect = self._get_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        # Avoid remote call via statement_mgr.get_statement_list() inside build_and_deploy_flink_statement_from_sql_content
        mock_get_statement_list.return_value = {}

        result, report = dm.build_deploy_pipelines_from_product(
            product_name="p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            force_ancestors=True,
            sequential=True,
            may_start_descendants=True,
            exclude_table_names=["src_y", "src_x"]
        )

        assert len(report.tables) == 14
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            if table.table_name in ["z", "x", "y", "d", "f", "p", "c", "e", "b",  "src_b", "src_a", "a"]:
                assert table.to_restart is True
            else:
                assert table.to_restart is False

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def test_filter_tables_from_exclude_table_file_for_table_deployment(self,
                mock_get_statement_list,
                mock_get_status,
                mock_get_compute_pool_list
        ) -> None:
        """
        given a leaf of the pipeline and a list of table to exclude,
        the function should make those table to not run and not restart.
        """

        mock_get_status.side_effect = self._get_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        # Avoid remote call via statement_mgr.get_statement_list() inside build_and_deploy_flink_statement_from_sql_content
        mock_get_statement_list.return_value = {}

        result, report = dm.build_deploy_pipeline_from_table(
            table_name="z",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            force_ancestors=True,
            sequential=True,
            may_start_descendants=True,
            exclude_table_names=["src_y", "src_x"]
        )
        print(f"@@@@ test_filter_tables_from_exclude_table_file_for_table_deployment report.tables: {report.model_dump_json(indent=3)}")
        assert len(report.tables) == 12 # no src_a as it is not part of z parents
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}\t{table.to_run}")
            if table.table_name in ["z", "x", "y", "d", "f", "p", "c", "e", "b",  "src_b"]:
                assert table.to_restart is True or table.to_run is True
            else:
                assert table.to_restart is False and table.to_run is False

        # another table, but a leaf:
        result, report = dm.build_deploy_pipeline_from_table(
            table_name="f",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            force_ancestors=False, # as the status is unknown, parents will be restarted
            sequential=True,
            may_start_descendants=False, # not child
            exclude_table_names=["src_y", "src_x"]
        )
        print(f"@@@@ test_filter_tables_from_exclude_table_file_for_table_deployment report.tables: {report.model_dump_json(indent=3)}")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}\t{table.to_run}")
            if table.table_name in ["z", "x", "y", "d", "f"]:
                assert table.to_run is True or table.to_restart is True
            else:
                assert table.to_restart is False and table.to_run is False


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def _test_filter_tables_from_exclude_table_file_for_list_tables_deployment(self,
                mock_get_statement_list,
                mock_get_status,
                mock_get_compute_pool_list
        ) -> None:
        """
        given a leaf of the pipeline and a list of table to exclude,
        the function should make those table to not run and not restart.
        """
        def _get_status(statement_name: str) -> StatementInfo:
            print(f"@@@@ get status {statement_name} -> RUNNING")
            return self._create_mock_get_statement_info(name=statement_name, status_phase="RUNNING")

        mock_get_status.side_effect = _get_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        # Avoid remote call via statement_mgr.get_statement_list() inside build_and_deploy_flink_statement_from_sql_content
        mock_get_statement_list.return_value = {}

        result, report = dm.build_and_deploy_all_from_table_list(
                    include_table_names=["z", "d", "f", "p", "c", "e"],
                    inventory_path=self.inventory_path,
                    compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                    execute_plan=False,
                    force_ancestors=True,
                    sequential=True,
                    may_start_descendants=True,
                    exclude_table_names=["src_y", "src_x"]
                )

        print(f"@@@@ report.tables: {report.model_dump_json(indent=3)}")
        assert len(report.tables) == 12
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            if table.table_name in ["z", "d", "f", "p", "c", "e", "b", "src_b", "x", "y"]:
                assert table.to_restart is True
            else:
                assert table.to_restart is False


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_list')
    def _test_filter_tables_from_exclude_table_file_for_directory_deployment(self,
                mock_get_statement_list,
                mock_get_status,
                mock_get_compute_pool_list
        ) -> None:
        """
        given a leaf of the pipeline and a list of table to exclude,
        the function should make those table to not run and not restart.
        """
        def _get_status(statement_name: str) -> StatementInfo:
            print(f"@@@@ get status {statement_name} -> RUNNING")
            return self._create_mock_get_statement_info(name=statement_name, status_phase="RUNNING")

        mock_get_status.side_effect = _get_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        # Avoid remote call via statement_mgr.get_statement_list() inside build_and_deploy_flink_statement_from_sql_content
        mock_get_statement_list.return_value = {}
        # intermediates/p2 contains a, b, c, d, z, x, y, z
        result, report = dm.build_and_deploy_all_from_directory(
                    directory=self.inventory_path + "/intermediates/p2",
                    inventory_path=self.inventory_path,
                    compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                    execute_plan=False,
                    force_ancestors=True,
                    may_start_descendants=True,
                    exclude_table_names=["src_x","src_y"]
                )

        print(f"@@@@ report.tables: {report.model_dump_json(indent=3)}")
        assert len(report.tables) == 13 # intermediates + src_a, src_b, and then children p,f,e
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            if table.table_name in ["a", "b", "x", "y", "z", "d", "c", "f", "p", "e", "src_b", "src_a"]:
                assert table.to_restart is True
            else:
                assert table.to_restart is False

if __name__ == '__main__':
    unittest.main()

