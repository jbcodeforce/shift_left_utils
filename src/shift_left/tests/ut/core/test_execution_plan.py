"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from unittest.mock import patch
import os
import pathlib


os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.deployment_mgr as dm
from shift_left.core.models.flink_statement_model import (
    StatementInfo
)

from shift_left.core.utils.report_mgr import DeploymentReport, StatementBasicInfo,TableReport
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition
from ut.core.BaseUT import BaseUT
import warnings

warnings.filterwarnings("ignore", category=SyntaxWarning)
class TestExecutionPlan(BaseUT):
    """
    validate the different scenario to build the execution plan.
    See the topology of flink statements https://github.com/jbcodeforce/shift_left_utils/blob/main/docs/images/flink_pipeline_for_test.drawio.png
    src_y ---> y -->  d -> f
                 \   /
    src_x ---> x - z -> p
          \          \
    src_a -> a        \
    src_b -> b ------>  c -> e
    Added more nodes to test cross product.
    """

    TEST_COMPUTE_POOL_ID_1 = "lfcp-121"

    def setUp(self) -> None:
        """Set up test environment before each individual test."""
        # Get the current PIPELINES path (which may be set by conftest.py fixture)
        self.inventory_path = os.getenv("PIPELINES")

        # Manually reset only the caches we need without affecting file operations
        try:
            import shift_left.core.statement_mgr as statement_mgr
            statement_mgr._statement_list_cache = None
            statement_mgr._runner_class = None
        except (ImportError, AttributeError):
            pass

        try:
            import shift_left.core.compute_pool_mgr as compute_pool_mgr
            compute_pool_mgr._compute_pool_list = None
            compute_pool_mgr._compute_pool_name_modifier = None
        except (ImportError, AttributeError):
            pass

        # Always ensure fresh pipeline definitions to prevent cross-test contamination
        # Other test classes may have deleted these files, so we must rebuild them
        pm.delete_all_metada_files(self.inventory_path)
        pm.build_all_pipeline_definitions(self.inventory_path)


    # ------------ TESTS ------------

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_leaf_table_f_while_parents_running(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        when direct parent d is running
        restarting the leaf "f"
        Should restart only current table f which has one parent d as all other ancestors are running.
        """
        print("\n--> test_build_execution_plan_for_one_table_while_parents_running should start node f only")

        def mock_statement(statement_name: str) -> StatementInfo:
            return self._create_mock_get_statement_info(status_phase="RUNNING")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list

        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="f",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=False, # should get same result if true
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.tables) == 7  # all nodes are present as we want to see running ones too
        for node in execution_plan.tables:
            if node.table_name in ["src_x", "x", "src_y", "y", "z", "d"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name == "f":
                assert node.to_run is False
                assert node.to_restart is True

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_leaf_table_f_while_direct_parent_not_running(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        when direct parent d is not running
        restarting the leaf "f"
        Should restart d then f
        src_y ---> y -->  d -> f
                 \   /
        src_x ---> x - z -> p -> c -> e -> f
              \          \
        src_a -> a        \
        src_b -> b ------>  c -> e
        """
        print("\n--> test_build_execution_plan_for_leaf_table_f_while_direct_parent_not_running should start nodes d and f")

        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-d", "dev-usw2-p2-dml-f"]:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")
            else:
                return self._create_mock_get_statement_info(status_phase="RUNNING")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list

        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="f",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=False,
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 7  # all nodes are present as we want to see running ones too
        for node in report.tables:
            if node.table_name in ["src_x", "x", "src_y", "y", "z"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in ["d"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name in ["f"]:
                assert node.to_restart is True


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_leaf_table_f_while_some_ancestors_not_running(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """ when [y, src_y] ancestors are not running, so z not running too
            restarting the leaf "f"
            Should lead to restart src_y, y, [d, z (any order)] f
            BUT as Z is restarted and it is stateful then p, c, e needs to be restarted too as may_start_descendants is True
            as C has src_b and b running, it can be started too
            src_y ---> y -->  d -> f
                        \   /
            src_x ---> x - z -> p
                   \        \
            src_a -> a       \
            src_b -> b ---->  c -> e
        """
        print("\n--> test_build_execution_plan_for_leaf_table_f_while_some_ancestors_not_running should start nodes src_y, y, z, d, c, p, e and f")

        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-x", "dev-usw2-p2-dml-src-b", "dev-usw2-p2-dml-b"]:
                print(f"mock_ get statement info: {statement_name} -> RUNNING")
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                print(f"mock_ get statement info: {statement_name} -> UNKNOWN")
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="f",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=True,
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 12
        for node in report.tables:
            if node.table_name in ["src_x", "x", "src_b", "b"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in  ["src_y", "y", "d", "z"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name in ["e", "c", "p", "f"]:
                assert node.to_restart is True
                assert node.to_run is False


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_leaf_table_e_while_some_ancestors_not_running(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """ when b, src_b, c ancestors are not running
            restarting the leaf "e"
            Should lead to restart src_b, b, c, e
        """
        print("\n--> test_build_execution_plan_for_leaf_table_e_while_some_ancestors_not_running should start nodes  src_b, b, c, e")

        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-src-x",
                                    "dev-usw2-p2-dml-x",
                                    "dev-usw2-p2-dml-z",
                                    "dev-usw2-p2-dml-src-y",
                                    "dev-usw2-p2-dml-y"]:
                print(f"mock_ get statement info: {statement_name} -> RUNNING")
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                print(f"mock_ get statement info: {statement_name} -> UNKNOWN")
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="e",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=True,
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 9
        for node in report.tables:
            if node.table_name in ["src_x", "x", "src_y", "y", "z"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in  ["src_b", "b", "c"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name in ["e"]:
                assert node.to_restart is True

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_execution_plan_for_z_restart_all_ancestors_without_children(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        when forcing to start all ancestors of z using force_ancestors=True
        even if z is stateful, it will not restart its children: d, p, c as may_start_descendants is False
        restart z
        should restart the 5 nodes
        """
        print("\n--> test_execution_plan_for_z__restart_all_ancestors should start node src_x, src_y, x, y, z")

        def mock_statement(statement_name: str) -> StatementInfo:
            return self._create_mock_get_statement_info(status_phase="RUNNING")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="z",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=False,
            force_ancestors=True,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 5
        for node in report.tables:
            if node.table_name in ["src_x", "x" , "src_y" , "y"]:
                assert node.to_restart is False
                assert node.to_run is True
            if node.table_name in ["e","f"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in ["z"]:
                assert node.to_run is False
                assert node.to_restart is True


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_table_z_ancestor_running_restart_children_of_z_only(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        when starting z without forcing ancestors and may_start_descendants=True
        should not start z ancestors but restart z children (d,p,c then f, e) and z itself

        """
        print("\n--> test_build_execution_plan_for_table_z_ancestor_running_restart_children_of_z_only should start node z, d,f,p,c,e")

        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-c", "dev-usw2-p2-dml-d", "dev-usw2-p2-dml-a"]:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")
            else:
                return self._create_mock_get_statement_info(status_phase="RUNNING")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="z",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=True,
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 12  # all nodes are present as we want to also see the running ones
        for node in report.tables:
            if node.table_name in ["src_x", "x", "src_y", "y" , "x", "src_b", "b"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in ["z"]:
                assert node.to_run is False
                assert node.to_restart is True
            if node.table_name in ["f", "p", "e", "d", "c"]:
                assert node.to_run is False
                assert node.to_restart is True


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_build_execution_plan_for_table_z_ancestors_and_children_of_z_restarted(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        when starting z with forcing ancestors and may_start_descendants=True
        should start z ancestors (src_x, src_y, x, y) and children of z (d,p,c, then e, f)
        z children (d,p,c, then e, f) needs to be restarted.
        as restarting src_x with may_start_descendants will restart a so src_a.
        """
        print("\n--> test_build_execution_plan_for_table_z_ancestors_and_children_of_z_restarted should start node src_x, src_y, x,y, z, d,f,p,c,e")

        def mock_statement(statement_name: str) -> StatementInfo:
            print(f"mock_ get statement info: {statement_name} -> RUNNING")
            return self._create_mock_get_statement_info(status_phase="RUNNING")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="z",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=True,
            force_ancestors=True,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 14
        for node in report.tables:
            if node.table_name in ["src_x", "x" , "src_y" ,"y", "src_b", "b", "src_a"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name in ["a", "c", "e","f", "d", "p", "z"]:
                assert node.to_run is False
                assert node.to_restart is True

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_build_execution_plan_for_one_table_parent_not_running(
        self,
        mock_get_status,
        mock_get_compute_pool_list,
        mock_assign_compute_pool_id
    ) -> None:
        """
        when starting e without forcing ancestors and may_start_descendants=False
        as the parent c is not running so the execution plan includes c before fact e.
        c has B and Z as parents, Z is running but not B so the plan should start with src_b, b, e.
        """
        print("test_build_execution_plan_for_one_table_parent_not_running")

        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-z", "dev-usw2-p2-dml-x", "dev-usw2-p2-dml-y", "dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")


        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool

        summary, report = dm.build_deploy_pipeline_from_table(
            table_name="e",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            dml_only=False,
            may_start_descendants=False,
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(report.tables) == 9
        for node in report.tables:
            if node.table_name in ["src_b", "b", "c"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name == "e":
                assert node.to_run is False
                assert node.to_restart is True


    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_from_product(self,
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from product.
        should get non running tables to restart
        """
        print("test_deploy_pipeline_from_product should get all non runnng tables created for p2")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-z", "dev-usw2-p2-dml-x", "dev-usw2-p2-dml-y", "dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000
        summary, report = dm.build_deploy_pipelines_from_product(
            product_name="p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=False,
            force_ancestors=False
        )
        print(f"{summary}\n")
        assert len(report.tables) == 14
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            if table.table_name in ["d", "f", "p", "c", "e", "b", "a", "src_a", "src_b"]:
                assert table.to_run is True
            else:
                assert table.to_restart is False and table.to_run is False

    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_from_product_enforce_all_tables(self,
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from product.
        should restart all tables to restart
        """
        print("test_deploy_pipeline_from_product_enforce_all_tables should get all tables created for p2")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-z", "dev-usw2-p2-dml-x", "dev-usw2-p2-dml-y", "dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000
        summary, report = dm.build_deploy_pipelines_from_product(
            product_name="p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=True,
            force_ancestors=True
        )
        print(f"{summary}\n")
        assert len(report.tables) == 14
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            assert table.to_run is True

    # ---- --dir options to build execution plan using directory
    # validate only needed sources or intermediates are restarted
    # enforced-ancestors T/F and may-start-descendants T/F

    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    def test_deploy_pipeline_for_non_running_sources_with_dir(self,
                                        mock_get_num_records_out,
                                        mock_get_pending_records,
                                        mock_get_retention_size,
                                        mock_assign_compute_pool_id,
                                        mock_get_compute_pool_list,
                                        mock_get_status) -> None:
        """
        Test deploying pipeline from a directory, like all sources,
         taking into account the running statements.
        should restart only the non running src_ tables
        """
        print("test_deploy_pipeline_for_non_running_sources_with_dir should get all source tables created for p2")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-z", "dev-usw2-p2-dml-x", "dev-usw2-p2-dml-y", "dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000

        summary, report = dm.build_and_deploy_all_from_directory(
            directory=self.inventory_path + "/sources/p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=False,
            force_ancestors=False
        )
        print(f"{summary}\n")
        assert len(report.tables) == 4
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            if table.table_name in ["src_a", "src_b"]:
                assert table.to_run is True
            else:
                # all src_ tables that are running should not be restarted.
                # non src_ tables should not be restarted.
                assert table.to_restart is False
                assert table.to_run is False



    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_for_all_sources_using_dir(self,
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from a directory, like all sources,
        with forces to restart
        """
        print("test_deploy_pipeline_for_all_sources_using_dir should get all tables created for p2")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-p2-dml-z", "dev-p2-dml-x", "dev-p2-dml-y", "dev-p2-dml-src-x", "dev-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000

        summary, report = dm.build_and_deploy_all_from_directory(
            directory=self.inventory_path + "/sources/p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=False,
            force_ancestors=True
        )
        print(f"{summary}\n")
        assert len(report.tables) == 4
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            assert table.to_run is True


    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_for_all_sources_and_children_using_dir(self,
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from a directory, like all sources, as may_start_descendants is true
        it should restart all tables and children of stateful tables
        """
        print("test_deploy_pipeline_for_all_sources_and_children_using_dir should get all tables created for p2")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-z",
                                "dev-usw2-p2-dml-x",
                                "dev-usw2-p2-dml-y",
                                "dev-usw2-p2-dml-src-x",
                                "dev-usw2-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(name=statement_name,status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(name=statement_name, status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000
        summary, report = dm.build_and_deploy_all_from_directory(
            directory=self.inventory_path + "/sources/p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=True,
            force_ancestors=True
        )
        print(f"{summary}\n")
        assert len(report.tables) == 5
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            assert table.to_restart is True or table.to_run is True

    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_intermediate_with_children_using_dir(self,
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from a directory, like all intermediates, as may_start_descendants is true
        it should restart all tables and children of stateful tables        """
        print("test_deploy_intermediate_with_children_using_dir should get all children of z")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in [ "dev-usw2-p2-dml-src-x",
                                "dev-usw2-p2-dml-src-y",
                                "dev-usw2-p2-dml-src-a",
                                "dev-usw2-p2-dml-src-b",
                                "dev-usw2-p2-dml-z",
                                 "dev-usw2-p2-dml-x",
                                  "dev-usw2-p2-dml-y",
                                "dev-usw2-p2-dml-b"]:
                return self._create_mock_get_statement_info(name=statement_name,status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(name=statement_name, status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000
        # intermediates/p2 contains z, x, y, a, b, d, c
        summary, report = dm.build_and_deploy_all_from_directory(
            directory=self.inventory_path + "/intermediates/p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=True,
            force_ancestors=False,
            pool_creation=False
        )
        print(f"{summary}\n")
        assert len(report.tables) == 12 # z is not restarted so
        print("Table\t\tStatement\t\tTo Restart\tTo Run")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}\t{table.to_run}")
            if table.table_name in ["src_a", "src_b", "src_x", "src_y", "b", "x", "y", "z"]:
                # Because most are stateless
                assert table.to_run is False
                assert table.to_restart is False
            else:
                assert table.to_run is True or table.to_restart is True


    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_for_facts_using_dir(self,
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from a directory, like all facts,
        with forces to restart all ancestors
        """
        print("test_deploy_pipeline_for_facts_using_dir should get all tables restarted for p2")
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-usw2-p2-dml-z", "dev-usw2-p2-dml-x", "dev-usw2-p2-dml-y", "dev-usw2-p2-dml-src-x", "dev-usw2-p2-dml-src-y"]:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_retention_size.return_value = 100000
        mock_get_pending_records.return_value = 10000
        mock_get_num_records_out.return_value = 100000

        summary, report = dm.build_and_deploy_all_from_directory(
            directory=self.inventory_path + "/facts/p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            execute_plan=False,
            may_start_descendants=False,
            force_ancestors=True
        )
        print(f"{summary}\n")
        assert len(report.tables) == 12 # a and src_a are not started because force_ancestors is True.
        print("Table\t\tStatement\t\tTo Restart\tTo Run")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}\t{table.to_run}")
            assert table.to_run is True

if __name__ == '__main__':
    unittest.main()
