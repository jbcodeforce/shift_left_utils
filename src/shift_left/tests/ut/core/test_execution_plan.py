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

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

import shift_left.core.pipeline_mgr as pm
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
from shift_left.core.compute_pool_mgr import ComputePoolList, ComputePoolInfo
import shift_left.core.deployment_mgr as dm
import shift_left.core.utils.report_mgr as report_mgr
from shift_left.core.models.flink_statement_model import (
    Statement, 
    StatementInfo,
    Status
)
from shift_left.core.deployment_mgr import (
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)
from shift_left.core.utils.report_mgr import DeploymentReport, StatementBasicInfo,TableReport
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition

class TestExecutionPlan(unittest.TestCase):
    """
    validate the different scenario to build the execution plan.
    See the topology of flink statements https://github.com/jbcodeforce/shift_left_utils/blob/main/docs/images/flink_pipeline_for_test.drawio.png 
    """
    
    TEST_COMPUTE_POOL_ID_1 = "lfcp-121"
    TEST_COMPUTE_POOL_ID_2 = "lfcp-122"
    TEST_COMPUTE_POOL_ID_3 = "lfcp-123"

    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pm.delete_all_metada_files(os.getenv("PIPELINES"))
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))
        cls.inventory_path = os.getenv("PIPELINES")

    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_get_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID_1
    ) -> StatementInfo:
        """Mock the call to get statement info"""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )
    
    def _mock_assign_compute_pool(self, node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node. deployment_mgr._assign_compute_pool_id_to_node()"""
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node
    
    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_1,
            name="test-pool-1",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_2 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_2,
            name="test-pool-2",
            env_id=env_id,
            max_cfu=100,
            current_cfu=50
        )
        pool_3 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID_3,
            name="dev-p1-fct-order",
            env_id=env_id,
            max_cfu=10,
            current_cfu=0
        )
        return ComputePoolList(pools=[pool_1, pool_2, pool_3])
    
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
        Should restart only current table f which has one parent d.
        f has one parent d. f-> d -> [y, z], z -> x, y-> src_y and x -> src_x.
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
        assert len(execution_plan.nodes) == 7  # all nodes are present as we want to see running ones too
        for node in execution_plan.nodes:
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
        f has one parent d. f-> d -> [y, z], z -> x, y-> src_y and x -> src_x.
        """
        print("\n--> test_build_execution_plan_for_leaf_table_f_while_direct_parent_not_running should start nodes d and f")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-p2-dml-d", "dev-p2-dml-f"]:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")
            else:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
 
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list

        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="f", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=False, 
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.nodes) == 7  # all nodes are present as we want to see running ones too
        for node in execution_plan.nodes:
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
        """ when y, src_y ancestors are not running, so z not running too
            restarting the leaf "f"
            Should lead to restart src_y, y, d, z, f
            BUT Z is restarted and its statefull so p,c,e needs to be restarted too
            as C has src_b and b running, it can be started too
        """
        print("\n--> test_build_execution_plan_for_leaf_table_f_while_some_ancestors_not_running should start nodes src_y, y, z, d, c, p, e and f")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-p2-dml-src-x", "dev-p2-dml-x", "dev-p2-dml-src-b", "dev-p2-dml-b"]:  
                print(f"mock_ get statement info: {statement_name} -> RUNNING")
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                print(f"mock_ get statement info: {statement_name} -> UNKNOWN")
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")
 
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="f", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=True, 
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.nodes) == 12
        for node in execution_plan.nodes:
            if node.table_name in ["src_x", "x", "src_b", "b"]: 
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in  ["src_y", "y", "d", "z", "p"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name in ["f", "e"]:
                assert node.to_restart is True

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
            if statement_name in ["dev-p2-dml-src-x", "dev-p2-dml-x", "dev-p2-dml-z", "dev-p2-dml-src-y", "dev-p2-dml-y"]:  
                print(f"mock_ get statement info: {statement_name} -> RUNNING")
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                print(f"mock_ get statement info: {statement_name} -> UNKNOWN")
                return self._create_mock_get_statement_info(status_phase="UNKNOWN")
 
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="e", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=True, 
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.nodes) == 9
        for node in execution_plan.nodes:
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
        print("\n--> test_execution_plan_for_z__restart_all_ancestors should start node src_x, src_y, x, y, z, src_a, a, d, p, src_b, b c, f, e")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            return self._create_mock_get_statement_info(status_phase="RUNNING")

        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="z", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=False, 
            force_ancestors=True,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.nodes) == 5 
        for node in execution_plan.nodes:
            if node.table_name in ["src_x", "x" , "src_y" , "y"]:
                assert node.to_restart is False
                assert node.to_run is True
            if node.table_name == "z":
                assert node.to_run is True
                assert node.to_restart is True
            if node.table_name in ["e","f"]:
                assert node.to_run is False
                assert node.to_restart is False


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
        should not start z ancestors but restart its children
        z children needs to be restarted. d -> [y, z], p -> z, c -> [z,b] f-> d, e-> c.
         
        """
        print("\n--> test_build_execution_plan_for_table_z_ancestor_running_restart_children_of_z_only should start node z, d,f,p,c,e")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            if statement_name in ["dev-p2-dml-c", "dev-p2-dml-d", "dev-p2-dml-a"]:  
                return self._create_mock_get_statement_info(status_phase="UNKNOWN") 
            else:
                return self._create_mock_get_statement_info(status_phase="RUNNING")
 
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="z", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=True, 
            force_ancestors=False,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.nodes) == 12  # all nodes are present as we want to also see the running ones
        for node in execution_plan.nodes:
            if node.table_name in ["src_x", "x", "src_y", "y" , "x", "src_b", "b"]:
                assert node.to_run is False
                assert node.to_restart is False
            if node.table_name in ["d", "c"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name in ["z", "f", "p", "e"]:
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
        should start z ancestors and children of z
        z childrent needs to be restarted. d -> [y, z], p -> z, c -> [z,b] f-> d, e-> c.
        """
        print("\n--> test_build_execution_plan_for_table_z_ancestors_and_children_of_z_restarted should start node src_x, src_y, x,y, z, d,f,p,c,e")
        
        def mock_statement(statement_name: str) -> StatementInfo:
            print(f"mock_ get statement info: {statement_name} -> RUNNING")
            return self._create_mock_get_statement_info(status_phase="RUNNING")
 
        mock_get_status.side_effect = mock_statement
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="z", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=True, 
            force_ancestors=True,
            execute_plan=False
        )
        print(f"{summary}")
        assert len(execution_plan.nodes) == 14  
        for node in execution_plan.nodes:
            if node.table_name in ["src_x", "x" , "src_y" ,"y", "src_b", "b", "src_p2_a", "a", "c"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name == "z":
                assert node.to_run is True
                assert node.to_restart is True
            if node.table_name in ["e","f"]:
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
            if statement_name in ["dev-p2-dml-z", "dev-p2-dml-x", "dev-p2-dml-y", "dev-p2-dml-src-x", "dev-p2-dml-src-y"]:  
                return self._create_mock_get_statement_info(status_phase="RUNNING")
            else:
                return self._create_mock_get_statement_info(status_phase="UNKNOWN") 
            

        mock_get_status.side_effect = mock_statement
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool

        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="e", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1, 
            dml_only=False, 
            may_start_descendants=False, 
            force_ancestors=False,
            execute_plan=False
        )  
        print(f"{summary}")
        assert len(execution_plan.nodes) == 9
        for node in execution_plan.nodes:
            if node.table_name in ["src_b", "b", "c"]:
                assert node.to_run is True
                assert node.to_restart is False
            if node.table_name == "e":
                assert node.to_run is True
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
            if table.table_name in ["d", "f", "p", "c", "e", "b", "a", "src_p2_a", "src_b"]:
                assert table.to_restart is True
            else:
                assert table.to_restart is False

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
            assert table.to_restart is True


    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_for_non_running_sources(self, 
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from a directory, like all sources,
         taking into account the running statements.
        should restart only the non running tables
        """
        print("test_deploy_pipeline_for_non_running_sources should get all tables created for p2")
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
            force_ancestors=False
        )
        print(f"{summary}\n")
        assert len(report.tables) == 4
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            if table.table_name in ["src_p2_a", "src_b"]:
                assert table.to_restart is True
            else:
                assert table.to_restart is False

    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_for_all_sources(self, 
                                        mock_get_status,
                                        mock_get_compute_pool_list,
                                        mock_assign_compute_pool_id,
                                        mock_get_retention_size,
                                        mock_get_pending_records,
                                        mock_get_num_records_out) -> None:
        """
        Test deploying pipeline from a directory, like all sources,
        As it forces to restar
        """
        print("test_deploy_pipeline_for_all_sources should get all tables created for p2")
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
            assert table.to_restart is True

    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_num_records_out')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_pending_records')
    @patch('shift_left.core.deployment_mgr.report_mgr.metrics_mgr.get_retention_size')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_deploy_pipeline_for_all_sources_and_children(self, 
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
        print("test_deploy_pipeline_for_all_sources should get all tables created for p2")
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
            may_start_descendants=True,
            force_ancestors=True
        )
        print(f"{summary}\n")
        assert len(report.tables) == 14 
        print("Table\t\tStatement\t\tTo Restart")
        for table in report.tables:
            print(f"{table.table_name}\t\t{table.statement_name}\t\t{table.to_restart}")
            assert table.to_restart is True



if __name__ == '__main__':
    unittest.main()