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
from shift_left.core.utils.report_mgr import DeploymentReport, StatementBasicInfo
from shift_left.core.models.flink_statement_model import Statement, StatementInfo
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition

class TestDeploymentManager(unittest.TestCase):
    """Test suite for the deployment manager functionality."""
    
    TEST_COMPUTE_POOL_ID = "test-pool-123"
    TEST_COMPUTE_POOL_ID_2 = "test-pool-120"
    TEST_COMPUTE_POOL_ID_3 = "test-pool-121"
    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def setUp(self) -> None:
        """Set up test case before each test."""
        self.config = get_config()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID
        self.table_name = "test_table"
        self.inventory_path = os.getenv("PIPELINES")

    # Following set of methods are used to create reusable mock objects and functions
    def _create_mock_statement_info(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID
    ) -> StatementInfo:
        """Create a mock StatementInfo object."""
        return StatementInfo(
            name=name,
            status_phase=status_phase,
            compute_pool_id=compute_pool_id
        )
    
    def _create_mock_statement(
        self, 
        name: str = "statement_name",
        status_phase: str = "UNKNOWN"
    ) -> Statement:
        """Create a mock Statement object."""
        status = Status(phase=status_phase)
        return Statement(name=name, status=status)


    def _create_mock_compute_pool_list(self, env_id: str = "test-env-123", region: str = "test-region-123") -> ComputePoolList:
        """Create a mock ComputePoolList object."""
        pool_1 = ComputePoolInfo(
            id=self.TEST_COMPUTE_POOL_ID,
            name="test-pool",
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

    def _create_mock_statement_node(
        self,
        table_name: str,
        product_name: str = "product1",
        dml_statement_name: str = "dml1",
        ddl_statement_name: str = "ddl1",
        compute_pool_id: str = TEST_COMPUTE_POOL_ID
    ) -> FlinkStatementNode:
        """Create a mock FlinkStatementNode object."""
        return FlinkStatementNode(
            table_name=table_name,
            product_name=product_name,
            dml_statement_name=dml_statement_name,
            ddl_statement_name=ddl_statement_name,
            compute_pool_id=compute_pool_id
        )

    def _mock_assign_compute_pool(self, node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
        """Mock function for assigning compute pool to node."""
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = "test-pool"
        return node

    def _mock_get_and_update_node(self, node: FlinkStatementNode) -> Statement:
        """Mock function for getting and updating node statement info."""
        node.existing_statement_info = self._create_mock_statement_info(
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_2
        )
        return node

    #  ----------- TESTS -----------
   




    @patch('shift_left.core.deployment_mgr.statement_mgr.drop_table')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_pipeline_definition_for_table')
    def _test_full_pipeline_undeploy_from_table_not_found(
        self,
        mock_drop_table,
        mock_get_def    ) -> None:
        """Test undeployment when table is not found."""
        print("test_full_pipeline_undeploy_from_table_not_found")
        # Setup mocks
        mock_get_def.return_value = None

        # Execute
        result = dm.full_pipeline_undeploy_from_table(
            table_name=self.table_name,
            inventory_path=self.inventory_path
        )

        # Verify
        self.assertEqual(
            result,
            f"ERROR: Table {self.table_name} not found in table inventory"
        )


    @patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file')
    def test_build_table_graph_for_node(self, mock_read) -> None:
        """Test building table graph for a node."""
        print("test_build_table_graph_for_node")
        # Setup
        start_node = self._create_mock_statement_node("start_table")

        mock_pipeline = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_read.return_value = mock_pipeline

        # Execute
        result = dm._build_statement_node_map(start_node)

        # Verify
        self.assertIsInstance(result, dict)
        self.assertIn(start_node.table_name, result)
        self.assertEqual(result[start_node.table_name], start_node)

    @patch('shift_left.core.deployment_mgr._deploy_ddl_dml')
    @patch('shift_left.core.deployment_mgr._deploy_dml')
    def test_execute_plan(self, mock_deploy_dml, mock_deploy_ddl_dml) -> None:
        """Test execution of a plan."""
        print("test_execute_plan")
        # Setup
        plan = FlinkStatementExecutionPlan(nodes=[])
        node1 = self._create_mock_statement_node("table1")
        node1.to_run = True
        node1.dml_only = False
        
        node2 = self._create_mock_statement_node("table2")
        node2.to_run = False
        node2.to_restart = True
        node2.dml_only = True
        plan.nodes = [node1, node2]

        mock_statement = MagicMock(spec=Statement)
        mock_deploy_ddl_dml.side_effect = self._create_mock_statement("ddl_statement", "COMPLETED")
        mock_deploy_dml.side_effect = self._create_mock_statement("dml_statement", "RUNNING")

        # Execute
        result = dm._execute_plan(plan, self.compute_pool_id)

        # Verify
        self.assertEqual(len(result), 2)
        mock_deploy_ddl_dml.assert_called_once()
        mock_deploy_dml.assert_called_once()

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    def test_execution_plan_with_src_running(
        self, 
        mock_get_status, 
        mock_get_compute_pool_list, 
        mock_assign_compute_pool_id
    ) -> None:
        """Test execution plan with source tables running."""
        print("test_execution_plan_with_src_running")
        
        def mock_status(statement_name: str) -> StatementInfo:
            print(f"mock_status: {statement_name}")
            if statement_name.startswith("dev-dml-src-") or statement_name.startswith("dev-p1-dml-src-"):
                return self._create_mock_statement_info(name=statement_name, status_phase="RUNNING")
            return self._create_mock_statement_info()
            
        mock_get_status.side_effect = mock_status
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        summary, execution_plan = dm.build_deploy_pipeline_from_table(
            table_name="p1_fct_order", 
            inventory_path=self.inventory_path, 
            compute_pool_id=self.TEST_COMPUTE_POOL_ID, 
            dml_only=False, 
            may_start_descendants=False, 
            force_ancestors=False,
            execute_plan=False
        )
        print(summary)
        
        # Verify source tables are in the execution plan with skip action
        for node in execution_plan.nodes:
            if node.table_name.startswith("src_"):
                assert not node.to_restart and not node.to_run, \
                    f"Source table {node.table_name} should not be restarted or run"
            
        # Verify the expected intermediate and fact tables are present
        assert len(execution_plan.nodes) == 6
        assert execution_plan.nodes[3].table_name in ("int_p1_table_1", "int_p1_table_2")
        assert execution_plan.nodes[4].table_name in ("int_p1_table_1", "int_p1_table_2")
        assert execution_plan.nodes[5].table_name == "p1_fct_order"

    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    @patch('shift_left.core.deployment_mgr.statement_mgr.reset_statement_list')
    def test_build_execution_plan_for_product(self, mock_reset_statement_list, mock_assign_compute_pool_id) -> None:
        """Test building execution plan for a product."""
        print("test_build_execution_plan_for_product")
        # Setup
        product_name = "p2"
        inventory_path = self.inventory_path
        compute_pool_id = self.TEST_COMPUTE_POOL_ID
        dml_only = False
        may_start_descendants = False
        force_ancestors = False
        execute_plan = False

        mock_reset_statement_list.return_value = None
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool

        summary, execution_plan = dm.build_deploy_pipelines_from_product(
            product_name=product_name,
            inventory_path=inventory_path,
            compute_pool_id=compute_pool_id,
            dml_only=dml_only,
            may_start_descendants=may_start_descendants,
            force_ancestors=force_ancestors,
            execute_plan=execute_plan
        )
        print(summary)
        print(execution_plan)

    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.build_deploy_pipeline_from_table')
    def _test_deploy_pipeline_from_product(self, mock_deploy_pipeline_from_table,
                                          mock_get_compute_pool_list) -> None:
        """
        Test deploying pipeline from product.
        -it will rebuild the table inventory
        """
        print("test_deploy_pipeline_from_product should get all tables created for p2")
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        def _mock_deploy_pipeline_from_table(table_name: str, 
                                             inventory_path: str, 
                                             compute_pool_id: str, 
                                             dml_only: bool, 
                                             may_start_children: bool, 
                                             force_sources: bool) -> Tuple[DeploymentReport, str]:
            statement = StatementBasicInfo(name="dml-" + table_name, 
                                           environment_id="dev", 
                                           compute_pool_id=compute_pool_id,
                                           created_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                           uid=uuid.uuid4().hex,
                                           execution_time=0,
                                           status="COMPLETED",
                                           status_details="")
            return DeploymentReport(table_name=table_name, 
                                    type="Both", 
                                    update_children=may_start_children, 
                                    flink_statements_deployed=[statement],
                                    ), f"deployed {table_name}"
        mock_deploy_pipeline_from_table.side_effect = _mock_deploy_pipeline_from_table
        
        reports, summary = dm.build_deploy_pipelines_from_product(
            product_name="p2",
            inventory_path=self.inventory_path,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID
        )
        print(summary)
        for report in reports:
            print(report)

if __name__ == '__main__':
    unittest.main()