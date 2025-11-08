import pathlib

import unittest
from unittest.mock import patch, MagicMock, call
from datetime import datetime
from shift_left.core.models.flink_statement_model import (
    FlinkStatementNode,
    FlinkStatementExecutionPlan,
    StatementInfo,
    Statement,
    Status
)
from ut.core.BaseUT import BaseUT
import shift_left.core.pipeline_mgr as pm
import shift_left.core.deployment_mgr as dm
from typing import List
import os

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

class TestParallelExecutePlan(BaseUT):

    @classmethod
    def setUpClass(cls) -> None:
        """Set up test environment before running tests."""
        pm.build_all_pipeline_definitions(os.getenv("PIPELINES"))

    def setUp(self):
        """Set up test fixtures"""
        super().setUp()

    def _create_mock_node(self, table_name: str,
                         product_name: str = "test_product",
                         to_run: bool = False,
                         to_restart: bool = False,
                         is_running: bool = False,
                         parents: list = [],
                         children: list = []) -> FlinkStatementNode:
        """Helper method to create a mock FlinkStatementNode"""
        node = FlinkStatementNode(
            table_name=table_name,
            product_name=product_name,
            dml_statement_name=f"dml_{table_name.replace('.', '_')}",
            ddl_statement_name=f"ddl_{table_name.replace('.', '_')}",
            to_run=to_run,
            to_restart=to_restart,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )

        if parents:
            for parent in parents:
                node.add_parent(parent)

        if children:
            for child in children:
                node.add_child(child)

        # Mock the is_running method
        if is_running:
            node.existing_statement_info = StatementInfo(
                name=f"dml_{table_name}",
                status_phase="RUNNING",
                compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
            )
        return node

    def _create_mock_execution_plan(self, nodes: list) -> FlinkStatementExecutionPlan:
        """Helper method to create a mock execution plan"""
        return FlinkStatementExecutionPlan(
            start_table_name=nodes[0].table_name if nodes else "test_table",
            environment_id="test_env",
            nodes=nodes
        )


    @patch('shift_left.core.deployment_mgr.compute_pool_mgr.get_compute_pool_list')
    @patch('shift_left.core.deployment_mgr.statement_mgr.get_statement_status_with_cache')
    @patch('shift_left.core.deployment_mgr._assign_compute_pool_id_to_node')
    def test_autonomous_and_nodes_to_execute(
        self,
        mock_assign_compute_pool_id,
        mock_get_status,
        mock_get_compute_pool_list
    ) -> None:
        """
        restarting the leaf "f" and all parents.
        """
        print("\n--> test_autonomous_and_nodes_to_execute, should runs all src in parallel")

        def mock_statement_info(statement_name: str) -> StatementInfo:
            return self._create_mock_get_statement_info(status_phase="UNKNOWN")

        mock_get_status.side_effect = mock_statement_info
        mock_assign_compute_pool_id.side_effect = self._mock_assign_compute_pool
        mock_get_compute_pool_list.side_effect = self._create_mock_compute_pool_list
        pipeline_def = pm.get_pipeline_definition_for_table("f", os.getenv("PIPELINES") or "")
        start_node = pipeline_def.to_node()
        combined_node_map = {}
        visited_nodes = set()
        node_map = dm._build_statement_node_map(start_node, visited_nodes, combined_node_map)

        ancestors = []
        ancestors = dm._build_topological_sorted_graph([start_node], node_map)
        execution_plan = dm._build_execution_plan_using_sorted_ancestors(ancestors= ancestors,
                                                                      node_map=node_map,
                                                                      force_ancestors=True,
                                                                      may_start_descendants=False,
                                                                      cross_product_deployment=False,
                                                                      compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                                                                      table_name="f",
                                                                      expected_product_name="p2",
                                                                      exclude_table_names=[],
                                                                      pool_creation=False)

        autonomous_nodes = dm._build_autonomous_nodes(execution_plan.nodes, started_nodes=[])
        assert len(autonomous_nodes) == 2
        assert autonomous_nodes[0].table_name == "src_x" or autonomous_nodes[1].table_name == "src_x"
        assert autonomous_nodes[0].table_name == "src_y" or autonomous_nodes[1].table_name == "src_y"
        nodes_to_execute = dm._get_nodes_to_execute(execution_plan.nodes)
        assert len(nodes_to_execute) == 7
        for node in nodes_to_execute:
            if node.table_name in ["src_x", "x", "src_y", "y", "z", "d"]:
                assert node.to_run is True
                assert node.to_restart is False


    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_parallel_execution_autonomous_nodes(self, mock_deploy):
        print("\n--> test_execute_plan_parallel_execution_autonomous_nodes, Test parallel execution with 3 autonomous nodes (no dependencies)")
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)
        node2 = self._create_mock_node("table2", to_run=True)
        node3 = self._create_mock_node("table3", to_run=True)

        def _mock_deploy(node: FlinkStatementNode, accept_exceptions: bool = False, compute_pool_id: str = None) -> Statement:
            return self._create_mock_statement(name=node.dml_statement_name, status_phase="RUNNING")

        execution_plan = self._create_mock_execution_plan([node1, node2, node3])

        mock_deploy.side_effect = _mock_deploy

        result = dm._execute_plan(
            execution_plan=execution_plan,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            accept_exceptions=False,
            sequential=False,
            max_thread=4
        )

        self.assertEqual(len(result), 3)
        self.assertEqual(mock_deploy.call_count, 3)

        # All nodes should be processed since they're autonomous
        for node in execution_plan.nodes:
            self.assertFalse(node.to_run)
            self.assertFalse(node.to_restart)
            print(node.existing_statement_info)

    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_mixed_dependencies(self, mock_deploy):
        """Test execution with mixed dependencies - some autonomous, some dependent"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)  # autonomous
        node2 = self._create_mock_node("table2", to_run=True)  # autonomous
        node3 = self._create_mock_node("table3", to_run=True, parents=[node1])  # dependent on node1
        node4 = self._create_mock_node("table4", to_run=True, parents=[node2])  # dependent on node2

        def _mock_deploy(node: FlinkStatementNode, accept_exceptions: bool = False, compute_pool_id: str = None) -> Statement:
            return self._create_mock_statement(name=node.dml_statement_name, status_phase="RUNNING")

        execution_plan = self._create_mock_execution_plan([node1, node2, node3, node4])

        mock_deploy.side_effect = _mock_deploy

        result = dm._execute_plan(
            execution_plan=execution_plan,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            accept_exceptions=False,
            sequential=False,
            max_thread=4
        )

        self.assertEqual(len(result), 4)
        self.assertEqual(mock_deploy.call_count, 4)

    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_skip_running_nodes(self, mock_deploy):
        """Test that nodes already running are skipped unless marked for restart"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=False, is_running=True)  # running, no restart
        node2 = self._create_mock_node("table2", to_run=True)  # needs to run
        node3 = self._create_mock_node("table3", to_restart=True, is_running=True)  # running, needs restart

        def _mock_deploy(node: FlinkStatementNode, accept_exceptions: bool = False, compute_pool_id: str = None) -> Statement:
            return self._create_mock_statement(name=node.dml_statement_name, status_phase="RUNNING")

        execution_plan = self._create_mock_execution_plan([node1, node2, node3])

        mock_deploy.side_effect = _mock_deploy

        # Act
        result = dm._execute_plan(
            execution_plan=execution_plan,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            accept_exceptions=False,
            sequential=False,
            max_thread=4
        )

        # Assert
        self.assertEqual(len(result), 2)  # Only node2 and node3 should be executed
        self.assertEqual(mock_deploy.call_count, 2)

    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_exception_handling_accept_exceptions_true(self, mock_deploy):
        """Test exception handling when accept_exceptions=True"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)
        node2 = self._create_mock_node("table2", to_run=True)

        execution_plan = self._create_mock_execution_plan([node1, node2])

        # Mock first deployment to fail, second to succeed
        mock_deploy.side_effect = [Exception("Deployment failed"), self._create_mock_statement()]

        # Act
        result = dm._execute_plan(
            execution_plan=execution_plan,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            accept_exceptions=True,
            sequential=False,
            max_thread=4
        )
        print(result)
        # Assert
        self.assertEqual(len(result), 1)  # Only successful deployment returns a statement
        self.assertEqual(mock_deploy.call_count, 2)

    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_exception_handling_accept_exceptions_false(self, mock_deploy):
        """Test exception handling when accept_exceptions=False"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)
        execution_plan = self._create_mock_execution_plan([node1])

        mock_deploy.side_effect = Exception("Deployment failed")

        # Act & Assert
        with self.assertRaises(Exception):
            dm._execute_plan(
                execution_plan=execution_plan,
                compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                accept_exceptions=False,
                sequential=False,
                max_thread=4
            )

    def test_get_nodes_to_execute_filters_correctly(self):
        """Test _get_nodes_to_execute helper function"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)
        node2 = self._create_mock_node("table2", to_restart=True)
        node3 = self._create_mock_node("table3", to_run=False, to_restart=False)

        nodes = [node1, node2, node3]

        # Act
        result = dm._get_nodes_to_execute(nodes)

        # Assert
        self.assertEqual(len(result), 2)
        self.assertIn(node1, result)
        self.assertIn(node2, result)
        self.assertNotIn(node3, result)

    def test_build_autonomous_nodes_no_parents(self):
        """Test _build_autonomous_nodes with nodes that have no parents"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)
        node2 = self._create_mock_node("table2", to_restart=True)
        node3 = self._create_mock_node("table3", to_run=False, to_restart=False)  # Not to be executed

        nodes = [node1, node2, node3]

        # Act
        result = dm._build_autonomous_nodes(nodes, started_nodes=[])

        # Assert
        self.assertEqual(len(result), 2)
        self.assertIn(node1, result)
        self.assertIn(node2, result)
        self.assertNotIn(node3, result)

    def test_build_autonomous_nodes_with_running_parents(self):
        """Test _build_autonomous_nodes with nodes whose parents are running"""
        # Arrange
        parent1 = self._create_mock_node("parent1", is_running=True)
        parent2 = self._create_mock_node("parent2", is_running=True)

        child1 = self._create_mock_node("child1", to_run=True, parents=[parent1])
        child2 = self._create_mock_node("child2", to_run=True, parents=[parent1, parent2])
        child3 = self._create_mock_node("child3", to_run=True, parents=[parent1])

        # Parent1 needs to run - this should block child1 and child3
        parent1.to_run = True

        nodes = [parent1, parent2, child1, child2, child3]

        # Act
        result = dm._build_autonomous_nodes(nodes, started_nodes=[])

        # Assert
        self.assertIn(parent1, result)  # Parent1 has no parents and needs to run
        self.assertNotIn(child1, result)  # Child1 blocked by parent1 that needs to run
        self.assertNotIn(child2, result)  # Child2 blocked by parent1 that needs to run
        self.assertNotIn(child3, result)  # Child3 blocked by parent1 that needs to run

    def test_build_autonomous_nodes_complex_scenario(self):
        """Test _build_autonomous_nodes with complex parent-child relationships"""
        # Arrange
        # Create a complex dependency graph
        root1 = self._create_mock_node("root1", to_run=True)
        root2 = self._create_mock_node("root2", to_run=False, to_restart=False)

        level1_1 = self._create_mock_node("level1_1", to_run=True, parents=[root2])  # can run (parent running)
        level1_2 = self._create_mock_node("level1_2", to_run=True, parents=[root1])  # blocked by root1

        level2_1 = self._create_mock_node("level2_1", to_run=True, parents=[level1_1])  # blocked by level1_1

        nodes = [root1, root2, level1_1, level1_2, level2_1]

        # Act
        result = dm._build_autonomous_nodes(nodes, started_nodes=[])
        print(result)
        # Assert
        self.assertIn(root1, result)  # Root1 is autonomous
        self.assertIn(level1_1, result)  # Level1_1 can run (parent is running and not marked for execution)
        self.assertNotIn(level1_2, result)  # Blocked by root1
        self.assertNotIn(level2_1, result)  # Blocked by level1_1



    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_empty_plan(self, mock_deploy):
        """Test execution with empty plan"""
        # Arrange
        execution_plan = self._create_mock_execution_plan([])

        # Act
        result = dm._execute_plan(
            execution_plan=execution_plan,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            accept_exceptions=False,
            sequential=False,
            max_thread=4
        )

        # Assert
        self.assertEqual(len(result), 0)
        mock_deploy.assert_not_called()

    @patch('shift_left.core.deployment_mgr._deploy_one_node')
    def test_execute_plan_deploy_returns_none(self, mock_deploy):
        """Test when _deploy_one_node returns None"""
        # Arrange
        node1 = self._create_mock_node("table1", to_run=True)
        execution_plan = self._create_mock_execution_plan([node1])

        mock_deploy.return_value = None

        # Act
        result = dm._execute_plan(
            execution_plan=execution_plan,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
            accept_exceptions=False,
            sequential=False,
            max_thread=4
        )

        # Assert
        self.assertEqual(len(result), 0)  # None results are not added to the list
        self.assertEqual(mock_deploy.call_count, 1)

if __name__ == '__main__':
    unittest.main()
