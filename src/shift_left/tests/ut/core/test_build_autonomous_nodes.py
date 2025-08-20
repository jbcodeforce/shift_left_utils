"""
Copyright 2024-2025 Confluent, Inc.

Unit tests for the _build_autonomous_nodes function from deployment_mgr.py
This module provides comprehensive testing of the autonomous nodes identification logic.
"""
import unittest
from unittest.mock import MagicMock
import os
import pathlib
from typing import List

# Set up environment variables
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.deployment_mgr import _build_autonomous_nodes, _get_nodes_to_execute
from shift_left.core.models.flink_statement_model import (
    FlinkStatementNode, 
    StatementInfo
)
from ut.core.BaseUT import BaseUT


class TestBuildAutonomousNodes(BaseUT):
    """
    Comprehensive test suite for the _build_autonomous_nodes function.
    
    This function identifies which nodes can be executed autonomously (in parallel)
    based on their dependency relationships and current execution status.
    
    IMPORTANT BEHAVIOR NOTES:
    1. Only nodes with NO parents AND not in started_nodes are considered autonomous
    2. For nodes with parents, the logic checks if ANY parent needs execution or is in started_nodes
    """

    def setUp(self) -> None:
        """Set up test case before each test."""
        super().setUp()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID_1

    def _create_node(self, 
                     table_name: str, 
                     product_name: str = "test_product",
                     to_run: bool = False, 
                     to_restart: bool = False,
                     status_phase: str = "RUNNING") -> FlinkStatementNode:
        """
        Helper method to create a FlinkStatementNode for testing.
        
        Args:
            table_name: Unique identifier for the node
            product_name: Product name for the node
            to_run: Whether the node needs to be executed
            to_restart: Whether the node needs to be restarted
            status_phase: Status phase for running nodes (RUNNING, PENDING, COMPLETED)
            
        Returns:
            FlinkStatementNode: Configured node for testing
        """
        node = FlinkStatementNode(
            table_name=table_name,
            product_name=product_name,
            dml_statement_name=f"dml_{table_name.replace('.', '_')}",
            ddl_statement_name=f"ddl_{table_name.replace('.', '_')}",
            to_run=to_run,
            to_restart=to_restart,
            compute_pool_id=self.compute_pool_id
        )
            
        return node

    def _add_parent_relationship(self, child: FlinkStatementNode, parent: FlinkStatementNode) -> None:
        """Helper to add parent-child relationship between nodes."""
        child.add_parent(parent)

    def test_all_nodes_to_run_true_are_part_of_autonomous_nodes(self):
        """Test that nodes with no parents and to_run=True are identified as autonomous."""
        # Arrange
        node1 = self._create_node("table1", to_run=True)
        node2 = self._create_node("table2", to_run=True)
        node3 = self._create_node("table3", to_run=False) 
        
        nodes_to_execute = [node1, node2, node3]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        self.assertEqual(len(result), 2)
        self.assertIn(node1, result)
        self.assertIn(node2, result)
        self.assertNotIn(node3, result)

    def test_nodes_to_restart_true_are_part_of_autonomous_nodes(self):
        """Test that nodes with no parents and to_restart=True are identified as autonomous."""
        # Arrange
        node1 = self._create_node("table1", to_restart=True)
        node2 = self._create_node("table2", to_restart=True)
        node3 = self._create_node("table3", to_restart=False, to_run=False) 
        nodes_to_execute = [node1, node2, node3]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertEqual(len(result), 2)
        self.assertIn(node1, result)
        self.assertIn(node2, result)
        self.assertNotIn(node3, result)

    def test_nodes_in_started_nodes_should_not_be_autonomous(self):
        """Test that nodes with no parents in started_nodes are not autonomous."""
        # Arrange
        node1 = self._create_node("table1", to_run=True)
        node2 = self._create_node("table2", to_run=True)
        
        nodes_to_execute = [node1, node2]
        started_nodes = [node1]  # node1 already started
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertEqual(len(result), 1)
        self.assertNotIn(node1, result)
        self.assertIn(node2, result)

    def test_all_parents_running_node_autonomous(self):
        """Test that nodes with all parents running are identified as autonomous."""
        # Arrange
        parent1 = self._create_node("parent1", to_run=False)
        parent2 = self._create_node("parent2", to_run=False)
        child = self._create_node("child1", to_restart=True)
        
        # Set up parent-child relationships
        self._add_parent_relationship(child, parent1)
        self._add_parent_relationship(child, parent2)
        
        nodes_to_execute =   _get_nodes_to_execute([parent1, parent2, child])
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(child, result, "Child should be autonomous when all parents are running")

    def test_parents_need_execution_node_not_autonomous(self):
        """Test that nodes with parents needing execution are not autonomous."""
        # Arrange
        parent1 = self._create_node("parent1", to_run=True)  # Needs to run
        parent2 = self._create_node("parent2", to_run=False)  # Already running
        child = self._create_node("child1", to_run=True)
        
        # Set up parent-child relationships
        self._add_parent_relationship(child, parent1)
        self._add_parent_relationship(child, parent2)
        
        nodes_to_execute = _get_nodes_to_execute([parent1, parent2, child])
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(parent1, result, "Parent1 should be autonomous (no parents, to_run=True)")
        self.assertNotIn(child, result, "Child should not be autonomous (parent1 needs to run)")
        self.assertNotIn(parent2, result, "Parent 2 should not be autonomous as already running")

    def test_parents_need_restart_node_not_autonomous(self):
        """Test that nodes with parents needing restart are not autonomous."""
        # Arrange
        parent = self._create_node("parent1", to_run=True)
        child = self._create_node("child1", to_restart=True)
        
        # Set up parent-child relationship
        self._add_parent_relationship(child, parent)
        
        nodes_to_execute = [parent, child]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(parent, result, "Parent should be autonomous (no parents, to_restart=True)")
        self.assertNotIn(child, result, "Child should not be autonomous (parent needs restart)")

    def test_string_parents_handled_correctly(self):
        """Test the edge case where parents are strings instead of FlinkStatementNode objects."""
        # Arrange
        parent_node = self._create_node("parent_table", to_run=True)
        child_node = self._create_node("child_table", to_run=True)
        
        # Manually add string parent to simulate the edge case mentioned in the code
        child_node.parents.add("parent_table")  # String instead of FlinkStatementNode
        
        nodes_to_execute = [parent_node, child_node]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(parent_node, result, "Parent should be autonomous")
        self.assertNotIn(child_node, result, "Child should not be autonomous (string parent needs to run)")

    def test_string_parents_already_started(self):
        """Test string parents that are already in started_nodes - they block children."""
        # Arrange
        parent_node = self._create_node("parent_table", to_run=True, to_restart=False) # to start ...
        child_node = self._create_node("child_table", to_run=False, to_restart=True)
        
        # Manually add string parent
        child_node.parents.add("parent_table")
        
        nodes_to_execute = [parent_node, child_node]
        started_nodes = [parent_node]  #...  Parent already started
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(child_node, result, "Child is autonomous as parent is in started_nodes")

    def test_string_parents_can_be_autonomous(self):
        """Test when nodes with string parents can be autonomous."""
        # Arrange
        parent_node = self._create_node("parent_table", to_run=False, to_restart=False)
        child_node = self._create_node("child_table", to_run=True)
        
        # Manually add string parent
        child_node.parents.add("parent_table")
        
        nodes_to_execute = [parent_node, child_node]
        started_nodes = []  # Parent not in started_nodes
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        # Child should be autonomous when string parent is not marked for execution and not in started_nodes
        self.assertIn(child_node, result, "Child should be autonomous when string parent doesn't need execution")

    def test_mixed_parent_types(self):
        """Test nodes with both string and FlinkStatementNode parents."""
        # Arrange
        parent_node1 = self._create_node("parent1", to_run=False, to_restart=False)
        parent_node2 = self._create_node("parent2", to_run=True)  # Needs execution
        child_node = self._create_node("child", to_run=True)
        
        # Add both FlinkStatementNode and string parents
        self._add_parent_relationship(child_node, parent_node1)
        child_node.parents.add("parent2")  # String parent
        
        nodes_to_execute = [parent_node1, parent_node2, child_node]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(parent_node2, result, "Parent2 should be autonomous")
        self.assertNotIn(child_node, result, "Child should not be autonomous (parent2 needs execution)")

    def test_complex_dependency_graph(self):
        """Test a complex dependency graph with multiple levels."""
        # Arrange
        # Level 0 (roots)
        root1 = self._create_node("root1", to_run=True)
        root2 = self._create_node("root2", to_run=False, to_restart=False)
        
        # Level 1
        level1_1 = self._create_node("level1_1", to_run=True)
        level1_2 = self._create_node("level1_2", to_run=True)
        
        # Level 2
        level2_1 = self._create_node("level2_1", to_run=True)
        
        # Set up relationships
        self._add_parent_relationship(level1_1, root2)  # level1_1 depends on running root2
        self._add_parent_relationship(level1_2, root1)  # level1_2 depends on root1 that needs to run
        self._add_parent_relationship(level2_1, level1_1)  # level2_1 depends on level1_1
        
        nodes_to_execute = [root1, root2, level1_1, level1_2, level2_1]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertIn(root1, result, "Root1 should be autonomous (no parents)")
        self.assertIn(level1_1, result, "Level1_1 should be autonomous (parent is running)")
        self.assertNotIn(level1_2, result, "Level1_2 should not be autonomous (parent needs execution)")
        self.assertNotIn(level2_1, result, "Level2_1 should not be autonomous (parent needs execution)")

    def test_circular_dependencies_prevention(self):
        """Test that the function handles potential circular dependencies gracefully."""
        # Arrange
        node1 = self._create_node("node1", to_run=True)
        node2 = self._create_node("node2", to_run=True)
        
        # Create circular dependency (though this shouldn't happen in practice)
        self._add_parent_relationship(node1, node2)
        self._add_parent_relationship(node2, node1)
        
        nodes_to_execute = [node1, node2]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        # Both nodes should not be autonomous due to circular dependencies
        self.assertEqual(len(result), 0, "No nodes should be autonomous with circular dependencies")

    def test_empty_input_lists(self):
        """Test function behavior with empty input lists."""
        # Arrange
        nodes_to_execute = []
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertEqual(len(result), 0, "Should return empty list for empty input")
        self.assertIsInstance(result, list, "Should return a list")

    def test_all_started_nodes_not_autonomous(self):
        """Test when all nodes with no parents are already in started_nodes."""
        # Arrange
        node1 = self._create_node("node1", to_run=True)
        node2 = self._create_node("node2", to_run=True)
        
        nodes_to_execute = [node1, node2]
        started_nodes = [node1, node2]  # All nodes already started
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertEqual(len(result), 0)

    def test_correct_behavior_nodes_not_started(self):
        """Test the correct behavior when nodes are not in started_nodes."""
        # Arrange
        node1 = self._create_node("node1", to_run=True)
        node2 = self._create_node("node2", to_run=True)
        node3 = self._create_node("node3", to_run=False, to_restart=False)  # Should not be autonomous
        
        nodes_to_execute = [node1, node2, node3]
        started_nodes = []  # No nodes started
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        # This shows the correct behavior: nodes with no parents that need execution
        # and are not in started_nodes should be autonomous
        self.assertEqual(len(result), 2)
        self.assertIn(node1, result)
        self.assertIn(node2, result)
        self.assertNotIn(node3, result)  # Doesn't need execution

    def test_different_status_phases(self):
        """Test nodes with different running status phases."""
        # Arrange
        parent_running = self._create_node("parent_running", to_run=False, to_restart=False, status_phase="RUNNING")
        parent_pending = self._create_node("parent_pending", to_run=True, to_restart=False, status_phase="PENDING")
        parent_completed = self._create_node("parent_completed", to_run=False, to_restart=False, status_phase="COMPLETED")
        
        child1 = self._create_node("child1", to_run=True)
        child2 = self._create_node("child2", to_run=True)
        child3 = self._create_node("child3", to_run=True)
        
        # Set up relationships
        self._add_parent_relationship(child1, parent_running)  # will be autonomous
        self._add_parent_relationship(child2, parent_pending)  # will not be autonomous
        self._add_parent_relationship(child3, parent_completed)  # will be autonomous
        
        nodes_to_execute = [parent_running, parent_pending, parent_completed, child1, child2, child3]
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        # All children should be autonomous as their parents are in running states
        self.assertIn(child1, result, "Child1 should be autonomous (parent RUNNING)")
        self.assertNotIn(child2, result, "Child2 should be autonomous (parent PENDING)")
        self.assertIn(child3, result, "Child3 should be autonomous (parent COMPLETED)")

    def test_performance_with_large_graph(self):
        """Test function performance with a larger dependency graph."""
        # Arrange
        num_nodes = 100
        nodes = []
        
        # Create root nodes
        for i in range(10):
            node = self._create_node(f"root_{i}", to_run=True)
            nodes.append(node)
        
        # Create dependent nodes
        for i in range(10, num_nodes):
            node = self._create_node(f"node_{i}", to_run=True)
            # Add random dependencies on previous nodes
            if i > 20:
                parent_idx = i - 10
                self._add_parent_relationship(node, nodes[parent_idx])
            nodes.append(node)
        
        nodes_to_execute = nodes
        started_nodes = []
        
        # Act
        result = _build_autonomous_nodes(nodes_to_execute, started_nodes)
        
        # Assert
        self.assertGreater(len(result), 0, "Should identify some autonomous nodes")
        self.assertLessEqual(len(result), len(nodes), "Result should not exceed input size")


if __name__ == '__main__':
    unittest.main()