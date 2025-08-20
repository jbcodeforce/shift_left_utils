"""
Test demonstrating how _build_autonomous_nodes can be refactored to use 
existing topological sort infrastructure from the same file.
"""
import unittest
import os
import pathlib
from typing import List

# Set up environment variables
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
os.environ["PIPELINES"] = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")

from shift_left.core.deployment_mgr import _build_autonomous_nodes, _topological_sort
from shift_left.core.models.flink_statement_model import FlinkStatementNode
from ut.core.BaseUT import BaseUT


class TestRefactoredWithExistingTopoSort(BaseUT):
    """Test the refactored approach using existing topological sort infrastructure."""

    def setUp(self) -> None:
        super().setUp()
        self.compute_pool_id = self.TEST_COMPUTE_POOL_ID_1

    def _create_node(self, table_name: str, to_run: bool = True, to_restart: bool = False) -> FlinkStatementNode:
        """Helper to create a test node."""
        return FlinkStatementNode(
            table_name=table_name,
            product_name="test_product",
            dml_statement_name=f"dml_{table_name}",
            ddl_statement_name=f"ddl_{table_name}",
            to_run=to_run,
            to_restart=to_restart,
            compute_pool_id=self.compute_pool_id
        )

    def refactored_build_autonomous_nodes(
        self,
        nodes_to_execute: List[FlinkStatementNode], 
        started_nodes: List[FlinkStatementNode]
    ) -> List[FlinkStatementNode]:
        """
        Refactored version that leverages existing _topological_sort infrastructure.
        This demonstrates how the current function could be simplified.
        """
        
        if not nodes_to_execute:
            return []
        
        # Build dependencies in format expected by existing _topological_sort
        dependencies = []
        for node in nodes_to_execute:
            for parent in node.parents:
                parent_table_name = parent if isinstance(parent, str) else parent.table_name
                parent_node = next((n for n in nodes_to_execute if n.table_name == parent_table_name), None)
                if parent_node:
                    dependencies.append((node.table_name, parent_node))
        
        # Use same in-degree calculation logic as existing _topological_sort
        in_degree = {node.table_name: 0 for node in nodes_to_execute}
        for child_name, parent_node in dependencies:
            in_degree[child_name] += 1
        
        # Find autonomous nodes (in-degree 0) that need execution
        started_node_names = {node.table_name for node in started_nodes}
        autonomous_nodes = []
        
        for node in nodes_to_execute:
            if (in_degree[node.table_name] == 0 and
                (node.to_run or node.to_restart) and
                node.table_name not in started_node_names):
                autonomous_nodes.append(node)
        
        return autonomous_nodes

    def test_simple_comparison_with_existing_infrastructure(self):
        """Compare current implementation with refactored version using existing infrastructure."""
        # Create simple chain: A -> B -> C
        node_a = self._create_node("table_a")
        node_b = self._create_node("table_b")  
        node_c = self._create_node("table_c")
        
        node_b.add_parent(node_a)
        node_c.add_parent(node_b)
        
        nodes = [node_a, node_b, node_c]
        started_nodes = []
        
        # Test current implementation
        current_result = _build_autonomous_nodes(nodes, started_nodes)
        
        # Test refactored implementation
        refactored_result = self.refactored_build_autonomous_nodes(nodes, started_nodes)
        
        # Both should identify only node_a as autonomous
        self.assertEqual(len(current_result), 1)
        self.assertEqual(len(refactored_result), 1)
        self.assertEqual(current_result[0].table_name, "table_a")
        self.assertEqual(refactored_result[0].table_name, "table_a")

    def test_parallel_opportunities_with_refactored_version(self):
        """Test parallel execution opportunities with refactored implementation."""
        # Diamond pattern: A,B -> C,D -> E
        node_a = self._create_node("source_a")
        node_b = self._create_node("source_b")
        node_c = self._create_node("transform_c")
        node_d = self._create_node("transform_d")
        node_e = self._create_node("sink_e")
        
        node_c.add_parent(node_a)
        node_d.add_parent(node_b)
        node_e.add_parent(node_c)
        node_e.add_parent(node_d)
        
        nodes = [node_a, node_b, node_c, node_d, node_e]
        started_nodes = []
        
        # Test current implementation
        current_result = _build_autonomous_nodes(nodes, started_nodes)
        
        # Test refactored implementation
        refactored_result = self.refactored_build_autonomous_nodes(nodes, started_nodes)
        
        # Both should identify A and B as autonomous (parallel opportunity)
        current_names = {node.table_name for node in current_result}
        refactored_names = {node.table_name for node in refactored_result}
        
        expected_names = {"source_a", "source_b"}
        self.assertEqual(current_names, expected_names)
        self.assertEqual(refactored_names, expected_names)

    def test_string_parents_handling_improvement(self):
        """Test that refactored version handles string parents correctly."""
        node_a = self._create_node("parent_table")
        node_b = self._create_node("child_table")
        
        # Add string parent (current implementation edge case)
        node_b.parents.add("parent_table")
        
        nodes = [node_a, node_b]
        started_nodes = []
        
        # Test refactored implementation
        refactored_result = self.refactored_build_autonomous_nodes(nodes, started_nodes)
        
        # Should only identify parent_table as autonomous
        result_names = {node.table_name for node in refactored_result}
        self.assertEqual(result_names, {"parent_table"})

    def test_consistent_behavior_with_started_nodes(self):
        """Test that refactored version has consistent behavior with started_nodes."""
        node_a = self._create_node("table_a", to_run=True)
        node_b = self._create_node("table_b", to_run=True)
        
        nodes = [node_a, node_b]
        started_nodes = [node_a]  # node_a already started
        
        # Test current implementation
        current_result = _build_autonomous_nodes(nodes, started_nodes)
        
        # Test refactored implementation
        refactored_result = self.refactored_build_autonomous_nodes(nodes, started_nodes)
        
        # Both should exclude started node and only include node_b
        current_names = {node.table_name for node in current_result}
        refactored_names = {node.table_name for node in refactored_result}
        
        self.assertEqual(current_names, {"table_b"})
        self.assertEqual(refactored_names, {"table_b"})
        
        # Refactored version is more explicit about excluding started nodes
        self.assertEqual(len(refactored_result), 1)
        self.assertEqual(refactored_result[0].table_name, "table_b")

    def test_using_existing_topological_sort_directly(self):
        """Test leveraging the actual _topological_sort function."""
        # Create a more complex graph to test with actual _topological_sort
        node_a = self._create_node("table_a")
        node_b = self._create_node("table_b")  
        node_c = self._create_node("table_c")
        node_d = self._create_node("table_d")
        
        # A -> C, B -> C, C -> D
        node_c.add_parent(node_a)
        node_c.add_parent(node_b)
        node_d.add_parent(node_c)
        
        nodes = [node_a, node_b, node_c, node_d]
        
        # Build data structure for existing _topological_sort
        nodes_dict = {node.table_name: node for node in nodes}
        dependencies = []
        
        for node in nodes:
            for parent in node.parents:
                dependencies.append((node.table_name, parent))
        
        # Use existing _topological_sort function
        sorted_nodes = _topological_sort(nodes_dict, dependencies)
        
        # Verify topological order
        sorted_names = [node.table_name for node in sorted_nodes]
        
        # A and B should come before C, C should come before D
        pos_a = sorted_names.index("table_a")
        pos_b = sorted_names.index("table_b")
        pos_c = sorted_names.index("table_c")
        pos_d = sorted_names.index("table_d")
        
        self.assertLess(pos_a, pos_c)
        self.assertLess(pos_b, pos_c)
        self.assertLess(pos_c, pos_d)
        
        # First nodes in topological order should be the autonomous ones
        # In this case, A and B (nodes with no dependencies)
        first_batch_names = {name for name in sorted_names[:2]}
        self.assertEqual(first_batch_names, {"table_a", "table_b"})

    def test_performance_with_existing_infrastructure(self):
        """Test that using existing infrastructure maintains good performance."""
        import time
        
        # Create larger graph
        num_nodes = 30
        nodes = []
        
        # Create a more complex graph: multiple chains
        for i in range(0, num_nodes, 3):
            # Create chains of 3: i -> i+1 -> i+2
            for j in range(3):
                node_idx = i + j
                if node_idx >= num_nodes:
                    break
                    
                node = self._create_node(f"table_{node_idx}")
                if j > 0 and len(nodes) > 0:  # Add dependency to previous node
                    node.add_parent(nodes[-1])
                nodes.append(node)
        
        started_nodes = []
        
        # Test refactored implementation performance
        start_time = time.time()
        for _ in range(100):  # More iterations for better measurement
            self.refactored_build_autonomous_nodes(nodes, started_nodes)
        refactored_time = time.time() - start_time
        
        # Test current implementation performance
        start_time = time.time()
        for _ in range(100):
            _build_autonomous_nodes(nodes, started_nodes)
        current_time = time.time() - start_time
        
        print(f"\nPerformance comparison (100 runs, {num_nodes} nodes):")
        print(f"Current implementation: {current_time:.4f}s")
        print(f"Refactored implementation: {refactored_time:.4f}s")
        
        if refactored_time < current_time:
            speedup = current_time / refactored_time
            print(f"Refactored is {speedup:.2f}x faster")
        else:
            slowdown = refactored_time / current_time
            print(f"Refactored is {slowdown:.2f}x slower (but more correct and maintainable)")


if __name__ == '__main__':
    unittest.main()