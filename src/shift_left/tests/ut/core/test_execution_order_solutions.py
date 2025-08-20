import unittest
from unittest.mock import patch, MagicMock
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from shift_left.core.deployment_mgr import _execute_plan
from shift_left.core.models.flink_statement_model import FlinkStatementNode, FlinkStatementExecutionPlan, Statement, Status, Spec
from ut.core.BaseUT import BaseUT


class TestExecutionOrderSolutions(BaseUT):
    """Test solutions for controlling execution order in parallel deployments"""

    def setUp(self):
        """Set up test fixtures"""
        super().setUp()

    def _create_mock_spec(self, compute_pool_id: str = None, statement_name: str = "test_statement") -> Spec:
        """Create a properly structured Spec object for pydantic validation"""
        return Spec(
            compute_pool_id=compute_pool_id or self.TEST_COMPUTE_POOL_ID_1,
            principal="test-principal",
            statement=f"CREATE TABLE {statement_name} AS SELECT * FROM source;",
            stopped=False,
            properties={}
        )

    def _create_mock_statement(self, statement_name: str, compute_pool_id: str = None, phase: str = "RUNNING") -> Statement:
        """Create a properly structured Statement object for pydantic validation"""
        return Statement(
            name=statement_name,
            status=Status(phase=phase),
            spec=self._create_mock_spec(compute_pool_id, statement_name),
            compute_pool_id=compute_pool_id or self.TEST_COMPUTE_POOL_ID_1
        )

    def _create_mock_deploy_function(self, execution_order: list):
        """Create a reusable mock deploy function that tracks execution order"""
        def mock_deploy(node: FlinkStatementNode, accept_exceptions: bool = False, compute_pool_id: str = None):
            delay = getattr(node, '_test_delay', 0.1)
            time.sleep(delay)
            execution_order.append(node.table_name)
            return self._create_mock_statement(
                statement_name=node.dml_statement_name,
                compute_pool_id=compute_pool_id or self.TEST_COMPUTE_POOL_ID_1
            )
        return mock_deploy

    def _create_mock_node(self, table_name: str, delay: float = 0.1, to_run: bool = True) -> FlinkStatementNode:
        """Create a mock node with controllable execution delay"""
        node = FlinkStatementNode(
            table_name=table_name,
            product_name="test_product",
            dml_statement_name=f"dml_{table_name}",
            ddl_statement_name=f"ddl_{table_name}",
            to_run=to_run,
            compute_pool_id=self.TEST_COMPUTE_POOL_ID_1
        )
        node._test_delay = delay  # Add test delay attribute
        return node

    def _create_mock_execution_plan(self, nodes: list) -> FlinkStatementExecutionPlan:
        """Create a mock execution plan"""
        return FlinkStatementExecutionPlan(
            start_table_name=nodes[0].table_name if nodes else "test_table",
            environment_id="test_env",
            nodes=nodes
        )

    def test_current_parallel_execution_race_condition(self):
        """
        Demonstrate the current race condition in _execute_plan
        where as_completed() can process nodes out of their dependency order
        """
        print("\n--> Testing current parallel execution race condition")
        
        execution_order = []
        mock_deploy_with_delay = self._create_mock_deploy_function(execution_order)
        
        # Create nodes where first node takes longest
        node1 = self._create_mock_node("slow_table", delay=0.3, to_run=True)
        node2 = self._create_mock_node("fast_table_1", delay=0.05, to_run=True) 
        node3 = self._create_mock_node("fast_table_2", delay=0.1, to_run=True)
        
        execution_plan = self._create_mock_execution_plan([node1, node2, node3])
        
        with patch('shift_left.core.deployment_mgr._deploy_one_node', side_effect=mock_deploy_with_delay):
            result = _execute_plan(
                execution_plan=execution_plan,
                compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                accept_exceptions=False,
                sequential=False,  # Parallel execution
                max_thread=3
            )
        
        print(f"Node submission order: [slow_table, fast_table_1, fast_table_2]")
        print(f"Actual execution completion order: {execution_order}")
        
        # Due to as_completed(), faster tables finish first despite submission order
        self.assertEqual(len(execution_order), 3)
        self.assertIn("fast_table_1", execution_order[0:2])  # One of the fast tables finishes first
        self.assertEqual(execution_order[-1], "slow_table")  # Slow table finishes last
        
        # This demonstrates the race condition where execution order != submission order

    def test_sequential_execution_preserves_order(self):
        """
        Test that sequential execution preserves the intended order
        """
        print("\n--> Testing sequential execution (control case)")
        
        execution_order = []
        mock_deploy_with_delay = self._create_mock_deploy_function(execution_order)
        
        # Same setup as parallel test
        node1 = self._create_mock_node("slow_table", delay=0.3, to_run=True)
        node2 = self._create_mock_node("fast_table_1", delay=0.05, to_run=True)
        node3 = self._create_mock_node("fast_table_2", delay=0.1, to_run=True)
        
        execution_plan = self._create_mock_execution_plan([node1, node2, node3])
        
        with patch('shift_left.core.deployment_mgr._deploy_one_node', side_effect=mock_deploy_with_delay):
            result = _execute_plan(
                execution_plan=execution_plan,
                compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                accept_exceptions=False,
                sequential=True,  # Sequential execution
                max_thread=1
            )
        
        print(f"Sequential execution order: {execution_order}")
        
        # Sequential execution should preserve submission order
        expected_order = ["slow_table", "fast_table_1", "fast_table_2"]
        self.assertEqual(execution_order, expected_order)

    def test_ordered_parallel_execution_solution(self):
        """
        Demonstrate a solution: collect all futures first, then process in submission order
        """
        print("\n--> Testing ordered parallel execution solution")
        
        execution_order = []
        processing_order = []
        
        def task_with_delay(task_name: str, delay: float):
            time.sleep(delay)
            execution_order.append(f"completed_{task_name}")
            return f"result_{task_name}"
        
        # Simulate the problematic scenario
        tasks = [("slow_task", 0.3), ("fast_task_1", 0.05), ("fast_task_2", 0.1)]
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit all tasks and store futures in order
            futures_with_names = []
            for task_name, delay in tasks:
                future = executor.submit(task_with_delay, task_name, delay)
                futures_with_names.append((future, task_name))
            
            # Process in submission order (not completion order)
            for future, task_name in futures_with_names:
                result = future.result()  # Wait for this specific future
                processing_order.append(task_name)
        
        print(f"Task execution completion order: {execution_order}")
        print(f"Result processing order: {processing_order}")
        
        # Processing order should match submission order
        expected_processing = ["slow_task", "fast_task_1", "fast_task_2"]
        self.assertEqual(processing_order, expected_processing)
        
        # But execution completion order reflects actual timing
        self.assertIn("fast_task_1", execution_order[0])  # Fast task completes first
        
    def test_batch_parallel_execution_with_dependencies(self):
        """
        Test batched parallel execution respecting dependencies
        """
        print("\n--> Testing batch parallel execution with dependencies")
        
        execution_order = []
        mock_deploy = self._create_mock_deploy_function(execution_order)
        
        # Create dependency graph: src1, src2 (parallel) -> intermediate -> sink
        src1 = self._create_mock_node("src1", delay=0.2, to_run=True)
        src2 = self._create_mock_node("src2", delay=0.1, to_run=True)
        intermediate = self._create_mock_node("intermediate", delay=0.05, to_run=True)
        sink = self._create_mock_node("sink", delay=0.05, to_run=True)
        
        # Set up dependencies
        intermediate.add_parent(src1)
        intermediate.add_parent(src2)
        sink.add_parent(intermediate)
        
        execution_plan = self._create_mock_execution_plan([src1, src2, intermediate, sink])
        
        # Test the current parallel execution
        with patch('shift_left.core.deployment_mgr._deploy_one_node', side_effect=mock_deploy):
            result = _execute_plan(
                execution_plan=execution_plan,
                compute_pool_id=self.TEST_COMPUTE_POOL_ID_1,
                accept_exceptions=False,
                sequential=False,
                max_thread=4
            )
        
        print(f"Execution order with dependencies: {execution_order}")
        
        # src1 and src2 should execute first (in either order due to parallel execution)
        self.assertIn("src1", execution_order[0:2])
        self.assertIn("src2", execution_order[0:2])
        
        # intermediate should execute after both sources
        intermediate_idx = execution_order.index("intermediate")
        src1_idx = execution_order.index("src1")
        src2_idx = execution_order.index("src2")
        self.assertGreater(intermediate_idx, src1_idx)
        self.assertGreater(intermediate_idx, src2_idx)
        
        # sink should execute last
        sink_idx = execution_order.index("sink")
        self.assertGreater(sink_idx, intermediate_idx)

    def test_race_condition_impact_on_dependencies(self):
        """
        Test how race conditions can break dependency relationships
        """
        print("\n--> Testing race condition impact on dependencies")
        
        # This test simulates what could happen if we don't respect dependencies
        execution_order = []
        
        def immediate_deploy(node_name: str):
            execution_order.append(node_name)
            return f"deployed_{node_name}"
        
        # Simulate submitting dependent tasks without proper ordering
        with ThreadPoolExecutor(max_workers=3) as executor:
            # Submit child before parent completes (wrong!)
            futures = []
            
            # Parent task that takes time
            parent_future = executor.submit(lambda: (time.sleep(0.2), immediate_deploy("parent_table")))
            futures.append(("parent", parent_future))
            
            # Child task submitted immediately (this is the problem!)
            child_future = executor.submit(lambda: immediate_deploy("child_table"))
            futures.append(("child", child_future))
            
            # Process with as_completed (child might finish first!)
            completion_order = []
            future_map = {f[1]: f[0] for f in futures}
            
            for future in as_completed(future_map.keys()):
                table_type = future_map[future]
                result = future.result()
                completion_order.append(table_type)
        
        print(f"Submission order: [parent, child]")
        print(f"Completion order: {completion_order}")
        print(f"Execution order: {execution_order}")
        
        # This demonstrates how child could execute before parent
        # In a real scenario, this would break Flink table dependencies
        if len(completion_order) > 1:
            # Child might complete first due to parent's delay
            self.assertIn("child", completion_order)
            self.assertIn("parent", completion_order)


if __name__ == '__main__':
    unittest.main()