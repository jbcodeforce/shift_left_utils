import unittest
from unittest.mock import patch, MagicMock
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from shift_left.core.deployment_mgr import full_pipeline_undeploy_from_product
from shift_left.core.models.flink_statement_model import FlinkStatementNode, StatementInfo
from ut.core.BaseUT import BaseUT


class TestRaceCondition(BaseUT):
    """Test race conditions in parallel execution of Flink statements"""

    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        self.execution_order = []
        self.lock = threading.Lock()

    def test_as_completed_order_vs_submission_order(self):
        """
        Demonstrate that as_completed() can return futures out of submission order
        This is the core issue in parallel deployment
        """
        print("\n--> Testing as_completed() vs submission order")

        def slow_task(task_id: str, delay: float):
            """Simulate work with varying delays"""
            time.sleep(delay)
            with self.lock:
                self.execution_order.append(f"completed_{task_id}")
            return f"result_{task_id}"

        # Submit tasks with different delays - first task takes longest
        submission_order = []
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures_to_id = {}

            # Submit in order A, B, C but A takes longest
            delays = [0.3, 0.1, 0.05]  # A=300ms, B=100ms, C=50ms
            for i, delay in enumerate(delays):
                task_id = chr(65 + i)  # A, B, C
                submission_order.append(f"submitted_{task_id}")
                future = executor.submit(slow_task, task_id, delay)
                futures_to_id[future] = task_id

            # Process as they complete
            completion_order = []
            for future in as_completed(futures_to_id):
                task_id = futures_to_id[future]
                completion_order.append(f"processed_{task_id}")
                result = future.result()

        print(f"Submission order: {submission_order}")
        print(f"Completion order: {completion_order}")
        print(f"Execution order: {self.execution_order}")

        # Assert that completion order differs from submission order
        expected_submission = ["submitted_A", "submitted_B", "submitted_C"]
        expected_completion = ["processed_C", "processed_B", "processed_A"]  # C finishes first

        self.assertEqual(submission_order, expected_submission)
        self.assertEqual(completion_order, expected_completion)

    @patch('shift_left.core.deployment_mgr._drop_node_worker')
    @patch('shift_left.core.deployment_mgr.get_or_build_inventory')
    @patch('shift_left.core.deployment_mgr.get_config')
    def test_undeploy_execution_order_race_condition(self,
                                                    mock_get_config,
                                                    mock_get_inventory,
                                                    mock_drop_worker):
        """
        Test that full_pipeline_undeploy_from_product can execute nodes out of plan order
        due to parallel execution with as_completed()
        """
        print("\n--> Testing undeploy race condition")

        # Setup mocks
        mock_get_config.return_value = {'flink': {'compute_pool_id': 'test_pool'}}

        # Create mock inventory with dependent tables
        mock_inventory = {
            'parent_table': {
                'table_name': 'parent_table',
                'product_name': 'test_product',
                'table_folder_name': '/path/to/parent'
            },
            'child_table': {
                'table_name': 'child_table',
                'product_name': 'test_product',
                'table_folder_name': '/path/to/child'
            }
        }
        mock_get_inventory.return_value = mock_inventory

        execution_order = []
        lock = threading.Lock()

        def mock_drop_with_delay(node):
            """Mock drop worker that simulates different execution times"""
            table_name = node.table_name

            # Parent table takes longer to drop than child
            if table_name == 'parent_table':
                time.sleep(0.2)  # 200ms
            else:
                time.sleep(0.05)  # 50ms

            with lock:
                execution_order.append(f"dropped_{table_name}")
            return f"Dropped {table_name}"

        mock_drop_worker.side_effect = mock_drop_with_delay

        # Mock the pipeline definitions and execution plan building
        with patch('shift_left.core.deployment_mgr.read_pipeline_definition_from_file') as mock_read_pipeline, \
             patch('shift_left.core.deployment_mgr._build_statement_node_map') as mock_build_map, \
             patch('shift_left.core.deployment_mgr._build_topological_sorted_graph') as mock_build_parents, \
             patch('shift_left.core.deployment_mgr._build_execution_plan_using_sorted_ancestors') as mock_build_plan:

            # Create mock nodes
            parent_node = FlinkStatementNode(
                table_name='parent_table',
                product_name='test_product',
                dml_statement_name='dml_parent',
                ddl_statement_name='ddl_parent',
                compute_pool_id='test_pool'
            )
            parent_node.existing_statement_info = StatementInfo(
                name='dml_parent',
                status_phase='RUNNING',
                compute_pool_id='test_pool'
            )

            child_node = FlinkStatementNode(
                table_name='child_table',
                product_name='test_product',
                dml_statement_name='dml_child',
                ddl_statement_name='ddl_child',
                compute_pool_id='test_pool'
            )
            child_node.existing_statement_info = StatementInfo(
                name='dml_child',
                status_phase='RUNNING',
                compute_pool_id='test_pool'
            )

            # Setup mock returns
            mock_read_pipeline.side_effect = lambda path: MagicMock(to_node=lambda:
                parent_node if 'parent' in path else child_node)
            mock_build_map.return_value = {}
            mock_build_parents.return_value = [parent_node, child_node]

            # Create mock execution plan - proper order should be parent first, then child
            mock_execution_plan = MagicMock()
            mock_execution_plan.nodes = [parent_node, child_node]  # Correct topological order
            mock_build_plan.return_value = mock_execution_plan

            # Execute the function
            result = full_pipeline_undeploy_from_product(
                product_name='test_product',
                inventory_path='/test/path',
                compute_pool_id='test_pool'
            )

        print(f"Execution order: {execution_order}")
        print(f"Result: {result}")

        # Due to parallel execution and as_completed(), child might finish before parent
        # even though the execution plan has parent before child
        if len(execution_order) == 2:
            # This demonstrates the race condition - child finished first despite plan order
            self.assertEqual(execution_order[0], 'dropped_child_table')
            self.assertEqual(execution_order[1], 'dropped_parent_table')

        # Verify both nodes were processed
        self.assertEqual(mock_drop_worker.call_count, 2)

    def test_controlled_execution_order_solution(self):
        """
        Demonstrate a solution: processing futures in submission order instead of completion order
        """
        print("\n--> Testing controlled execution order solution")

        def task_with_delay(task_id: str, delay: float):
            time.sleep(delay)
            return f"result_{task_id}"

        # Method 1: Process in submission order (wait for each in order)
        submission_order = []
        processing_order = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []

            # Submit tasks
            delays = [0.3, 0.1, 0.05]  # A takes longest
            for i, delay in enumerate(delays):
                task_id = chr(65 + i)
                submission_order.append(task_id)
                future = executor.submit(task_with_delay, task_id, delay)
                futures.append((future, task_id))

            # Process in submission order (not completion order)
            for future, task_id in futures:
                result = future.result()  # This waits for completion in order
                processing_order.append(task_id)

        print(f"Submission order: {submission_order}")
        print(f"Processing order: {processing_order}")

        # Processing order should match submission order
        self.assertEqual(submission_order, processing_order)

    def test_race_condition_frequency(self):
        """
        Test how often the race condition occurs with repeated runs
        """
        print("\n--> Testing race condition frequency")

        race_conditions = 0
        total_runs = 10

        for run in range(total_runs):
            execution_order = []
            lock = threading.Lock()

            def variable_delay_task(task_id: str):
                # Variable delays to increase chance of race condition
                import random
                delay = random.uniform(0.01, 0.1)
                time.sleep(delay)
                with lock:
                    execution_order.append(task_id)
                return f"result_{task_id}"

            submission_order = ['A', 'B', 'C']

            with ThreadPoolExecutor(max_workers=3) as executor:
                future_to_id = {}

                # Submit in order A, B, C
                for task_id in submission_order:
                    future = executor.submit(variable_delay_task, task_id)
                    future_to_id[future] = task_id

                # Process with as_completed
                completion_order = []
                for future in as_completed(future_to_id):
                    task_id = future_to_id[future]
                    completion_order.append(task_id)

            # Check if completion order differs from submission order
            if completion_order != submission_order:
                race_conditions += 1
                print(f"Run {run}: Race condition detected - {completion_order}")

        print(f"Race conditions in {race_conditions}/{total_runs} runs")
        # Race conditions should occur at least sometimes with random delays
        # This demonstrates the non-deterministic nature of the issue


if __name__ == '__main__':
    unittest.main()
