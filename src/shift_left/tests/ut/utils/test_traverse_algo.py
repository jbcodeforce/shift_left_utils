"""
Copyright 2024-2025 Confluent, Inc.
"""
from collections import deque
import os
import unittest
from typing import Dict, List, Tuple
from shift_left.core.utils.app_config import get_config
from shift_left.core.models.flink_statement_model import StatementInfo
from shift_left.core.utils.file_search import (
    read_pipeline_definition_from_file,
    FlinkStatementNode,
    PIPELINE_JSON_FILE_NAME
)


def execute_node(node):
    """Simulates the execution of a node's statement_name."""
    print(f"Executing program '{node.path}' for node '{node.table_name}'...")
    # Simulate statement_name execution (replace with actual statement_name call)
    import time
    time.sleep(0.5)
    node.existing_statement_info = StatementInfo(status_phase="RUNNING")
    print(f"Node '{node.table_name}' finished execution.")

def _search_parents_to_run(nodes_to_run, node, visited_nodes):
    if node not in visited_nodes:
        nodes_to_run.append(node)
        visited_nodes.add(node)
        for p in node.parents:
            if not p.is_running():
                _search_parents_to_run(nodes_to_run, p, visited_nodes)

def _add_non_running_parents(node, execution_plan):
    for p in node.parents:
        if not p.is_running() and p not in execution_plan:
            # position the parent before the node to be sure it is started
            idx=execution_plan.index(node)
            execution_plan.insert(idx,p)
            p.is_runnning = True

def build_execution_plan(start_node):
    """
    Builds and executes the execution plan for a related node.

    Args:
        start_node: The node that is being started.
    """
    execution_plan = []
    nodes_to_run = []

    # 1. Find all non-running ancestors (DFS from the start node)
    visited_nodes = set()   
    _search_parents_to_run(nodes_to_run, start_node, visited_nodes)
            
    #nodes_to_run.insert(0,start_node)
    start_node.existing_statement_info = StatementInfo(status_phase="RUNNING")
    # All the parents - grandparents... reacheable by DFS from the start_node are in nodes_to_run
    # 2. Add the non-running ancestors to the execution plan 
    for node in reversed(nodes_to_run):
        execution_plan.append(node)
        # to be starteable each parent needs to be part of the running ancestors
        _add_non_running_parents(node, execution_plan)
        node.to_run = True  # Mark as running as they will be executed

    # execution_plan.append(("run", start_node))
    # start_node.is_running = True  # Mark the starting node as running

    # 3. Restart all children of each node in the execution plan if they are not yet there
    for node in execution_plan:
        for c in node.children:
            if not c.is_running() and not c.to_run and c not in execution_plan:
                _search_parents_to_run(execution_plan, c, visited_nodes)
                #execution_plan.append(c)
                c.to_restart = True
                #_add_non_running_parents(c, execution_plan)
               
    return execution_plan

def _bfs_on_children(start_node, execution_plan):
    queue = deque(start_node.children)
    restarted_children = set()

    while queue:
        child = queue.popleft()
        if child not in execution_plan:
            if child not in restarted_children:
                execution_plan.append(child)
                child.is_running = False  # Mark for potential future execution
                restarted_children.add(child)
        queue.extend(child.children)

def get_ancestor_subgraph(start_node)-> Tuple[Dict[str, FlinkStatementNode], Dict[str, List[FlinkStatementNode]]]:
    """Builds a subgraph containing all ancestors of the start node."""
    ancestors = {}
    queue = deque([start_node])
    visited = {start_node}

    while queue:
        current_node = queue.popleft()
        for parent in current_node.parents:
            if parent not in visited:
                ancestors[parent.table_name] = parent
                visited.add(parent)
                queue.append(parent)
            if parent not in ancestors:  # Ensure parent itself is in the set
                 ancestors[parent.table_name] = parent
    ancestors[start_node.table_name] = start_node
    # Include dependencies within the ancestor subgraph
    ancestor_dependencies = []
    for node in ancestors:
        for parent in ancestors[node].parents:
            ancestor_dependencies.append((node, parent))

    return ancestors, ancestor_dependencies

def topological_sort(nodes: Dict[str, FlinkStatementNode], dependencies: Tuple[str, FlinkStatementNode])-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG."""
    # compute in_degree for each node as the number of incoming edges. the edges are in the dependencies
    in_degree = {node.table_name: 0 for node in nodes.values()}
    for node in nodes.values():
        for tbname, _ in dependencies:
            if node.table_name == tbname:
                in_degree[node.table_name] += 1
    queue = deque([node for node in nodes.values() if in_degree[node.table_name] == 0])
    sorted_nodes = []

    while queue:
        node = queue.popleft()
        sorted_nodes.append(node)
        for tbname, neighbor in dependencies:
            if neighbor.table_name == node.table_name:
                in_degree[tbname] -= 1
                if in_degree[tbname] == 0:
                    queue.append(nodes[tbname])

    if len(sorted_nodes) == len(nodes):
        return sorted_nodes
    else:
        raise ValueError("Graph has a cycle, cannot perform topological sort.")

def build_execution_plan_with_topo_sort(start_node):
    """
    Builds an execution plan with topological sorting for ancestor dependencies.
    """
    execution_plan = []

    # 1. Build the ancestor subgraph
    ancestor_nodes, ancestor_dependencies = get_ancestor_subgraph(start_node)

    # 2. Perform topological sort on the ancestors
    try:
        sorted_ancestors = topological_sort(ancestor_nodes, ancestor_dependencies)
        # Add 'run' actions for ancestors in topological order
        for ancestor in sorted_ancestors:
            if not ancestor.is_running:
                execution_plan.append(("run", ancestor))
                ancestor.is_running = True
    except ValueError as e:
        print(f"Error: {e}")
        return []  # Or handle the cycle error appropriately

    # 3. Run the starting node
    execution_plan.append(("run", start_node))
    start_node.is_running = True

    # 4. Restart all children (BFS)
    queue = deque(start_node.children)
    restarted_children = set()
    while queue:
        child = queue.popleft()
        if child not in restarted_children:
            execution_plan.append(("restart", child))
            child.is_running = False
            restarted_children.add(child)
            queue.extend(child.children)

    return execution_plan

def execute_plan(plan):
    """Executes the generated execution plan."""
    print("\n--- Execution Plan ---")
    for node in plan:
        print(f"node: '{node.table_name}'")
        if node.is_running:
            execute_node(node)
        else:
            print(f"Restarting node: '{node.table_name}' (program: '{node.path}')")

def build_edges_from_node(node):
    """Builds edges from a given node to all its ancestors recursively."""
    edges = []
    visited = set()

    def _traverse(current_node):
        for parent in current_node.parents:
            if parent not in visited:
                edges.append((current_node, parent))
                visited.add(parent)
                _traverse(parent)  # Recursively traverse parent's parents

    _traverse(node)
    return edges


class TestExecutionPlanBuilder(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src_a = FlinkStatementNode(table_name= "Src_A", product_name= "p1", path= "initial_setup_a.sh")
        cls.node_a = FlinkStatementNode(table_name= "A", product_name= "p1", path= "process_a.py")
        cls.src_b = FlinkStatementNode(table_name= "Src_B", product_name= "p1", path= "initial_setup_b.sh")
        cls.node_b = FlinkStatementNode(table_name= "B", product_name= "p1", path= "process_b.py")
        cls.src_x = FlinkStatementNode(table_name= "Src_X", product_name= "p1", path= "initial_setup_x.sh")
        cls.src_y = FlinkStatementNode(table_name= "Src_Y", product_name= "p1", path= "initial_setup_y.sh")
        cls.node_x = FlinkStatementNode(table_name= "X", product_name= "p1", path= "process_x.py")
        cls.node_y = FlinkStatementNode(table_name= "Y", product_name= "p1", path= "process_y.py")
        cls.node_z = FlinkStatementNode(table_name= "Z",  product_name= "p1", path= "combine_results.py")
        cls.node_p = FlinkStatementNode(table_name= "P", product_name= "p1", path= "final_report.py")
        cls.node_c = FlinkStatementNode(table_name= "C", product_name= "p1", path= "process_c")
        cls.node_d = FlinkStatementNode(table_name= "D", product_name= "p1", path= "process_d")
        cls.node_e = FlinkStatementNode(table_name= "E", product_name= "p1", path= "process_e")
        cls.node_f = FlinkStatementNode(table_name= "F", product_name= "p1", path= "process_f")

        cls.src_x.add_child(cls.node_x)
        cls.src_a.add_child(cls.node_a)
        cls.src_x.add_child(cls.node_a)
        cls.src_y.add_child(cls.node_y)
        cls.src_b.add_child(cls.node_b)
        cls.node_x.add_child(cls.node_z)
        cls.node_y.add_child(cls.node_z)  # Node Z has multiple parents (X and Y)
        cls.node_z.add_child(cls.node_p)
        cls.node_z.add_child(cls.node_c)
        cls.node_z.add_child(cls.node_d)
        cls.node_c.add_child(cls.node_f)
        cls.node_d.add_child(cls.node_e)
        cls.node_b.add_child(cls.node_c) 

    
    def _reset_nodes(self):
        for node in [self.src_a, self.src_b, self.src_x, self.src_y, self.node_a, self.node_b, self.node_x, self.node_y, self.node_z, self.node_p, self.node_c, self.node_d, self.node_e, self.node_f]:
            node.to_run = False
            node.existing_statement_info = StatementInfo(status_phase=None)

    def _test_1(self):
        edges= [[2,3], [1,2], [5,2], [5,1],[0,1],[4,5]]
        adj = [[] for _ in range(6)]
        for u,v in edges:
            adj[u].append(v)
        print(adj)
        in_degree = [0]*6   
        for i in range(6):
            for v in adj[i]:
                in_degree[v] += 1
        print(in_degree)
        queue = deque([i for i in range(6) if in_degree[i] == 0])
        print(queue)
        result = []
        while queue:
            node = queue.popleft()
            result.append(node)
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        print(result)

    def _test_start_with_int(self):
        print("\n--- Scenario 1: Starting node 'Z' ---")
        self._reset_nodes()
        self.node_z.to_run = False # Ensure it's not running initially
        plan1 = build_execution_plan(self.node_z)
        print(plan1)
        execute_plan(plan1)
        self._reset_nodes()
    
    def _test_start_from_leaf(self):
        print("\n--- Scenario 2: Starting node 'E' ---")
        self._reset_nodes()
        plan2 = build_execution_plan(self.node_e)
        print(plan2)
        execute_plan(plan2)
        self._reset_nodes()

    def _test_start_from_root(self):
        # Scenario 3: Starting the root node
        print("\n--- Scenario 3: Starting node 'Root' ---")
        self._reset_nodes()
        plan3 = build_execution_plan(self.src_a)
        print(plan3)
        execute_plan(plan3)

    def test_build_ancestor_subgraph(self):
        print("\n--- Scenario 4: Build ancestor subgraph ---")
        edges=build_edges_from_node(self.node_e)
        for edge in edges:
            print(f"edge: {edge[0].table_name} -> {edge[1].table_name}")
        ancestor_nodes, ancestor_dependencies = get_ancestor_subgraph(self.node_e)
        for tbname, node in ancestor_dependencies:
            print(f"node: {tbname} -> {node.table_name}")
        for node in ancestor_nodes:
            print(f"node: {node}")
        sorted_ancestors = topological_sort(ancestor_nodes, ancestor_dependencies)
        print(f"sorted_ancestors: {[node.table_name for node in sorted_ancestors]}")

        print("\n--- Scenario 5: Build execution plan with topological sort ---")
        ancestor_nodes, ancestor_dependencies = get_ancestor_subgraph(self.node_f)
        sorted_ancestors = topological_sort(ancestor_nodes, ancestor_dependencies)
        print(f"sorted_ancestors: {[node.table_name for node in sorted_ancestors]}")




if __name__ == "__main__":
    unittest.main()
