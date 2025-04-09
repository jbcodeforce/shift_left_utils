from collections import deque
import os
import unittest

from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import (
    read_pipeline_definition_from_file,
    FlinkStatementNode,
    PIPELINE_JSON_FILE_NAME
)


def execute_node(node):
    """Simulates the execution of a node's statement_name."""
    print(f"Executing program '{node.flink_statement}' for node '{node.name}'...")
    # Simulate statement_name execution (replace with actual statement_name call)
    import time
    time.sleep(0.5)
    node.is_running = True
    print(f"Node '{node.name}' finished execution.")

def _search_parents_to_run(nodes_to_run, node, visited_nodes):
    if node not in visited_nodes:
        nodes_to_run.append(node)
        visited_nodes.add(node)
        for p in node.parents:
            if not p.is_running:
                _search_parents_to_run(nodes_to_run, p, visited_nodes)

def _add_non_running_parents(node, execution_plan):
    for p in node.parents:
        if not p.is_running and p not in execution_plan:
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
    start_node.is_running = True
    # All the parents - grandparents... reacheable by DFS from the start_node are in nodes_to_run
    # 2. Add the non-running ancestors to the execution plan 
    for node in reversed(nodes_to_run):
        execution_plan.append(node)
        # to be starteable each parent needs to be part of the running ancestors
        _add_non_running_parents(node, execution_plan)
        node.is_running = True  # Mark as running as they will be executed

    # execution_plan.append(("run", start_node))
    # start_node.is_running = True  # Mark the starting node as running

    # 3. Restart all children of each node in the execution plan if they are not yet there
    for node in execution_plan:
        for c in node.children:
            if not c.is_running and c not in execution_plan:
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

def get_ancestor_subgraph(start_node):
    """Builds a subgraph containing all ancestors of the start node."""
    ancestors = set()
    queue = deque([start_node])
    visited = {start_node}

    while queue:
        current_node = queue.popleft()
        for parent in current_node.parents:
            if parent not in visited:
                ancestors.add(parent)
                visited.add(parent)
                queue.append(parent)
            if parent not in ancestors:  # Ensure parent itself is in the set
                ancestors.add(parent)

    # Include dependencies within the ancestor subgraph
    ancestor_nodes = list(ancestors)
    ancestor_dependencies = {node: {p for p in node.parents if p in ancestors} for node in ancestor_nodes}

    return ancestor_nodes, ancestor_dependencies

def topological_sort(nodes, dependencies):
    """Performs topological sort on a DAG."""
    in_degree = {node: len(dependencies[node]) for node in nodes}
    queue = deque([node for node in nodes if in_degree[node] == 0])
    sorted_nodes = []

    while queue:
        node = queue.popleft()
        sorted_nodes.append(node)
        for child in [n for n in nodes if node in n.parents and n in dependencies]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

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
            print(f"Restarting node: '{node.table_name}' (program: '{node.flink_statement}')")


class TestExecutionPlanBuilder(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.src_a = FlinkStatementNode(table_name= "Src_A", path= "initial_setup_a.sh")
        cls.node_a = FlinkStatementNode(table_name= "A", path= "process_a.py")
        cls.src_b = FlinkStatementNode(table_name= "Src_B", path= "initial_setup_b.sh")
        cls.src_c = FlinkStatementNode(table_name= "Src_C", path= "initial_setup_c.sh")
        cls.node_x = FlinkStatementNode(table_name= "X", path= "process_x.py")
        cls.node_y = FlinkStatementNode(table_name= "Y", path= "process_y.py")
        cls.node_z = FlinkStatementNode(table_name= "Z", path= "combine_results.py")
        cls.node_p = FlinkStatementNode(table_name= "P", path= "final_report.py")
        cls.node_c = FlinkStatementNode(table_name= "C", path= "process_c")
        cls.node_d = FlinkStatementNode(table_name= "D", path= "process_d")
        cls.node_e = FlinkStatementNode(table_name= "E", path= "process_e")
        cls.node_f = FlinkStatementNode(table_name= "F", path= "process_f")

        cls.src_a.add_child(cls.node_x)
        cls.src_a.add_child(cls.node_a)
        cls.src_b.add_child(cls.node_y)
        cls.src_c.add_child(cls.node_a)
        cls.node_x.add_child(cls.node_z)
        cls.node_y.add_child(cls.node_z)  # Node Z has multiple parents (X and Y)
        cls.node_z.add_child(cls.node_p)
        cls.node_z.add_child(cls.node_c)
        cls.node_z.add_child(cls.node_d)
        cls.node_c.add_child(cls.node_e)
        cls.node_d.add_child(cls.node_f)
        cls.node_y.add_child(cls.node_d) 

    
    def _reset_nodes(self):
        for node in [self.src_a, self.src_b, self.src_c, self.node_a, self.node_x, self.node_y, self.node_z, self.node_p, self.node_c, self.node_d, self.node_e, self.node_f]:
            node.is_running = False

    def _test_start_with_int(self):
        print("\n--- Scenario 1: Starting node 'Z' ---")
        self.node_z.is_running = False # Ensure it's not running initially
        plan1 = build_execution_plan(self.node_z)
        assert plan1[0] == self.src_a or plan1[0] == self.src_b
        execute_plan(plan1)
        self._reset_nodes()
    
    def _test_start_from_leaf(self):
        print("\n--- Scenario 2: Starting node 'E' ---")
        plan2 = build_execution_plan(self.node_e)
        print(plan2)
        execute_plan(plan2)
        self._reset_nodes()

    def _test_start_from_root(self):
        # Scenario 3: Starting the root node
        print("\n--- Scenario 3: Starting node 'Root' ---")
        plan3 = build_execution_plan(self.src_a)
        print(plan3)
        execute_plan(plan3)


    def _test_build_graph_from_pipeline_def(self):
        print("1-load a pipeline def, create nodes for it and parent and children, add edges\n")
        pipe_def = read_pipeline_definition_from_file( os.getenv("PIPELINES") + "/intermediates/aqem/tag_tag_dummy/" + PIPELINE_JSON_FILE_NAME)
        assert pipe_def
        import shift_left.core.deployment_mgr as dm
        
        graph: FlinkStatementNode = dm._build_table_graph(pipe_def.to_node())
        assert graph
        print(graph)


    def test_deploy_int_table(self):
        import shift_left.core.deployment_mgr as dm
        config = get_config()
        graph = dm.deploy_pipeline_from_table("int_aqem_tag_tag_dummy", 
                                              os.getenv("PIPELINES"), 
                                              config['flink']['compute_pool_id'])


if __name__ == "__main__":
    unittest.main()
