from collections import deque

class Node:
    def __init__(self, name, program):
        self.name = name
        self.program = program
        self.parent = None
        self.children = []
        self.is_running = False

    def add_child(self, child):
        self.children.append(child)
        child.parent = self

    def __repr__(self):
        return f"Node(name='{self.name}', program='{self.program}', running={self.is_running})"

def execute_node(node):
    """Simulates the execution of a node's program."""
    print(f"Executing program '{node.program}' for node '{node.name}'...")
    # Simulate program execution (replace with actual program call)
    import time
    time.sleep(0.5)
    node.is_running = True
    print(f"Node '{node.name}' finished execution.")

def build_execution_plan(start_node):
    """
    Builds and executes the execution plan for a related node.

    Args:
        start_node: The node that is being started.
    """
    execution_plan = []

    # 1. Run any non-running parents (DFS)
    def run_parents(node):
        if node.parent and not node.parent.is_running:
            run_parents(node.parent)
            execution_plan.append(("run", node.parent))

    run_parents(start_node)
    execution_plan.append(("run", start_node))
    start_node.is_running = True  # Mark the starting node as running

    # 2. Restart all children (BFS)
    queue = deque(start_node.children)
    restarted_children = set()

    while queue:
        child = queue.popleft()
        if child not in restarted_children:
            execution_plan.append(("restart", child))
            child.is_running = False  # Mark for potential future execution
            restarted_children.add(child)
            queue.extend(child.children)

    return execution_plan

def execute_plan(plan):
    """Executes the generated execution plan."""
    print("\n--- Execution Plan ---")
    for action, node in plan:
        print(f"{action.capitalize()} node: '{node.name}'")
        if action == "run":
            execute_node(node)
        elif action == "restart":
            print(f"Restarting node: '{node.name}' (program: '{node.program}')")

if __name__ == "__main__":
    # Example Node Structure
    root = Node("Root", "initial_setup.sh")
    node_a = Node("A", "process_data_a.py")
    node_b = Node("B", "process_data_b.py")
    node_c = Node("C", "analyze_results.py")
    node_d = Node("D", "report_generation.py")
    node_e = Node("E", "cleanup.py")
    node_f = Node("F", "secondary_analysis.py")

    root.add_child(node_a)
    root.add_child(node_b)
    node_a.add_child(node_c)
    node_b.add_child(node_d)
    node_c.add_child(node_e)
    node_b.add_child(node_f)

    # Scenario 1: Starting a leaf node (node_e)
    print("\n--- Scenario 1: Starting node 'E' ---")
    node_e.is_running = False # Ensure it's not running initially
    plan1 = build_execution_plan(node_e)
    execute_plan(plan1)

    # Reset running status for the next scenario
    for node in [root, node_a, node_b, node_c, node_d, node_e, node_f]:
        node.is_running = False

    # Scenario 2: Starting an intermediate node (node_b)
    print("\n--- Scenario 2: Starting node 'B' ---")
    plan2 = build_execution_plan(node_b)
    execute_plan(plan2)

    # Reset running status
    for node in [root, node_a, node_b, node_c, node_d, node_e, node_f]:
        node.is_running = False

    # Scenario 3: Starting the root node
    print("\n--- Scenario 3: Starting node 'Root' ---")
    plan3 = build_execution_plan(root)
    execute_plan(plan3)