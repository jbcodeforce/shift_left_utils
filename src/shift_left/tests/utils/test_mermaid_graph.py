def generate_mermaid_graph(nodes, edges, graph_type="flowchart TD"):
    """
    Generates a Mermaid graph definition string.

    Args:
        nodes: A list of tuples, where each tuple represents a node.
               (node_id, node_label).
        edges: A list of tuples, where each tuple represents an edge.
               (source_node_id, target_node_id, edge_label). edge_label is optional.
        graph_type: The type of Mermaid graph (e.g., "flowchart TD", "graph LR").

    Returns:
        A string containing the Mermaid graph definition.
    """

    mermaid_string = f"{graph_type}\n"

    # Add nodes
    for node_id, node_label in nodes:
        mermaid_string += f"    {node_id}[{node_label}]\n"

    # Add edges
    for edge in edges:
        source, target = edge[0], edge[1]

        if len(edge) == 3:
            label = edge[2]
            mermaid_string += f"    {source} -->|{label}| {target}\n"
        else:
            mermaid_string += f"    {source} --> {target}\n"

    return mermaid_string

# Example usage:
nodes = [
    ("A", "Start"),
    ("B", "Process Data"),
    ("C", "Display Result"),
    ("D", "End"),
]

edges = [
    ("A", "B"),
    ("B", "C", "Data processed"),
    ("C", "D"),
]

mermaid_code = generate_mermaid_graph(nodes, edges)

print(mermaid_code)

# Example usage with a different graph type and more nodes/edges
nodes2 = [
    ("User", "User"),
    ("API", "API Server"),
    ("DB", "Database"),
    ("Cache", "Cache"),
]

edges2 = [
    ("User", "API", "Request"),
    ("API", "Cache", "Check Cache"),
    ("Cache", "DB", "Data not in Cache"),
    ("DB", "Cache", "Store Data"),
    ("Cache", "API", "Return Data"),
    ("API", "User", "Response"),
]

mermaid_code2 = generate_mermaid_graph(nodes2, edges2, "sequenceDiagram")

print("\n" + mermaid_code2)

nodes3 = [
    ("ClassA", "ClassA"),
    ("ClassB", "ClassB"),
    ("ClassC", "ClassC"),
]

edges3 = [
    ("ClassA","ClassB","Aggregation"),
    ("ClassB","ClassC","Composition")
]

mermaid_code3 = generate_mermaid_graph(nodes3, edges3, "classDiagram")
print("\n" + mermaid_code3)