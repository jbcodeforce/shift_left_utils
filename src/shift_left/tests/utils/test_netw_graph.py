import networkx as nx
import matplotlib.pyplot as plt

def display_graph(nodes, edges):
    """
    Displays a graph using networkx and matplotlib.

    Args:
        nodes: A list of node identifiers.
        edges: A list of tuples, where each tuple represents an edge
               (source_node, target_node).
    """

    G = nx.Graph()  # Create a graph object

    # Add nodes
    G.add_nodes_from(nodes)

    # Add edges
    G.add_edges_from(edges)

    # Draw the graph
    pos = nx.spring_layout(G)  # Layout algorithm (you can try others)
    nx.draw(G, pos, with_labels=True, node_size=700, node_color="skyblue", font_size=15, font_weight="bold")

    # Show the graph
    plt.show()

# Example usage:
nodes = ["A", "B", "C", "D"]
edges = [("A", "B"), ("B", "C"), ("C", "D"), ("D", "A"), ("B", "D")]

display_graph(nodes, edges)

# Example usage of a directed graph:
def display_directed_graph(nodes, edges):
    """
    Displays a directed graph using networkx and matplotlib.

    Args:
        nodes: A list of node identifiers.
        edges: A list of tuples, where each tuple represents a directed edge
               (source_node, target_node).
    """

    G = nx.DiGraph()  # Create a directed graph object

    # Add nodes
    G.add_nodes_from(nodes)

    # Add edges
    G.add_edges_from(edges)

    # Draw the graph
    pos = nx.spring_layout(G)  # Layout algorithm
    nx.draw(G, pos, with_labels=True, node_size=700, node_color="lightgreen", font_size=15, font_weight="bold", arrowsize=20)

    # Show the graph
    plt.show()

nodes_directed = ["User", "API", "Database"]
edges_directed = [("User", "API"), ("API", "Database"), ("Database", "API")]

display_directed_graph(nodes_directed, edges_directed)

#Example with node attributes.
def display_attributed_graph(nodes_with_attributes, edges):
    G = nx.Graph()

    for node_id, attributes in nodes_with_attributes.items():
        G.add_node(node_id, **attributes)

    G.add_edges_from(edges)

    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=700, node_color=[G.nodes[node]['color'] for node in G.nodes()], font_size=15, font_weight="bold")

    plt.show()

nodes_attributed = {
    "A": {"color": "red"},
    "B": {"color": "blue"},
    "C": {"color": "green"}
}

edges_attributed = [("A","B"),("B","C")]

display_attributed_graph(nodes_attributed, edges_attributed)