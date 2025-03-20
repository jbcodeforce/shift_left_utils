import typer
from rich import print
from rich.tree import Tree
from rich.console import Console
from shift_left.core.pipeline_mgr import (
    build_pipeline_definition_from_table, 
    walk_the_hierarchy_for_report_from_table, 
    build_all_pipeline_definitions,
    report_running_dmls,
    ReportInfoNode,
    delete_metada_files)
from typing_extensions import Annotated
from shift_left.core.deployment_mgr import deploy_pipeline_from_table

import yaml
import os
import sys
import logging
import networkx as nx
import matplotlib.pyplot as plt

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
"""
Manage a pipeline entity:
- build the inventory of tables
- build the inventory of tables
- generate pipeline reports
"""
app = typer.Typer(no_args_is_help=True)



@app.command()
def build_metadata(dml_file_name:  Annotated[str, typer.Argument(help = "The path to the DML file. e.g. $PIPELINES/table-name/sql-scripts/dml.table-name.sql")], 
             pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")]):
    """
    Build a pipeline from a sink table: add or update {} each table in the pipeline
    """
    if not dml_file_name.endswith(".sql"):
        print(f"[red]Error: the first parameter needs to be a dml sql file[/red]")
        raise typer.Exit(1)

    print(f"Build pipeline from sink table {dml_file_name}")
    if pipeline_path and pipeline_path != os.getenv("PIPELINES"):
        print(f"Using pipeline path {pipeline_path}")
        os.environ["PIPELINES"] = pipeline_path

    build_pipeline_definition_from_table(dml_file_name, pipeline_path)
    print(f"Pipeline built from {dml_file_name}")

@app.command()
def delete_metadata(path_from_where_to_delete:  Annotated[str, typer.Argument()]):
    """
    Delete a pipeline definitions from a given folder path
    """
    print(f"Delete pipeline definition from {path_from_where_to_delete}")
    delete_metada_files(path_from_where_to_delete)


@app.command()
def build_all_metadata(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")]):
    """
    Go to the hierarchy of folders for dimensions, views and facts and build the pipeline definitions for each table found using recurring walk through
    """
    print(f"Build all pipeline definitions for dimension, fact tables in {pipeline_path}")
    build_all_pipeline_definitions(pipeline_path)
    print("Done")
    

@app.command()
def report(table_name: Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json. e.g. src_aqem_tag_tag. The name has to exist in inventory as a key.")],
          inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")],
          to_yaml: bool = typer.Option(False, "--yaml", help="Output the report in YAML format"),
          to_json: bool = typer.Option(False, "--json", help="Output the report in JSON format"),
        to_graph: bool = typer.Option(False, "--graph", help="Output the report in Graphical tree")):
    """
    Generate a report showing the pipeline hierarchy for a given table using its pipeline_definition.json
    """
    console = Console()
    print(f"Generating pipeline report for table {table_name}")
    try:
        pipeline_def=walk_the_hierarchy_for_report_from_table(table_name, inventory_path)
        if pipeline_def is None:
            print(f"[red]Error: pipeline definition not found for table {table_name}[/red]")
            raise typer.Exit(1)
    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    if to_yaml:
        print("[bold]YAML:[/bold]")
        print(yaml.dump(pipeline_def.model_dump()))
    if to_json:
        print("[bold]JSON:[/bold]")
        print(pipeline_def.model_dump_json(indent=3))
    if to_graph:
        print("[bold]Graph:[/bold]")
        _process_hierarchy_to_node(pipeline_def)
        
    # Create a rich tree for visualization
    tree = Tree(f"[bold blue]{table_name}[/bold blue]")
    
    def add_nodes_to_tree(node_data, tree_node):
        if node_data.parents:
            for parent in node_data.parents:
                parent_node = tree_node.add(f"[green]{parent['table_name']}[/green]")
                # Add file references
                if parent.get('dml_path'):
                    parent_node.add(f"[dim]DML: {parent['dml_path']}[/dim]")
                if parent.get('ddl_path'):
                    parent_node.add(f"[dim]DDL: {parent['ddl_path']}[/dim]")
                parent_node.add(f"[dim]Base: {parent['base_path']}[/dim]")
                parent_node.add(f"[dim]Type: parent[/dim]")
        if node_data.children:
            for child in node_data.children:
                child_node = tree_node.add(f"[green]{child['table_name']}[/green]")
                # Add file references
                if child.get('dml_path'):
                    child_node.add(f"[dim]DML: {child['dml_path']}[/dim]")
                if child.get('ddl_path'):
                    child_node.add(f"[dim]DDL: {child['ddl_path']}[/dim]")
                child_node.add(f"[dim]Base: {child['base_path']}[/dim]")
                child_node.add(f"[dim]Type: child[/dim]")
    
    # Add the root node details
    tree.add(f"[dim]DML: {pipeline_def.dml_path}[/dim]")
    tree.add(f"[dim]DDL: {pipeline_def.ddl_path}[/dim]")
    
    # Add parent nodes
    add_nodes_to_tree(pipeline_def, tree)
    
    # Print the tree
    console.print("\n[bold]Pipeline Hierarchy:[/bold]")
    console.print(tree)

@app.command()
def deploy(table_name:  Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json.")],
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")],
        compute_pool_id: Annotated[str, typer.Option(envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will create a pool.")]):
    """
    Deploy a pipeline from a given folder
    """
    print(f"#### Deploy pipeline from table {table_name} in {compute_pool_id}")
    try:
        deploy_pipeline_from_table(table_name, inventory_path, compute_pool_id)

    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    print(f"#### Pipeline deployed from {table_name}")


#@app.command()
# Not yet implemented
def report_running_dmls(table_name:  Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json.")],
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")]):
    """
    Assess for a given table, what are the running dmls from its children, using recursively. 
    """
    print(f"Assess runnning Flink DML part of the pipeline from given table name")
    try:
        report=report_running_dmls(table_name, inventory_path)
        print(report)
    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    print(f"Running DML deployed from {table_name}")


def _display_directed_graph(nodes, edges):
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

def _process_children(nodes_directed, edges_directed, current_node):
    if current_node.children:
        for child in current_node.children:
            child_ref = ReportInfoNode.model_validate(child)
            nodes_directed.append(child_ref.table_name)
            edges_directed.append((current_node.table_name, child_ref.table_name))
            _process_children(nodes_directed, edges_directed, child_ref)

def _process_hierarchy_to_node(pipeline_def):
    nodes_directed = [pipeline_def.table_name]
    edges_directed = []
    _process_children(nodes_directed, edges_directed, pipeline_def)
    _display_directed_graph(nodes_directed, edges_directed)