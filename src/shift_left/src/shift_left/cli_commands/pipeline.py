import typer
import json
from rich import print
from rich.tree import Tree
from rich.console import Console
from shift_left.core.pipeline_mgr import (
    build_pipeline_definition_from_table, 
    build_pipeline_report_from_table, 
    build_all_pipeline_definitions,
    delete_all_metada_files)
from typing_extensions import Annotated
from shift_left.core.deployment_mgr import deploy_pipeline_from_table, DeploymentReport, full_pipeline_undeploy_from_table, report_running_flink_statements

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
    Build a pipeline by reading the Flink dml SQL content: add or update each table in the pipeline.
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
def delete_all_metadata(path_from_where_to_delete:  Annotated[str, typer.Argument(help="Delete metadata pipeline_definitions.json in the given folder")]):
    """
    Delete all pipeline definition json files from a given folder path
    """
    print(f"Delete pipeline definition from {path_from_where_to_delete}")
    delete_all_metada_files(path_from_where_to_delete)


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
        pipeline_def=build_pipeline_report_from_table(table_name, inventory_path)
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
                parent_node = tree_node.add(f"[green]{parent.table_name}[/green]")
                # Add file references
                if parent.dml_ref:
                    parent_node.add(f"[dim]DML: {parent.dml_ref}[/dim]")
                if parent.ddl_ref:
                    parent_node.add(f"[dim]DDL: {parent.ddl_ref}[/dim]")
                parent_node.add(f"[dim]Path: {parent.path}[/dim]")
                parent_node.add(f"[dim]Type: parent[/dim]")
        if node_data.children:
            for child in node_data.children:
                child_node = tree_node.add(f"[green]{child.table_name}[/green]")
                # Add file references
                if child.dml_ref:
                    child_node.add(f"[dim]DML: {child.dml_ref}[/dim]")
                if child.ddl_ref:
                    child_node.add(f"[dim]DDL: {child.ddl_ref}[/dim]")
                child_node.add(f"[dim]Base: {child.path}[/dim]")
                child_node.add(f"[dim]Type: child[/dim]")
    
    # Add the root node details
    tree.add(f"[dim]DML: {pipeline_def.dml_ref}[/dim]")
    tree.add(f"[dim]DDL: {pipeline_def.ddl_ref}[/dim]")
    
    # Add parent nodes
    add_nodes_to_tree(pipeline_def, tree)
    
    # Print the tree
    console.print("\n[bold]Pipeline Hierarchy:[/bold]")
    console.print(tree)

@app.command()
def deploy(table_name:  Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json.")],
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")],
        compute_pool_id: str= typer.Option(None, help="Flink compute pool ID. If not provided, it will create a pool."),
        dml_only: bool = typer.Option(False, help="By default the deployment will do DDL and DML, with this flag it will deploy only DML"),
        force: bool = typer.Option(False, help="The children deletion will be done only if they are stateful. This Flag force to drop table and recreate all (ddl, dml)"),
        ):
    """
    Deploy a pipeline from a given table name, an inventory path and the compute pool id to use. If not pool is given, it uses the config.yaml content.
    """
    print(f"#### Deploy pipeline from table {table_name} in {compute_pool_id}")
    try:
        result: DeploymentReport = deploy_pipeline_from_table(table_name, inventory_path, compute_pool_id, dml_only, force)

    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    print(f"#### Pipeline deployed from {result}")


@app.command()
def report_running_dmls(table_name:  Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json to get child list")],
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")]):
    """
    Assess for a given table, what are the running dmls from its children, using recursively. 
    """
    print(f"Assess runnning Flink DML part of the pipeline from given table name")
    try:
        report=report_running_flink_statements(table_name, inventory_path)
        print(json.dumps(report, indent=3))
    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    print(f"Running DMLs done")

@app.command()
def undeploy(table_name:  Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json.")],
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")]):
    """
    From a given table, when it is a sink it goes all the way to the full pipeline and delete tables and Flink statements not shared
    """
    print(f"#### Full Delete of a pipeline from table {table_name} for not shareable tables")
    result = full_pipeline_undeploy_from_table(table_name, inventory_path)
    print(result)

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
            nodes_directed.append(child.table_name)
            edges_directed.append((current_node.table_name, child.table_name))
            _process_children(nodes_directed, edges_directed, child)

def _process_parents(nodes_directed, edges_directed, current_node):
    if current_node.parents:
        for parent in current_node.parents:
            nodes_directed.append(parent.table_name)
            edges_directed.append((parent.table_name, current_node.table_name))
            _process_parents(nodes_directed, edges_directed, parent)


def _process_hierarchy_to_node(pipeline_def):
    nodes_directed = [pipeline_def.table_name]
    edges_directed = []
    if len(pipeline_def.children) > 0:
        _process_children(nodes_directed, edges_directed, pipeline_def)
    if len(pipeline_def.parents) > 0:
        _process_parents(nodes_directed, edges_directed, pipeline_def)
    _display_directed_graph(nodes_directed, edges_directed)