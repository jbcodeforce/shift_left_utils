import typer
from rich import print
from rich.tree import Tree
from rich.console import Console
from shift_left.core.pipeline_mgr import (
    build_pipeline_definition_from_table, 
    walk_the_hierarchy_for_report_from_table, 
    build_all_pipeline_definitions,
    delete_metada_files)
from typing_extensions import Annotated
from shift_left.core.deployment_mgr import deploy_pipeline_from_table

import yaml
import os
import sys
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
"""
Manage a pipeline entity:
- build the inventory of tables
- build the inventory of tables
- generate pipeline reports
"""
app = typer.Typer(no_args_is_help=True)



@app.command()
def build_metadata(dml_file_name:  Annotated[str, typer.Argument()], 
             pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"])]):
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
    Delete a pipeline definitions from a given folder
    """
    print(f"Delete pipeline definition from {path_from_where_to_delete}")
    delete_metada_files(path_from_where_to_delete)


@app.command()
def build_all_metadata(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"])]):
    """
    Go to the hierarchy of folders for dimensions and facts and build the pipeline definitions for each table found using recurring walk through
    """
    print("Build all pipeline definitions for dimension and fact tables")
    build_all_pipeline_definitions(pipeline_path)
    print("Done")
    

@app.command()
def report(table_name: Annotated[str, typer.Argument(help="The table name (folder name) containing pipeline_definition.json")],
          inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder")],
          to_yaml: bool = typer.Option(False, "--yaml", help="Output the report in YAML format"),
          to_json: bool = typer.Option(False, "--json", help="Output the report in JSON format")):
    """
    Generate a report showing the pipeline hierarchy for a given table using its pipeline_definition.json
    """
    console = Console()
    print(f"Generating pipeline report for table {table_name}")
    try:
        pipeline_def=walk_the_hierarchy_for_report_from_table(table_name,inventory_path )
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
def deploy(table_name:  Annotated[str, typer.Argument()],
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder")],
        compute_pool_id: Annotated[str, typer.Option(envvar=["CPOOL_ID"], help="Flink compute pool ID")]):
    """
    Deploy a pipeline from a given folder
    """
    print(f"Deploy pipeline from folder")
    try:
        deploy_pipeline_from_table(table_name, inventory_path, compute_pool_id)

    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    print(f"Pipeline deployed from {table_name}")