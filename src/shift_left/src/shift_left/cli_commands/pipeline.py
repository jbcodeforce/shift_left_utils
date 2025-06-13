"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import networkx as nx
import matplotlib.pyplot as plt
from pydantic_yaml import to_yaml_str
import typer
import datetime
from rich import print
from rich.tree import Tree
from rich.console import Console
from typing_extensions import Annotated
import shift_left.core.utils.app_config as app_config
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.pipeline_mgr as pipeline_mgr


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
    Build a pipeline definition metadata by reading the Flink dml SQL content for the given dml file.
    """
    if not dml_file_name.endswith(".sql"):
        print(f"[red]Error: the first parameter needs to be a dml sql file[/red]")
        raise typer.Exit(1)

    print(f"Build pipeline from table {dml_file_name}")
    if pipeline_path and pipeline_path != os.getenv("PIPELINES"):
        print(f"Using pipeline path {pipeline_path}")
        os.environ["PIPELINES"] = pipeline_path
    ddl_file_name = dml_file_name.replace("dml.", "ddl.")
    pipeline_mgr.build_pipeline_definition_from_ddl_dml_content(dml_file_name, ddl_file_name, pipeline_path)
    print(f"Pipeline built from {dml_file_name}")

@app.command()
def delete_all_metadata(path_from_where_to_delete:  Annotated[str, typer.Argument(help="Delete metadata pipeline_definitions.json in the given folder")]):
    """
    Delete all pipeline definition json files from a given folder path
    """
    print(f"Delete pipeline definition from {path_from_where_to_delete}")
    pipeline_mgr.delete_all_metada_files(path_from_where_to_delete)


@app.command()
def build_all_metadata(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")]):
    """
    Go to the hierarchy of folders for dimensions, views and facts and build the pipeline definitions for each table found using recursing walk through
    """
    print(f"Build all pipeline definitions for all tables in {pipeline_path}")
    pipeline_mgr.build_all_pipeline_definitions(pipeline_path)
    print("Done")
    

@app.command()
def report(table_name: Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json. e.g. src_aqem_tag_tag. The name has to exist in inventory as a key.")],
          inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")],
          to_yaml: bool = typer.Option(False, "--yaml", help="Output the report in YAML format"),
          to_json: bool = typer.Option(False, "--json", help="Output the report in JSON format"),
        to_graph: bool = typer.Option(False, "--graph", help="Output the report in Graphical tree"),
        children_too: bool = typer.Option(False, help="By default the report includes only parents, this flag focuses on getting children"),
        parent_only: bool = typer.Option(True, help="By default the report includes only parents"),
        output_file_name: str= typer.Option(None, help="Output file name to save the report."),):
    """
    Generate a report showing the static pipeline hierarchy for a given table using its pipeline_definition.json
    """
    console = Console()
    print(f"Generating pipeline report for table {table_name}")
    try:
        parent_only = not children_too
        pipeline_def=pipeline_mgr.get_static_pipeline_report_from_table(table_name, inventory_path, parent_only, children_too)
        if pipeline_def is None:
            print(f"[red]Error: pipeline definition not found for table {table_name}[/red]")
            raise typer.Exit(1)
    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)

    if to_yaml:
        print("[bold]YAML:[/bold]")
        result=to_yaml_str(pipeline_def)
        print(result)
    elif to_json:
        print("[bold]JSON:[/bold]")
        result=pipeline_def.model_dump_json(indent=3)
        print(result)
    else:
        result=pipeline_def
    if output_file_name:
        with open(output_file_name,"w") as f:
            f.write(result)
    if to_graph:
        print("[bold]Graph:[/bold]")
        _process_hierarchy_to_node(pipeline_def)
        
    # Create a rich tree for visualization
    tree = Tree(f"[bold blue]{table_name}[/bold blue]")
    
    def add_nodes_to_tree(node_data, tree_node, summary: str):
        # Process parents recursively
        if parent_only and node_data.parents:
            for parent in node_data.parents:
                parent_node = tree_node.add(f"[green]{parent.table_name}[/green]")
                summary+=f"{parent.table_name}\n"
                # Add file references
                if parent.dml_ref:
                    parent_node.add(f"[dim]DML: {parent.dml_ref}[/dim]")
                if parent.ddl_ref:
                    parent_node.add(f"[dim]DDL: {parent.ddl_ref}[/dim]")
                parent_node.add(f"[dim]Path: {parent.path}[/dim]")
                parent_node.add(f"[dim]Type: {parent.type}[/dim]")
                # Recursively process parents of parents
                summary=add_nodes_to_tree(parent, parent_node, summary)
        
        # Process children recursively
        if children_too and node_data.children:
            for child in node_data.children:
                child_node = tree_node.add(f"[green]{child.table_name}[/green]")
                summary+=f"\t{child.table_name}\n"
                # Add file references
                if child.dml_ref:
                    child_node.add(f"[dim]DML: {child.dml_ref}[/dim]")
                if child.ddl_ref:
                    child_node.add(f"[dim]DDL: {child.ddl_ref}[/dim]")
                child_node.add(f"[dim]Base: {child.path}[/dim]")
                child_node.add(f"[dim]Type: {child.type}[/dim]")
                # Recursively process children of children
                summary=add_nodes_to_tree(child, child_node, summary)
        return summary
    
    # Add the root node details
    tree.add(f"[dim]DML: {pipeline_def.dml_ref}[/dim]")
    tree.add(f"[dim]DDL: {pipeline_def.ddl_ref}[/dim]")
    
    # Add parent nodes
    summary = ""
    summary = add_nodes_to_tree(pipeline_def, tree, summary)
    
    # Print the tree
    console.print(f"\n[bold]Pipeline with {'children' if children_too else 'parents'} hierarchy:[/bold]")
    console.print(tree)
    console.print(f"\n[bold]table names only:[/bold]")
    console.print(summary)

@app.command()
def deploy(
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")],
        table_name:  str =  typer.Option(default= None, help="The table name containing pipeline_definition.json."),
        product_name: str =  typer.Option(None, help="The product name to deploy."),
        compute_pool_id: str= typer.Option(None, help="Flink compute pool ID. If not provided, it will create a pool."),
        dml_only: bool = typer.Option(False, help="By default the deployment will do DDL and DML, with this flag it will deploy only DML"),
        may_start_descendants: bool = typer.Option(False, help="The children deletion will be done only if they are stateful. This Flag force to drop table and recreate all (ddl, dml)"),
        force_ancestors: bool = typer.Option(False, help="When reaching table with no ancestor, this flag forces restarting running Flink statements."),
        cross_product_deployment: bool = typer.Option(False, help="By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products."),
        dir: str = typer.Option(None, help="The directory to deploy the pipeline from. If not provided, it will deploy the pipeline from the table name."),
        sequential: bool = typer.Option(False, help="By default the deployment will deploy the pipeline in parallel. This flag will deploy the pipeline in sequential.")
        ):
    """
    Deploy a pipeline from a given table name , product name or a directory.
    """
    _build_deploy_pipeline( 
        table_name=table_name, 
        product_name=product_name, 
        dir=dir, 
        inventory_path=inventory_path, 
        compute_pool_id=compute_pool_id, 
        dml_only=dml_only, 
        may_start_descendants=may_start_descendants, 
        force_ancestors=force_ancestors,
        cross_product_deployment=cross_product_deployment,
        sequential=sequential,
        execute_plan=True)
    
    print(f"#### Pipeline deployed ####")

@app.command()
def build_execution_plan(
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")],
        table_name:  str =  typer.Option(default= None, help="The table name to deploy from. Can deploy ancestors and descendants." ),
        product_name: str =  typer.Option(None, help="The product name to deploy from. Can deploy ancestors and descendants of the tables part of the product."),
        dir: str = typer.Option(None, help="The directory to deploy the pipeline from."),
        compute_pool_id: str= typer.Option(None, help="Flink compute pool ID to use as default."),
        dml_only: bool = typer.Option(False, help="By default the deployment will do DDL and DML, with this flag it will deploy only DML"),
        may_start_descendants: bool = typer.Option(False, help="The descendants will not be started by default. They may be started differently according to the fact they are stateful or stateless."),
        force_ancestors: bool = typer.Option(False, help="This flag forces restarting running ancestorsFlink statements."),
        cross_product_deployment: bool = typer.Option(False, help="By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products.")):
    """
    From a given table, this command goes all the way to the full pipeline and assess the execution plan taking into account parent, children
    and existing Flink Statement running status.
    """
    _build_deploy_pipeline( 
        table_name=table_name, 
        product_name=product_name, 
        dir=dir, 
        inventory_path=inventory_path, 
        compute_pool_id=compute_pool_id, 
        dml_only=dml_only, 
        may_start_descendants=may_start_descendants, 
        force_ancestors=force_ancestors,
        cross_product_deployment=cross_product_deployment,
        execute_plan=False)

@app.command()
def report_running_statements(
        dir: str = typer.Option(None, help="The directory to report the running statements from. If not provided, it will report the running statements from the table name."),
        table_name: str =  typer.Option(None, help="The table name containing pipeline_definition.json to get child list"),
        product_name: str =  typer.Option(None, help="The product name to report the running statements from."),
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")]= os.getenv("PIPELINES"),
        from_date: str = typer.Option(None, help="The date from which to report the metrics from. Format: YYYY-MM-DDThh:mm:ss")
):
    """
    Assess for a given table, what are the running dmls from its descendants. When the directory is specified, it will report the running statements from all the tables in the directory.
    """
    print(f"Assess running Flink DMLs")
    try:
        if table_name:
            results = "\n" + "#"*40 + f" Table: {table_name} " + "#"*40 + "\n"
            results+= deployment_mgr.report_running_flink_statements_for_a_table(table_name, 
                                                                                 inventory_path, 
                                                                                 from_date)
        elif dir:
            results= deployment_mgr.report_running_flink_statements_for_all_from_directory(dir, 
                                                                                           inventory_path,
                                                                                           from_date)
        elif product_name:
            results= deployment_mgr.report_running_flink_statements_for_a_product(product_name, 
                                                                                  inventory_path, 
                                                                                  from_date)
        else:
            print(f"[red]Error: either table_name or dir must be provided[/red]")
            raise typer.Exit(1)
        print(results)
    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    print(f"#"*120)

@app.command()
def undeploy(
        table_name:  str = typer.Option(default= None, help="The sink table name from where the undeploy will run."),
        product_name: str =  typer.Option(default= None, help="The product name to undeploy from"),
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")]= os.getenv("PIPELINES")):
    """
    From a given sink table, this command goes all the way to the full pipeline and delete tables and Flink statements not shared with other statements.
    """
   
    if table_name:
        print(f"#### Full undeployment of a pipeline from the table {table_name} for not shareable tables")
        result = deployment_mgr.full_pipeline_undeploy_from_table(table_name, inventory_path)
    elif product_name:
        print(f"#### Full undeployment of all tables for product: {product_name} except shareable tables")
        result = deployment_mgr.full_pipeline_undeploy_from_product(product_name, inventory_path)


@app.command()
def  prepare(sql_file_name: str = typer.Argument(help="The sql file to prepare tables from."),
            compute_pool_id: str= typer.Option(None, help="Flink compute pool ID to use as default."),
       ):
    """
    Execute the content of the sql file, line by line as separate Flink statement. It is used to alter table. for deployment by adding the necessary comments and metadata.
    """
    print(f"Prepare tables using sql file {sql_file_name}")
    deployment_mgr.prepare_tables_from_sql_file(sql_file_name, compute_pool_id)
    print(f"Content of {sql_file_name} executed")

# ----- Private APIs -----
    
def _build_deploy_pipeline(
        table_name: str, 
        product_name: str, 
        dir: str, 
        inventory_path: str, 
        compute_pool_id: str, 
        dml_only: bool, 
        may_start_descendants: bool, 
        force_ancestors: bool,
        cross_product_deployment: bool,
        sequential: bool = False,
        execute_plan: bool=False):
    summary="Nothing done"
    try:
        report=None
        if table_name:
            print(f"Build an execution plan for table {table_name}")
            summary,execution_plan=deployment_mgr.build_deploy_pipeline_from_table(table_name=table_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_descendants=may_start_descendants,
                                                        force_ancestors=force_ancestors,
                                                        cross_product_deployment=cross_product_deployment,
                                                        sequential=sequential,
                                                        execute_plan=execute_plan)
            print(f"Execution plan built and persisted for table {table_name}")
            print(f"Potential Impacted tables:\n" + "-"*30 )
            for node in execution_plan.nodes:
                if node.to_run or node.to_restart:
                    print(f"{node.table_name}")
            print(f"\n" + "-"*30 + "\n")

        elif product_name:
            print(f"Build an execution plan for product {product_name}")
            summary,report=deployment_mgr.build_deploy_pipelines_from_product(product_name=product_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_descendants=may_start_descendants,
                                                        force_ancestors=force_ancestors,
                                                        cross_product_deployment=cross_product_deployment,  
                                                        sequential=sequential,
                                                        execute_plan=execute_plan)
            print(f"Execution plan built and persisted for product {product_name}")

        elif dir:
            print(f"Build an execution plan for directory {dir}")
            summary, report=deployment_mgr.build_and_deploy_all_from_directory(directory=dir, 
                                                                    inventory_path=inventory_path,
                                                                    compute_pool_id=compute_pool_id,
                                                                    dml_only=dml_only,
                                                                    may_start_descendants=may_start_descendants,
                                                                    force_ancestors=force_ancestors,
                                                                    cross_product_deployment=cross_product_deployment,
                                                                    sequential=sequential,
                                                                    execute_plan=execute_plan)
        if not execute_plan:
            print(summary)
        if report:
            print(f"Table_name | Status | Pending Records | Num Records Out")
            for table_info in report.tables:
                print(f"{table_info.table_name} | {table_info.status} | {table_info.pending_records} | {table_info.num_records_out}")
        print("Done.")
        
    except Exception as e:
        print(f"[red]Error: {e}[/red]")
        raise typer.Exit(1)
    
   



def _display_directed_graph(nodes, edges):
    """
    Displays a directed graph using networkx and matplotlib.

    Args:
        nodes: A list of node identifiers.
        edges: A list of tuples, where each tuple represents a directed edge
               (source_node, target_node).
    """
    G = nx.DiGraph()  # Create a directed graph object
    G.add_nodes_from(nodes)
    G.add_edges_from(edges)

    # Draw the graph
    pos = nx.spring_layout(G)  # Layout algorithm
    nx.draw(G, pos, with_labels=True,
            node_size=600, node_color="lightblue", font_size=10, 
            font_weight="bold", 
            arrowsize=20)
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