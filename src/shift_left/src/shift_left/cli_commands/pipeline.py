"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import time
from datetime import datetime, timezone
from typing import List
import json
import networkx as nx
from pydantic_yaml import to_yaml_str
import typer
import builtins
from rich import print
from rich.tree import Tree
from rich.console import Console
from typing_extensions import Annotated
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.file_search import get_or_build_inventory
from shift_left.core.utils.error_sanitizer import safe_error_display
from shift_left.core.utils.secure_typer import create_secure_typer_app
import shift_left.core.deployment_mgr as deployment_mgr
import shift_left.core.pipeline_mgr as pipeline_mgr
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
import shift_left.core.integration_test_mgr as integration_mgr
from shift_left.core.compute_pool_usage_analyzer import ComputePoolUsageAnalyzer
from shift_left.core.utils.app_config import logger


"""
Manage a pipeline entity:
- build the pipeline definition from a dml file
- delete all pipeline definition json files from a given folder path
- build the pipeline definition for all tables in a given folder path
- generate a pipeline report for a given table
- deploy a pipeline from a given table name, product name or a directory
- build an execution plan for a given table, product name or a directory
- report the running statements for a given table, product name or a directory
- undeploy a pipeline from a given table name, product name or a directory
"""
app = create_secure_typer_app(no_args_is_help=True, pretty_exceptions_show_locals=False)



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
def delete_all_metadata(path_from_where_to_delete:  Annotated[str, typer.Argument(envvar=["PIPELINES"],help="Delete metadata pipeline_definitions.json in the given folder")]):
    """
    Delete all pipeline definition json files from a given folder path
    """
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Delete pipeline definitions from {path_from_where_to_delete}")
    pipeline_mgr.delete_all_metada_files(path_from_where_to_delete)
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Done")


@app.command()
def build_all_metadata(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")]):
    """
    Go to the hierarchy of folders for dimensions, views and facts and build the pipeline definitions for each table found using recursing walk through
    """
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Build all pipeline definitions for all tables in {pipeline_path}")
    pipeline_mgr.build_all_pipeline_definitions(pipeline_path)
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Done")


@app.command()
def report(table_name: Annotated[str, typer.Argument(help="The table name containing pipeline_definition.json. e.g. src_aqem_tag_tag. The name has to exist in inventory as a key.")],
          inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")],
          to_yaml: bool = typer.Option(False, "--yaml", help="Output the report in YAML format"),
          to_json: bool = typer.Option(False, "--json", help="Output the report in JSON format"),
        children_too: bool = typer.Option(False, help="By default the report includes only parents, this flag focuses on getting children"),
        parent_only: bool = typer.Option(True, help="By default the report includes only parents"),
        output_file_name: str= typer.Option(None, help="Output file name to save the report."),):
    """
    Generate a report showing the static pipeline hierarchy for a given table using its pipeline_definition.json
    """
    console = Console()
    table_name = table_name.lower()
    print(f"Generating pipeline report for table {table_name}")
    try:
        parent_only = not children_too
        pipeline_def=pipeline_mgr.get_static_pipeline_report_from_table(table_name, inventory_path, parent_only, children_too)
        if pipeline_def is None:
            print(f"[red]Error: pipeline definition not found for table {table_name}[/red]")
            raise typer.Exit(1)
    except Exception as e:
        sanitized_error = safe_error_display(e)
        print(f"[red]Error: {sanitized_error}[/red]")
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
def healthcheck(product_name: Annotated[str, typer.Argument(help="The product name. e.g. qx, aqem, mx. The name has to exist in inventory as a key.")],
          inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")],):
    """
    Generate a healthcheck report of a given product pipeline
    """
    product_name = product_name.lower()
    console = Console()
    print(f"Generating pipeline healthcheck for product {product_name}")
    try:
        # get all the views, facts, dimension tables for a given product and build the pipeline hierarchy
        # This approach covers all the intermediates & sources and cross product tables. .
        inventory = get_or_build_inventory(inventory_path, inventory_path, False)
        sink_tables = {
            k: v
            for k, v in inventory.items()
            if v.get('product_name') == product_name and v.get('type') in ('view','dimension','fact')
        }
        logger.info(f"Inventory for product {product_name}: {json.dumps(sink_tables, indent=2)}")

        # get all statements created time & status
        statement_list={}
        statement_list = statement_mgr.get_statement_list().copy()
        statement_info = {}
        for statement_name in statement_list:
            statement = statement_list[statement_name]
            statement_info[statement.name] = {
                'created_at': statement.created_at,
                'status_phase': statement.status_phase,
                'compute_pool_id': statement.compute_pool_id,
                'compute_pool_name': statement.compute_pool_name    #-- TODO: why is this None for all statements?
            }
        logger.info(f"Statement Info: {statement_info}")

        # get all compute pools CFU information
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()


        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Generating Health Report for {product_name.upper()} ( ETA <1min ) ...")
        health_report = {}
        pipeline_status = pipeline_mgr.PipelineStatusTree(statement_info, inventory_path)

        for table_name, _ in sink_tables.items():
            logger.info(f"Table: {table_name}")
            #This tree gives all parents including the parents that are reapeated for multiple children.
            pipeline_report=pipeline_mgr.get_static_pipeline_report_from_table(table_name, inventory_path)
            #This tree gives all parents of a table without the repeated ones
            #pipeline_def = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
            if pipeline_report is None:
                print(f"[red]Error: pipeline definition not found for table {table_name}[/red]")
                raise typer.Exit(1)
            else:
                # Create a rich tree for visualization for debugging purposes
                tree = Tree(f"[bold blue]{table_name}[/bold blue]")
                # Accumulates the pipeline status for each table called.
                health_report = pipeline_status.pipeline_status( table_name, pipeline_report, tree)

        logger.info(f"Tree: {tree}")
        logger.info(f"Health Report: {json.dumps(health_report, indent=2)}")

        header = f"{product_name.upper()} Health Report"
        header_width = 85
        console.print(f"\n[bold]{header.center(header_width, '-')}[/bold]")
        #---- Statement Health Report ----
        console.print(f"[bold]{'Statements'.center(header_width, '-')}[/bold]")
        console.print(f"[bold]Product    | RUNNING COMPLETED PENDING DEGRADED STOPPED FAILED | UNKNOWN OUT-OF-ORDER[/bold]")
        console.print(f"[bold]{'-'*header_width}[/bold]")
        t_running = t_completed = t_pending = t_degraded = t_stopped = t_failed = t_unknown = t_out_of_order = 0
        for product_name in health_report:
            running = completed = pending = degraded = stopped = failed = unknown = out_of_order = 0
            if "RUNNING" in health_report[product_name]:
                t_running += (running := len(health_report[product_name]["RUNNING"]))
            if "COMPLETED" in health_report[product_name]:
                t_completed += (completed := len(health_report[product_name]["COMPLETED"]))
            if "PENDING" in health_report[product_name]:
                t_pending += (pending := len(health_report[product_name]["PENDING"]))
            if "DEGRADED" in health_report[product_name]:
                t_degraded += (degraded := len(health_report[product_name]["DEGRADED"]))
            if "STOPPED" in health_report[product_name]:
                t_stopped += (stopped := len(health_report[product_name]["STOPPED"]))
            if "FAILED" in health_report[product_name]:
                t_failed += (failed := len(health_report[product_name]["FAILED"]))
            if "UNKNOWN" in health_report[product_name]:
                t_unknown += (unknown := len(health_report[product_name]["UNKNOWN"]))
            if "OUT_OF_ORDER" in health_report[product_name]:
                t_out_of_order += (out_of_order := len(health_report[product_name]["OUT_OF_ORDER"]))
            total = running + pending + degraded + stopped + failed + unknown
            console.print(f"{product_name:<10} | {running:>7} {completed:>9} {pending:>7} {degraded:>8} {stopped:>7} {failed:>6} | {unknown:>7} {out_of_order:>12}")
        console.print(f"[bold]{'-'*header_width}[/bold]")
        console.print(f"{'Total':<10} | {t_running:>7} {t_completed:>9} {t_pending:>7} {t_degraded:>8} {t_stopped:>7} {t_failed:>6} | {t_unknown:>7} {t_out_of_order:>12}")
        #---- statements out-of-order report ----
        if t_out_of_order > 0:
            console.print(f"\n[bold]{'Statements Out-Of-Order'.center(header_width, '-')}[/bold]")
            for product_name in health_report:
                if "OUT_OF_ORDER" in health_report[product_name]:
                    for p_c_dml_info in health_report[product_name]["OUT_OF_ORDER"].values():
                        p_dml_n, p_created_at, c_dml_n, c_created_at = p_c_dml_info.split(',')
                        console.print(f"[bold]{p_dml_n:<65} created at {p_created_at:>20}[/bold]")
                        console.print(f"[bold]└─{c_dml_n:<63} created at {c_created_at:>20}[/bold]")
        #---- statements not-running report ----
        if t_unknown > 0:
            console.print(f"\n[bold]{'Statements Not RUNNING'.center(header_width, '-')}[/bold]")
            for product_name in health_report:
                if "UNKNOWN" in health_report[product_name]:
                    for dml_name in health_report[product_name]["UNKNOWN"].keys():
                        console.print(f"[bold]{dml_name}[/bold]")

        #---- Compute Poool Health Report ----
        # Get all compute pools CFU information in dictionary format
        pool_info={}
        for pool in compute_pool_list.pools:
            if pool.id not in pool_info:
                pool_info[pool.id]={}
            pool_info[pool.id]["name"] = pool.name
            pool_info[pool.id]["current_cfu"] = pool.current_cfu
            pool_info[pool.id]["max_cfu"] = pool.max_cfu

        console.print(f"\n[bold]{'Compute Pools'.center(header_width, '-')}[/bold]")
        console.print(f"[bold]Product    | #Pools | #CFUs | #Pools>1CFU | Max Pool ( CFU )[/bold]")
        console.print(f"[bold]{'-'*header_width}[/bold]")
        t_cfus = t_pools = t_plus1pools = t_max_cfu = 0
        t_max_cfu_pool = None
        for product_name in health_report:
            num_pools = num_cfus = plus1pools = max_cfu = 0
            max_cfu_pool = None
            if "POOLS" in health_report[product_name]:
                unique_pool_ids = list(set(health_report[product_name]["POOLS"]))
                num_pools = len(unique_pool_ids)
                t_pools += num_pools
                for pool_id in unique_pool_ids:
                    if pool_id in pool_info:
                        num_cfus += pool_info[pool_id]["current_cfu"]
                        if pool_info[pool_id]["current_cfu"] > 1:
                            plus1pools += 1
                        if pool_info[pool_id]["current_cfu"] > max_cfu:
                            max_cfu = pool_info[pool_id]["current_cfu"]
                            max_cfu_pool = pool_id
                t_cfus += num_cfus
                t_plus1pools += plus1pools
                if max_cfu > t_max_cfu:
                    t_max_cfu = max_cfu
                    t_max_cfu_pool = max_cfu_pool
                console.print(f"{product_name:<10} | {num_pools:>6}   {num_cfus:>5}   {plus1pools:>11}   {f'{max_cfu_pool} ({max_cfu})':<15}")
        console.print(f"[bold]{'-'*header_width}[/bold]")
        console.print(f"{'Total':<10} | {t_pools:>6}   {t_cfus:>5}   {t_plus1pools:>11}   {f'{t_max_cfu_pool} ({t_max_cfu})':<15}\n")

    except Exception as e:
        sanitized_error = safe_error_display(e)
        print(f"[red]Error: {sanitized_error}[/red]")
        raise typer.Exit(1)

@app.command()
def deploy(
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")],
        table_name:  str =  typer.Option(default= None, help="The table name containing pipeline_definition.json."),
        product_name: str =  typer.Option(None, help="The product name to deploy."),
        table_list_file_name: str = typer.Option(None, help="The file containing the list of tables to deploy."),
        exclude_table_file_name: str = typer.Option(None, help="The file containing the list of tables to exclude from the deployment."),
        compute_pool_id: str= typer.Option(None, help="Flink compute pool ID. If not provided, it will create a pool."),
        dml_only: bool = typer.Option(False, help="By default the deployment will do DDL and DML, with this flag it will deploy only DML"),
        may_start_descendants: bool = typer.Option(False, help="The children deletion will be done only if they are stateful. This Flag force to drop table and recreate all (ddl, dml)"),
        force_ancestors: bool = typer.Option(False, help="When reaching table with no ancestor, this flag forces restarting running Flink statements."),
        cross_product_deployment: bool = typer.Option(False, help="By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products."),
        dir: str = typer.Option(None, help="The directory to deploy the pipeline from. If not provided, it will deploy the pipeline from the table name."),
        parallel: bool = typer.Option(False, help="By default the deployment will deploy the pipeline in parallel. This flag will deploy the pipeline in parallel."),
        max_thread: int = typer.Option(1, help="The maximum number of threads to use when deploying the pipeline in parallel."),
        pool_creation: bool = typer.Option(False, help="By default the deployment will not create a compute pool per table. This flag will create a pool."),
        version: str = typer.Option(None, help="The default version to use, at the table level for modified flink statements. But when set the tool will increasing existing version in SQL. Example of string is '_v2'. Only used if table_list_file_name is provided.")
        ):
    """
    Deploy a pipeline from a given table name , product name or a directory taking into account the execution plan.
    It can run the deployment in parallel or sequential.
    Four approaches are possible:
    1. Deploy from a given table name
    2. Deploy from a product name
    3. Deploy from a directory
    4. Deploy from a table list file name
    """

    if max_thread > 1:
        parallel = True
    else:
        parallel = False

    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Deploying pipeline in Env [ {get_config()['kafka']['cluster_type']} ] Id [ {get_config()['confluent_cloud']['environment_id']} ]", flush=True)
    builtins.print(f"------------------------------------------ Command Parameters ---------------------------------------------")
    builtins.print(f"| may_start_descendants | force_ancestors | cross_product_deployment | pool_creation | Parallel | Threads |")

    if product_name:
        builtins.print(f"| {'False':>21} | {str(bool(force_ancestors)):>15} | {'False':>24} | {str(bool(pool_creation)):>13} | {str(bool(parallel)):>8} | {str(max_thread):>7} |")
    else:
        builtins.print(f"| {str(bool(may_start_descendants)):>21} | {str(bool(force_ancestors)):>15} | {str(bool(cross_product_deployment)):>24} | {str(bool(pool_creation)):>13} | {str(bool(parallel)):>8} | {str(max_thread):>7} |")
    builtins.print(f"----------------------------------------------------------------------------------------------------------")

    _build_deploy_pipeline(
        table_name=table_name,
        product_name=product_name,
        dir=dir,
        table_list_file_name=table_list_file_name,
        exclude_table_file_name=exclude_table_file_name,
        inventory_path=inventory_path,
        compute_pool_id=compute_pool_id,
        dml_only=dml_only,
        may_start_descendants=may_start_descendants,
        force_ancestors=force_ancestors,
        cross_product_deployment=cross_product_deployment,
        parallel=parallel,
        max_thread=max_thread,
        execute_plan=True,
        pool_creation=pool_creation,
        version=version)

    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Pipeline DEPLOYED")

@app.command()
def build_execution_plan(
        inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")],
        table_name:  str =  typer.Option(default= None, help="The table name to deploy from. Can deploy ancestors and descendants." ),
        product_name: str =  typer.Option(None, help="The product name to deploy from. Can deploy ancestors and descendants of the tables part of the product."),
        dir: str = typer.Option(None, help="The directory to deploy the pipeline from."),
        table_list_file_name: str = typer.Option(None, help="The file containing the list of tables to deploy."),
        exclude_table_file_name: str = typer.Option(None, help="The file containing the list of tables to exclude from the deployment."),
        compute_pool_id: str= typer.Option(None, help="Flink compute pool ID to use as default."),
        dml_only: bool = typer.Option(False, help="By default the deployment will do DDL and DML, with this flag it will deploy only DML"),
        may_start_descendants: bool = typer.Option(False, help="The descendants will not be started by default. They may be started differently according to the fact they are stateful or stateless."),
        force_ancestors: bool = typer.Option(False, help="This flag forces restarting running ancestorsFlink statements."),
        cross_product_deployment: bool = typer.Option(False, help="By default the deployment will deploy only tables from the same product. This flag allows to deploy tables from different products when considering descendants only.")):
    """
    From a given table, this command goes all the way to the full pipeline and assess the execution plan taking into account parent, children
    and existing Flink Statement running status. It does not deploy. This is a command for analysis.
    """
    if compute_pool_id:
        pool_creation = False
    else:
        pool_creation = True
    _build_deploy_pipeline(
        table_name=table_name,
        product_name=product_name,
        dir=dir,
        table_list_file_name=table_list_file_name,
        exclude_table_file_name=exclude_table_file_name,
        inventory_path=inventory_path,
        compute_pool_id=compute_pool_id,
        dml_only=dml_only,
        may_start_descendants=may_start_descendants,
        force_ancestors=force_ancestors,
        cross_product_deployment=cross_product_deployment,
        execute_plan=False,
        parallel=False,
        pool_creation=pool_creation)

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
            product_name = product_name.lower()
            results= deployment_mgr.report_running_flink_statements_for_a_product(product_name,
                                                                                  inventory_path,
                                                                                  from_date)
        else:
            print(f"[red]Error: either table-name, product-name or dir must be provided[/red]")
            raise typer.Exit(1)
        print(results)
    except Exception as e:
        sanitized_error = safe_error_display(e)
        print(f"[red]Error: {sanitized_error}[/red]")
        raise typer.Exit(1)
    print(f"#"*120)

@app.command()
def undeploy(
        table_name:  str = typer.Option(default= None, help="The sink table name from where the undeploy will run."),
        product_name: str =  typer.Option(default= None, help="The product name to undeploy from"),
        no_ack: bool = typer.Option(False, help="By default the undeploy will ask for confirmation. This flag will undeploy without confirmation."),
        cross_product: bool = typer.Option(False, help="By default the undeployment will process tables from the same product (valid with product-name). This flag allows to undeploy tables from different products."),
        compute_pool_id: str= typer.Option(None, help="Flink compute pool ID to use as default."),
		inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help="Path to the inventory folder, if not provided will use the $PIPELINES environment variable.")]= os.getenv("PIPELINES")):
    """
    From a given sink table, this command goes all the way to the full pipeline and delete tables and Flink statements not shared with other statements.
    """
    print(f"Undeploying pipeline in Env [ {get_config()['kafka']['cluster_type']} ] Id [ {get_config()['confluent_cloud']['environment_id']} ]")
    if not no_ack:
        print("Are you sure you want to undeploy? (y/n)")
        answer = input()
        if answer != "y":
            print("Undeploy cancelled")
            raise typer.Exit(1)
    if table_name:
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Full undeployment of a pipeline from the table {table_name} for not shareable tables")
        result = deployment_mgr.full_pipeline_undeploy_from_table(table_name, inventory_path)
    elif product_name:
        product_name = product_name.lower()
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Full undeployment of all tables for product: {product_name} except shareable tables")
        result = deployment_mgr.full_pipeline_undeploy_from_product(product_name, inventory_path, compute_pool_id, cross_product)
    else:
        print(f"[red]Error: either table-name or product-name must be provided[/red]")
        raise typer.Exit(1)
    print(result)


@app.command()
def  prepare(sql_file_name: str = typer.Argument(help="The sql file to prepare tables from."),
            compute_pool_id: str= typer.Option(None, help="Flink compute pool ID to use as default."),
       ):
    """
    Execute the content of the sql file, line by line as separate Flink statement. It is used to alter table.
    For deployment by adding the necessary comments and metadata.
    """
    print(f"Prepare tables using sql file {sql_file_name}")
    deployment_mgr.prepare_tables_from_sql_file(sql_file_name, compute_pool_id)
    print(f"Content of {sql_file_name} executed")


# ----- Private APIs -----

def _build_deploy_pipeline(
        table_name: str,
        product_name: str,
        dir: str,
        table_list_file_name: str,
        exclude_table_file_name: str,
        inventory_path: str,
        compute_pool_id: str,
        dml_only: bool = False,
        may_start_descendants: bool = False,
        force_ancestors: bool = False,
        cross_product_deployment: bool = False,
        parallel: bool = False,
        max_thread: int = 4,
        execute_plan: bool=False,
        pool_creation: bool = False,
        version: str = ""):
    summary="Nothing done"
    try:
        report=None
        if exclude_table_file_name:
            exclude_table_names = _load_table_names_from_file(exclude_table_file_name)
        else:
            exclude_table_names = []
        if table_list_file_name:
            table_names = _load_table_names_from_file(table_list_file_name)
        else:
            table_names = []
        if table_name:
            table_name = table_name.lower()
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Build an execution plan for table {table_name}")
            summary,report=deployment_mgr.build_deploy_pipeline_from_table(table_name=table_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_descendants=may_start_descendants,
                                                        force_ancestors=force_ancestors,
                                                        cross_product_deployment=cross_product_deployment,
                                                        sequential=not parallel,
                                                        max_thread=max_thread,
                                                        execute_plan=execute_plan,
                                                        exclude_table_names=exclude_table_names,
                                                        pool_creation=pool_creation)
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Execution plan built and persisted for table {table_name}")
            print(f"Potential Impacted tables:\n" + "-"*30 )
            for node in report.tables:
                if node.to_restart:
                    print(f"{node.table_name}")
            print(f"\n" + "-"*30 + "\n")

        elif product_name:
            product_name = product_name.lower()
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Build an execution plan for product {product_name}")
            summary,report=deployment_mgr.build_deploy_pipelines_from_product(product_name=product_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_descendants=False, # not used for product
                                                        force_ancestors=force_ancestors,
                                                        cross_product_deployment=False,  # not used for product
                                                        sequential=not parallel,
                                                        execute_plan=execute_plan,
                                                        max_thread=max_thread,
                                                        exclude_table_names=exclude_table_names,
                                                        pool_creation=pool_creation)
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Execution plan built and persisted for product {product_name}")

        elif dir:
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')}Build an execution plan for directory {dir}")
            summary, report=deployment_mgr.build_and_deploy_all_from_directory(directory=dir,
                                                                    inventory_path=inventory_path,
                                                                    compute_pool_id=compute_pool_id,
                                                                    dml_only=dml_only,
                                                                    may_start_descendants=may_start_descendants,
                                                                    force_ancestors=force_ancestors,
                                                                    cross_product_deployment=cross_product_deployment,
                                                                    sequential=not parallel,
                                                                    execute_plan=execute_plan,
                                                                    max_thread=max_thread,
                                                                    exclude_table_names=exclude_table_names,
                                                                    pool_creation=pool_creation)
        elif table_list_file_name:
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Build an execution plan for tables in {table_list_file_name}")
            summary, report=deployment_mgr.build_and_deploy_all_from_table_list(include_table_names=table_names,
                                                                    inventory_path=inventory_path,
                                                                    compute_pool_id=compute_pool_id,
                                                                    dml_only=dml_only,
                                                                    may_start_descendants=may_start_descendants,
                                                                    force_ancestors=force_ancestors,
                                                                    cross_product_deployment=cross_product_deployment,
                                                                    sequential=not parallel,
                                                                    execute_plan=execute_plan,
                                                                    exclude_table_names=exclude_table_names,
                                                                    max_thread=max_thread,
                                                                    pool_creation=pool_creation,
                                                                    version=version
                                                                )
        else:
            print(f"[red]Error: either table-name, product-name, dir or table-list-file-name must be provided[/red]")
            raise typer.Exit(1)
        if not execute_plan:
            print(summary)
        if report:
            builtins.print("-"*104)
            builtins.print(f"{'Table_name':55} | {'Status':10} | {'Pending Records':15} | {'Num Records Out':15}")
            builtins.print("-"*104)
            for table_info in report.tables:
                builtins.print(f"{table_info.table_name:<55} | {table_info.status:<10} | {table_info.pending_records:>15,} | {table_info.num_records_out:>15,}")
            builtins.print("-"*104)

    except Exception as e:
        sanitized_error = safe_error_display(e)
        print(f"[red]Error: {sanitized_error}[/red]")
        raise typer.Exit(1)


@app.command()
def analyze_pool_usage(
    inventory_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline path, if not provided will use the $PIPELINES environment variable.")] = None,
    product_name: Annotated[str, typer.Option("--product-name", "-p", help="Analyze pool usage for a specific product only")] = None,
    directory: Annotated[str, typer.Option("--directory", "-d", help="Analyze pool usage for pipelines in a specific directory")] = None
):
    """
    Analyze compute pool usage and assess statement consolidation opportunities.

    This command will:
    - Analyze current usage across compute pools (optionally filtered by product or directory)
    - Identify running statements in each pool
    - Assess opportunities for statement consolidation
    - Generate optimization recommendations using simple heuristics

    Examples:
        # Analyze all pools
        shift-left pipeline analyze-pool-usage

        # Analyze for specific product
        shift-left pipeline analyze-pool-usage --product-name saleops

        # Analyze for specific directory
        shift-left pipeline analyze-pool-usage --directory /path/to/facts/saleops

        # Combine product and directory filters
        shift-left pipeline analyze-pool-usage --product-name saleops --directory /path/to/facts
    """
    try:
        # Build analysis scope description
        scope_desc = "all pools"
        if product_name and directory:
            scope_desc = f"product '{product_name}' in directory '{directory}'"
        elif product_name:
            scope_desc = f"product '{product_name}'"
        elif directory:
            scope_desc = f"directory '{directory}'"

        print(f"[blue]Analyzing compute pool usage and statement consolidation opportunities for {scope_desc}...[/blue]")

        analyzer = ComputePoolUsageAnalyzer()
        report = analyzer.analyze_pool_usage(inventory_path, product_name, directory)

        # Print summary to console
        summary = analyzer.print_analysis_summary(report)
        print(summary)

        # Save detailed report to file
        from shift_left.core.utils.app_config import shift_left_dir
        import json

        report_file = f"{shift_left_dir}/pool_usage_analysis_{report.created_at.strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            f.write(report.model_dump_json(indent=2))

        print(f"[green]Detailed analysis report saved to: {report_file}[/green]")

        # Highlight key findings
        if report.recommendations:
            print(f"\n[yellow]🔍 Found {len(report.recommendations)} consolidation opportunities![/yellow]")
            total_potential_savings = sum(rec.estimated_cfu_savings for rec in report.recommendations)
            print(f"[yellow]💰 Potential CFU savings: {total_potential_savings}[/yellow]")
        else:
            print(f"\n[green]✅ Your compute pools appear to be efficiently utilized![/green]")

    except Exception as e:
        safe_error_display(f"Error analyzing compute pool usage", e)





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

    # TODO to replace with better graph visualization




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

# 09/09/2025 to adapt in the future for a better graph visualization
def _process_hierarchy_to_node(pipeline_def):
    nodes_directed = [pipeline_def.table_name]
    edges_directed = []
    if len(pipeline_def.children) > 0:
        _process_children(nodes_directed, edges_directed, pipeline_def)
    if len(pipeline_def.parents) > 0:
        _process_parents(nodes_directed, edges_directed, pipeline_def)
    _display_directed_graph(nodes_directed, edges_directed)

def _load_table_names_from_file(file_name: str) -> list[str]:
    if not os.path.exists(file_name):
        print(f"[red]Error: file {file_name} does not exist[/red]")
        raise typer.Exit(1)
    with open(file_name, 'r') as f:
        table_names = f.read().splitlines()
        return table_names
