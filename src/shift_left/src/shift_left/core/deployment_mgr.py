"""
Copyright 2024-2025 Confluent, Inc.

Manage deployment of pipelines. Support building execution plans
and then execute them.
The execution plan is a graph of Flink statements to be executed.
The graph is built from the pipeline definition and the existing deployed statements.
The execution plan is persisted to a JSON file.
The execution plan is used to execute the statements in the correct order.
The execution plan is used to undeploy a pipeline.
"""
import time
import os
import multiprocessing
import threading
from datetime import datetime
from collections import deque
from typing import List, Any, Set, Tuple, Dict, Final
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from shift_left.core import pipeline_mgr
from shift_left.core import compute_pool_mgr
from shift_left.core import statement_mgr
from shift_left.core.models.flink_statement_model import (
    ErrorData,
    Statement,
    StatementError,
    FlinkStatementNode,
    FlinkStatementExecutionPlan,
    StatementInfo
)

from shift_left.core.utils.report_mgr import (
    TableReport
)
import shift_left.core.utils.report_mgr as report_mgr
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.file_search import (
    PIPELINE_JSON_FILE_NAME,
    FlinkTableReference,
    FlinkTablePipelineDefinition,
    get_ddl_dml_names_from_pipe_def,
    read_pipeline_definition_from_file,
    get_or_build_inventory
)

# Constants
MAX_CFU_INCREMENT: Final[int] = 20


def build_deploy_pipeline_from_table(
    table_name: str,
    inventory_path: str,
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_descendants: bool = False,
    force_ancestors: bool = False,
    cross_product_deployment: bool = False,
    execute_plan: bool = False,
    sequential: bool = True,
    pool_creation: bool = True,
    exclude_table_names: List[str] = [],
    max_thread: int = 1
) -> Tuple[str, TableReport]:
    """
    Build an execution plan from the static relationship between Flink Statements.
    Deploy a pipeline starting from a given table.

    Args:
        table_name: Name of the table to deploy
        inventory_path: Path to the pipeline inventory
        compute_pool_id: ID of the compute pool to use
        dml_only: Whether to only deploy DML statements
        may_start_children: Whether to start child pipelines
        force_ancestors: Whether to force source table deployment

    Returns:
        Tuple containing the deployment report and summary

    Raises:
        ValueError: If the not able to process the execution plan
    """
    logger.info("#"*10 + f"# Build and/or deploy pipeline from table {table_name} " + "#"*10)
    start_time = time.perf_counter()
    #statement_mgr.reset_statement_list()
    try:

        pipeline_def = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
        start_node = pipeline_def.to_node()
        start_node.created_at = datetime.now()
        start_node.dml_only = dml_only
        start_node.compute_pool_id = compute_pool_id
        start_node = _assign_compute_pool_id_to_node(node=start_node, compute_pool_id=compute_pool_id, pool_creation=pool_creation)
        start_node.update_children = may_start_descendants
        start_node = _get_and_update_statement_info_compute_pool_id_for_node(start_node)
        start_node.to_restart = True
        # Build the static graph from the Flink statement relationship
        combined_node_map = {}
        visited_nodes = set()
        node_map = _build_statement_node_map(start_node, visited_nodes, combined_node_map)

        ancestors = []
        ancestors = _build_topological_sorted_graph([start_node], node_map)
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors= ancestors,
                                                                      node_map=node_map,
                                                                      force_ancestors=force_ancestors,
                                                                      may_start_descendants=may_start_descendants,
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id,
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=exclude_table_names,
                                                                      pool_creation=pool_creation)
        _persist_execution_plan(execution_plan)
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary=report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=False)
        logger.info(f"Execute the plan before deployment: {summary}")
        if execute_plan:
            statements = _execute_plan(execution_plan=execution_plan, compute_pool_id=compute_pool_id, accept_exceptions=False, sequential=sequential, max_thread=max_thread)
            result = report_mgr.build_deployment_report(table_name, pipeline_def.dml_ref, may_start_descendants, statements)

            result.execution_time = int(time.perf_counter() - start_time)
            result.start_time = start_time
            logger.info(
                f"Done in {result.execution_time} seconds to deploy pipeline from table {table_name}: "
                f"{result.model_dump_json(indent=3)}"
            )
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Done in {result.execution_time} seconds to deploy pipeline from table {table_name}")
            table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=False)
            #logger.info(f"Execute the plan after deployment: {simple_report}")
            summary+="\n"+f"#"*40 + f" Deployed {len(table_report.tables)} tables " + "#"*40 + "\n"
        return summary, table_report
    except Exception as e:
        logger.error(f"Failed to deploy pipeline from table {table_name} error is: {str(e)}")
        raise

def build_deploy_pipelines_from_product(
    product_name: str,
    inventory_path: str,
    compute_pool_id: str = "",
    dml_only: bool = False,
    may_start_descendants: bool = False,
    force_ancestors: bool = False,
    cross_product_deployment: bool = False,
    execute_plan: bool = False,
    sequential: bool = True,
    pool_creation: bool = True,
    exclude_table_names: List[str] = [],
    max_thread: int = 1
) -> Tuple[str, TableReport]:
    """Deploy the pipelines for a given product. Will process all the views, then facts then dimensions.
    As each statement deployment is creating an execution plan, previously started statements will not be restarted.
    """
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    start_time = time.perf_counter()
    if not compute_pool_id:
        compute_pool_id = get_config()['flink']['compute_pool_id']
    #statement_mgr.reset_statement_list()
    nodes_to_process = []
    combined_node_map = {}
    visited_nodes = set()  # Shared across all calls to avoid redundant processing
    count=0
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Building Node map")
    for table_ref_dict in table_inventory.values():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref.product_name == product_name:
            pipe_def = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
            if pipe_def:
                node = pipe_def.to_node()
                node.dml_only = dml_only
                nodes_to_process.append(node)
                # Build the static graph to keep accurate references from the Flink statement relationship
                # Pass shared visited_nodes and node_map to avoid reprocessing already analyzed nodes
                combined_node_map = _build_statement_node_map(node, visited_nodes, combined_node_map)
                count+=1
            else:
                logger.error(f"Data consistency issue for {table_ref.table_folder_name}: no pipeline definition found or wrong reference in {table_ref.table_name}. The execution plan may not deploy successfully")
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Finished Node map")
    if count > 0:
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Building topological sorted parents for {count} tables")
        ancestors = _build_topological_sorted_graph(nodes_to_process, combined_node_map)
        ancestors = _filtering_out_descendant_nodes(ancestors, product_name, may_start_descendants)
        start_node = ancestors[-1]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors,
                                                                      node_map=combined_node_map,
                                                                      force_ancestors=force_ancestors,
                                                                      may_start_descendants=may_start_descendants,
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id,
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=exclude_table_names,
                                                                      pool_creation=pool_creation)
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Start building topological sorted parents for {count} tables")
        if start_node.is_running() and not force_ancestors:
            start_node.to_restart = False
            start_node.to_run = False
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary = report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=False)
        if execute_plan:
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Executing plan summary: {summary}")
            start_time = time.perf_counter()
            _execute_plan(execution_plan=execution_plan, compute_pool_id=compute_pool_id, accept_exceptions=True, sequential=sequential, max_thread=max_thread)
            execution_time = int(time.perf_counter() - start_time)
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Execution time: {execution_time} seconds")
            summary+=f"\n{time.strftime('%Y%m%d_%H:%M:%S')} Execution time: {execution_time} seconds"
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Build table report now")
            table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=True)
        summary+="\n"+f"#"*40 + f" Deployed {count} tables " + "#"*40 + "\n"
        return summary, table_report
    else:
        return "Nothing run.", TableReport()

def build_and_deploy_all_from_directory(
    directory: str,
    inventory_path: str,
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_descendants: bool = False,
    force_ancestors: bool = False,
    cross_product_deployment: bool = False,
    execute_plan: bool = False,
    sequential: bool = True,
    pool_creation: bool = True,
    exclude_table_names: List[str] = [],
    max_thread: int = 1
) -> Tuple[str, TableReport]:
    """
    Deploy all the pipelines within a directory tree. The approach is
    to define a combined execution plan for all tables in the directory as it is important
    to start Flink statements only one time and in the correct order.
    """
    #statement_mgr.reset_statement_list()
    start_time = time.perf_counter()
    nodes_to_process = []
    combined_node_map = {}
    visited_nodes = set()  # Shared across all calls to avoid redundant processing
    for root, _, files in os.walk(directory):
        if PIPELINE_JSON_FILE_NAME in files:
            file_path=root + "/" + PIPELINE_JSON_FILE_NAME
            node = read_pipeline_definition_from_file(file_path).to_node()
            #node.to_restart = True
            nodes_to_process.append(node)
            # Build the static graph from the Flink statement relationship
            # Pass shared visited_nodes and node_map to avoid reprocessing already analyzed nodes
            combined_node_map = _build_statement_node_map(node, visited_nodes, combined_node_map)
    count = len(nodes_to_process)
    logger.info(f"Found {count} tables to process")
    if count > 0:
        ancestors = _build_topological_sorted_graph(nodes_to_process, combined_node_map)
        start_node = ancestors[-1]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors,
                                                                      node_map=combined_node_map,
                                                                      force_ancestors=force_ancestors,
                                                                      may_start_descendants=may_start_descendants,
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id,
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=exclude_table_names,
                                                                      pool_creation=pool_creation
                                                                      )
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary = report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=False)
        if execute_plan:
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Executing plan: {summary}")
            accept_exceptions= [True if "sources" in directory else False]
            _execute_plan(execution_plan=execution_plan, compute_pool_id=compute_pool_id, accept_exceptions=accept_exceptions, sequential=sequential, max_thread=max_thread)
            execution_time = int(time.perf_counter() - start_time)
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Execution time: {execution_time} seconds")
            summary+=f"\nExecution time: {execution_time} seconds"
            table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=True)
            summary+="\n"+f"#"*40 + f" Deployed {count} tables " + "#"*40 + "\n"
        return summary, table_report
    else:
        return "Nothing run. Do you have a pipeline_definition.json files", TableReport()


def build_and_deploy_all_from_table_list(
    include_table_names: List[str],
    inventory_path: str,
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_descendants: bool = False,
    force_ancestors: bool = False,
    cross_product_deployment: bool = False,
    execute_plan: bool = False,
    sequential: bool = True,
    pool_creation: bool = True,
    exclude_table_names: List[str] = [],
    max_thread: int = 1,
    version: str = ""
) -> Tuple[str, TableReport]:
    """
    Deploy all the pipelines in the table list file.
    """
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    start_time = time.perf_counter()
    nodes_to_process = []
    combined_node_map = {}
    count=0
    visited_nodes = set()
    print(f"Processing {len(include_table_names)} tables")
    for table_name in include_table_names:
            table_ref_dict = table_inventory[table_name]
            table_ref = FlinkTableReference(**table_ref_dict)
            pipe_def = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
            if pipe_def:
                node = pipe_def.to_node()
                node.version = version
                nodes_to_process.append(node)
                node.to_restart = True
                # Build the static graph from the Flink statement relationship
                combined_node_map = _build_statement_node_map(node, visited_nodes, combined_node_map)
                count+=1
    if count > 0:
        ancestors = _build_topological_sorted_graph(nodes_to_process, combined_node_map)
        start_node = ancestors[-1]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors,
                                                                      node_map=combined_node_map,
                                                                      force_ancestors=force_ancestors,
                                                                      may_start_descendants=may_start_descendants,
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id,
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=exclude_table_names,
                                                                      pool_creation=pool_creation)
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary = report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=False)
        if execute_plan:
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Executing plan: {summary}")
            _execute_plan(execution_plan=execution_plan,
                         compute_pool_id=compute_pool_id,
                         accept_exceptions=False,
                         sequential=sequential,
                         max_thread=max_thread)
            execution_time = int(time.perf_counter() - start_time)
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Execution time: {execution_time} seconds")
            summary+=f"\nExecution time: {execution_time} seconds"
            table_report = report_mgr.build_TableReport(start_node.product_name, execution_plan.nodes, from_date="", get_metrics=True)
            summary+="\n"+f"#"*40 + f" Deployed {count} tables " + "#"*40 + "\n"
        return summary, table_report
    else:
        return "Nothing run. Do you have a pipeline_definition.json files", TableReport()

def report_running_flink_statements_for_a_table(
    table_name: str,
    inventory_path: str,
    from_date: str
) -> str:
    """
    Report running flink statements for a table execution plan
    """
    config = get_config()
    _, report = build_deploy_pipeline_from_table(table_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_descendants=False,
                                                        force_ancestors=False,
                                                        pool_creation=False)
    return report_mgr.build_simple_report(report)

def report_running_flink_statements_for_all_from_directory(
    directory: str,
    inventory_path: str,
    from_date: str
) -> str:
    """
    Review execution plans for all the pipelines in the directory.
    """
    # Extract last two folders from directory path
    path_parts = directory.rstrip('/').split('/')
    if len(path_parts) >= 2:
        report_name = f"{path_parts[-2]}_{path_parts[-1]}"
    else:
        report_name = path_parts[-1]
    nodes_to_process= []
    count = 0
    visited_nodes = set()
    combined_node_map = {}
    for root, _, files in os.walk(directory):
        if PIPELINE_JSON_FILE_NAME in files:
            file_path=root + "/" + PIPELINE_JSON_FILE_NAME
            pipeline_def = read_pipeline_definition_from_file(file_path)
            node=pipeline_def.to_node()
            node.existing_statement_info = statement_mgr.get_statement_status_with_cache(node.dml_statement_name)
            node.compute_pool_id = node.existing_statement_info.compute_pool_id
            nodes_to_process.append(node)
            combined_node_map = _build_statement_node_map(node, visited_nodes, combined_node_map)
            count+=1
    if count > 0:
        ancestors = _build_topological_sorted_graph(nodes_to_process, combined_node_map)
        start_node = ancestors[0]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors,
                                                                      node_map=combined_node_map,
                                                                      force_ancestors=False,
                                                                      may_start_descendants=False,
                                                                      cross_product_deployment=False,
                                                                      compute_pool_id=None,
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=[],
                                                                      pool_creation=False)

        table_report = report_mgr.build_TableReport(report_name, execution_plan.nodes, from_date=from_date, get_metrics=True)
        return report_mgr.persist_table_reports(table_report, report_name)
    return "Nothing run. Do you have a pipeline_definition.json files?"



def report_running_flink_statements_for_a_product(
    product_name: str,
    inventory_path: str,
    from_date: str
) -> str:
    """
    Report running flink statements for all the pipelines in the product.
    """
    report_name = f"product:{product_name}"
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    count = 0
    visited_nodes = set()
    nodes_to_process = []
    combined_node_map = {}
    for _, table_ref_dict in table_inventory.items():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref.product_name == product_name:
            file_path=table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME
            pipeline_def = read_pipeline_definition_from_file(file_path)
            node=pipeline_def.to_node()
            node.existing_statement_info = statement_mgr.get_statement_status_with_cache(node.dml_statement_name)
            node.compute_pool_id = node.existing_statement_info.compute_pool_id
            nodes_to_process.append(node)
            combined_node_map = _build_statement_node_map(node, visited_nodes, combined_node_map)
            count+=1
    if count > 0:
        ancestors = _build_topological_sorted_graph(nodes_to_process, combined_node_map)
        start_node = ancestors[0]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors,
                                                                      node_map=combined_node_map,
                                                                      force_ancestors=False,
                                                                      may_start_descendants=False,
                                                                      cross_product_deployment=False,
                                                                      compute_pool_id="",
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=[],
                                                                      pool_creation=False)

        table_report = report_mgr.build_TableReport(report_name, execution_plan.nodes, from_date=from_date, get_metrics=True)
        return report_mgr.persist_table_reports(table_report, report_name)
    return "Nothing run. Do you have a pipeline_definition.json files?"


def full_pipeline_undeploy_from_table(
    table_name: str,
    inventory_path: str
) -> str:
    """
    Stop DML statement and drop tables: look at the parents of the current table
    and remove the parent that has one running child. Delete all the children of the current table.
    """
    logger.info("\n"+"#"*20 + f"\n# Full pipeline delete from table {table_name}\n" + "#"*20)
    start_time = time.perf_counter()
    table_pipeline_def: FlinkTablePipelineDefinition = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
    config = get_config()
    summary, report = build_deploy_pipeline_from_table(table_name=table_pipeline_def.table_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_descendants=True,
                                                        force_ancestors=False,
                                                        pool_creation=False)
    config = get_config()
    trace = f"Full pipeline delete from table {table_name}\n"
    print(f"{trace}")
    for table_info in reversed(report.tables):
        if table_info.to_restart: # remove only the tables that was marked as to restart to avoid stopping ancestors
            statement_mgr.delete_statement_if_exists(table_info.statement_name)
            rep= statement_mgr.drop_table(table_info.table_name, table_info.compute_pool_id)
            trace+=f"Dropped table {table_info.table_name} with result: {rep}\n"
            print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Dropped table {table_info.table_name}")
    execution_time = int(time.perf_counter() - start_time)
    logger.info(f"Done in {execution_time} seconds to undeploy pipeline from table {table_name}")
    return trace

def full_pipeline_undeploy_from_product(product_name: str,
										inventory_path: str,
										compute_pool_id: str,
										cross_product: bool = False) -> str:
    """
    To undeploy we need to build an integrated execution plan for all the tables in the product.
    Undeploy in the reverse order of the execution plan, but keep table that have other product(s) as children
    """
    compute_pool_id = compute_pool_id or get_config()['flink']['compute_pool_id']
    start_time = time.perf_counter()
    nodes_to_process = []
    combined_node_map = {}
    visited_nodes = set()
    count=0
    trace = ""
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    for table_ref_dict in table_inventory.values():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref and table_ref.product_name == product_name:
            node = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME).to_node()
            nodes_to_process.append(node)
            # Build the static graph from the Flink statement relationship
            combined_node_map = _build_statement_node_map(node, visited_nodes, combined_node_map)
            count+=1
    if count > 0:
        ancestors = _build_topological_sorted_graph(nodes_to_process, combined_node_map)
        start_node = ancestors[0]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors,
                                                                      node_map=combined_node_map,
                                                                      force_ancestors=False,
                                                                      may_start_descendants=True,
                                                                      cross_product_deployment=False,
                                                                      compute_pool_id=compute_pool_id,
                                                                      table_name=start_node.table_name,
                                                                      expected_product_name=start_node.product_name,
                                                                      exclude_table_names=[],
                                                                      pool_creation=False)

        execution_plan.nodes.reverse()
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Integrated execution plan for {product_name} with {len(execution_plan.nodes)} nodes")
        for node in execution_plan.nodes:
            state = "EXISTS"
            if node.existing_statement_info == None or node.existing_statement_info.status_phase == "UNKNOWN":
                state = "NOT EXISTS"
            print(f"Table: {report_mgr.pad_or_truncate(node.table_name, 40)} product: {report_mgr.pad_or_truncate(node.product_name, 40)} {state} {node.compute_pool_id}")

        trace = f"Full pipeline delete from product {product_name}\n"

        # Filter nodes that need to be processed
        nodes_to_drop = [node for node in execution_plan.nodes if (node.product_name == product_name
                                                                and (node.is_running() or node.existing_statement_info.status_phase != "UNKNOWN"))]  # 08-14 path to remove ant node
        # nodes_to_drop = [node for node in execution_plan.nodes if node.product_name == product_name]
        count = len(nodes_to_drop)
        if count == 0:
            return "No table found for product " + product_name + " in inventory " + inventory_path

        # Get number of CPU cores for max workers
        max_workers = multiprocessing.cpu_count()

        # Process nodes in parallel using a thread pool
        results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks and get future objects
            future_to_node = {executor.submit(_drop_node_worker, node): node for node in nodes_to_drop}

            # Process completed tasks as they finish
            for future in as_completed(future_to_node):
                node = future_to_node[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    results.append(f"Failed to process {node.table_name}: {str(e)}\n")

        trace += "".join(results)


    execution_time = int(time.perf_counter() - start_time)
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Undeploy pipeline product {product_name} Done in {execution_time} seconds")
    return trace


def prepare_tables_from_sql_file(sql_file_name: str,
                                 compute_pool_id: str):
    """
    Execute the content of the sql file, line by line as separate Flink statement. It is used to alter table. for deployment by adding the necessary comments and metadata.
    """
    config = get_config()
    compute_pool_id = compute_pool_id or config['flink']['compute_pool_id']
    transformer = statement_mgr.get_or_build_sql_content_transformer()
    stmnt_suffix = datetime.now().strftime("%Y%m%d%H%M%S")
    with open(sql_file_name, "r") as f:
        idx=0
        for line in f:
            if line.lstrip().startswith('--'):
                continue
            _, sql_out= transformer.update_sql_content(line,
                                                       "",
                                                       "")
            print(sql_out)
            statement_name = f"prepare-table-{stmnt_suffix}-{idx}"
            statement = statement_mgr.post_flink_statement(compute_pool_id,
                                                           statement_name,
                                                           sql_out)
            while statement.status.phase not in ["COMPLETED", "FAILED"]:
                time.sleep(2)
                statement = statement_mgr.get_statement(statement_name)
                logger.info(f"Prepare table {statement_name} status is: {statement}")
            idx+=1
            statement_mgr.delete_statement_if_exists(statement_name)
#
# ------------------------------------- private APIs  ---------------------------------
#

def _drop_node_worker(node: FlinkStatementNode) -> str:
    """Worker function to drop a single node's table and statements."""
    try:
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Dropping table {node.table_name}")
        statement_mgr.delete_statement_if_exists(node.dml_statement_name)
        statement_mgr.delete_statement_if_exists(node.ddl_statement_name)
        rep = statement_mgr.drop_table(node.table_name, node.compute_pool_id)
        return f"Dropped table {node.table_name} with result: {rep}\n"
    except Exception as e:
        return f"Failed to drop table {node.table_name}: {str(e)}\n"

def _build_execution_plan_using_sorted_ancestors(ancestors: List[FlinkStatementNode],
                                                 node_map: Dict[str, FlinkStatementNode],
                                                 force_ancestors: bool,
                                                 may_start_descendants: bool,
                                                 cross_product_deployment: bool,
                                                 compute_pool_id: str,
                                                 table_name: str,
                                                 expected_product_name: str,
                                                 exclude_table_names: List[str],
                                                 pool_creation: bool = True):
    """
    Build the execution plan using the sorted ancestors, and then taking into account children of each node and their stateful mode.
    The execution plan is a DAG of nodes that need to be executed in the correct order.
    State is always needed if the output of processing a row is not only determined by that row itself, but also depends on the rows,
    which have previously been processed. A join needs to materialize both sides of the join, as if a row in left hand side is updated
    the statements needs to emit an updated match for all matching rows in the right hand side.
    """
    try:
        execution_plan = FlinkStatementExecutionPlan(
            created_at=datetime.now(),
            environment_id=get_config()['confluent_cloud']['environment_id'],
            start_table_name=table_name
        )
        # Process all parents and grandparents reachable by DFS from start_node. Ancestors may not be
        # in the same product family as the start_node. The ancestor list is sorted so first node needs to run first
        execution_plan = _process_ancestors(ancestors, execution_plan, force_ancestors, compute_pool_id, may_start_descendants, pool_creation)
        execution_plan.nodes = _filter_nodes_to_exclude(execution_plan.nodes, exclude_table_names)
        # At this level, execution_plan.nodes has the list of ancestors from the  starting node.
        # For each node, we need to assess if children and ancestors needs to be started.
        # Only restart ancestors, if user forced to do so: The current node once it deletes its output table(s) will reprocess
        # records from the earliest and regenerates its states and aggregations.
        # The children needs to be restarted if the current node is stateful to avoid duplicates records
        if may_start_descendants:
            accepted_common_products = get_config()['app']['accepted_common_products']
            for node in execution_plan.nodes:
                # The execution plan nodes list may be updated by processing children. As to start a child it may be needed
                # to start a parent not yet in the execution plan.
                if node.to_run or node.to_restart:
                    # the approach is to add to the execution plan all children that need to be restarted.
                    for child in node.children:
                        child_node = _get_static_info_update_node_map(child, node_map) # need all the information of the child.
                        if child_node:
                            if ((node.upgrade_mode == "Stateful" )
                            or (node.upgrade_mode != "Stateful" and child_node.upgrade_mode == "Stateful")):
                                if (child_node not in execution_plan.nodes
                                    and (child_node.product_name == expected_product_name
                                        or child_node.product_name in accepted_common_products
                                        or cross_product_deployment)):
                                    child_node.to_restart = not child_node.to_run  # should we restart non same product ? assume yes because may_start_descendants is True
                                    child_node=_assign_compute_pool_id_to_node(node=child_node, compute_pool_id=compute_pool_id, pool_creation=pool_creation)
                                    child_node.parents.remove(node)  # do not reprocess current node as parent of current child
                                    new_ancestors = _build_topological_sorted_graph([child_node], node_map)
                                    execution_plan = _process_ancestors(new_ancestors, execution_plan, force_ancestors, compute_pool_id, may_start_descendants, pool_creation)
                                    sorted_children = _build_topological_sorted_children(child_node, node_map)
                                    for _child in sorted_children:
                                        if _child.table_name != child_node.table_name:
                                            _child.to_restart = not _child.to_run
                                            _child = _assign_compute_pool_id_to_node(node=_child, compute_pool_id=compute_pool_id, pool_creation=pool_creation)
                                    execution_plan.nodes = _merge_graphs(execution_plan.nodes, list(reversed(sorted_children)))
                                    execution_plan.nodes = _filter_nodes_to_exclude(execution_plan.nodes, exclude_table_names)
                        else:
                            logger.warning(f"Child {child.table_name} not found in node_map")

        logger.info(f"Done with execution plan construction: [{len(execution_plan.nodes)} nodes]")
        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Done with execution plan construction [{len(execution_plan.nodes)} nodes]")
        logger.debug(execution_plan)
        return execution_plan
    except Exception as e:
        logger.error(f"Failed to build execution plan. Error is : {str(e)}")
        raise

def _get_static_info_update_node_map(simple_node: FlinkStatementNode,
                                     node_map: Dict[str, FlinkStatementNode]
) -> FlinkStatementNode | None:
    """
    When navigating to ancestors and assessing if children of those ancestors have to be started,
    we need to get the static info of the current table and may be modify the node map content
    """
    if simple_node.table_name in node_map:
        node = node_map[simple_node.table_name]
        node = _get_and_update_statement_info_compute_pool_id_for_node(node)
        return node
    else:
        return None


def _process_ancestors(ancestors: List[FlinkStatementNode],
                       execution_plan: FlinkStatementExecutionPlan,
                       force_ancestors: bool,
                       compute_pool_id: str,
                       may_start_descendants: bool = False,
                       pool_creation: bool = True
)-> FlinkStatementExecutionPlan:
    """
    Process the ancestors of the current node. Ancestor needs to be started
    if it is not running so the current node will run successfully.
    A stateful node enforces restarting its children.
    If force_ancestors is True, all ancestors will be started.
    """
    for node in ancestors:
        if force_ancestors and not node.to_restart:
            node.to_run = True
        else:
            node = _get_and_update_statement_info_compute_pool_id_for_node(node)
            if not node.is_running() and not node.to_restart:
                node.to_run = True
        if (node.to_run or node.to_restart) and node.upgrade_mode == "Stateful":
            node.update_children = may_start_descendants
        if node.to_run and not node.compute_pool_id:
            node = _assign_compute_pool_id_to_node(node, compute_pool_id, pool_creation=pool_creation)
        if node not in execution_plan.nodes:  # do add all nodes to the execution plan because we need to know the dependencies
            execution_plan.nodes.append(node)
    return execution_plan

def _persist_execution_plan(execution_plan: FlinkStatementExecutionPlan, filename: str = None):
    """
    Persist the execution plan to a JSON file, handling circular references.

    Args:
        execution_plan: The execution plan to persist
    """
    if not filename:
        filename = f"{shift_left_dir}/{execution_plan.start_table_name}_execution_plan.json"
    logger.info(f"Persist execution plan to {filename}")

    # Add nodes with their parent and child references
    for node in execution_plan.nodes:
        parent_names = []
        for p in node.parents:
            if isinstance(p, FlinkStatementNode):
                parent_names.append(p.table_name)
            else:
                parent_names.append(p)
        child_names = []
        for c in node.children:
            if isinstance(c, FlinkStatementNode):
                child_names.append(c.table_name)
            else:
                child_names.append(c)
        node.parents = set(parent_names)
        node.children = set(child_names)

    # Write to file with proper JSON formatting
    with open(filename, "w") as f:
        f.write(execution_plan.model_dump_json(indent=2))  # default=str handles datetime serialization




def _get_ancestor_subgraph(start_node: FlinkStatementNode, node_map)-> Tuple[Dict[str, FlinkStatementNode],
                                                         Dict[str, List[FlinkStatementNode]]]:
    """Builds a subgraph containing all ancestors of the start node.
    Returns a dictionary of unique ancestor and a dict of <table_name, list of ancestors> tuple
    for each parent of a node.
    """
    ancestors = {}
    queue = deque([start_node])
    visited = {start_node}
    # recursively add the parents of the current node to the ancestors to treat.
    while queue:
        current_node = queue.popleft()
        for parent in current_node.parents:
            if parent not in visited:
                enriched_parent = _get_static_info_update_node_map(parent, node_map)
                if enriched_parent:
                    ancestors[parent.table_name] = enriched_parent
                    visited.add(enriched_parent)
                    queue.append(enriched_parent)
            #if parent not in ancestors:  # Ensure parent itself is unique in the set
            #     ancestors[parent.table_name] = parent
    ancestors[start_node.table_name] = start_node
    # List of tuple <table_name, parent> for each parent of a node, will help to count the number of incoming edges
    # for each node in the topological sort. The ancestor dependencies has one record per <table_name, parent> tuple.
    # a node with 3 ancestors will have 3 records in the ancestor dependencies list.
    ancestor_dependencies = []

    def _add_parent_dependencies(node: FlinkStatementNode,
                                 node_map: dict,
                                 new_ancestors: dict,
                                 ancestor_dependencies: list) -> None:
        """Iteratively add <table-name, parent> tuples to ancestor_dependencies.
        Also update node_map and new_ancestors with static info for encountered parents.
        """
        stack = [node]
        seen = set()
        while stack:
            current_node = stack.pop()
            if current_node.table_name in seen:
                continue
            seen.add(current_node.table_name)
            enriched_node = _get_static_info_update_node_map(current_node, node_map)
            if not enriched_node:
                continue
            for parent in enriched_node.parents:
                ancestor_dependencies.append((enriched_node.table_name, parent))
                if parent.table_name not in new_ancestors.keys():
                    new_ancestors[parent.table_name] = parent
                stack.append(parent)


    new_ancestors = ancestors.copy()
    #for node in ancestors.values():
    node = ancestors[start_node.table_name]
    _add_parent_dependencies(node, node_map, new_ancestors, ancestor_dependencies)
    ancestors.update(new_ancestors)
    return ancestors, ancestor_dependencies


def _build_topological_sorted_graph(current_nodes: List[FlinkStatementNode],
                                      node_map: Dict[str, FlinkStatementNode])-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG of the current node parents
    the node_map is a hashmap of table name and direct static relationships of the table with its parents and children
    For each node, navigate the subgraph to reach other nodes to compute the dependencies weights.
    """
    ancestor_nodes = {}
    ancestor_dependencies = []
    for current_node in current_nodes:
        current_ancestors, dependencies = _get_ancestor_subgraph(current_node, node_map)
        ancestor_nodes.update(current_ancestors)
        for dep in dependencies:
            ancestor_dependencies.append(dep)
    return _topological_sort(ancestor_nodes, ancestor_dependencies)

def _build_topological_sorted_children(
        current_node: FlinkStatementNode,
        node_map: Dict[str, FlinkStatementNode]
)-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG of the current node"""
    nodes, dependencies = _get_descendants_subgraph(current_node, node_map)
    return _topological_sort(nodes, dependencies)

def _topological_sort(
    nodes: Dict[str, FlinkStatementNode],
    dependencies: Dict[str, List[FlinkStatementNode]]
)-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG using Kahn Algorithm"""

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
        logger.error(f"Flink StatementGraph has a cycle, cannot perform topological sort.")
        raise ValueError("Graph has a cycle, cannot perform topological sort.")


def _get_descendants_subgraph(start_node: FlinkStatementNode,
                              node_map: Dict[str, FlinkStatementNode]
)-> Tuple[Dict[str, FlinkStatementNode], Dict[str, List[FlinkStatementNode]]]:
    """Builds a subgraph containing all descendants of the start node."""
    descendants = {}
    queue = deque([start_node])
    visited = {start_node}

    while queue:
        current_node = queue.popleft()
        for child in current_node.children:
            if child not in visited:
                descendants[child.table_name] = child
                visited.add(child)
                queue.append(child)
            if child not in descendants:  # Ensure child itself is in the set
                 descendants[child.table_name] = child
    descendants[start_node.table_name] = start_node
    # Include dependencies within the ancestor subgraph
    descendant_dependencies = []

    def _add_child_dependencies(node: FlinkStatementNode, node_map: dict, new_descendants: dict) -> None:
        enriched_node = _get_static_info_update_node_map(node, node_map)
        if enriched_node:
            for child in enriched_node.children:
                descendant_dependencies.append((node.table_name, child))
                if child.table_name not in new_descendants:
                    new_descendants[child.table_name] = child
                _add_child_dependencies(child, node_map, new_descendants)


    new_descendants = descendants.copy()
    for node in descendants.values():
        _add_child_dependencies(node, node_map, new_descendants)
    descendants.update(new_descendants)
    return descendants, descendant_dependencies

def _get_and_update_statement_info_compute_pool_id_for_node(node: FlinkStatementNode) -> FlinkStatementNode:
    """
    Update node with current statem

    Args:
        node: Node to update

    Returns:
        Updated node with existing_statement_info field getting the retrieved statement info
        and compute_pool_id and compute_pool_name fields getting the values from the statement info
    """
    if not node.existing_statement_info:
        node.existing_statement_info = statement_mgr.get_statement_status_with_cache(node.dml_statement_name)
        if isinstance(node.existing_statement_info, StatementInfo) and node.existing_statement_info.compute_pool_id:
            node.compute_pool_id = node.existing_statement_info.compute_pool_id
            node.compute_pool_name = node.existing_statement_info.compute_pool_name
            node.created_at = node.existing_statement_info.created_at
        else:
            logger.warning(f"Statement {node.existing_statement_info}")
    return node

def _merge_graphs(in_out_graph:  List[FlinkStatementNode], in_graph:  List[FlinkStatementNode]) -> List[FlinkStatementNode]:
    """
    It may be possible while navigating to the children that some parents of those children are not part
    of the current graph, so there is a need to merge the graphs
    """
    for node in in_graph:
        if node not in in_out_graph:
            in_out_graph.append(node)
    return in_out_graph


def _assign_compute_pool_id_to_node(node: FlinkStatementNode, compute_pool_id: str, pool_creation: bool = True) -> FlinkStatementNode:
    """
    Assign a compute pool id to a node. Node may already have an assigned compute pool id from a running statement or because it
    was set as argument of the command line.
    If the node is an ancestor or a child of a running node, it may be possible there is no running
    statement for that node so no compute pool id is set.
    In this case we need to find a compute pool to use by looking at the table name and the naming convention
    applied to the compute pool.
    """
    logger.info(f"Assign compute pool id to node {node.table_name}, backup pool is {compute_pool_id}")
    # If the node already has an assigned compute pool, continue using that
    if node.compute_pool_id and compute_pool_mgr.is_pool_valid(node.compute_pool_id):  # this may be loaded from the statement info
        node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
        return node
    # get the list of compute pools available that match the table name
    pools=compute_pool_mgr.search_for_matching_compute_pools(table_name=node.table_name)
    # If we don't have any matching compute pool, we need to find a pool to use
    if  not pools or len(pools) == 0:
        logger.info(f"No matching compute pool found for {node.table_name}")
        # assess user's parameter for compute pool id
        if compute_pool_id and compute_pool_mgr.is_pool_valid(compute_pool_id):
            node.compute_pool_id = compute_pool_id
            node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
        elif pool_creation:
            # assess compute pool id from config.yaml
            # configured_compute_pool_id = get_config()['flink']['compute_pool_id']
            #if configured_compute_pool_id and compute_pool_mgr.is_pool_valid(configured_compute_pool_id):
            #    node.compute_pool_id = configured_compute_pool_id
            #    node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
            #else:
            node.compute_pool_id, node.compute_pool_name =compute_pool_mgr.create_compute_pool(node.table_name)
        else:
            logger.warning(f"Compute pool {compute_pool_id} is not available for {node.table_name} it should be created")
        return node
    if len(pools) == 1:
        # matching pool found, Do not need to assess for capacity as the pool is reused.
        node.compute_pool_id = pools[0].id
        node.compute_pool_name = pools[0].name
        return node
    # more than one? let use the configured compute pool id if it is valid
    configured_compute_pool_id = get_config()['flink']['compute_pool_id']
    if configured_compute_pool_id and compute_pool_mgr.is_pool_valid(configured_compute_pool_id):
        node.compute_pool_id = configured_compute_pool_id
        node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
    else:
        raise Exception(f"Compute pool {configured_compute_pool_id} is not available for {node.table_name}")
    return node



def _execute_plan(execution_plan: FlinkStatementExecutionPlan,
                  compute_pool_id: str,
                  accept_exceptions: bool = False,
                  sequential: bool = True,
                  max_thread: int = 1) -> List[Statement]:
    """Execute statements in the execution plan.
    It enables parallel deployment of Flink statements that
    have no dependencies (autonomous nodes), significantly
    speeding up the deployment process compared to sequential
    execution
    Args:
        plan: Execution plan containing nodes to execute
        compute_pool_id: ID of the compute pool to use

    Returns:
        List of deployed statements

    Raises:
        RuntimeError: If statement execution fails
    """
    logger.info(f"--- Execute Plan for {execution_plan.start_table_name} started ---")
    statements = []
    autonomous_nodes=[]
    started_nodes = []
    nodes_to_execute = _get_nodes_to_execute(execution_plan.nodes)
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Execute for {execution_plan.start_table_name} started. {len(nodes_to_execute)} statements to execute")
    while len(nodes_to_execute) > 0:
        if not sequential:
            # for parallel execution split the statements to execute into buckets for the one with no parent
            # or all parents are running and not to be restarted.
            if max_thread == 1:
                max_thread = multiprocessing.cpu_count()
            if max_thread > 10:
                max_thread = 10
            autonomous_nodes = _build_autonomous_nodes(nodes_to_execute, started_nodes)
            if len(autonomous_nodes) > 0:
                print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Deploying {len(autonomous_nodes)} flink statements using parallel processing on {max_thread} workers")
                while len(autonomous_nodes) > 0:
                    _max_workers = max_thread
                    if len(autonomous_nodes) < max_thread:
                        _max_workers = len(autonomous_nodes)
                    to_process = [] # need to use a separate list as we can have more elements in autonomous_nodes than max_workers
                    for _ in range(_max_workers):
                        to_process.append(autonomous_nodes.pop(0))
                    started_nodes, statements= _execute_statements_in_parallel(to_process, _max_workers, accept_exceptions, compute_pool_id, started_nodes, statements)
            else:
                # no parallel execution possible, so execute sequentially
                started_nodes, statements= _execute_statements_in_sequence(nodes_to_execute, accept_exceptions, compute_pool_id, started_nodes, statements)
        else:
            started_nodes, statements= _execute_statements_in_sequence(nodes_to_execute,
                                            accept_exceptions,
                                            compute_pool_id,
                                            started_nodes,
                                            statements)
        nodes_to_execute = _get_nodes_to_execute(execution_plan.nodes)
    return statements

def _get_nodes_to_execute(nodes: List[FlinkStatementNode]) -> List[FlinkStatementNode]:
    """
    Build a list of nodes to execute.
    """
    nodes_to_execute = []
    for node in nodes:
        if node.to_run or node.to_restart:
            nodes_to_execute.append(node)
    return nodes_to_execute

def _execute_statements_in_parallel(to_process: List[FlinkStatementNode],
                                    max_workers: int,
                                    accept_exceptions: bool = False,
                                    compute_pool_id: str = "",
                                    started_nodes: List[FlinkStatementNode] = [],
                                    statements: List[Statement] = []) -> Tuple[List[FlinkStatementNode], List[Statement]]:
    """
    Execute statements in parallel.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_deploy_one_node, node, accept_exceptions, compute_pool_id) for node in to_process]
        for future in as_completed(futures):
            try:
                result = future.result(timeout=60)  # will wait up to timeout seconds.
                logger.info(f"{result}")
                if isinstance(result, Statement):  # Only append if we got a valid result
                    statements.append(result)
                    if result.status.phase not in ["COMPLETED", "RUNNING"]:
                        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Statement {result.name} failed, move to next node")
                        logger.error(f"Statement {result.name} failed, move to next node")
                        nodes_to_execute = _modify_impacted_nodes(result, nodes_to_execute)
                        pass
                else:
                    logger.warning(f"Result from future is StatementError")
            except Exception as e:
                logger.error(f"Failed to get result from future: {str(e)}")
                if not accept_exceptions:
                    raise
        for node in to_process: # need this to avoid re-running the same node
            node.to_run = False
            node.to_restart = False
            started_nodes.append(node)
    return started_nodes, statements

def _execute_statements_in_sequence(nodes_to_execute: List[FlinkStatementNode],
                                    accept_exceptions: bool = False,
                                    compute_pool_id: str = "",
                                    started_nodes: List[FlinkStatementNode] = [],
                                    statements: List[Statement] = []) -> Tuple[List[FlinkStatementNode], List[Statement]]:
    """
    Execute statements in sequence.
    """
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Still {len(nodes_to_execute)} statements to execute")
    node = nodes_to_execute.pop(0)
    statement = _deploy_one_node(node, accept_exceptions, compute_pool_id)
    if isinstance(statement, Statement):
        statements.append(statement)
    else:
        logger.info(f"Statement {node.dml_statement_name} not deployed, move to next node")
    node.to_run = False
    node.to_restart = False
    started_nodes.append(node)
    return started_nodes, statements

def _build_autonomous_nodes(
        nodes_to_execute: List[FlinkStatementNode],
        started_nodes: List[FlinkStatementNode]
    ) -> List[FlinkStatementNode]:
    """
    Build a list of autonomous statements: a statement is autonomous when it can be executed
    in parallel of other statements in the list, because it has no parents or all
    its parents are running.
    """

    if not nodes_to_execute:
        return []

    # Build dependencies (child->parent) for each child,parent relation in the graph
    dependencies = []
    for node in nodes_to_execute:
        for parent in node.parents:
            parent_table_name = parent if isinstance(parent, str) else parent.table_name
            parent_node = next((n for n in nodes_to_execute if n.table_name == parent_table_name), None)
            if parent_node and (parent_node.to_run or parent_node.to_restart) and parent_node not in started_nodes:
                dependencies.append((node.table_name, parent_node))

    # Use same in-degree calculation logic as existing _topological_sort
    in_degree = {node.table_name: 0 for node in nodes_to_execute}
    for child_name, parent_node in dependencies:
        in_degree[child_name] += 1

    # Find autonomous nodes (in-degree 0) that need execution
    started_node_names = {node.table_name for node in started_nodes}
    autonomous_nodes = []

    for node in nodes_to_execute:
        if (in_degree[node.table_name] == 0 and
            (node.to_run or node.to_restart) and
            node.table_name not in started_node_names):
            autonomous_nodes.append(node)

    return autonomous_nodes


def _modify_impacted_nodes(statement: Statement, nodes_to_execute: List[FlinkStatementNode]) -> None:
    """
    Modify the nodes to execute based on the statement result of a previous statement.
    remove direct children nodes of the failed statement from the nodes to execute.
    """
    # TODO: implement this
    pass

def _deploy_one_node(node: FlinkStatementNode,
                     accept_exceptions: bool = False,
                     compute_pool_id: str = ""
)-> Statement | StatementError:
    """
    Deploy one Statement as described in the FlinkStatementNode. Keep the node concept as it is part of the execution plan graph.
    """
    if not node.compute_pool_id:
            node.compute_pool_id = compute_pool_id
    logger.info(f"Deploy table: {node.table_name}'")
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Deploy table {node.table_name}")
    try:
        if not node.dml_only:
            statement = _deploy_ddl_dml(node)
        else:
            statement = _deploy_dml(node, False)
        if isinstance(statement, StatementError):
            return statement
        node.existing_statement_info = statement_mgr.map_to_statement_info(statement)
        return statement
    except Exception as e:
        if not accept_exceptions:
            logger.error(f"Failed to execute statement {node.dml_statement_name}: {str(e)}")
            raise RuntimeError(f"{time.strftime('%Y%m%d_%H:%M:%S')} Statement execution failed: {str(e)}")
        else:
            logger.error(f"Statement execution for: {node.table_name} failed: {str(e)}, move to next node")
            return StatementError(errors =[ ErrorData(id=node.table_name, status="FAILED", detail=f"Statement execution for: {node.table_name} failed: {str(e)}")])


def _deploy_ddl_dml(node_to_process: FlinkStatementNode)-> Statement | StatementError:
    """
    Deploy the DDL and then the DML for the given table to process.
    """

    logger.info(f"{node_to_process.ddl_ref} to {node_to_process.compute_pool_id}, first delete dml statement")
    statement_mgr.delete_statement_if_exists(node_to_process.dml_statement_name) # need this to be able to drop table
    statement_mgr.delete_statement_if_exists(node_to_process.ddl_statement_name)
    rep= statement_mgr.drop_table(node_to_process.table_name, node_to_process.compute_pool_id)
    logger.info(f"Dropped table {node_to_process.table_name} status is : {rep}")
    # create the table with ddl.
    statement = statement_mgr.build_and_deploy_flink_statement_from_sql_content(node_to_process,
                                                                            node_to_process.ddl_ref,
                                                                            node_to_process.ddl_statement_name)
    if isinstance(statement, Statement):
        while statement.status.phase not in ["COMPLETED"]:
            time.sleep(2)
            statement = statement_mgr.get_statement(node_to_process.ddl_statement_name)
            logger.info(f"DDL deployment status is: {statement.status.phase}")
            if statement.status.phase in ["FAILED"]:
                raise RuntimeError(f"DDL deployment failed for {node_to_process.table_name}")
    else:
        logger.error(f"DDL deployment failed for {node_to_process.table_name}")
        raise RuntimeError(f"DDL deployment failed for {node_to_process.table_name}")
    return _deploy_dml(node_to_process, True)


def _deploy_dml(to_process: FlinkStatementNode, dml_already_deleted: bool= False)-> Statement | StatementError:
    logger.info(f"Run {to_process.dml_statement_name} for {to_process.table_name} table to {to_process.compute_pool_id}")
    if not dml_already_deleted:
        statement_mgr.delete_statement_if_exists(to_process.dml_statement_name)

    statement = statement_mgr.build_and_deploy_flink_statement_from_sql_content(to_process,
                                                                                to_process.dml_ref,
                                                                                to_process.dml_statement_name)
    compute_pool_mgr.save_compute_pool_info_in_metadata(to_process.dml_statement_name, to_process.compute_pool_id)
    if isinstance(statement, Statement):
        while statement.status.phase in ["PENDING"]:
            time.sleep(5)
            statement = statement_mgr.get_statement(to_process.dml_statement_name)
            logger.debug(f"DML deployment status is: {statement.status.phase}")
        if statement.status.phase == "FAILED":
            raise RuntimeError(f"DML deployment failed for {to_process.table_name}")
    logger.info(f"DML deployment completed for {to_process.table_name}")
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} DML deployment completed for {to_process.table_name}")
    return statement



def _delete_not_shared_parent(current_node: FlinkStatementNode, trace:str, config ) -> str:
    for parent in current_node.parents:
        if len(parent.children) == 1:
            # as the parent is not shared it can be deleted
            statement_mgr.delete_statement_if_exists(parent.ddl_statement_name)
            statement_mgr.delete_statement_if_exists(parent.dml_statement_name)
            statement_mgr.drop_table(parent.table_name, config['flink']['compute_pool_id'])
            trace+= f"{parent.table_name} deleted\n"
            pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
            trace = _delete_not_shared_parent(pipeline_def, trace, config)
        else:
            trace+=f"{parent.table_name} has more than {current_node.table_name} as child, so no delete"
    if len(current_node.children) == 1:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(current_node)
        statement_mgr.delete_statement_if_exists(ddl_statement_name)
        statement_mgr.delete_statement_if_exists(dml_statement_name)
        statement_mgr.drop_table(current_node.table_name, config['flink']['compute_pool_id'])
        trace+= f"{current_node.table_name} deleted\n"
    return trace


def _build_statement_node_map(current_node: FlinkStatementNode,
                               visited_nodes: Set[FlinkStatementNode] = set(),
                               node_map: dict[str,FlinkStatementNode] = {}) -> dict[str,FlinkStatementNode]:
    """
    Define the complete static graph of the related parents and children for the current node.
    The returned node_map is a dict with each table name as key and the node with accurate list of parents and children.
    The function uses a DFS to reach all parents, and then a BFS to construct the list of reachable children with their own parents
    It should exclude all children because the decision to filter per product should be done during
    the execution plan construction taking into account if a parent needs to be restarted and if it is stateful.

    Args:
        current_node: The starting node to build the graph from
        visited_nodes: Set of already processed nodes to avoid reprocessing (optional)
        node_map: Existing node map to extend (optional)
    """
    logger.info(f"start build tables static graph for {current_node.table_name} product: {current_node.product_name}")

    # Initialize parameters if not provided (backward compatibility)
    # <k: str, v: FlinkStatementNode> use a map to search with table name as key.

    # Early return if this node was already processed
    if current_node in visited_nodes and current_node.table_name in node_map:
        logger.info(f"Node {current_node.table_name} already processed, skipping")
        return node_map

    queue = deque()  # Queue for BFS processing to search children of nodes in the queue
    def _search_parent_from_current_update_node_map(node_map: dict[str, FlinkStatementNode],
                                    current: FlinkStatementNode,
                                    visited_nodes: Set[FlinkStatementNode]):
        """
        Goal: build an exhaustive static graph of the related ancestors for the current node.
        Recursively search for parents of the current node and update the node map to keep reference data.
        As parents have parents, update the node map with all the parents of the current node.
        """
        if current not in visited_nodes:
            node_map[current.table_name] = current
            visited_nodes.add(current)
            for p in current.parents:
                if p not in visited_nodes:
                    # this is needed to get the up to date metadata for the parent
                    pipe_def = read_pipeline_definition_from_file( p.path + "/" + PIPELINE_JSON_FILE_NAME)
                    if pipe_def:
                        node_p = pipe_def.to_node()
                        _search_parent_from_current_update_node_map(node_map, node_p, visited_nodes)
                        queue.append(node_p)  # Add new nodes to the queue for processing
                    else:
                        logger.error(f"Data consistency issue for {p.path}: no pipeline definition found or wrong reference in {current.table_name}. The execution plan may not deploy successfully")


    def _search_children_from_current(node_map: dict[str, FlinkStatementNode],
                                      current: FlinkStatementNode,
                                      visited_nodes: Set[FlinkStatementNode]):
        """
        Goal: build an exhaustive static graph of the related descendants for the current node.
        Recursively search for children of the current node and update the node map to keep reference data.
        As children have children, update the node map with all the children of the current node
        """
        for c in current.children:
            if c.table_name not in node_map:
                # a child may have been a parent of another node so do not need to process it
                pipe_def = read_pipeline_definition_from_file( c.path + "/" + PIPELINE_JSON_FILE_NAME)
                if not pipe_def:
                    logger.error(f"Data consistency issue for {c.path}: no pipeline definition found or wrong reference in {c.table_name}. The execution plan may not deploy successfully")
                    continue
                else:
                    node_c = pipe_def.to_node()
                    if node_c not in visited_nodes:
                        # process the child's parents to be sure they are considered before the child
                        _search_parent_from_current_update_node_map(node_map, node_c, visited_nodes)
                        queue.append(node_c)

    _search_parent_from_current_update_node_map(node_map, current_node, visited_nodes)
    queue.append(current_node)  # need to process children of current node

    # Process nodes to update their children descendants using BFS
    while queue:
        current = queue.popleft()
        _search_children_from_current(node_map, current, visited_nodes)
    logger.info(f"End build table graph for {current_node.table_name} with {len(node_map)} nodes")
    #print(f"End build table graph for {current_node.table_name} with {len(node_map)} nodes")
    #logger.debug("\n\n".join("{}\t{}".format(k,v) for k,v in node_map.items()))
    return node_map

# --- to work on for stateless ---------------

def _filtering_out_descendant_nodes(ancestors: List[FlinkStatementNode],
                        product_name: str,
                        may_start_descendants: bool) -> List[FlinkStatementNode]:
    """
    when may start descendant is true
    we need to keep nodes that are in the same product or in the accepted_common_products
    Filter ancestors based on product name and descendant policies.
    Remove ancestors that don't belong to the expected product and have no children in the expected product.

    Args:
        ancestors: List of ancestor nodes to filter
        product_name: Expected product name
        may_start_descendants: Whether descendants may be started

    Returns:
        Filtered list of ancestors
    """
    ancestors_to_remove = []
    for ancestor in ancestors:
        if may_start_descendants:
            if ancestor.product_name == product_name or ancestor.product_name in get_config()['app']['accepted_common_products']:
                continue
            else:
                # Check if any children of this ancestor belong to the expected product
                has_children_in_product = any(
                    child.product_name == product_name or
                    child.product_name in get_config()['app']['accepted_common_products']
                    for child in ancestor.children
                )
                if not has_children_in_product:
                    ancestors_to_remove.append(ancestor)
    # Remove ancestors that don't meet the criteria
    for ancestor in ancestors_to_remove:
        ancestors.remove(ancestor)
    return ancestors


def _filter_nodes_to_exclude(nodes: List[FlinkStatementNode], exclude_table_names: List[str]) -> List[FlinkStatementNode]:
    """
    Filter nodes to exclude based on the table names.
    """
    for node in nodes:
        if node.table_name in exclude_table_names:
           node.to_run = False
           node.to_restart = False
    return nodes
