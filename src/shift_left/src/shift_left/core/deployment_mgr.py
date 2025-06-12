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
from typing import Optional, List, Any, Set, Tuple, Dict, Final
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue

from shift_left.core import (
    pipeline_mgr,
    compute_pool_mgr,
    statement_mgr
)
from shift_left.core.models.flink_statement_model import (
    Statement,
    FlinkStatementNode,
    FlinkStatementExecutionPlan,
    StatementInfo
)

from shift_left.core.utils.report_mgr import (
    DeploymentReport,
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
    execute_plan: bool = False
) -> Tuple[str, FlinkStatementExecutionPlan]:
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
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        pipeline_def = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
        start_node = pipeline_def.to_node()
        start_time = start_time or datetime.now()
        start_node.created_at = datetime.now()
        start_node.dml_only = dml_only
        start_node.compute_pool_id = compute_pool_id
        start_node = _assign_compute_pool_id_to_node(node=start_node, compute_pool_id=compute_pool_id)
        start_node.update_children = may_start_descendants
        # Build the static graph from the Flink statement relationship
        node_map = _build_statement_node_map(start_node)

        ancestors = []
        start_node = _get_and_update_statement_info_compute_pool_id_for_node(start_node)
        start_node.to_restart = True
        ancestors = _build_topological_sorted_parents([start_node], node_map)
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors= ancestors, 
                                                                      node_map=node_map, 
                                                                      force_ancestors=force_ancestors, 
                                                                      may_start_descendants=may_start_descendants, 
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id, 
                                                                      table_name=start_node.table_name, 
                                                                      expected_product_name=start_node.product_name)
        _persist_execution_plan(execution_plan)
        summary=report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        logger.info(f"Execute the plan before deployment: {summary}")
        
        if execute_plan:
            statements = _execute_plan(execution_plan, compute_pool_id)
            result = report_mgr.build_deployment_report(table_name, pipeline_def.dml_ref, may_start_descendants, statements)
        
            result.execution_time = time.perf_counter() - start_time
            result.start_time = start_time
            logger.info(
                f"Done in {result.execution_time} seconds to deploy pipeline from table {table_name}: "
                f"{result.model_dump_json(indent=3)}"
            )
            print(f"Done in {result.execution_time} seconds to deploy pipeline from table {table_name}")
            simple_report=report_mgr.build_simple_report(execution_plan)
            #logger.info(f"Execute the plan after deployment: {simple_report}")
            summary+=f"\n{simple_report}"
        return summary, execution_plan
    except Exception as e:
        logger.error(f"Failed to deploy pipeline from table {table_name} error is: {str(e)}")
        raise

def build_deploy_pipelines_from_product(
    product_name: str,
    inventory_path: str,
    compute_pool_id: str = None,
    dml_only: bool = False, 
    may_start_descendants: bool = False,
    force_ancestors: bool = False,
    cross_product_deployment: bool = False,
    execute_plan: bool = False
) -> Tuple[str, TableReport]:
    """Deploy the pipelines for a given product. Will process all the views, then facts then dimensions. 
    As each statement deployment is creating an execution plan, previously started statements will not be restarted.
    """
    inventory_path = inventory_path or os.getenv("PIPELINES")
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    start_time = time.perf_counter()
    if not compute_pool_id:
        compute_pool_id = get_config()['flink']['compute_pool_id']
    #statement_mgr.reset_statement_list()
    nodes_to_process = []
    combined_node_map = {}
    count=0
    for table_name, table_ref_dict in table_inventory.items():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref.product_name == product_name:
            node = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME).to_node()
            nodes_to_process.append(node)
            # Build the static graph from the Flink statement relationship
            combined_node_map |= _build_statement_node_map(node)
            count+=1
    if count > 0:            
        ancestors = _build_topological_sorted_parents(nodes_to_process, combined_node_map)
        start_node = ancestors[0]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors, 
                                                                      node_map=combined_node_map, 
                                                                      force_ancestors=force_ancestors, 
                                                                      may_start_descendants=may_start_descendants, 
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id, 
                                                                      table_name=start_node.table_name, 
                                                                      expected_product_name=start_node.product_name)
        if start_node.is_running() and not force_ancestors:
            start_node.to_restart = False
            start_node.to_run = False
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary = report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        if execute_plan:
            print(f"Executing plan: {summary}")
            start_time = time.perf_counter()
            _execute_plan(execution_plan, compute_pool_id)
        execution_time = (time.perf_counter() - start_time)
        print(f"Execution time: {execution_time} seconds")
        summary+=f"\nExecution time: {execution_time} seconds"
        table_report = report_mgr.build_TableReport(start_node.product_name)
        for node in execution_plan.nodes:
            table_info = report_mgr.build_TableInfo(node)
            table_report.tables.append(table_info)

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
    execute_plan: bool = False
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
    for root, _, files in os.walk(directory):
        if PIPELINE_JSON_FILE_NAME in files:
            file_path=root + "/" + PIPELINE_JSON_FILE_NAME
            node = read_pipeline_definition_from_file(file_path).to_node()
            node.to_restart = True
            nodes_to_process.append(node)
            # Build the static graph from the Flink statement relationship
            combined_node_map |= _build_statement_node_map(node)
    count = len(nodes_to_process)
    logger.info(f"Found {count} tables to process")
    if count > 0:            
        ancestors = _build_topological_sorted_parents(nodes_to_process, combined_node_map)
        start_node = ancestors[0]
        start_node.to_restart = True
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors, 
                                                                      node_map=combined_node_map, 
                                                                      force_ancestors=force_ancestors, 
                                                                      may_start_descendants=may_start_descendants, 
                                                                      cross_product_deployment=cross_product_deployment,
                                                                      compute_pool_id=compute_pool_id, 
                                                                      table_name=start_node.table_name, 
                                                                      expected_product_name=start_node.product_name)
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary = report_mgr.build_summary_from_execution_plan(execution_plan, compute_pool_list)
        table_report = report_mgr.build_TableReport(start_node.product_name)
        if execute_plan:
            print(f"Executing plan: {summary}")
            accept_exceptions= [True if "sources" in directory else False]
            _execute_plan(execution_plan, compute_pool_id, accept_exceptions=accept_exceptions)
            execution_time = (time.perf_counter() - start_time)
            print(f"Execution time: {execution_time} seconds")
            summary+=f"\nExecution time: {execution_time} seconds"
            
            for node in execution_plan.nodes:
                table_info = report_mgr.build_TableInfo(node)
                table_report.tables.append(table_info)
            summary+="\n"+f"#"*40 + f" Deployed {count} tables " + "#"*40 + "\n"
        return summary, table_report
    else:
        return "Nothing run. Do you have a pipeline_definition.json files", TableReport()



def report_running_flink_statements_for_a_table(
    table_name: str,
    inventory_path: str
) -> str:
    """
    Report running flink statements for a table execution plan
    """
    config = get_config()
    _, execution_plan = build_deploy_pipeline_from_table(table_name, 
                                                        inventory_path=inventory_path, 
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_descendants=False,
                                                        force_ancestors=False)
    return report_mgr.build_simple_report(execution_plan)

def report_running_flink_statements_for_all_from_directory(
    directory: str, 
    inventory_path: str
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
    table_report = report_mgr.build_TableReport(report_name)
    for root, _, files in os.walk(directory):
        if PIPELINE_JSON_FILE_NAME in files:
            file_path=root + "/" + PIPELINE_JSON_FILE_NAME
            pipeline_def = read_pipeline_definition_from_file(file_path)
            _update_table_report_with_table_info(pipeline_def, table_report)
    result = _prepare_table_report(table_report, report_name)
    return result

  

def report_running_flink_statements_for_a_product(
    product_name: str, 
    inventory_path: str
) -> str:
    """
    Report running flink statements for all the pipelines in the product.
    """
    table_report = report_mgr.build_TableReport(f"product:{product_name}")
    
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    for _, table_ref_dict in table_inventory.items():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref.product_name == product_name:
            file_path=table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME
            pipeline_def = read_pipeline_definition_from_file(file_path)
            _update_table_report_with_table_info(pipeline_def, table_report)
    result = _prepare_table_report(table_report, product_name)
    return result   

def full_pipeline_undeploy_from_table(
    sink_table_name: str, 
    inventory_path: str
) -> str:
    """
    Stop DML statement and drop tables: look at the parents of the current table 
    and remove the parent that has one running child. Delete all the children of the current table.
    """
    logger.info("\n"+"#"*20 + f"\n# Full pipeline delete from table {sink_table_name}\n" + "#"*20)
    start_time = time.perf_counter()
    sink_table_pipeline_def: FlinkTablePipelineDefinition = pipeline_mgr.get_pipeline_definition_for_table(sink_table_name, inventory_path)
    config = get_config()
    summary, execution_plan = build_deploy_pipeline_from_table(table_name=sink_table_pipeline_def.table_name,  
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_descendants=True,
                                                        force_ancestors=True)
    config = get_config()
    trace = f"Full pipeline delete from table {sink_table_name}\n"
    print(f"{trace}")
    for node in reversed(execution_plan.nodes):
        statement_mgr.delete_statement_if_exists(node.dml_statement_name)
        rep= statement_mgr.drop_table(node.table_name, node.compute_pool_id)
        trace+=f"Dropped table {node.table_name} with result: {rep}\n"
        print(f"Dropped table {node.table_name}")
    execution_time = time.perf_counter() - start_time
    logger.info(f"Done in {execution_time} seconds to undeploy pipeline from table {sink_table_name}")
    return trace

def full_pipeline_undeploy_from_product(product_name: str, inventory_path: str, compute_pool_id: str = None) -> str:
    """
    To undeploy we need to build an integrated execution plan for all the tables in the product.
    Undeploy in the reverse order of the execution plan, but keep table that have other product as children
    """
    compute_pool_id = compute_pool_id or get_config()['flink']['compute_pool_id']
    inventory_path = inventory_path or os.getenv("PIPELINES")
    start_time = time.perf_counter()
    nodes_to_process = []
    combined_node_map = {}
    count=0
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    for table_name, table_ref_dict in table_inventory.items():
        table_ref = FlinkTableReference(**table_ref_dict)
        if table_ref.product_name == product_name:
            node = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME).to_node()
            nodes_to_process.append(node)
            # Build the static graph from the Flink statement relationship
            combined_node_map |= _build_statement_node_map(node)
            count+=1
    if count > 0:            
        ancestors = _build_topological_sorted_parents(nodes_to_process, combined_node_map)
        start_node = ancestors[0]
        execution_plan = _build_execution_plan_using_sorted_ancestors(ancestors=ancestors, 
                                                                      node_map=combined_node_map, 
                                                                      force_ancestors=False, 
                                                                      may_start_descendants=True, 
                                                                      cross_product_deployment=False,
                                                                      compute_pool_id=compute_pool_id, 
                                                                      table_name=start_node.table_name, 
                                                                      expected_product_name=start_node.product_name)
       
        execution_plan.nodes.reverse()
        print(f"Integrated execution plan for {product_name} with {len(execution_plan.nodes)} nodes")
        for node in execution_plan.nodes:
            print(f"Table: {report_mgr.pad_or_truncate(node.table_name, 40)} product: {report_mgr.pad_or_truncate(node.product_name, 40)} {'RUNNING' if node.is_running() else 'NOT RUNNING'} {node.compute_pool_id}")
            
        count=0
        trace = f"Full pipeline delete from product {product_name}\n"
        
        # Filter nodes that need to be processed
        nodes_to_drop = [node for node in execution_plan.nodes if node.product_name == product_name and node.is_running()]
        
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
        count = len(nodes_to_drop)
                
    execution_time = time.perf_counter() - start_time
    print(f"Done in {execution_time} seconds to undeploy pipeline from product {product_name}")
    return trace


def prepare_tables_from_sql_file(sql_file_name: str, 
                                 compute_pool_id: str = None):
    """
    Execute the content of the sql file, line by line as separate Flink statement. It is used to alter table. for deployment by adding the necessary comments and metadata.
    """
    config = get_config()
    compute_pool_id = compute_pool_id or config['flink']['compute_pool_id']
    transformer = statement_mgr.get_or_build_sql_content_transformer()
    with open(sql_file_name, "r") as f:
        idx=0
        for line in f:
            _, sql_out= transformer.update_sql_content(line, 
                                                       "", 
                                                       "")
            print(sql_out)
            statement_name = f"prepare-table-{idx}"
            statement = statement_mgr.post_flink_statement(compute_pool_id, 
                                                           statement_name, 
                                                           sql_out)
            while statement.status.phase != "COMPLETED":
                time.sleep(1)
                statement = statement_mgr.get_statement_info(statement_name)
            idx+=1
            statement_mgr.delete_statement_if_exists(statement_name)
#
# ------------------------------------- private APIs  ---------------------------------
#

def _drop_node_worker(node: FlinkStatementNode) -> str:
    """Worker function to drop a single node's table and statements."""
    try:
        print(f"Dropping table {node.table_name}")
        statement_mgr.delete_statement_if_exists(node.dml_statement_name)   
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
                                                 expected_product_name: str):
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
        execution_plan = _process_ancestors(ancestors, execution_plan, force_ancestors, compute_pool_id)
    
        # At this level, execution_plan.nodes has the list of ancestors from the  starting node. 
        # For each node, we need to assess if children and ancestors needs to be started. 
        # Only restart ancestors, if user forced to: The current node once it deletes its output table will reprocess
        # records from the earliest and regenerates its states and aggregations. 
        # The children needs to be restarted if the current node is stateful. 
        for node in execution_plan.nodes:
            # The execution plan nodes list may be updated by processing children. As to start a child it may be needed
            # to start a parent not yet in the execution plan.
            if node.to_run or node.to_restart:
                # the approach is to add to the execution plan all children that need to be restarted.
                for child in node.children:
                    if ((node.upgrade_mode == "Stateful" and may_start_descendants) 
                    or (node.upgrade_mode != "Stateful" and may_start_descendants and child.upgrade_mode == "Stateful")):
                        if (child not in execution_plan.nodes 
                            and (child.product_name == expected_product_name and not cross_product_deployment)):
                            node_map, child_node = _get_static_info_update_node_map(child, node_map)
                            child_node.to_restart = not child_node.to_run
                            child_node=_assign_compute_pool_id_to_node(node=child_node, compute_pool_id=compute_pool_id)
                            child_node.parents.remove(node)  # do not reprocess current node as parent of current child
                            new_ancestors = _build_topological_sorted_parents([child_node], node_map)
                            execution_plan = _process_ancestors(new_ancestors, execution_plan, force_ancestors, compute_pool_id)
                            sorted_children = _build_topological_sorted_children(child_node, node_map)
                            for _child in sorted_children:
                                _child.to_restart = not _child.to_run
                                _child = _assign_compute_pool_id_to_node(node=_child, compute_pool_id=compute_pool_id)
                            execution_plan.nodes = _merge_graphs(execution_plan.nodes, list(reversed(sorted_children)))

                    #elif child in execution_plan.nodes and  child.product_name != expected_product_name: # to remove
                    #    execution_plan.nodes.remove(child)
                            
        #execution_plan.nodes = _build_topological_sorted_parents(execution_plan.nodes, node_map)                    
        logger.info(f"Done with execution plan construction: got {len(execution_plan.nodes)} nodes")
        logger.debug(execution_plan)
        return execution_plan
    except Exception as e:
        logger.error(f"Failed to build execution plan. Error is : {str(e)}")
        raise

def _get_static_info_update_node_map(simple_node: FlinkStatementNode, 
                                     node_map: Dict[str, FlinkStatementNode]
) -> Tuple[Dict[str, FlinkStatementNode], FlinkStatementNode]:
    """
    When navigating to ancestors and assessing if children of those ancestors have to be started,
    we need to get the static info of the current table and may be modify the node map content
    """
    try:
        if not simple_node.table_name in node_map:
            logger.info(f"Building node map for {simple_node.table_name} len(node_map): {len(node_map)}")
            node_map |= _build_statement_node_map(simple_node)
        node = node_map[simple_node.table_name]
        node = _get_and_update_statement_info_compute_pool_id_for_node(node)
        return node_map, node
    except Exception as e:
        logger.error(f"Failed to build node map for {simple_node.table_name}. Error is : {str(e)}")
        raise e

    

def _process_ancestors(ancestors: List[FlinkStatementNode], 
                       execution_plan: FlinkStatementExecutionPlan, 
                       force_ancestors: bool,
                       compute_pool_id: str
)-> FlinkStatementExecutionPlan:
    """
    Process the ancestors of the current node. Ancestor needs to be started
    if it is not running so the current node will run successfully.
    A stateful node enforces restarting its children.
    """
    for node in ancestors:
        node = _get_and_update_statement_info_compute_pool_id_for_node(node)
        if node.is_running():
            node.to_run = force_ancestors
        else:
            node.to_run = True
        if node.to_run and node.upgrade_mode == "Stateful": 
            node.update_children = True
        if node.to_run and not node.compute_pool_id:
            node = _assign_compute_pool_id_to_node(node, compute_pool_id)
        if node not in execution_plan.nodes:
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
    Returns a dictionary of unique ancestor and a list of <table_name, ancestor> tuple 
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
                node_map, enriched_parent = _get_static_info_update_node_map(parent, node_map)
                ancestors[parent.table_name] = enriched_parent
                visited.add(enriched_parent)
                queue.append(enriched_parent)
            #if parent not in ancestors:  # Ensure parent itself is unique in the set
            #     ancestors[parent.table_name] = parent
    ancestors[start_node.table_name] = start_node
    # List of tuple <table_name, parent> for each parent of a node, will help to count the number of incoming edges
    # for each node in the topological sort. The ancestor dependencies has one record per table_name, parent tuple.
    # a node with 3 ancestors will have 3 records in the ancestor dependencies list.
    ancestor_dependencies = []

    def _add_parent_dependencies(node: FlinkStatementNode, node_map: dict, new_ancestors: dict) -> None:
        """Add the <table-name, parent> tuple to the ancestor dependencies list. Update the node_map to be
        sur it gets alls the static info of the node and its parents"""
        node_map, enriched_node = _get_static_info_update_node_map(node, node_map)
        for parent in enriched_node.parents:
            ancestor_dependencies.append((node.table_name, parent))
            if parent.table_name not in new_ancestors:
                new_ancestors[parent.table_name] = parent
            _add_parent_dependencies(parent, node_map, new_ancestors)


    new_ancestors = ancestors.copy()
    for node in ancestors.values():
        _add_parent_dependencies(node, node_map, new_ancestors)
    ancestors.update(new_ancestors)
    return ancestors, ancestor_dependencies


def _build_topological_sorted_parents(current_nodes: List[FlinkStatementNode], 
                                      node_map: Dict[str, FlinkStatementNode])-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG of the curent node parents
    the node_map is a hashmap of table name and direct static relationships of the table with its parents and children
    For each node compute the subgraph to reach other nodes to compute the dependencies weights.
    """
    ancestor_nodes = {}
    ancestor_dependencies = []
    for current_node in current_nodes:
        nodes, dependencies = _get_ancestor_subgraph(current_node, node_map)
        ancestor_nodes.update(nodes)
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
        node_map, enriched_node = _get_static_info_update_node_map(node, node_map)
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
    Update node with current statement info.
    
    Args:
        node: Node to update
        
    Returns:
        Updated node with existing_statement_info field getting the retrieved statement info
        and compute_pool_id and compute_pool_name fields getting the values from the statement info
    """
    node.existing_statement_info = statement_mgr.get_statement_status_with_cache(node.dml_statement_name)
    if isinstance(node.existing_statement_info, StatementInfo) and node.existing_statement_info.compute_pool_id:
        node.compute_pool_id = node.existing_statement_info.compute_pool_id
        node.compute_pool_name = node.existing_statement_info.compute_pool_name
        node.created_at = node.existing_statement_info.created_at
    else:
        logger.warning(f"Statement {node.existing_statement_info} is not a StatementInfo")
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


def _assign_compute_pool_id_to_node(node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
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
    # get the list of compute pools available in the environment that match the table name
    pools=compute_pool_mgr.search_for_matching_compute_pools(table_name=node.table_name)
    # If we don't have any matching compute pool, we need to find a pool to use
    if  not pools or len(pools) == 0:
        logger.info(f"No matching compute pool found for {node.table_name}")
        # assess user's parameter for compute pool id
        if compute_pool_id and compute_pool_mgr.is_pool_valid(compute_pool_id):
            node.compute_pool_id = compute_pool_id
            node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
        else:
            # assess compute pool id from config.yaml
            # configured_compute_pool_id = get_config()['flink']['compute_pool_id']
            #if configured_compute_pool_id and compute_pool_mgr.is_pool_valid(configured_compute_pool_id):
            #    node.compute_pool_id = configured_compute_pool_id
            #    node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
            #else:
            logger.info(f"Create compute pool {node.compute_pool_name} for {node.table_name} ... it may take a while")
            node.compute_pool_id, node.compute_pool_name =compute_pool_mgr.create_compute_pool(node.table_name)
           
        return node
    if len(pools) == 1:
        # matching pool found, assess capacity  
        if compute_pool_mgr.is_pool_valid(pools[0].id):
            node.compute_pool_id = pools[0].id
            node.compute_pool_name = pools[0].name  
        else:
            configured_compute_pool_id = get_config()['flink']['compute_pool_id']
            if configured_compute_pool_id and compute_pool_mgr.is_pool_valid(configured_compute_pool_id):
                node.compute_pool_id = configured_compute_pool_id
                node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
            else:
                raise Exception(f"Compute pool {pools[0].name} is not available for {node.table_name}")
        return node
    # more than one? let use the configured compute pool id if it is valid
    configured_compute_pool_id = get_config()['flink']['compute_pool_id']
    if configured_compute_pool_id and compute_pool_mgr.is_pool_valid(configured_compute_pool_id):
        node.compute_pool_id = configured_compute_pool_id
        node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
    else:
        raise Exception(f"Compute pool {configured_compute_pool_id} is not available for {node.table_name}")
    return node



def _execute_plan(plan: FlinkStatementExecutionPlan, 
                  compute_pool_id: str, 
                  accept_exceptions: bool = False) -> List[Statement]:
    """Execute statements in the execution plan.
    
    Args:
        plan: Execution plan containing nodes to execute
        compute_pool_id: ID of the compute pool to use
        
    Returns:
        List of deployed statements
        
    Raises:
        RuntimeError: If statement execution fails
    """
    logger.info(f"--- Execution Plan for {plan.start_table_name} started ---")
    print(f"--- Execution Plan for {plan.start_table_name} started ---")
    statements = []
    max_workers = multiprocessing.cpu_count()
    nodes_to_execute = _get_nodes_to_execute(plan.nodes)
    autonomous_nodes = _build_autonomous_nodes(plan.nodes)
    while len(nodes_to_execute) > 0:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(_deploy_one_node, node, accept_exceptions, compute_pool_id) for node in autonomous_nodes]
            for future in as_completed(futures):
                statements.append(future.result())
        for node in autonomous_nodes:
            node.to_run = False
            node.to_restart = False
        nodes_to_execute = _get_nodes_to_execute(plan.nodes)
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

def _build_autonomous_nodes(nodes: List[FlinkStatementNode]) -> List[FlinkStatementNode]:
    """
    Build a list of autonomous nodes.
    """
    autonomous_nodes = []
    for node in nodes:
        if node.parents:
            for p in node.parents:
                for n in nodes:
                    if n.table_name == p:
                        if not (n.to_run or n.to_restart) and node not in autonomous_nodes:
                            autonomous_nodes.append(node)
                            break
        else:
            if (node.to_run or node.to_restart) and node not in autonomous_nodes:
                autonomous_nodes.append(node)
    return autonomous_nodes

def _deploy_one_node(node: FlinkStatementNode,accept_exceptions: bool = False, compute_pool_id: str = None)-> Statement:
    if not node.compute_pool_id:
            node.compute_pool_id = compute_pool_id
            
    logger.info(f"Deploy table: '{node.table_name}'")
    print(f"Deploy table: '{node.table_name}' using Flink: {node.dml_statement_name}")
    try:
        if not node.dml_only:
            statement = _deploy_ddl_dml(node)
        else:
            statement = _deploy_dml(node, False)
        node.existing_statement_info = statement_mgr.map_to_statement_info(statement) 
        return statement
    except Exception as e:
        if not accept_exceptions:
            logger.error(f"Failed to execute statement {node.dml_statement_name}: {str(e)}")
            raise RuntimeError(f"Statement execution failed: {str(e)}")
        else:
            logger.info(f"Statement execution failed: {str(e)}, move to next node")
    

def _deploy_ddl_dml(node_to_process: FlinkStatementNode)-> Statement:
    """
    Deploy the DDL and then the DML for the given table to process.
    """

    logger.debug(f"{node_to_process.ddl_ref} to {node_to_process.compute_pool_id}, first delete dml statement")
    statement_mgr.delete_statement_if_exists(node_to_process.dml_statement_name)
    statement_mgr.delete_statement_if_exists(node_to_process.ddl_statement_name)
    rep= statement_mgr.drop_table(node_to_process.table_name, node_to_process.compute_pool_id)
    logger.info(f"Dropped table {node_to_process.table_name} status is : {rep}")

    statement = statement_mgr.build_and_deploy_flink_statement_from_sql_content(node_to_process, 
                                                                            node_to_process.ddl_ref, 
                                                                            node_to_process.ddl_statement_name)
    while statement.status.phase in ["PENDING"]:
        time.sleep(1)
        statement = statement_mgr.get_statement(node_to_process.ddl_statement_name)
        logger.debug(f"DDL deployment status is: {statement.status.phase}")
    if statement.status.phase in ["FAILED", "FAILING"]:
        raise RuntimeError(f"DDL deployment failed for {node_to_process.table_name}")
    return _deploy_dml(node_to_process, True)


def _deploy_dml(to_process: FlinkStatementNode, dml_already_deleted: bool= False)-> Statement:
    logger.info(f"Run {to_process.dml_statement_name} for {to_process.table_name} table to {to_process.compute_pool_id}")
    if not dml_already_deleted:
        statement_mgr.delete_statement_if_exists(to_process.dml_statement_name)
    
    statement = statement_mgr.build_and_deploy_flink_statement_from_sql_content(to_process, 
                                                                                to_process.dml_ref, 
                                                                                to_process.dml_statement_name)
    compute_pool_mgr.save_compute_pool_info_in_metadata(to_process.dml_statement_name, to_process.compute_pool_id)
    while statement.status.phase in ["PENDING"]:
        time.sleep(5)
        statement = statement_mgr.get_statement(to_process.dml_statement_name)
        logger.debug(f"DML deployment status is: {statement.status.phase}")
    if statement.status.phase == "FAILED":
        raise RuntimeError(f"DML deployment failed for {to_process.table_name}")
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
        

def _build_statement_node_map(current_node: FlinkStatementNode) -> dict[str,FlinkStatementNode]:
    """
    Define the complete static graph of the related parents and children for the current node. It uses a DFS
    to reach all parents, and then a BFS to construct the list of reachable children.
    The graph built has no loop.
    """
    logger.debug(f"start build tables static graph for {current_node.table_name}")
    visited_nodes = set()
    node_map = {}   # <k: str, v: FlinkStatementNode> use a map to search for table name as key.
    queue = deque()  # Queue for BFS processing of nodes

    def _update_parents_of_parent(current: FlinkStatementNode, parent_name: str, grand_parents: List[FlinkStatementNode]):
        for parent in current.parents:
            if parent.table_name == parent_name:
                parent.parents = grand_parents
                break
    def _search_parent_from_current(list_of_parents: dict[str, FlinkStatementNode], 
                                    current: FlinkStatementNode, 
                                    visited_nodes: Set[FlinkStatementNode]):
        if current not in visited_nodes:
            visited_nodes.add(current)
            for p in current.parents:
                pipe_def = read_pipeline_definition_from_file( p.path + "/" + PIPELINE_JSON_FILE_NAME)
                if pipe_def:
                    node_p = pipe_def.to_node()
                    if node_p not in visited_nodes:
                        node_p_parents = _search_parent_from_current(list_of_parents, node_p, visited_nodes)
                        _update_parents_of_parent(current, node_p.table_name, node_p_parents)
                        list_of_parents[node_p.table_name] = node_p
                        queue.append(node_p)  # Add new nodes to the queue for processing
                else:
                    logger.warning(f"Data consistency issue for {p.path}: no pipeline definition found or wrong reference in {current.table_name}. The execution plan may not deploy successfuly")
        return current.parents       

    def _search_children_from_current(node_map: dict[str, FlinkStatementNode], 
                                      current: FlinkStatementNode, 
                                      visited_nodes: Set[FlinkStatementNode],
                                      matching_product_name: str):
        for c in current.children:
            if (c.product_name == matching_product_name 
                and c.table_name not in node_map): 
                # a child may have been a parent of another node so do not need to process it
                pipe_def = read_pipeline_definition_from_file( c.path + "/" + PIPELINE_JSON_FILE_NAME)
                node_c = pipe_def.to_node()
                if node_c not in visited_nodes and _accepted_to_process(current, node_c):
                    _search_parent_from_current(node_map, node_c, visited_nodes)
                    _search_children_from_current(node_map, node_c, visited_nodes, matching_product_name)
                    node_map[node_c.table_name] = node_c
            

    _search_parent_from_current(node_map, current_node, visited_nodes)
    node_map[current_node.table_name] = current_node
    visited_nodes.add(current_node)
    queue.append(current_node)  # Start with the current node

    # Process nodes using BFS
    while queue:
        current = queue.popleft()
        _search_children_from_current(node_map, current, visited_nodes, current_node.product_name)
    logger.debug(f"End build table graph for {current_node.table_name} with {len(node_map)} nodes")
    logger.debug("\n\n".join("{}\t{}".format(k,v) for k,v in node_map.items()))
    return node_map



def _accepted_to_process(current: FlinkStatementNode, node: FlinkStatementNode) -> bool:
    """
    Validate if a node should be processed based on its relationship with the current node.
    Prevents processing nodes that would create circular dependencies.
    """
    return node.product_name == current.product_name

def _update_table_report_with_table_info(pipeline_def: FlinkTablePipelineDefinition, table_report: TableReport):
    if pipeline_def:
        node: FlinkStatementNode = pipeline_def.to_node()
        node.existing_statement_info = statement_mgr.get_statement_status_with_cache(node.dml_statement_name)    
        table_info = report_mgr.build_TableInfo(node)
        print(f"Table info: {table_info.table_name} {table_info.status} pool_id: {table_info.compute_pool_id} pending records: {table_info.pending_records}")
        table_report.tables.append(table_info)

def _prepare_table_report(table_report: TableReport, base_file_name):
    table_count=0
    running_count=0
    non_running_count=0
    csv_content= "environment_id,catalog_name,database_name,table_name,type,upgrade_mode,statement_name,status,compute_pool_id,compute_pool_name,created_at,retention_size,message_count,pending_records\n"
    for table in table_report.tables:
        csv_content+=f"{table_report.environment_id},{table_report.catalog_name},{table_report.database_name},{table.table_name},{table.type},{table.upgrade_mode},{table.statement_name},{table.status},{table.compute_pool_id},{table.compute_pool_name},{table.created_at},{table.retention_size},{table.message_count},{table.pending_records}\n"   
        if table.status == 'RUNNING':
            running_count+=1
        else:
            non_running_count+=1
        table_count+=1
    print(f"Writing report to {shift_left_dir}/{base_file_name}_report.csv and {shift_left_dir}/{base_file_name}_report.json")
    with open(f"{shift_left_dir}/{base_file_name}_report.csv", "w") as f:
        f.write(csv_content)
    with open(f"{shift_left_dir}/{base_file_name}_report.json", "w") as f:
        f.write(table_report.model_dump_json(indent=4))
    result=f"#"*120 + "\n\tEnvironment: " + get_config()['confluent_cloud']['environment_id'] + "\n"
    result+=f"\tCatalog: " + get_config()['flink']['catalog_name'] + "\n"
    result+=f"\tDatabase: " + get_config()['flink']['database_name'] + "\n"
    result+=csv_content
    result+="#"*120 + f"\n\tRunning tables: {running_count}" + "\n"
    result+=f"\tNon running tables: {non_running_count}" + "\n"
    return result 
# --- to work on for stateless ---------------

