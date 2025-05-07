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
from datetime import datetime
from collections import deque
from typing import Optional, List, Any, Set, Tuple, Dict, Final

from pydantic import BaseModel, Field

from shift_left.core import (
    pipeline_mgr,
    compute_pool_mgr,
    statement_mgr
)
from shift_left.core.models.flink_statement_model import (
    Statement,
    FlinkStatementNode,
    FlinkStatementExecutionPlan
)
from shift_left.core.utils.report_mgr import (
    DeploymentReport,
    TableInfo,
    TableReport,
    build_simple_report,
    build_summary_from_execution_plan
)
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.file_search import (
    PIPELINE_JSON_FILE_NAME,
    FlinkTablePipelineDefinition,
    get_ddl_dml_names_from_pipe_def,
    read_pipeline_definition_from_file
)

# Constants
MAX_CFU_INCREMENT: Final[int] = 20


def deploy_pipeline_from_table(
    table_name: str,
    inventory_path: str,
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_children: bool = False,
    force_sources: bool = False
) -> Tuple[DeploymentReport, str]:
    """Deploy a pipeline starting from a given table.
    
    Args:
        table_name: Name of the table to deploy
        inventory_path: Path to the pipeline inventory
        compute_pool_id: ID of the compute pool to use
        dml_only: Whether to only deploy DML statements
        may_start_children: Whether to start child pipelines
        force_sources: Whether to force source table deployment
        
    Returns:
        Tuple containing the deployment report and summary
        
    Raises:
        ValueError: If the not able to process the execution plan
    """
    logger.info("#"*10 + f"# Start deploying pipeline from table {table_name} " + "#"*10)
    start_time = time.perf_counter()
    
    try:
        pipeline_def = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)

        # Validate and get compute pool
        if compute_pool_id and not compute_pool_mgr.is_pool_valid(compute_pool_id):
            compute_pool_id = compute_pool_mgr.get_or_build_compute_pool(compute_pool_id, pipeline_def)

        execution_plan = build_execution_plan_from_any_table(
            pipeline_def=pipeline_def,
            compute_pool_id=compute_pool_id,
            dml_only=dml_only,
            may_start_children=may_start_children,
            force_sources=force_sources,
            start_time=datetime.now()
        )
        
        persist_execution_plan(execution_plan)
        compute_pool_list = compute_pool_mgr.get_compute_pool_list()
        summary = build_summary_from_execution_plan(execution_plan, compute_pool_list)
        logger.info(f"Execute the plan before deployment: {summary}")
        
        statements = _execute_plan(execution_plan, compute_pool_id)
        result = DeploymentReport(
            table_name=table_name,
            compute_pool_id=compute_pool_id,
            update_children=may_start_children,
            ddl_dml="DML" if dml_only else "Both",
            flink_statements_deployed=statements
        )
        
        execution_time = time.perf_counter() - start_time
        logger.debug(
            f"Done in {execution_time} seconds to deploy pipeline from table {table_name}: "
            f"{result.model_dump_json(indent=3)}"
        )
        return result, summary
        
    except Exception as e:
        logger.error(f"Failed to deploy pipeline from table {table_name}: {str(e)}")
        raise

def deploy_from_execution_plan(
    execution_plan: FlinkStatementExecutionPlan, 
    compute_pool_id: str
) -> list[Statement]:
    """
    Execute the statements in the execution plan.
    """
    return _execute_plan(execution_plan, compute_pool_id)

def load_and_deploy_from_execution_plan(execution_plan_file: str, 
                              compute_pool_id: str) -> list[Statement]:
    """
    Execute the statements in the execution plan.
    """
    with open(execution_plan_file, "r") as f:
        execution_plan = FlinkStatementExecutionPlan.model_validate_json(f.read())
    return _execute_plan(execution_plan, compute_pool_id)

def build_execution_plan_from_table_and_persist(
    table_name: str,
    inventory_path: str,
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_children: bool = False,
    force_sources: bool = False,
    start_time: Optional[datetime] = None
) -> str:
    
    pipeline_def=  pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
    execution_plan=build_execution_plan_from_any_table(pipeline_def=pipeline_def,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_children=may_start_children,
                                                        force_sources=force_sources,
                                                        start_time=start_time)
    persist_execution_plan(execution_plan)
    compute_pool_list = compute_pool_mgr.get_compute_pool_list()
    summary=build_summary_from_execution_plan(execution_plan, compute_pool_list)
    return summary

def build_execution_plan_from_any_table(
    pipeline_def: FlinkTablePipelineDefinition,
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_children: bool = False,
    force_sources: bool = False,
    start_time: Optional[datetime] = None
) -> FlinkStatementExecutionPlan:
    """Build an execution plan from the static relationship between Flink Statements.
    
    Args:
        pipeline_def: Pipeline definition containing table information
        compute_pool_id: ID of the compute pool to use
        dml_only: Whether to only deploy DML statements
        may_start_children: Whether to start child pipelines
        force_sources: Whether to force source table deployment
        start_time: Optional start time for the execution plan
        
    Returns:
        FlinkStatementExecutionPlan containing the execution plan
        
    Raises:
        ValueError: If the execution plan cannot be built
    """
    logger.info(f"Build execution plan for {pipeline_def.table_name}")
    
    try:
        start_node = pipeline_def.to_node()
        start_time = start_time or datetime.now()
        start_node.created_at = start_time
        start_node.dml_only = dml_only
        start_node = _assign_compute_pool_id_to_node(node=start_node, compute_pool_id=compute_pool_id)
        start_node.update_children = may_start_children

        # Build the static graph from the Flink statement relationship
        node_map = _build_statement_node_map(start_node)
        # TODO assess if we need to reload a persisted execution plan
        execution_plan = FlinkStatementExecutionPlan(
            created_at=start_time,
            environment_id=get_config()['confluent_cloud']['environment_id'],
            start_table_name=pipeline_def.table_name
        )

        ancestors = []
        visited_nodes = set()
        start_node = _get_and_update_statement_info_for_node(start_node)
        ancestors = _build_topological_sorted_parents(start_node, node_map)

        # Process all parents and grandparents reachable by DFS from start_node. Ancestors may not be
        # in the same product family as the start_node. The ancestor list is sorted so first node needs to run first
        for node in ancestors:
            node = _get_and_update_statement_info_for_node(node)
            if node.is_running():
                if node.type == "source":
                    node.to_run = force_sources
                else:
                    node.to_run = False
                node.to_restart = False
            else:
                node.to_run = True
                node.to_restart = False
            if node.to_run and node.upgrade_mode == "Stateful":
                node.update_children = may_start_children
            if node.to_run and not node.compute_pool_id:
                node = _assign_compute_pool_id_to_node(node, compute_pool_id)
            if node.product_name == start_node.product_name or node.product_name in ['', 'common', 'stage']:
                execution_plan.nodes.append(node)       

        start_node.to_restart = True
        
        # When may_start_children, restart all children of each ancestor node that needs to be restarted or run.
        for node in execution_plan.nodes:
            if node.type == "source" and node.is_running():  # the list of nodes may have been changed so this is needed again.
                node.to_run = force_sources
            if node.to_run or node.to_restart:
                for child in node.children:
                    if (child not in execution_plan.nodes and
                        node.update_children and
                        child.product_name == start_node.product_name):
                            child_node = node_map[child.table_name]
                            child_node = _get_and_update_statement_info_for_node(child_node)
                            child_node.to_restart = node.update_children
                            child_node=_assign_compute_pool_id_to_node(node=child_node, compute_pool_id=compute_pool_id)
                            if node.update_children:
                                child_node.parents.remove(node)  # do not reprocess parent of current child
                                _merge_graphs(execution_plan.nodes, _build_topological_sorted_parents(child_node, node_map))
                                _merge_graphs(execution_plan.nodes, _build_topological_sorted_children(child_node, node_map))
                    elif child in execution_plan.nodes and  child.product_name != start_node.product_name:
                        execution_plan.nodes.remove(child)
                            
                            

        logger.info(f"Done with execution plan construction: got {len(execution_plan.nodes)} nodes")
        logger.debug(execution_plan)
        return execution_plan
        
    except Exception as e:
        logger.error(f"Failed to build execution plan: {str(e)}")
        raise

def report_running_flink_statements_for_a_table_execution_plan(
    table_name: str, 
    inventory_path: str
) -> str:
    """
    Report running flink statements for a table execution plan
    """
    config = get_config()
    pipeline_def: FlinkTablePipelineDefinition = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
    execution_plan: FlinkStatementExecutionPlan = build_execution_plan_from_any_table(pipeline_def, 
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_children=False, 
                                                        start_time=datetime.now(),
                                                        force_sources=False)
    return build_simple_report(execution_plan)

def report_running_flink_statements_for_all_from_directory(
    directory: str, 
    inventory_path: str, 
) -> str:
    """
    Review execution plan for all the pipelines in the directory.
    """
    result = "#"*120 + "\n\tEnvironment: " + get_config()['confluent_cloud']['environment_id'] + "\n"
    result+= "\tCatalog: " + get_config()['flink']['catalog_name'] + "\n"
    result+= "\tDatabase: " + get_config()['flink']['database_name'] + "\n"
    config = get_config()
    count=0
    for root, _, files in os.walk(directory):
        if PIPELINE_JSON_FILE_NAME in files:
            file_path=root + "/" + PIPELINE_JSON_FILE_NAME
            pipeline_def = read_pipeline_definition_from_file(file_path)
            logger.info(f"Build report of running flink statements for {pipeline_def.table_name}")
            result+= "#"*40 + f" Table: {pipeline_def.table_name} " + "#"*40 + "\n"
            execution_plan: FlinkStatementExecutionPlan = build_execution_plan_from_any_table(pipeline_def, 
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_children=False, 
                                                        start_time=datetime.now(),
                                                        force_sources=False)
            result+= build_simple_report(execution_plan) + "\n"
            count+=1
    result+=f"#"*40 + f" Found {count} tables with running flink statements " + "#"*40 + "\n"
    return result

def report_running_flink_statements_for_all_from_product(
    product_name: str, 
    inventory_path: str
) -> str:
    """
    Report running flink statements for all the pipelines in the product.
    """
    table_report = TableReport()
    table_report.product_name = product_name
    table_report.environment_id = get_config().get('environment_id')
    table_report.catalog_name = get_config().get('flink').get('catalog_name')
    table_report.database_name = get_config().get('flink').get('database_name')
    compute_pool_list = compute_pool_mgr.get_compute_pool_list()
    
    for root, dirs, files in os.walk(inventory_path):
        if product_name in root:
            if 'sql-scripts' in dirs:
                file_path=root + "/" + PIPELINE_JSON_FILE_NAME
                pipeline_def = read_pipeline_definition_from_file(file_path)
                if pipeline_def:
                    node: FlinkStatementNode = pipeline_def.to_node()
                    node.existing_statement_info = statement_mgr.get_statement_status(node.dml_statement_name)
                    
                    table_info = TableInfo()
                    table_info.table_name = node.table_name
                    table_info.type = node.type
                    table_info.upgrade_mode = node.upgrade_mode
                    table_info.statement_name = node.dml_statement_name
                    table_info.status = node.existing_statement_info.status_phase
                    table_info.compute_pool_id = node.existing_statement_info.compute_pool_id
                    pool = compute_pool_mgr.get_compute_pool_with_id(compute_pool_list, table_info.compute_pool_id)
                    if pool:
                        table_info.compute_pool_name = pool.name
                    else:
                        table_info.compute_pool_name = "UNKNOWN"
                    table_info.created_at = node.existing_statement_info.created_at
                    table_report.tables.append(table_info)
    table_count=0
    running_count=0
    non_running_count=0
    csv_content= "environment_id,catalog_name,database_name,table_name,type,upgrade_mode,statement_name,status,compute_pool_id,compute_pool_name,created_at\n"
    for table in table_report.tables:
        csv_content+=f"{table_report.environment_id},{table_report.catalog_name},{table_report.database_name},{table.table_name},{table.type},{table.upgrade_mode},{table.statement_name},{table.status},{table.compute_pool_id},{table.compute_pool_name},{table.created_at}\n"
        if table.status == 'RUNNING':
            running_count+=1
        else:
            non_running_count+=1
        table_count+=1
    print(csv_content)
    print(f"Total tables: {table_count}")
    print(f"Running tables: {running_count}")
    print(f"Non running tables: {non_running_count}")
    with open(f"{shift_left_dir}/{product_name}_report.csv", "w") as f:
        f.write(csv_content)
    with open(f"{shift_left_dir}/{product_name}_report.json", "w") as f:
        f.write(table_report.model_dump_json(indent=4))
    result=f"#"*120 + "\n\tEnvironment: " + get_config()['confluent_cloud']['environment_id'] + "\n"
    result+=f"\tCatalog: " + get_config()['flink']['catalog_name'] + "\n"
    result+=f"\tDatabase: " + get_config()['flink']['database_name'] + "\n"
    result+=csv_content
    result+="#"*120 + f"\n\tRunning tables: {running_count}" + "\n"
    result+=f"\tNon running tables: {non_running_count}" + "\n"
    return result   


def deploy_all_from_directory(
    directory: str, 
    inventory_path: str, 
    compute_pool_id: str,
    dml_only: bool = False,
    may_start_children: bool = False,
    force_sources: bool = False
) -> str:
    """
    Deploy all the pipelines in the directory.
    """
    result = "#"*120 + "\n\tEnvironment: " + get_config()['confluent_cloud']['environment_id'] + "\n"
    result+= "\tCatalog: " + get_config()['flink']['catalog_name'] + "\n"
    result+= "\tDatabase: " + get_config()['flink']['database_name'] + "\n"
    count=0
    for root, _, files in os.walk(directory):
        if PIPELINE_JSON_FILE_NAME in files:
            file_path=root + "/" + PIPELINE_JSON_FILE_NAME
            pipe_def = read_pipeline_definition_from_file(file_path)
            logger.info(f"Deploying pipeline from table {pipe_def.table_name}")
            result+= "#"*40 + f" Deploy table: {pipe_def.table_name} " + "#"*40 + "\n"
            report, summary = deploy_pipeline_from_table(table_name=pipe_def.table_name,
                                                        inventory_path=inventory_path,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_children=may_start_children,
                                                        force_sources=force_sources)
            result+=summary + "\n" + "#"*100 + "\n"
            count+=1
           
    result+=f"#"*40 + f" Deployed {count} tables " + "#"*40 + "\n"
    return result



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
    pipeline_def: FlinkTablePipelineDefinition = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
    config = get_config()
    execution_plan: FlinkStatementExecutionPlan = build_execution_plan_from_any_table(pipeline_def, 
                                                        compute_pool_id=config['flink']['compute_pool_id'],
                                                        dml_only=False,
                                                        may_start_children=True, 
                                                        start_time=datetime.now())
    config = get_config()
    trace = f"Full pipeline delete from table {table_name}\n"
    for node in reversed(execution_plan.nodes):
        # start in 
        r=_delete_not_shared_parent(pipeline_def, trace, config)
        execution_time = time.perf_counter() - start_time
        logger.info(f"Done in {execution_time} seconds to undeploy pipeline from table {table_name} with result: {r}")
        return r



def persist_execution_plan(execution_plan: FlinkStatementExecutionPlan):
    """
    Persist the execution plan to a JSON file, handling circular references.
    
    Args:
        execution_plan: The execution plan to persist
    """
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


#
# ------------------------------------- private APIs  ---------------------------------
#

        
def _get_ancestor_subgraph(start_node: FlinkStatementNode, node_map)-> Tuple[Dict[str, FlinkStatementNode], 
                                                         Dict[str, List[FlinkStatementNode]]]:
    """Builds a subgraph containing all ancestors of the start node."""
    ancestors = {}
    queue = deque([start_node])
    visited = {start_node}

    while queue:
        current_node = queue.popleft()
        for parent in current_node.parents:
            if parent not in visited:
                ancestors[parent.table_name] = parent
                visited.add(parent)
                queue.append(parent)
            if parent not in ancestors:  # Ensure parent itself is in the set
                 ancestors[parent.table_name] = parent
    ancestors[start_node.table_name] = start_node
    # Include dependencies within the ancestor subgraph
    ancestor_dependencies = []

    def _add_parent_dependencies(table_name: str, node_map: dict, new_ancestors: dict) -> None:
        node = node_map[table_name]
        for parent in node.parents:
            ancestor_dependencies.append((table_name, parent))
            if parent.table_name not in new_ancestors:
                new_ancestors[parent.table_name] = parent
            _add_parent_dependencies(parent.table_name, node_map, new_ancestors)


    new_ancestors = ancestors.copy()
    for table_name in ancestors.keys():
        _add_parent_dependencies(table_name, node_map, new_ancestors)
    ancestors.update(new_ancestors)
    return ancestors, ancestor_dependencies


def _build_topological_sorted_parents(current_node: FlinkStatementNode, 
                                      node_map: Dict[str, FlinkStatementNode])-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG of the curent node parents"""
    ancestor_nodes, ancestor_dependencies = _get_ancestor_subgraph(current_node, node_map)
    return _topological_sort(current_node, ancestor_nodes, ancestor_dependencies)

def _topological_sort(
    current_node: FlinkStatementNode, 
    nodes: Dict[str, FlinkStatementNode], 
    dependencies: Dict[str, List[FlinkStatementNode]]
)-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG of the current node ancestors using Kahn Algorithm"""

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


def _get_descendants_subgraph(start_node: FlinkStatementNode, node_map: Dict[str, FlinkStatementNode])-> Tuple[Dict[str, FlinkStatementNode], 
                                                         Dict[str, List[FlinkStatementNode]]]:
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

    def _add_child_dependencies(table_name: str, node_map: dict, new_descendants: dict) -> None:
        node = node_map[table_name]
        for child in node.children:
            descendant_dependencies.append((table_name, child))
            if child.table_name not in new_descendants:
                new_descendants[child.table_name] = child
            _add_child_dependencies(child.table_name, node_map, new_descendants)


    new_descendants = descendants.copy()
    for table_name in descendants.keys():
        _add_child_dependencies(table_name, node_map, new_descendants)
    descendants.update(new_descendants)
    return descendants, descendant_dependencies


def _build_topological_sorted_children(current_node: FlinkStatementNode, node_map: Dict[str, FlinkStatementNode])-> List[FlinkStatementNode]:
    """Performs topological sort on a DAG of the curent node"""
    nodes, dependencies = _get_descendants_subgraph(current_node, node_map)
    return _topological_sort(current_node, nodes, dependencies)



def _get_and_update_statement_info_for_node(node: FlinkStatementNode) -> FlinkStatementNode:
    """
    Update node with current statement info.
    
    Args:
        node: Node to update
        
    Returns:
        Updated node
    """
    node.existing_statement_info = statement_mgr.get_statement_status(node.dml_statement_name)
    if node.existing_statement_info.compute_pool_id:
        node.compute_pool_id = node.existing_statement_info.compute_pool_id
        node.compute_pool_name = node.existing_statement_info.compute_pool_name
    return node 

def _merge_graphs(in_out_graph, in_graph) -> dict[str, FlinkStatementNode]:
    """
    It may be possible while navigating to the children that some parent of those children are not part
    of the current graph, so there is a need to merge the graph
    """
    for node in in_graph:
        if node not in in_out_graph:
            in_out_graph.append(node)
    return in_out_graph

def _add_non_running_parents(node, execution_plan, node_map):
    for p in node.parents:
        node_p = node_map[p.table_name]
        node_p = _get_and_update_statement_info_for_node(node_p)
        if not node_p.is_running() and not node_p.to_run and node_p not in execution_plan:
            # position the parent before the node to be sure it is started before it
            idx=execution_plan.nodes.index(node)
            execution_plan.nodes.insert(idx,node_p)
            node_p.to_run = True

def _assign_compute_pool_id_to_node(node: FlinkStatementNode, compute_pool_id: str) -> FlinkStatementNode:
    
    # If the node already has an assigned compute pool, continue using that
    if node.compute_pool_id and compute_pool_mgr.is_pool_valid(node.compute_pool_id):  # this may be loaded from the statement info
        return node

    compute_pool_list = compute_pool_mgr.get_compute_pool_list(get_config().get('confluent_cloud').get('environment'), 
                                              get_config().get('confluent_cloud').get('region'))
    
    pools=compute_pool_mgr.search_for_matching_compute_pools(compute_pool_list=compute_pool_list,
                                      table_name=node.table_name)
    
    # If we don't have any matching compute pools, we need to find a pool to use
    if  not pools:
        configured_compute_pool_id = get_config()['flink']['compute_pool_id']
        # If the config has a valid compute pool, use that one
        if configured_compute_pool_id and compute_pool_mgr.is_pool_valid(configured_compute_pool_id):
            node.compute_pool_id = configured_compute_pool_id
        else:
            node.compute_pool_id =compute_pool_mgr.create_compute_pool(node.table_name)
        node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
        return node
    if len(pools) == 1:
        node.compute_pool_id = pools[0].id
        node.compute_pool_name = pools[0].name
        return node
    # If we supply a specific compute_pool_id, use that if it's valid
    if compute_pool_id and compute_pool_mgr.is_pool_valid(compute_pool_id):
        node.compute_pool_id = compute_pool_id
        node.compute_pool_name = compute_pool_mgr.get_compute_pool_name(node.compute_pool_id)
    return node



def _execute_plan(plan: FlinkStatementExecutionPlan, compute_pool_id: str) -> List[Statement]:
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
    statements = []
    
    try:
        for node in plan.nodes:
            logger.info(f"Processing table: '{node.table_name}'")
            
            if not node.compute_pool_id:
                node.compute_pool_id = compute_pool_id
                
            if node.to_run or node.to_restart:
                logger.debug(f"Execute statement '{node.dml_statement_name}' with dml only: {node.dml_only}")
                try:
                    if not node.dml_only:
                        statement = _deploy_ddl_dml(node)
                    else:
                        statement = _deploy_dml(node, False)
                    node.existing_statement_info = statement
                    statements.append(statement)
                except Exception as e:
                    logger.error(f"Failed to execute statement {node.dml_statement_name}: {str(e)}")
                    raise RuntimeError(f"Statement execution failed: {str(e)}")
            else:
                logger.info(f"No restart or no to_run, {node.dml_statement_name} already running!")
                
        return statements
        
    except Exception as e:
        logger.error(f"Failed to execute plan: {str(e)}")
        raise



def _deploy_ddl_dml(node_to_process: FlinkStatementNode)-> Statement:
    """
    Deploy the DDL and then the DML for the given table to process.
    """

    logger.debug(f"{node_to_process.ddl_ref} to {node_to_process.compute_pool_id}, first delete dml statement")
    statement_mgr.delete_statement_if_exists(node_to_process.dml_statement_name)
    statement_mgr.delete_statement_if_exists(node_to_process.ddl_statement_name)
    rep= statement_mgr.drop_table(node_to_process.table_name, node_to_process.compute_pool_id)
    logger.info(f"Dropped table {node_to_process.table_name} status is : {rep}")
    try:
        statement_mgr.build_and_deploy_flink_statement_from_sql_content(node_to_process.ddl_ref, 
                                node_to_process.compute_pool_id, 
                                node_to_process.ddl_statement_name)
    except Exception as e:
        logger.error(f"Error deploying DDL for {node_to_process.table_name}: {e}")
        raise e
    return _deploy_dml(node_to_process, True)


def _deploy_dml(to_process: FlinkStatementNode, dml_already_deleted: bool= False)-> Statement:
    logger.info(f"Run {to_process.dml_statement_name} for {to_process.table_name} table to {to_process.compute_pool_id}")
    if not dml_already_deleted:
        statement_mgr.delete_statement_if_exists(to_process.dml_statement_name)
    
    statement = statement_mgr.build_and_deploy_flink_statement_from_sql_content(to_process.dml_ref, 
                                    to_process.compute_pool_id, 
                                    to_process.dml_statement_name)
    compute_pool_mgr.save_compute_pool_info_in_metadata(to_process.dml_statement_name, to_process.compute_pool_id)
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

# --- to work on for stateless ---------------

