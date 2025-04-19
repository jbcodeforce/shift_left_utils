"""
Copyright 2024-2025 Confluent, Inc.

Manage deployment of a pipelines. Support building execution plans
and then execute them.
The execution plan is a graph of Flink statements to be executed.
The graph is built from the pipeline definition and the existing deployed statements.
The execution plan is persisted to a JSON file.
The execution plan is used to execute the statements in the correct order.
The execution plan is used to undeploy a pipeline.
"""
import time
from datetime import datetime
import json
from importlib import import_module
from pydantic import BaseModel, Field
from collections import deque
from typing import Optional, List, Any, Set

import shift_left.core.pipeline_mgr as pipeline_mgr 
import shift_left.core.compute_pool_mgr as compute_pool_mgr 
import shift_left.core.table_mgr as table_mgr 
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.flink_statement_model import (
    Statement, 
    FlinkStatementNode, 
    FlinkStatementExecutionPlan
)

from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.naming_convention import DefaultDmlNameModifier, DmlNameModifier
from shift_left.core.utils.file_search import ( 
    PIPELINE_JSON_FILE_NAME,
    FlinkTableReference,
    get_table_ref_from_inventory,
    FlinkTablePipelineDefinition,
    get_ddl_dml_names_from_pipe_def,
    read_pipeline_definition_from_file
)

class DeploymentReport(BaseModel):
    table_name: Optional[str] =  Field(default=None)
    compute_pool_id: Optional[str] =  Field(default=None)
    ddl_dml:  Optional[str] =  Field(default="Both",description="The type of deployment: DML only, or both")
    update_children: Optional[bool] =  Field(default=False)
    flink_statements_deployed: List[Any]

    
def deploy_pipeline_from_table(table_name: str, 
                               inventory_path: str, 
                               compute_pool_id: str,
                               dml_only: bool = False,
                               may_start_children: bool = False ) -> DeploymentReport:
    """
    Given the table name, executes the dml and ddl to deploy a pipeline.
    If the compute pool id is present it will use it. If not it will 
    get the existing pool_id from the table already deployed, if none
    is defined it will create a new pool and assign the pool_id.

    A deployment may impact children statement depending of the semantic of the current
    DDL and the children's one. 
    A pipeline deployment start from the current_table:
        add current_table to node to process
        for each node to process
            if there is a parent not yet deployed add parent to node to process. recursive call to do a BFS
            if current table Flinkalready exist
                if only dml, redeploy dml taking into accound children
                else deploy ddl and dml
            else deploy ddl and dml
    """    
    logger.info("#"*10 + f"# Start deploying pipeline from table {table_name} " + "#"*10)
    start_time = time.perf_counter()
    pipeline_def: FlinkTablePipelineDefinition = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)

    execution_plan: FlinkStatementExecutionPlan = build_execution_plan_from_any_table(pipeline_def,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        may_start_children=may_start_children, 
                                                        start_time=datetime.now())
    persist_execution_plan(execution_plan)
    logger.info(f"{build_summary_from_execution_plan(execution_plan)}")
    statements = _execute_plan(execution_plan, compute_pool_id)
    result = DeploymentReport(table_name=table_name, 
                            compute_pool_id=compute_pool_id,
                            update_children=may_start_children,
                            ddl_dml= "DML" if dml_only else "Both",
                            flink_statements_deployed=statements)
    execution_time = time.perf_counter() - start_time
    build_summary_from_execution_plan(execution_plan)
    logger.info(f"Done in {execution_time} seconds to deploy pipeline from table {table_name}: {result.model_dump_json(indent=3)}")
    return result

def deploy_from_execution_plan(execution_plan: FlinkStatementExecutionPlan, 
                              compute_pool_id: str) -> list[Statement]:
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

def build_execution_plan_from_any_table(pipeline_def: FlinkTablePipelineDefinition, 
                               compute_pool_id: str,
                               dml_only: bool = False,
                               may_start_children: bool = False,
                               start_time= None ) -> FlinkStatementExecutionPlan:
    """
    Build an execution plan from the static relationship between Flink Statements
    node_map includes all the FlinkStatement information has a hash map <flink_statement_name>, <statement_metadata>
    start_node is the matching statement metadata to be the root of the graph navigation. 
    """
    logger.info(f"Build execution plan for {pipeline_def.table_name}")
    start_node = pipeline_def.to_node()
    if not start_time:
        start_time = str(datetime.now())
    start_node.created_at = start_time
    start_node.dml_only = dml_only
    start_node.to_run = True
    _assign_compute_pool_id_to_node(node=start_node, compute_pool_id=compute_pool_id)
    start_node.update_children = may_start_children
    

    # 1 Build the static graph from the Flink statement relationship
    node_map = _build_statement_node_map(start_node)

    # TODO assess if we need to reload a persisted execution plan
    execution_plan = FlinkStatementExecutionPlan(created_at=start_time,
                                                 start_table_name=pipeline_def.table_name)
    nodes_to_run = []
    visited_nodes = set()
    start_node = _get_and_update_statement_info_for_node(start_node)
    # Search if there is a need to run parent to run
    _search_parents_to_run(nodes_to_run, start_node, visited_nodes, node_map)
    
     # All the parents - grandparents... reacheable by DFS from the start_node and need to be executed are in nodes_to_run
    # Execution plan should start from the source, higher level of the hiearchy. A node may have to restart its children
    # if enforced by the update_children flag or if the children is stateful
    for node in reversed(nodes_to_run):
        node.update_children = may_start_children 
        if node.upgrade_mode == "Stateful":
            node.update_children = True
        if not node.compute_pool_id:
            node.compute_pool_id = _assign_compute_pool_id_to_node(node, compute_pool_id)
        execution_plan.nodes.append(node)

    # Restart all children of each runnable node in the execution plan if they are not yet the    for node in execution_plan.nodes:
        if node.to_run or node.to_restart:
            for c in node.children: 
                if (c not in execution_plan.nodes 
                    and node.update_children 
                    and c.product_name == node.product_name):
                    node_c = node_map[c.table_name]  # c children may not have grand children or its  parent but node_c will have the static hierachy
                    _get_and_update_statement_info_for_node(node_c)
                    if not node_c.is_running() and not node_c.to_run:
                        # it is possible that node_c have parents that are not running, so we need to know its parent hierarchy
                        #graph=_merge_graphs(graph, _build_table_graph_for_node(node_c))
                        node_c.parents.remove(node)
                        _search_parents_to_run(execution_plan.nodes, node_c, visited_nodes, node_map)
                        node_c.to_restart = True    # TODO be more precise by looking at upgrade mode
                        _assign_compute_pool_id_to_node(node=node_c,
                                                                 compute_pool_id=compute_pool_id)
          
        #node.parents=set()
    logger.info("Done with execution plan construction")
    logger.debug(execution_plan)
    return execution_plan
    

def full_pipeline_undeploy_from_table(table_name: str, 
                               inventory_path: str ) -> str:
    """
    Stop DML statement and drop table
    Navigate to the parent(s) and continue if there is no children 
    """
    logger.info("\n"+"#"*20 + f"\n# Full pipeline delete from table {table_name}\n" + "#"*20)
    start_time = time.perf_counter()
    table_inventory = table_mgr.get_or_create_inventory(inventory_path)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    if not table_ref:
        return f"ERROR: Table {table_name} not found in table inventory"
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    config = get_config()
    if pipeline_def.children:
        return f"ERROR: Could not perform a full delete from a non sink table like {table_name}"
    else:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(pipeline_def, config['kafka']['cluster_type'])
        statement_mgr.delete_statement_if_exists(ddl_statement_name)
        statement_mgr.delete_statement_if_exists(dml_statement_name)
        statement_mgr.drop_table(table_name, config['flink']['compute_pool_id'])
        trace = f"{table_name} deleted\n"
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
        parent_names = [p.table_name for p in node.parents]
        child_names = [c.table_name for c in node.children]
        node.parents = set(parent_names)
        node.children = set(child_names)
    
    # Write to file with proper JSON formatting
    with open(filename, "w") as f:
        f.write(execution_plan.model_dump_json(indent=2))  # default=str handles datetime serialization



def build_summary_from_execution_plan(execution_plan: FlinkStatementExecutionPlan) -> str:
    """
    Build a summary of the execution plan showing which statements need to be executed.
    
    Args:
        execution_plan: The execution plan containing nodes to be processed
        
    Returns:
        A formatted string summarizing the execution plan
    """
    summary_parts = [
        f"To deploy {execution_plan.start_table_name} the following statements need to be executed in the order\n"
    ]
    
    # Separate nodes into parents and children
    parents = [node for node in execution_plan.nodes if node.to_run]
    children = [node for node in execution_plan.nodes if not node.to_run]
    
    # Build parent section
    if parents:
        summary_parts.extend([
            "\n--- Parents impacted ---",
            "Statement Name\t\t\tStatus\t\tCompute Pool ID\t\tAction\tUpgrade Mode",
            "-" * 90
        ])
        for node in parents:
            action = "Run" if node.to_run else "Skip"
            if node.to_restart:
                action += "/restart children"
            summary_parts.append(
                f"{node.dml_statement}\t\t\t{node.existing_statement_info.status_phase}\t\t{node.compute_pool_id}\t\t{action}\t{node.upgrade_mode}"
            )
    
    # Build children section
    if children:
        summary_parts.extend([
            f"\n--- {len(parents)} parents to run",
            "--- Children to restart ---",
            "Statement Name\t\t\tStatus\t\tCompute Pool ID\t\tAction",
            "-" * 90
        ])
        for node in children:
            action = "Run as parent" if node.to_run else "Restart as child" if node.to_restart else "Skip"
            summary_parts.append(
                f"{node.dml_statement}\t\t\t{node.existing_statement_info.status_phase}\t\t{node.compute_pool_id}\t\t{action}\t{node.upgrade_mode}"
            )
        summary_parts.append(f"--- {len(children)} children to restart")
    
    return "\n".join(summary_parts)

#
# ------------------------------------- private APIs  ---------------------------------
#



def _get_and_update_statement_info_for_node(node: FlinkStatementNode) -> FlinkStatementNode:
    statement_name = _get_statement_name_modifier().modify_statement_name(node, node.dml_statement, get_config().get('kafka').get('cluster_type'))
    node.dml_statement = statement_name
    node.ddl_statement =  _get_statement_name_modifier().modify_statement_name(node, node.ddl_statement, get_config().get('kafka').get('cluster_type'))
    node.existing_statement_info = statement_mgr.get_statement_status(statement_name)
    if node.existing_statement_info.compute_pool_id:
        node.compute_pool_id = node.existing_statement_info.compute_pool_id
    return node 

_statmenent_name_modifier = None
def _get_statement_name_modifier() -> DefaultDmlNameModifier:
    global _statmenent_name_modifier
    if not _statmenent_name_modifier:
        if get_config().get('app').get('dml_naming_convention_modifier'):
            class_to_use = get_config().get('app').get('dml_naming_convention_modifier')
            module_path, class_name = class_to_use.rsplit('.',1)
            mod = import_module(module_path)
            _statmenent_name_modifier = getattr(mod, class_name)()
        else:
            _statmenent_name_modifier = DmlNameModifier()
    return _statmenent_name_modifier


def _search_parents_to_run(nodes_to_run, current_node, visited_nodes, node_map):
    """
    DFS to process all the parents from the current node.
    - nodes_to_run list, is what needs to be constructed via this recursion, and get the list of flink statements to run
    - current_node is the current node in the graph navigation
    - visited nodes, to control and avoid loops
    - node_map: the hash map of all the node to get the metadata to infer if the statement runs or not
    """
    if not current_node in visited_nodes:
        nodes_to_run.append(current_node)
        visited_nodes.add(current_node)
        for p in current_node.parents:
            try:
                node_p = node_map[p.table_name]
                node_p= _get_and_update_statement_info_for_node(node_p)
                if not node_p.is_running():
                    if not node_p.to_run:  # navigating from another branch may have already set to run
                        if not node_p.compute_pool_id:  # no existing compute pool take the one specified as argument
                            _assign_compute_pool_id_to_node(node_p, current_node.compute_pool_id)
                        node_p.to_run = True
                        node_p.update_children = current_node.update_children  # inherit the update children flag from children to parent
                        current_node.to_restart = True
                        #node_p.children.remove(current_node)  # do not need to create a loop
                        _search_parents_to_run(nodes_to_run, node_p, visited_nodes, node_map)
                else:
                    node_p.to_run = False  # already running, so do not need to run
                    node_p.to_restart = False
            except Exception as e:
                logger.error(f"{p.table_name} is not found in the node map, this may be due to a data consitency issue")


def _merge_graphs(in_out_graph, in_graph) -> dict[str, FlinkStatementNode]:
    """
    It may be possible while navigating to the children that some parent of those children are not part
    of the current graph, so there is a need to merge the graph
    """
    for table_name in in_graph:
        if table_name not in in_out_graph:
            in_out_graph[table_name]=in_graph[table_name]
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

def _assign_compute_pool_id_to_node(node, compute_pool_id) -> str:
    
    if node.compute_pool_id:  # this may be loaded from the statement info
        return node.compute_pool_id
    compute_pool_list = compute_pool_mgr.get_compute_pool_list(get_config().get('confluent_cloud').get('environment'), 
                                              get_config().get('confluent_cloud').get('region'))
    logger.debug(f"Compute pool list has {len(compute_pool_list.pools)} pools")
    pools=compute_pool_mgr.search_for_matching_compute_pools(compute_pool_list=compute_pool_list,
                                      table_name=node.table_name)
    if not compute_pool_id:
        node.compute_pool_id = get_config()['flink']['compute_pool_id']
    if not pools:
        pools= compute_pool_list.pools
    for pool in pools:
        if (pool.name.startswith(get_config().get('kafka').get('cluster_type'))
            and pool.current_cfu < int(get_config().get('flink').get('max_cfu'))):
            node.compute_pool_id = pool.id
            break
    logger.info(f"Assign compute pool {node.compute_pool_id} to {node.table_name}")
    return node.compute_pool_id

def _execute_plan(plan: FlinkStatementExecutionPlan, compute_pool_id: str) -> list[Statement]:
    logger.info("--- Execution Plan  started ---")
    statements = []
    for node in plan.nodes:
        logger.info(f"table: '{node.table_name}' on pool: {node.compute_pool_id}")
        if not node.compute_pool_id:
            node.compute_pool_id = compute_pool_id
        if node.to_run or node.to_restart:
            logger.info(f"Execute statement '{node.dml_statement}' with dml only: {node.dml_only}")
            if not node.dml_only:
                statement=_deploy_ddl_dml(node)
            else:
                statement=_deploy_dml(node, False)
            statements.append(statement)
        else:
            logger.info(f"No restart or no to_run, it should not be in the execution plan!")
    return statements



def _deploy_ddl_dml(node_to_process: FlinkStatementNode):
    """
    Deploy the DDL and then the DML for the given table to process.
    """

    logger.debug(f"{node_to_process.table_name} to {node_to_process.compute_pool_id}")
    if not node_to_process.dml_only:
        statement_mgr.delete_statement_if_exists(node_to_process.ddl_statement)

    statement_mgr.delete_statement_if_exists(node_to_process.dml_statement)
    rep= statement_mgr.drop_table(node_to_process.table_name, node_to_process.compute_pool_id)
    logger.info(f"Dropped table {node_to_process.table_name} status is : {rep}")

    statement_mgr.build_and_deploy_flink_statement_from_sql_content(node_to_process.ddl_ref, 
                                node_to_process.compute_pool_id, 
                                node_to_process.ddl_statement)

    _deploy_dml(node_to_process, True)


def _deploy_dml(to_process: FlinkStatementNode, dml_already_deleted: bool= False):
    logger.info(f"Run {to_process.dml_statement} for {to_process.table_name} table")
    if not dml_already_deleted:
        statement_mgr.delete_statement_if_exists(to_process.dml_statement)
    
    statement_mgr.build_and_deploy_flink_statement_from_sql_content(to_process.dml_ref, 
                                    to_process.compute_pool_id, 
                                    to_process.dml_statement)
    compute_pool_mgr.save_compute_pool_info_in_metadata(to_process.dml_statement, to_process.compute_pool_id)
 


def _delete_not_shared_parent(current_ref: FlinkTablePipelineDefinition, trace:str, config ) -> str:
    for parent in current_ref.parents:
        if len(parent.children) == 1:
            # as the parent is not shared it can be deleted
            ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(parent, config['kafka']['cluster_type'])
            statement_mgr.delete_statement_if_exists(ddl_statement_name)
            statement_mgr.delete_statement_if_exists(dml_statement_name)
            statement_mgr.drop_table(parent.table_name, config['flink']['compute_pool_id'])
            trace+= f"{parent.table_name} deleted\n"
            pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
            trace = _delete_not_shared_parent(pipeline_def, trace, config)
        else:
            trace+=f"{parent.table_name} has more than {current_ref.table_name} as child, so no delete"
    if len(current_ref.children) == 1:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(current_ref, config['kafka']['cluster_type'])
        statement_mgr.delete_statement_if_exists(ddl_statement_name)
        statement_mgr.delete_statement_if_exists(dml_statement_name)
        statement_mgr.drop_table(current_ref.table_name, config['flink']['compute_pool_id'])
        trace+= f"{current_ref.table_name} deleted\n"
    return trace
        

def _build_statement_node_map(current_node: FlinkStatementNode) -> dict[str,FlinkStatementNode]:
    """
    Define the complete static graph of the related parents and children for the current node. It uses a DFS
    to reach all parents, and then a BFS to construct the list of reachable children.
    The graph built has no loop.
    """
    logger.info(f"start build tables static graph for {current_node.table_name}")
    visited_nodes = set()
    node_map = {}   # <k: str, v: FlinkStatementNode> use a map to search for statement name as key.
    queue = deque()  # Queue for BFS processing of nodes

    def _search_parent_from_current(list_of_parents: dict[str, FlinkStatementNode], current: FlinkStatementNode, visited_nodes):
        if current not in visited_nodes:
            visited_nodes.add(current)
            for p in current.parents:
                pipe_def = read_pipeline_definition_from_file( p.path + "/" + PIPELINE_JSON_FILE_NAME)
                if pipe_def:
                    node_p = pipe_def.to_node()
                    if node_p not in visited_nodes:
                        _search_parent_from_current(list_of_parents, node_p, visited_nodes)
                        list_of_parents[node_p.table_name] = node_p
                        queue.append(node_p)  # Add new nodes to the queue for processing
                else:
                    logger.warning(f"Data consistency issue for {p.path} as no pipeline definition found or wrong reference in {current.table_name}. The execution plan may not deploy successfuly")
                

    def _search_children_from_current(node_map: dict[str, FlinkStatementNode], current: FlinkStatementNode, visited_nodes: Set[FlinkStatementNode]):
        for c in current.children:
            if c.table_name not in node_map: # a child may have been a parent of another node so do not need to process it
                pipe_def = read_pipeline_definition_from_file( c.path + "/" + PIPELINE_JSON_FILE_NAME)
                node_c = pipe_def.to_node()
                if node_c not in visited_nodes and _accepted_to_process(current, node_c):
                    _search_parent_from_current(node_map, node_c, visited_nodes)
                    _search_children_from_current(node_map, node_c, visited_nodes)
                    node_map[node_c.table_name] = node_c
            

    _search_parent_from_current(node_map, current_node, visited_nodes)
    node_map[current_node.table_name] = current_node
    visited_nodes.add(current_node)
    queue.append(current_node)  # Start with the current node

    # Process nodes using BFS
    while queue:
        current = queue.popleft()
        _search_children_from_current(node_map, current, visited_nodes)
    logger.info(f"End build table graph for {current_node.table_name} with {len(node_map)} nodes")
    logger.debug("\n\n".join("{}\t{}".format(k,v) for k,v in node_map.items()))
    return node_map



def _accepted_to_process(current: FlinkStatementNode, node: FlinkStatementNode) -> bool:
    """
    Validate if a node should be processed based on its relationship with the current node.
    Prevents processing nodes that would create circular dependencies.
    """
    return node.product_name == current.product_name


# --- to work on for stateless ---------------

