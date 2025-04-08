"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import time

from pydantic import BaseModel, Field
from shift_left.core.pipeline_mgr import (
    build_pipeline_report_from_table
)  

from shift_left.core.compute_pool_mgr import get_or_build_compute_pool, save_compute_pool_info_in_metadata

from collections import deque
from typing import Optional, List

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger

from shift_left.core.utils.file_search import ( 
    PIPELINE_JSON_FILE_NAME,
    FlinkTableReference,
    FlinkStatementNode,
    get_or_build_inventory,
    get_table_ref_from_inventory,
    FlinkTablePipelineDefinition,
    get_ddl_dml_names_from_pipe_def,
    read_pipeline_definition_from_file,
    from_pipeline_to_absolute
)
from shift_left.core.statement_mgr import delete_statement_if_exists, get_statement_list, deploy_flink_statement
from shift_left.core.flink_statement_model import StatementResult, Statement



class DeploymentReport(BaseModel):
    table_name: str
    compute_pool_id: str
    statement_name: str
    ddl_dml:  Optional[str] =  Field(default="Both",description="The type of deployment: DML only, or both")
    update_children: bool
    flink_statements_deployed: List[str]

    
def deploy_pipeline_from_table(table_name: str, 
                               inventory_path: str, 
                               compute_pool_id: str,
                               dml_only: bool = False,
                               force_children: bool = False ) -> DeploymentReport:
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
    logger.info("#"*20 + f"\n# Start deploying pipeline from table {table_name}\n" + "#"*20)
    start_time = time.perf_counter()
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    if not table_ref:
        raise Exception(f"Table {table_name} not found. Stop processing")
    
    pipeline_def: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    current_node= pipeline_def.to_node()
    current_node.dml_only = dml_only
    current_node.compute_pool_id = get_or_build_compute_pool(compute_pool_id, pipeline_def)
    current_node.update_children = force_children
    graph = _build_table_graph(current_node)
    execution_plan = _build_execution_plan(graph, current_node)
    statement = _execute_plan(execution_plan)
    logger.info(statement)
    flink_statement_deployed=[pipeline_def.dml_statement_name]
    result = DeploymentReport(table_name=table_name, 
                                             compute_pool_id=compute_pool_id, 
                                             statement_name=statement.name, 
                                             update_children=force_children,
                                             ddl_dml= "DML" if dml_only else "Both",
                                             flink_statements_deployed=flink_statement_deployed)
    execution_time = time.perf_counter() - start_time
    logger.info(f"Done in {execution_time} seconds to deploy pipeline from table {table_name}: {result.model_dump_json(indent=3)}")
    logger.info(f"Deployment state {pipeline_def.model_dump_json(indent=3)}")
    print(pipeline_def.model_dump_json(indent=3))
    return result


def full_pipeline_undeploy_from_table(table_name: str, 
                               inventory_path: str ) -> str:
    """
    Stop DML statement and drop table
    Navigate to the parent(s) and continue if there is no children 
    """
    logger.info("\n"+"#"*20 + f"\n# Full pipeline delete from table {table_name}\n" + "#"*20)
    start_time = time.perf_counter()
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    if not table_ref:
        return f"ERROR: Table {table_name} not found in table inventory"
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    config = get_config()
    if pipeline_def.children:
        return f"ERROR: Could not perform a full delete from a non sink table like {table_name}"
    else:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(pipeline_def, config['kafka']['cluster_type'])
        delete_statement_if_exists(ddl_statement_name)
        delete_statement_if_exists(dml_statement_name)
        drop_table(table_name, config['flink']['compute_pool_id'])
        trace = f"{table_name} deleted\n"
        r=_delete_parent_not_shared(pipeline_def, trace, config)
        execution_time = time.perf_counter() - start_time
        logger.info(f"Done in {execution_time} seconds to undeploy pipeline from table {table_name} with result: {r}")
        return r



    

def get_table_structure(table_name: str, compute_pool_id: Optional[str] = None) -> str | None:
    """
    Run a show create table statement to get information about a table
    return the statement with the description of the table
    """
    logger.debug(f"{table_name}")
    statement_name = "show-" + table_name.replace('_','-')
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    client = ConfluentCloudClient(config)
    sql_content = f"show create table {table_name};"
    properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    delete_statement_if_exists(statement_name)
    try:
        statement = client.post_flink_statement(compute_pool_id, statement_name, sql_content, properties)
        if statement and statement.status.phase in ("RUNNING", "COMPLETED"):
            statement_result = client.get_statement_results(statement_name)
            if len(statement_result.results.data) > 0:
                result_str = str(statement_result.results.data)
                logger.debug(f"Run show create table in {result_str}")
                client.delete_flink_statement(statement_name)
                return result_str
        return None
    except Exception as e:
        logger.error(f"get_table_structure {e}")
        client.delete_flink_statement(statement_name)
        return None

def drop_table(table_name: str, compute_pool_id: Optional[str] = None):
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    client = ConfluentCloudClient(config)
    sql_content = f"drop table {table_name};"
    properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    statement_name = "drop-" + table_name.replace('_','-')
    delete_statement_if_exists(statement_name)
    try:
        result= client.post_flink_statement(compute_pool_id, statement_name, sql_content, properties)
        logger.debug(f"Run drop table {result}")
    except Exception as e:
        logger.error(e)
    delete_statement_if_exists(statement_name)


#
# ------------------------------------- private APIs  ---------------------------------
#

def _get_statement_status(statement_name: str) -> str:
    #statement_list = get_statement_list()
    statement_list = None
    if statement_list and statement_name in statement_list:
        return statement_list[statement_name]
    return "UNKNOWN"

def _search_parents_to_run(nodes_to_run, node, visited_nodes):
    if not node in visited_nodes:
        nodes_to_run.append(node)
        visited_nodes.add(node)
        for p in node.parents:
            p.statement_status = _get_statement_status(p.dml_statement)
            if not p.is_running() and not p.to_run:
                _search_parents_to_run(nodes_to_run, p, visited_nodes)

def _add_non_running_parents(node, execution_plan):
    for p in node.parents:
        p.statement_status = _get_statement_status(p.dml_statement)
        if not p.is_running() and not p.to_run and p not in execution_plan:
            # position the parent before the node to be sure it is started before it
            idx=execution_plan.index(node)
            execution_plan.insert(idx,p)
            p.to_run = True

def _build_execution_plan(graph: FlinkStatementNode, start_node: FlinkStatementNode):
    """
    Build an execution plan from the static relationship between Flink Statements

    """
    execution_plan = []
    nodes_to_run = []
    visited_nodes = set()
    for node in graph:
        if node.name == start_node.name:
            node.dml_only=start_node.dml_only
            node.update_children= start_node.update_children
            node.compute_pool_id = start_node.compute_pool_id
            start_node = node
            break
    _search_parents_to_run(nodes_to_run, start_node, visited_nodes)
    start_node.to_run = True
     # All the parents - grandparents... reacheable by DFS from the start_node are in nodes_to_run
    # 2. Add the non-running ancestors to the execution plan 
    for node in reversed(nodes_to_run):
        execution_plan.append(node)
        # to be starteable each parent needs to be part of the running ancestors
        _add_non_running_parents(node, execution_plan)
        node.to_run = True  
    # 3. Restart all children of each node in the execution plan if they are not yet there
    for node in execution_plan:
        for c in node.children:
            if not c.is_running() and not c.to_run and c not in execution_plan:
                _search_parents_to_run(execution_plan, c, visited_nodes)
                c.to_restart = True
    return execution_plan

def _execute_plan(plan) -> Statement:
    print("\n--- Execution Plan ---")
    for node in plan:
        print(f"node: '{node.name}' {node.statement_status}")
        if node.to_run:
            print(f"Execute this {node}")
        else:
            print(f"Restarting node: '{node.name}' (program: '{node.flink_statement}')")


def _process_table_deployment_2(current_pipeline_def,
                              queue_to_process,
                              already_processed_list) -> Statement:
    """
    For the given ddl if this is a sink (no children), we need to assess if parents are running or not. Create non running parent.
    As a recursive function, going to parent means intermediate and source tables are processed too. This is a way to start a full pipeline.
    for each node to process
            if there is a parent not yet deployed add parent to node to process. recursive call to do a BFS
            if current table already exist
                if only dml, redeploy dml taking into accound children when intermediate or src and not coming from a leaf path
                else deploy ddl and dml navigate to children if not coming from leaf path as the recursive will take care of the children
            else deploy ddl and dml
    """
    if len(queue_to_process) > 0:
        current_pipeline_def: FlinkTablePipelineDefinition  = queue_to_process.pop()
        logger.info(f"Start processing {current_pipeline_def.table_name}")
        logger.debug(f"--- {current_pipeline_def}")
        for parent in current_pipeline_def.parents:
            parent.compute_pool_id= current_pipeline_def.compute_pool_id
            if not _table_exists(parent):
                logger.info(f"Table: {parent.table_name} not present, add it for processing.")
                parent_pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
                parent_pipeline_def.update_children=False
                parent_pipeline_def.dml_only=False
                queue_to_process.add(parent_pipeline_def)
                _build_execution_plan(queue_to_process, already_processed_list)
            else:
                logger.debug(f"Parent {parent.table_name} is running, there is no need to change that!")
        # process current node to run dml with ddl: will be in the order of higher in the hierarchy to the lower 
        logger.info(f"Perform dml or ddl deployment for {current_pipeline_def.table_name}")
        if not current_pipeline_def.dml_only:
            statement=_deploy_ddl_dml(current_pipeline_def, False)
        else:
            statement=_deploy_dml(current_pipeline_def, current_pipeline_def.dml_statement_name, False)
        return statement 
    

def _table_exists(table_ref: FlinkTablePipelineDefinition) -> bool:
    return False

def _table_exists_1(table_ref: FlinkTablePipelineDefinition) -> bool:
    """
    Table exists if there is a running dml writing to it, or if "show create table" returns a result
    """
    global statement_list
    config = get_config()
    ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(table_ref, config['kafka']['cluster_type'])
    table_ref.dml_statement_name=dml_statement_name
    statement_list=get_statement_list()
    if statement_list and dml_statement_name in statement_list:
        if statement_list[dml_statement_name] == "RUNNING":
            return True
        elif  statement_list[dml_statement_name] in ("FAILED", "STOPPED"):
            delete_statement_if_exists(dml_statement_name)
            return False
    else: 
        statement = get_table_structure(table_ref.table_name, table_ref.compute_pool_id)
        logger.debug(f"{statement}")
        return statement != None

def _deploy_ddl_dml(to_process: FlinkTablePipelineDefinition, dml_already_deleted: bool):
    """
    Deploy the DDL and then the DML for the given table to process.
    """
    config = get_config()
    if not to_process.dml_statement_name:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(to_process, config['kafka']['cluster_type'])
        to_process.dml_statement_name=dml_statement_name
    logger.debug(f"{to_process.table_name} to {to_process.compute_pool_id}")
    if not to_process.dml_only:
        delete_statement_if_exists(ddl_statement_name)
    # need to delete the dml and the table
    if not dml_already_deleted:
        delete_statement_if_exists(dml_statement_name)
        dml_already_deleted= True
    rep= drop_table(to_process.table_name)
    logger.info(f"Dropped table {to_process.table_name} status is : {rep}")

    statement=deploy_flink_statement(to_process.ddl_ref, 
                                to_process.compute_pool_id, 
                                ddl_statement_name, 
                                config)
    logger.info(f"Create table {to_process.table_name} status is : {statement.status}")
    get_statement_list()[ddl_statement_name]=statement.status.phase   # important to avoid doing an api call
    delete_statement_if_exists(ddl_statement_name)
    statement = _deploy_dml(to_process, to_process.dml_statement_name, dml_already_deleted)
    return statement   
    

def _process_children_2(current_pipeline_def: FlinkTablePipelineDefinition):
    """
    For each child of the current node in the pipeline reprocess ddl or/and dml
    """

    if not current_pipeline_def.children or len(current_pipeline_def.children) == 0:
        return
    logger.debug(f"Process childen of {current_pipeline_def.table_name}")
    for child in current_pipeline_def.children:
        child_pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(child.path + "/" + PIPELINE_JSON_FILE_NAME)
        child_pipeline_def.compute_pool_id=current_pipeline_def.compute_pool_id  # change when managing pool to statement    
        if child.state_form == "Stateful":
            child_pipeline_def.update_children=True
            child_pipeline_def.dml_only=False  # as stateful need to drop the table
            _deploy_ddl_dml(child_pipeline_def, False)
            _process_children_2(child_pipeline_def)  # continue for grand children
        else:
            child.dml_only= True
            child_pipeline_def.update_children=False
            logger.debug(f"Stop {child.dml_ref}")
            logger.debug(f"Update {child.dml_ref} with offser")
            logger.debug(f"Resume {child.dml_ref}")


def _process_children(root_pipeline_def: FlinkTablePipelineDefinition) -> FlinkTablePipelineDefinition:
    queue=deque()
    queue.append(root_pipeline_def)
    if root_pipeline_def not in queue:
        _build_child_queue(root_pipeline_def, queue)
    _process_queue_element(root_pipeline_def, queue)
    return root_pipeline_def

def _build_parent_queue(current_table_info: FlinkTablePipelineDefinition, queue):
    """
    Get all non running parents of the current table to the queue
    """
    for parent in current_table_info.parents:
        if parent not in queue:
            parent.compute_pool_id= current_table_info.compute_pool_id
            if not _table_exists(parent):
                logger.info(f"Parent table: {parent.table_name} not present, add it for processing.")
                parent_table_info: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
                parent_table_info = _complement_pipeline_definition(parent_table_info, current_table_info.compute_pool_id)
                parent_table_info.update_children=True
                parent_table_info.dml_only=False
                queue.append(parent_table_info)  # FIFO 
                _build_parent_queue(parent_table_info, queue)  # DFS
            else:
                logger.debug(f"Parent {parent.table_name} is running, there is no need to change that!")
    _build_child_queue(current_table_info, queue)
    return 0

def _build_child_queue(current_table_info: FlinkTablePipelineDefinition, queue):
    """
    For current node needs to stop all children before redeploying. So this is recurring down to the leaf. Fro the following hierarchy:
    0 -> 1,2,3  
    1 -> 1.1
    2 -> 2.1
    3 -> 2.1, 3.1
    The queue is a FIFO: 0,1,2,3,1.1,2.1,3.1
    """
    logger.debug(f"Add children of {current_table_info.table_name} to queue if not present")
    for child in current_table_info.children:
        if child not in queue:
            child_pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(child.path + "/" + PIPELINE_JSON_FILE_NAME)
            child_pipeline_def = _complement_pipeline_definition(child_pipeline_def, current_table_info.compute_pool_id)
            queue.append(child_pipeline_def)
    for child in current_table_info.children:  # BFS
        child_ref = _peek_from_queue(queue,child)
        current_table_info.children.remove(child_ref) # this swap is needed to keep all information about deployment
        current_table_info.children.add(child_ref)
        _build_child_queue(child_ref, queue)   # add children of child in a BFS navigation


def _complement_pipeline_definition(current: FlinkTablePipelineDefinition, compute_pool_id: str) -> FlinkTablePipelineDefinition:
    config = get_config()
    current.compute_pool_id=compute_pool_id  # change when managing pool to statement    
    if not current.dml_statement_name:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(current, config['kafka']['cluster_type'])
        current.dml_statement_name = dml_statement_name
    if current.state_form == "Stateful":
        current.update_children=True
        current.dml_only=False
    else:
        current.update_children=False
        current.dml_only=True
    return current

def _peek_from_queue(queue, child):
    for idx in range(0,len(queue)):
        if queue[idx] == child:
            return queue[idx] 

def _process_queue_element(root_pipeline_def, queue: set):
    for idx in range(0,len(queue)):
        print(f"delete_statement_if_exists({queue[idx].dml_statement_name}")
    while queue:
        current = queue.popleft()
        #r=_deploy_ddl_dml(current, True)
        #logger.debug(f"{r.model_dump_json(indent=2)}")
        print(f"deploy dml {current.dml_statement_name}")
        current.statement_status = "RUNNING"
    logger.debug("Done with child processing")

def _deploy_dml(to_process: FlinkTablePipelineDefinition, 
                dml_statement_name: str= None,
                dml_already_deleted: bool = False):
    config = get_config()
    if not dml_statement_name:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(to_process, config['kafka']['cluster_type'])
        to_process.dml_statement_name = dml_statement_name
    logger.info(f"Run {dml_statement_name} for {to_process.table_name} table")
    if not dml_already_deleted:
        delete_statement_if_exists(dml_statement_name)
    
    statement=deploy_flink_statement(to_process.dml_ref, 
                                    to_process.compute_pool_id, 
                                    dml_statement_name, 
                                    config)
    get_statement_list()[dml_statement_name]=statement.status
    save_compute_pool_info_in_metadata(dml_statement_name, to_process.compute_pool_id)
    return statement


def _delete_parent_not_shared(current_ref: FlinkTablePipelineDefinition, trace:str, config ) -> str:
    for parent in current_ref.parents:
        if len(parent.children) == 1:
            # as the parent is not shared it can be deleted
            ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(parent, config['kafka']['cluster_type'])
            delete_statement_if_exists(ddl_statement_name)
            delete_statement_if_exists(dml_statement_name)
            drop_table(parent.table_name, config['flink']['compute_pool_id'])
            trace+= f"{parent.table_name} deleted\n"
            pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
            trace = _delete_parent_not_shared(pipeline_def, trace, config)
        else:
            trace+=f"{parent.table_name} has more than {current_ref.table_name} as child, so no delete"
    if len(current_ref.children) == 1:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(current_ref, config['kafka']['cluster_type'])
        delete_statement_if_exists(ddl_statement_name)
        delete_statement_if_exists(dml_statement_name)
        drop_table(current_ref.table_name, config['flink']['compute_pool_id'])
        trace+= f"{current_ref.table_name} deleted\n"
    return trace
        

def _stop_dml_statement(table_name: str, statement):
    logger.info(f"Stopping DML statements for {table_name} with {statement}")
    client = ConfluentCloudClient(get_config())
    rep = client.update_flink_statement(statement, True)
    logger.info(rep)

def _stop_child_dmls(pipeline_def: FlinkTablePipelineDefinition, inventory_path: str):
    if not pipeline_def.children or len(pipeline_def.children) == 0:
        return
    for node in pipeline_def.children:
        logger.info(node)
        node_ref=build_pipeline_report_from_table(node['table_name'], inventory_path )
        _stop_dml_statement(node['table_name']) # stop or delete and recreate
        _stop_child_dmls(node_ref)



def _build_table_graph(current_node: FlinkStatementNode) -> FlinkStatementNode:
    """
    Define the complete static graph of the related parent and children for the pipe_def
    """

    visited_nodes = set()
    list_of_roots = set()
    graph = []

    def _search_all_roots(list_of_roots, node, visited_nodes):
        if node not in visited_nodes:
            visited_nodes.add(node)
            if not node.parents:
                list_of_roots.add(node)
            else:
                for p in node.parents:
                    pipe_def = read_pipeline_definition_from_file( p.path + "/" + PIPELINE_JSON_FILE_NAME)
                    node_p = pipe_def.to_node()
                    _search_all_roots(list_of_roots, node_p, visited_nodes)

    def _search_children(graph, node, visited_nodes):
        if node not in visited_nodes and node not in graph:
            graph.append(node)
            visited_nodes.add(node)
            for c in node.children:
                pipe_def = read_pipeline_definition_from_file( c.path + "/" + PIPELINE_JSON_FILE_NAME)
                node_c = pipe_def.to_node()
                _search_children(graph, node_c, visited_nodes)

    _search_all_roots(list_of_roots, current_node, visited_nodes)
    visited_nodes=set()
    for root in list_of_roots:
        _search_children(graph, root, visited_nodes)
    return graph

    
