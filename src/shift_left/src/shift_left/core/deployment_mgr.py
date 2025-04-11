"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import time

from pydantic import BaseModel, Field
from shift_left.core.pipeline_mgr import (
    get_pipeline_definition_for_table
)  

from shift_left.core.compute_pool_mgr import get_or_build_compute_pool, save_compute_pool_info_in_metadata

from collections import deque
from typing import Optional, List

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger, log_dir

from shift_left.core.utils.file_search import ( 
    PIPELINE_JSON_FILE_NAME,
    FlinkTableReference,
    FlinkStatementNode,
    get_or_build_inventory,
    get_table_ref_from_inventory,
    FlinkTablePipelineDefinition,
    get_ddl_dml_names_from_pipe_def,
    read_pipeline_definition_from_file
)
from shift_left.core.statement_mgr import delete_statement_if_exists, get_statement_list, deploy_flink_statement
from shift_left.core.flink_statement_model import Statement, StatementInfo, StatementResult



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
    logger.info("#"*10 + f"# Start deploying pipeline from table {table_name}" + "#"*10)
    start_time = time.perf_counter()
    pipeline_def: FlinkTablePipelineDefinition = get_pipeline_definition_for_table(table_name, inventory_path)
    compute_pool_id =  get_or_build_compute_pool(compute_pool_id, pipeline_def)
    execution_plan = build_execution_plan_from_any_table(pipeline_def,
                                                        compute_pool_id=compute_pool_id,
                                                        dml_only=dml_only,
                                                        force_children=force_children)
    persist_execution_plan(execution_plan, table_name)

    statements = _execute_plan(execution_plan, compute_pool_id)
    result = DeploymentReport(table_name=table_name, 
                            compute_pool_id=compute_pool_id,
                            update_children=force_children,
                            ddl_dml= "DML" if dml_only else "Both",
                            flink_statements_deployed=statements)
    execution_time = time.perf_counter() - start_time
    logger.info(f"Done in {execution_time} seconds to deploy pipeline from table {table_name}: {result.model_dump_json(indent=3)}")
    return result


def build_execution_plan_from_any_table(pipeline_def: FlinkTablePipelineDefinition, 
                               compute_pool_id: str,
                               dml_only: bool = False,
                               force_children: bool = False ) -> List[FlinkStatementNode]:
    """
    Build execution plan, helps to assess the current situation of running statement and assess what needs to be deployed or not.
    """
    
    current_node= pipeline_def.to_node()
    current_node.dml_only = dml_only
    current_node.compute_pool_id = compute_pool_id
    current_node.update_children = force_children
    graph = _build_table_graph(current_node)
    return _build_execution_plan(graph, current_node, compute_pool_id)

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
        r=_delete_not_shared_parent(pipeline_def, trace, config)
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
    sql_content = f"drop table if exists {table_name};"
    properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    statement_name = "drop-" + table_name.replace('_','-')
    delete_statement_if_exists(statement_name)
    try:
        result= client.post_flink_statement(compute_pool_id, 
                                            statement_name, 
                                            sql_content, 
                                            properties)
        logger.debug(f"Run drop table {result}")
    except Exception as e:
        logger.error(e)
    delete_statement_if_exists(statement_name)


def persist_execution_plan(execution_plan, table_name: str):
    with open(log_dir + "/last_execution_plan.json","w") as f:
        f.write(f"To deploy {table_name} the following statements need to be executed in the order\n\n")
        count_p = 0
        count_c = 0
        for node in execution_plan:
            if node.to_run:
                if count_p == 0:
                    f.write("--- Parents impacted ---\n")
                count_p+=1
                message = f"\t{node.dml_statement} is : {node.existing_statement_info.status_phase} on cpool_id: {node.compute_pool_id} may run-as-parent: {node.to_run} or restart-as-child: {node.to_restart}\n"
                f.write(message)
            else:
                if count_c == 0:
                    f.write(f"--- {count_p} parents to run\n")
                    f.write("--- Children to restart ---\n")
                message = f"\t{node.dml_statement} is : {node.existing_statement_info.status_phase} on cpool_id: {node.compute_pool_id} may run-as-parent: {node.to_run} or restart-as-child: {node.to_restart}\n"
                f.write(message)
                count_c+=1
        f.write(f"--- {count_c} children to restart")
            
    logger.info(f"Execution plan in {log_dir}/last_execution_plan.json")
    if logger.level == "INFO":
        with open(log_dir + "/last_execution_plan.json","r") as f:
            print(f.read())
        
#
# ------------------------------------- private APIs  ---------------------------------
#

def _get_statement_status(statement_name: str) -> StatementInfo:
    statement_list = get_statement_list()
    #statement_list = None
    if statement_list and statement_name in statement_list:
        return statement_list[statement_name]
    statement_info = StatementInfo(name=statement_name,
                                   status_phase="UNKNOWN",
                                   status_detail="Statement not found int the existing deployed Statements"
                                )
    return statement_info

def _update_statement_info_for_node(node):
    node.existing_statement_info = _get_statement_status(node.dml_statement)
    if node.existing_statement_info.compute_pool_id:
        node.compute_pool_id = node.existing_statement_info.compute_pool_id

def _search_parents_to_run(nodes_to_run, current_node, visited_nodes, node_map):
    """
    DFS to process all the parents from the current node.
    - nodes to run is what needs to be constructed via this recursion, and get the list of flink statements to run
    - current node in the graph navigation
    - visited nodes, to control and avoid loops
    - node_map: the hash map of all the node to get the metadata to infer if the statement runs or not
    """
    if not current_node in visited_nodes:
        nodes_to_run.append(current_node)
        visited_nodes.add(current_node)
        for p in current_node.parents:
            node_p = node_map[p.table_name]
            _update_statement_info_for_node(node_p)
            if not node_p.is_running():
                if not node_p.to_run:  # could add in the future if the sql did not change do nothing
                    if not node_p.compute_pool_id:  # no existing compute pool take the one specified as argument
                        node_p.compute_pool_id = current_node.compute_pool_id
                    node_p.to_run = True
                    _search_parents_to_run(nodes_to_run, node_p, visited_nodes, node_map)
            else:
                node_p.to_run = False



def _build_execution_plan(node_map: dict[FlinkStatementNode], start_node: FlinkStatementNode, compute_pool_id: str) -> List[FlinkStatementNode]:
    """
    Build an execution plan from the static relationship between Flink Statements
    node_map includes all the FlinkStatement information has a hash map <flink_statement_name>, <statement_metadata>
    start_node is the matching statement metadata to be the root of the graph navigation. 
    """
    logger.info("Build execution plan")
    execution_plan = []
    nodes_to_run = []
    visited_nodes = set()
    _update_statement_info_for_node(start_node)
    _search_parents_to_run(nodes_to_run, start_node, visited_nodes, node_map)
    start_node.to_run = True
     # All the parents - grandparents... reacheable by DFS from the start_node and need to be executed are in nodes_to_run
    # Execution plan should start from the source, higher level of the hiearchy

    for node in reversed(nodes_to_run):
        execution_plan.append(node)
        """
        NOT SURE about that code anymore:
        # to be starteable each parent needs to be part of the running ancestors
        _add_non_running_parents(node, execution_plan, node_map)
        node.to_run = True  
    """
    # 3. Restart all children of each runnable node in the execution plan if they are not yet there
    for node in execution_plan:
        if node.to_run or node.to_restart:
            for c in node.children:
                _update_statement_info_for_node(c)
                if not c.is_running() and not c.to_run and c not in execution_plan:
                    _search_parents_to_run(execution_plan, c, visited_nodes, node_map)
                    c.to_restart = True    # TODO be more precise by looking at upgrade mode
                    if c.existing_statement_info.compute_pool_id:
                        c.compute_pool_id = c.existing_statement_info.compute_pool_id
                    else:
                        c.compute_pool_id = compute_pool_id
    logger.info("Done with execution plan construction")
    logger.debug(execution_plan)
    return execution_plan

def _add_non_running_parents(node, execution_plan, node_map):
    for p in node.parents:
        node_p = node_map[p.table_name]
        _update_statement_info_for_node(node_p)
        if not node_p.is_running() and not node_p.to_run and node_p not in execution_plan:
            # position the parent before the node to be sure it is started before it
            idx=execution_plan.index(node)
            execution_plan.insert(idx,node_p)
            node_p.to_run = True



def _execute_plan(plan: List[FlinkStatementNode], compute_pool_id: str) -> list[Statement]:
    logger.info("--- Execution Plan  started ---")
    statements = []
    for node in plan:
        logger.info(f"table: '{node.table_name}'")
        if not node.compute_pool_id:
            node.compute_pool_id = compute_pool_id
        if node.to_run:
            logger.info(f"Execute statement {node.dml_statement}' with dml only: {node.dml_only}")
            if not node.dml_only:
                statement=_deploy_ddl_dml(node)
            else:
                statement=_deploy_dml(node, False)
            statements.append(statement)
        elif node.to_restart:
            logger.info(f"Restarting statement: {node.dml_statement} with dml only: {node.dml_only})")
            if not node.dml_only:
                statement=_deploy_ddl_dml(node)
            else:
                statement=_deploy_dml(node, False)
            statements.append(statement)
        else:
            logger.info(f"No restart no to run, strange!")
    return statements



def _deploy_ddl_dml(to_process: FlinkStatementNode):
    """
    Deploy the DDL and then the DML for the given table to process.
    """
    config = get_config()
    logger.debug(f"{to_process.table_name} to {to_process.compute_pool_id}")
    if not to_process.dml_only:
        delete_statement_if_exists(to_process.ddl_statement)

    delete_statement_if_exists(to_process.dml_statement)
    rep= drop_table(to_process.table_name)
    logger.info(f"Dropped table {to_process.table_name} status is : {rep}")

    statement: Statement =deploy_flink_statement(to_process.ddl_ref, 
                                to_process.compute_pool_id, 
                                to_process.ddl_statement, 
                                config)

    logger.info(f"Statement: {to_process.ddl_statement} status is: {statement.status.phase}")
    get_statement_list()[to_process.ddl_statement]=statement.status.phase   # important to avoid doing an api call
    delete_statement_if_exists(to_process.ddl_statement)
    statement = _deploy_dml(to_process, True)
    
    return statement   
    


def _deploy_dml(to_process: FlinkStatementNode, dml_already_deleted: bool= False):
    config = get_config()
    logger.info(f"Run {to_process.dml_statement} for {to_process.table_name} table")
    if not dml_already_deleted:
        delete_statement_if_exists(to_process.dml_statement)
    
    statement: StatementResult =deploy_flink_statement(to_process.dml_ref, 
                                    to_process.compute_pool_id, 
                                    to_process.dml_statement, 
                                    config)
    get_statement_list()[to_process.dml_statement]=statement.status
    save_compute_pool_info_in_metadata(to_process.dml_statement, to_process.compute_pool_id)
    logger.info(f"Statement: {to_process.dml_statement} status is: {statement.status.phase}")
    return statement


def _delete_not_shared_parent(current_ref: FlinkTablePipelineDefinition, trace:str, config ) -> str:
    for parent in current_ref.parents:
        if len(parent.children) == 1:
            # as the parent is not shared it can be deleted
            ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(parent, config['kafka']['cluster_type'])
            delete_statement_if_exists(ddl_statement_name)
            delete_statement_if_exists(dml_statement_name)
            drop_table(parent.table_name, config['flink']['compute_pool_id'])
            trace+= f"{parent.table_name} deleted\n"
            pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
            trace = _delete_not_shared_parent(pipeline_def, trace, config)
        else:
            trace+=f"{parent.table_name} has more than {current_ref.table_name} as child, so no delete"
    if len(current_ref.children) == 1:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(current_ref, config['kafka']['cluster_type'])
        delete_statement_if_exists(ddl_statement_name)
        delete_statement_if_exists(dml_statement_name)
        drop_table(current_ref.table_name, config['flink']['compute_pool_id'])
        trace+= f"{current_ref.table_name} deleted\n"
    return trace
        

def _build_table_graph(current_node: FlinkStatementNode) -> dict[FlinkStatementNode]:
    """
    Define the complete static graph of the related parents and children for the current node. It uses a DFS
    to reach all parents, and then a BFS to construct the list of reachable children.
    The graph built has no loop.
    """
    logger.debug("start build table graph")
    visited_nodes = set()
    node_map = {}   # <k: str, v: FlinkStatementNode> use a map to search for statement name as key.

    def _search_parent_from_current(list_of_parents: dict[str, FlinkStatementNode], current: FlinkStatementNode, visited_nodes):
        if current not in visited_nodes:
            visited_nodes.add(current)
            for p in current.parents:
                pipe_def = read_pipeline_definition_from_file( p.path + "/" + PIPELINE_JSON_FILE_NAME)
                node_p = pipe_def.to_node()
                _search_parent_from_current(list_of_parents, node_p, visited_nodes)
                list_of_parents[node_p.table_name] = node_p
                

    def _search_children(node_map: dict[str, FlinkStatementNode], node, visited_nodes):
        for c in node.children:
            if c.table_name not in node_map:
                pipe_def = read_pipeline_definition_from_file( c.path + "/" + PIPELINE_JSON_FILE_NAME)
                node_c = pipe_def.to_node()
                if node_c not in visited_nodes:
                    _search_children(node_map, node_c, visited_nodes)
                    node_map[node_c.table_name] = node_c
            

    _search_parent_from_current(node_map, current_node, visited_nodes)
    node_map[current_node.table_name]=current_node
    visited_nodes.add(current_node)
    names = list(node_map.keys())
    for parent in names:
        _search_children(node_map, node_map[parent], visited_nodes)
    logger.debug("End build table graph:\n" + "\n\n".join("{}\t{}".format(k,v) for k,v in node_map.items()))
    return node_map


# --- to work on for stateless ---------------


#
# --- dead code -----
#
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
        logger.info(f"delete_statement_if_exists({queue[idx].dml_statement_name}")
    while queue:
        current = queue.popleft()
        #r=_deploy_ddl_dml(current, True)
        #logger.debug(f"{r.model_dump_json(indent=2)}")
        logger.info(f"deploy dml {current.dml_statement_name}")
        current.existing_statement_info.status_phase = "RUNNING"
    logger.debug("Done with child processing")