
import os
import time
import json

from pydantic import BaseModel
from shift_left.core.pipeline_mgr import (
    read_pipeline_definition_from_file,
    PIPELINE_JSON_FILE_NAME,
    FlinkTablePipelineDefinition,
    update_pipeline_definition_file,
    PipelineReport,
    load_existing_inventory,
    get_table_ref_from_inventory,
    build_pipeline_report_from_table)  
from shift_left.core.utils.file_search import (
    FlinkTableReference
)

from typing import Tuple

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger

from shift_left.core.utils.file_search import ( 
    get_ddl_dml_names_from_table, 
    extract_product_name,
    from_pipeline_to_absolute
)
from shift_left.core.flink_statement_model import *


STATEMENT_COMPUTE_POOL_FILE=os.getenv("PIPELINES") + "/pool_assignments.json"
statement_list = []  # cache the statment loaded to limit the number of call to CC API

class DeploymentReport(BaseModel):
    table_name: str
    compute_pool_id: str
    statement_name: str
    flink_statements_deployed: List[str]

@DeprecationWarning
def search_existing_flink_statement(statement_name: str) -> None | Statement:
    """
    Given a table name the Flink SQL statement for dml includes the name of the table changing '_' to '-'
    """
    logger.info(f"Searching statement {statement_name} in deployed statements")
    statements = []
    client = ConfluentCloudClient(get_config())
    for statement in client.get_flink_statement_list()["data"]:
        if statement_name in statement["name"]:
            statements.append(statement)
    logger.info(f"Found those statements {statements}")
    if len(statements) > 1:
        logger.error(f"Stopping: found more than one statement for the expected statement {statement_name}")
        logger.error(f"Found those statements {statements}")
        raise Exception(f"Found more than one statement for the expected statement {statement_name}")
    if len(statements) == 1:
        return Statement(**statements[0])
    return None
    
def get_statement(statement_name: str) -> None | Statement:
    """
    Get the statement given the statement name
    """
    logger.info(f"Verify {statement_name} Flink statement status")
    client = ConfluentCloudClient(get_config())
    statement = client.get_statement_info(statement_name)
    if statement:
        logger.debug(f"Retrieved statement is {statement.model_dump_json(indent=3)}")
    return statement

    
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
            if current table already exist
                if only dml, redeploy dml taking into accound children
                else deploy ddl and dml
            else deploy ddl and dml
    """    
    logger.info("#"*20 + f"\n# Start deploying pipeline from table {table_name}\n" + "#"*20)
    table_inventory = load_existing_inventory(inventory_path)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    compute_pool_id = get_or_build_compute_pool(compute_pool_id, pipeline_def)
    to_process = set()
    to_process.add(pipeline_def)
    statement = _process_table_deployment(to_process, set(), compute_pool_id, dml_only, force_children)
    flink_statement_deployed=[pipeline_def.ddl_ref, pipeline_def.dml_ref]
    result = DeploymentReport.model_validate(table_name=table_name, 
                                             compute_pool_id=compute_pool_id, 
                                             statement_name=statement.name, 
                                             flink_statements_deployed=flink_statement_deployed)
    logger.info(f"Done with deploying pipeline from table {table_name}: {result.model_dump_json(indent=3)}")
    return result


def full_pipeline_undeploy_from_table(table_name: str, 
                               inventory_path: str ) -> str:
    """
    Stop DML statement and drop table
    Navigate to the parent(s) and continue if there is no children 
    """
    logger.info("\n"+"#"*20 + f"\n# Full pipeline delete from table {table_name}\n" + "#"*20)
    table_inventory = load_existing_inventory(inventory_path)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    if not table_ref:
        return f"ERROR: Table {table_name} not found in table inventory"
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    config = get_config()
    if pipeline_def.children:
        return f"ERROR: Could not perform a full delete from a non sink table like {table_name}"
    else:
        ddl_statement_name, dml_statement_name = _return_ddl_dml_names(pipeline_def, config)
        delete_statement_if_exists(ddl_statement_name)
        delete_statement_if_exists(dml_statement_name)
        drop_table(table_name, config['flink']['compute_pool_id'])
        trace = f"{table_name} deleted\n"
        return _delete_parent_not_shared(pipeline_def, trace, config)


def get_or_build_compute_pool(compute_pool_id: str, pipeline_def: FlinkTablePipelineDefinition):
    """
    if the compute pool is given, use it, else assess if there is a statement
    for this table already running and reuse the compute pool, if not
    reuse the compute pool persisted in the table's pipeline definition.
    else create a new pool.
    """
    config = get_config()
    if not compute_pool_id:
        compute_pool_id = config['flink']['compute_pool_id']
    logger.info(f"Validate the {compute_pool_id} exists and has enough resources")

    client = ConfluentCloudClient(config)
    if compute_pool_id and _validate_a_pool(client, compute_pool_id):
        return compute_pool_id
    else:
        logger.info(f"Look at the compute pool, currently used by {pipeline_def.dml_ref} by querying statement")
        product_name = extract_product_name(pipeline_def.path)
        _, dml_statement_name = get_ddl_dml_names_from_table(pipeline_def.table_name, 
                                                    config['kafka']['cluster_type'], 
                                                    product_name)
        statement = get_statement(dml_statement_name)
        pool_id= statement.spec.compute_pool_id
        if pool_id:
            return pool_id
        else:
            logger.info(f"Build a new compute pool")
            pool_spec = _build_compute_pool_spec(pipeline_def.table_name)
            return _create_compute_pool(pool_spec)
        
            


def deploy_flink_statement(flink_statement_file_path: str, 
                           compute_pool_id: str, 
                           statement_name: str, 
                           config: dict) -> StatementResult:
    """
    Read the SQL content for the flink_statement file name, and deploy to
    the assigned compute pool. If the statement fails, propagate the exception to higher level.
    """
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    if not statement_name:
        statement_name = os.path.basename(flink_statement_file_path).replace('.sql','').replace('_','-')
    full_file_path = from_pipeline_to_absolute(flink_statement_file_path)
    with open(full_file_path, "r") as f:
        sql_content = f.read()
        client = ConfluentCloudClient(config)
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
        statement= client.post_flink_statement(compute_pool_id, 
                                           statement_name, 
                                           sql_content,  
                                           properties )
        if "king" in statement and statement["king"] == "Statement":
            return StatementResult(results={"data" : statement})
        else:
            return statement
    

def get_table_structure(table_name: str, compute_pool_id: Optional[str] = None) -> str | None:
    """
    Run a show create table statement to get information about a table
    """

    statement_name = "show-" + table_name.replace('_','-')
    statement_list = _get_or_load_statement_list()
    if statement_name in statement_list:
        return statement_list[statement_name]
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
                statement_list[statement_name]=statement.status.phase
                return statement_list[statement_name]
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


def report_running_flink_statements(table_name: str, inventory_path: str):
    table_inventory = load_existing_inventory(inventory_path)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    config = get_config()
    client = ConfluentCloudClient(config)
    statement_list = client.get_flink_statement_list()
    results = {}
    results = _update_results_from_node(pipeline_def, statement_list,  results, table_inventory, config)
    return results


def delete_statement_if_exists(statement_name):
    statement_list = _get_or_load_statement_list()
    if statement_name in statement_list:
        config = get_config()
        client = ConfluentCloudClient(config)
        client.delete_flink_statement(statement_name)
        statement_list.pop(statement_name)
        return 
    else: # do remote API call
        statement = get_statement(statement_name)
        if statement:
            config = get_config()
            client = ConfluentCloudClient(config)
            client.delete_flink_statement(statement_name)

# ---- private API

def _process_table_deployment(to_process,
                              already_process,  
                              compute_pool_id: Optional[str] = None,
                              dml_only: bool = False,
                              force_children: bool = False,
                              come_from_child: bool = False) -> Statement:
    """
    For the given ddl if this is a sink (no children), we need to assess if parents are running up to
    the sources if not need to start them too. This is a way to start a full pipeline.
    for each node to process
            if there is a parent not yet deployed add parent to node to process. recursive call to do a BFS
            if current table already exist
                if only dml, redeploy dml taking into accound children
                else deploy ddl and dml
            else deploy ddl and dml
    """
    if len(to_process) > 0:
        config = get_config()
        current_pipeline_def: FlinkTablePipelineDefinition  = to_process.pop()
        logger.info(f"Start processing {current_pipeline_def.table_name}")
        logger.debug(f"--- {current_pipeline_def}")
        for parent in current_pipeline_def.parents:
            ddl_statement_name, dml_statement_name = _return_ddl_dml_names(parent, config)
            if not _table_exists(parent.table_name, dml_statement_name, compute_pool_id):
                logger.info(f"Table: {parent.table_name} not present, add it for processing.")
                parent_pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
                to_process.add(parent_pipeline_def)
                _process_table_deployment(to_process, already_process, compute_pool_id, dml_only, force_children, False, True)
            else:
                logger.debug(f"Parent {parent.table_name} is running, there is no need to change that!")
        ddl_statement_name, dml_statement_name = _return_ddl_dml_names(current_pipeline_def, config)
        if not _table_exists(current_pipeline_def.table_name, dml_statement_name, compute_pool_id):
            statement=_deploy_ddl_dml(current_pipeline_def, compute_pool_id, False, force_children)
        elif dml_only:
            statement=_deploy_dml(current_pipeline_def, compute_pool_id, force_children)
        else: # table exists need to recreate it only if it is children of a modified parent.
            if not come_from_child:
                statement=_deploy_ddl_dml(current_pipeline_def, compute_pool_id, True, force_children)
        return statement 
    

def _table_exists(table_name, statement_name, compute_pool_id) -> bool:
    """
    Table exists if there is a running dml writing to it, or if show create table returns a result
    """
    global statement_list
    statement_list=_get_or_load_statement_list()
    if statement_list and statement_name in statement_list:
        if statement_list[statement_name] == "RUNNING":
            return True
        elif  statement_list[statement_name] in ("FAILED", "STOPPED"):
            delete_statement_if_exists(statement_name)
            return False
        
        return get_table_structure(table_name, compute_pool_id) != None
    

def _get_or_load_statement_list() -> dict:
    """
    Caching the current statement list to avoid too many API requests
    """
    global statement_list
    if not statement_list:
        logger.info("Load the current list of Flink statements")
        config = get_config()
        client = ConfluentCloudClient(config)
        statement_list = client.get_flink_statement_list()
        logger.debug(statement_list)
    return statement_list

def _process_children(current_pipeline_def: FlinkTablePipelineDefinition, 
                      compute_pool_id: str):
    """
    """

    if not current_pipeline_def.children or len(current_pipeline_def.children) == 0:
        return
    logger.debug(f"Process childen of {current_pipeline_def.table_name}")
    for child in current_pipeline_def.children:
        if child.state_form == "Stateful":
            _deploy_ddl_dml(child, compute_pool_id, True)
            _process_children(child, compute_pool_id)
        else:
            logger.debug(f"Stop-delete {child.dml_ref}")
            logger.debug(f"Update {child.dml_ref} with offser")
            logger.debug(f"Resume {child.dml_ref}")


def _deploy_ddl_dml(to_process: FlinkTablePipelineDefinition, compute_pool_id: str, table_exists: bool = False,  force_children: bool = False):
    """
    Deploy the DDL and then the DML for the given table to process.
    """
    config = get_config()
    dml_already_deleted = False
    ddl_statement_name, dml_statement_name = _return_ddl_dml_names(to_process, config)
    logger.info(f"- Deploy ddl: {ddl_statement_name} for {to_process.table_name}")
    if table_exists:
        # need to delete the dml and the table
        delete_statement_if_exists(dml_statement_name)
        dml_already_deleted= True
        rep= drop_table(to_process.table_name)
        logger.debug(f"Dropped table {to_process.table_name} status is : {rep}")
    delete_statement_if_exists(ddl_statement_name)
    statement=deploy_flink_statement(to_process.ddl_ref, 
                                compute_pool_id, 
                                ddl_statement_name, 
                                config)
    logger.debug(f"Create table {to_process.table_name} status is : {statement}")
    delete_statement_if_exists(ddl_statement_name)
    statement = _deploy_dml(to_process, compute_pool_id, dml_statement_name, dml_already_deleted)
    return statement   

def _deploy_dml(to_process: FlinkTablePipelineDefinition, 
                compute_pool_id: str, 
                dml_statement_name: str= None,
                dml_already_delete: bool = True,  
                force_children: bool = False):
    config = get_config()
    logger.info(f"_deploy_dml() - {dml_statement_name} for {to_process.table_name}")
    if not dml_statement_name:
        _, dml_statement_name = _return_ddl_dml_names(to_process, config)
    if not dml_already_delete:
        delete_statement_if_exists(dml_statement_name)
    statement=deploy_flink_statement(to_process.dml_ref, 
                                    compute_pool_id, 
                                    dml_statement_name, 
                                    config)
    _save_compute_pool_info_in_metadata(dml_statement_name, compute_pool_id)
    _process_children(to_process,compute_pool_id)
    return statement

def _return_ddl_dml_names(to_process: FlinkTablePipelineDefinition, config: dict) -> Tuple[str,str]:
    product_name = extract_product_name(to_process.path)
    return get_ddl_dml_names_from_table(to_process.table_name, 
                                        config['kafka']['cluster_type'], 
                                        product_name)



def _delete_parent_not_shared(current_ref: FlinkTablePipelineDefinition, trace:str, config ) -> str:
    for parent in current_ref.parents:
        if len(parent.children) == 1:
            # as the parent is not shared it can be deleted
            ddl_statement_name, dml_statement_name = _return_ddl_dml_names(parent, config)
            delete_statement_if_exists(ddl_statement_name)
            delete_statement_if_exists(dml_statement_name)
            drop_table(parent.table_name, config['flink']['compute_pool_id'])
            trace+= f"{parent.table_name} deleted\n"
            pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(parent.path + "/" + PIPELINE_JSON_FILE_NAME)
            trace = _delete_parent_not_shared(pipeline_def, trace, config)
        else:
            trace+=f"{parent.table_name} has more than {current_ref.table_name} as child, so no delete"
    if len(current_ref.children) == 1:
        ddl_statement_name, dml_statement_name = _return_ddl_dml_names(current_ref, config)
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


def _save_compute_pool_info_in_metadata(statement_name, compute_pool_id: str):
    data = {}
    if os.path.exists(STATEMENT_COMPUTE_POOL_FILE):
        with open(STATEMENT_COMPUTE_POOL_FILE, "r")  as f:
            data=json.load(f) 
    data[statement_name] = {"statement_name": statement_name, "compute_pool_id": compute_pool_id}
    with open(STATEMENT_COMPUTE_POOL_FILE, "w") as f:
        json.dump(data, f, indent=4)

def _update_results_from_node(node: FlinkTablePipelineDefinition, statement_list, results, table_inventory, config: dict):
    for parent in node.parents:
        results= _search_statement_status(parent, statement_list, results, table_inventory, config)
    ddl_statement_name, dml_statement_name = _return_ddl_dml_names(node, config)
    if dml_statement_name in statement_list:
        status = statement_list[dml_statement_name]
        results[dml_statement_name]=status["phase"]
    return results

def _search_statement_status(node: FlinkTablePipelineDefinition, statement_list, results, table_inventory, config: dict):
    ddl_statement_name, statement_name = _return_ddl_dml_names(node, config)
    if statement_name in statement_list:
        status = statement_list[statement_name]
        results[statement_name]=status["phase"]
        table_ref: FlinkTableReference = get_table_ref_from_inventory(node.table_name, table_inventory)
        pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
        results = _update_results_from_node(pipeline_def, statement_list, results, table_inventory, config)
    return results

# ---- compute pool related functions

def _create_compute_pool(table_name: str) -> str:
    config = get_config()
    spec = _build_compute_pool_spec(table_name, config)
    client = ConfluentCloudClient(get_config())
    result= client.create_compute_pool(client,spec)
    if result:
        pool_id = result['id']
        _verify_compute_pool_provisioned(pool_id)


def _build_compute_pool_spec(table_name: str, config: dict) -> dict:
    spec = {}
    spec['display_name'] = "cp-" + table_name.replace('_','-')
    spec['cloud'] = config['confluent_cloud']['provider']
    spec['region'] = config['confluent_cloud']['region']
    spec['max_cfu'] =  config['flink']['max_cfu']
    spec['environment'] = { 'id': config['confluent_cloud']['environment_id']}
    return spec

def _verify_compute_pool_provisioned(client, pool_id: str) -> bool:
    """
    Wait for the compute pool is provisionned
    """
    provisioning = True
    failed = False
    while provisioning:
        logger.info("Wait ...")
        time.sleep(5)
        result= client.get_compute_pool_info(pool_id)
        provisioning = (result['status']['phase'] == "PROVISIONING")
        failed = (result['status']['phase'] == "FAILED")
    return False if failed else True


def _get_pool_usage(pool_info: dict) -> float:
    current = pool_info['status']['current_cfu']
    max = pool_info['spec']['max_cfu']
    return (current / max)

def _validate_a_pool(client: ConfluentCloudClient, compute_pool_id: str):
    """
    Validate a pool exist and with enough resources
    """
    pool_info=client.get_compute_pool_info(compute_pool_id)
    if pool_info == None:
        logger.info(f"Compute Pool not found")
        raise Exception(f"The given compute pool {compute_pool_id} is not found, prefer to stop")
    logger.info(f"Using compute pool {compute_pool_id} with {pool_info['status']['current_cfu']} CFUs for a max: {pool_info['spec']['max_cfu']} CFUs")
    ratio = _get_pool_usage(pool_info) 
    if ratio >= 0.7:
        raise Exception(f"The CFU usage at {ratio} % is too high for {compute_pool_id}")
    return pool_info['status']['phase'] == "PROVISIONED"


            