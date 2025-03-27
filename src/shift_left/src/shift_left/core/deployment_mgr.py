
import logging, os
import time
from logging.handlers import RotatingFileHandler
from pydantic import BaseModel
from shift_left.core.pipeline_mgr import (
    read_pipeline_definition_from_file,
    PIPELINE_JSON_FILE_NAME,
    FlinkTablePipelineDefinition,
    update_pipeline_definition_file,
    build_pipeline_report_from_table)  
from typing import Tuple

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config

from shift_left.core.utils.file_search import ( 
    get_ddl_dml_names_from_table, 
    extract_product_name,
    from_pipeline_to_absolute
)
from shift_left.core.flink_statement_model import *

log_dir = os.path.join(os.getcwd(), 'logs')
logger = logging.getLogger("deployment")
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(log_dir, "deployment_mgr.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=1024*1024,  # 1MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)


class DeploymentReport(BaseModel):
    table_name: str
    compute_pool_id: str
    statement_name: str
    flink_statement_deployed: List[str]


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
    DDL and the children's one. And will look at parents to be sure the deployment will work
    to avoid table not found issues.
    """    
    logger.info(f"Start deploying pipeline from table {table_name}")
    pipeline_def: FlinkTablePipelineDefinition = build_pipeline_report_from_table(table_name, inventory_path )
    result: DeploymentReport = None
    compute_pool_id= get_or_build_compute_pool(compute_pool_id, pipeline_def)
   
    statement = deploy_table(pipeline_def, compute_pool_id, dml_only, force_children)
    flink_statement_deployed=[pipeline_def.ddl_ref, pipeline_def.dml_ref]
    result = DeploymentReport(table_name, compute_pool_id, statement.statement_name, flink_statement_deployed)
    logger.info(f"Done with deploying pipeline from table {table_name}: {result.model_dump_json(indent=3)}")
    return result
 
def get_or_build_compute_pool(compute_pool_id: str, pipeline_def: FlinkTablePipelineDefinition):
    """
    if the compute pool is given, use it, else assess if there is a statement
    for this table already running and reuse the compute pool, if not
    reuse the compute pool persisted in the table's pipeline definition.
    else create a new pool.
    """
    logger.info(f"Validate the {compute_pool_id} exists and has enough resources")
    config = get_config()
    client = ConfluentCloudClient(config)
    if compute_pool_id and _validate_a_pool(client, compute_pool_id):
        return compute_pool_id
    else:
        pool_id= read_pipeline_definition_from_file(pipeline_def.path).compute_pool_id
        logger.info(f"Look at alternate compute pool, reusing the {pool_id} defined in the  {pipeline_def.table_name}'s metadata")
        if pool_id and _validate_a_pool(client, pool_id):
            return pool_id
        else:
            logger.info(f"Look at the compute pool, currently used by {pipeline_def.dml_ref} by querying statement")
            product_name = extract_product_name(pipeline_def.path)
            _, dml_statement_name = get_ddl_dml_names_from_table(pipeline_def.table_name, 
                                                      config['kafka']['cluster_type'], 
                                                      product_name)
            statement = search_existing_flink_statement(dml_statement_name)
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
    the assigned compute pool.
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
        return client.post_flink_statement(compute_pool_id, 
                                           statement_name, 
                                           sql_content,  
                                           properties )




def deploy_table(pipeline_def: FlinkTablePipelineDefinition, 
                              compute_pool_id: Optional[str] = None,
                              dml_only: bool = False,
                              force_children: bool = False) -> Statement:
    """
    For the given ddl if this is a sink (no children), we need to assess if parents are running up to
    the sources if not need to start them too. This is a way to start a full pipeline.
    """
    config = get_config()
    product_name = extract_product_name(pipeline_def.path)
    ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_table(pipeline_def.table_name, 
                                                      config['kafka']['cluster_type'], 
                                                      product_name)
         
    # At this stage: there is no more ddl and dml statements, but table still exists
    to_process = [(pipeline_def, ddl_statement_name, dml_statement_name)]        
    statement = _deploy_current_with_parents_when_needed(to_process, 
                                                           pipeline_def, 
                                                           compute_pool_id, 
                                                           dml_only,
                                                           config)
    _process_children(pipeline_def, compute_pool_id, force_children, config)
    return statement

def get_table_structure(table_name: str, compute_pool_id: Optional[str] = None) -> str:
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    client = ConfluentCloudClient(config)
    sql_content = f"show create table {table_name};"
    statement_name = "show-" + table_name.replace('_','-')
    properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    statement = search_existing_flink_statement(statement_name)
    if statement:
        client.delete_flink_statement(statement_name)
    try:
        statement_result= client.post_flink_statement(compute_pool_id, statement_name, sql_content, properties)
        if len(statement_result.results) > 0:
            result_str = statement_result.results[0]['results']['data'][0]['row'][0]
        logger.debug(f"Run show create table in {result_str}")
        return result_str
    except Exception as e:
        logger.info(e)
        return None

def drop_table(table_name: str, compute_pool_id: Optional[str] = None):
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    client = ConfluentCloudClient(config)
    sql_content = f"drop table {table_name};"
    properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
    statement_name = "drop-" + table_name.replace('_','-')
    statement = search_existing_flink_statement(statement_name)
    if statement:
        client.delete_flink_statement(statement_name)
    result= client.post_flink_statement(compute_pool_id, statement_name, sql_content, properties)
    logger.debug(f"Run drop table {result}")

def delete_flink_statement(statement_name: str) -> str:
    logger.info(f"Deleting Flink statement: {statement_name}")
    client = ConfluentCloudClient(get_config())
    return client.delete_flink_statement(statement_name)


# ---- private API

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



def _process_children(current_pipeline_def: FlinkTablePipelineDefinition, 
                      compute_pool_id: str,
                      config: dict):
    if not current_pipeline_def.children or len(current_pipeline_def.children) == 0:
        return
    for child in current_pipeline_def.children:
        product_name = extract_product_name(child.path)
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_table(child.table_name, 
                                                      config['kafka']['cluster_type'], 
                                                      product_name)
        if child.compute_pool_id or len(child.compute_pool_id) > 0:
            compute_pool_id = child.compute_pool_id
        if child.state_form == "Stateful":
            logger.debug(f"Stop-delete {child.dml_ref}")
            statement = get_statement(dml_statement_name)
            if statement:
                delete_flink_statement(dml_statement_name)
            logger.debug(f"Drop table {child.table_name}")
            drop_table(child.table_name, compute_pool_id)
            logger.debug(f"Create {child.ddl_ref}")
            statement=deploy_flink_statement(child.ddl_ref, 
                                    compute_pool_id, 
                                    ddl_statement_name, 
                                    config)
            delete_flink_statement(ddl_statement_name)
            logger.debug(f"Start {child.dml_ref}")
            statement=deploy_flink_statement(child.dml_ref, 
                                    compute_pool_id, 
                                    dml_statement_name, 
                                    config)
            logger.debug(f"Update definition of this table")
            #_save_compute_pool_info_in_metadata(child, compute_pool_id)
            logger.debug(f"Process childen of {child.table_name}")
            _process_children(child)
        else:
            logger.debug(f"Stop-delete {child.dml_ref}")
            logger.debug(f"Update {child.dml_ref} with offser")
            logger.debug(f"Resume {child.dml_ref}")



def _deploy_current_with_parents_when_needed(table_list_to_process: Tuple[FlinkTablePipelineDefinition, str, str], 
                              current_pipeline_def: FlinkTablePipelineDefinition, 
                              compute_pool_id: str,
                              dml_only: bool = False, 
                              config: dict = None):
    """
    The current table may have parents not yet deployed so we need to deploy them
    then deploy the dml and ddl of the current table. This function is recurring and do a Breadth first traversal
    """
    
    if current_pipeline_def.parents == None or len(current_pipeline_def.parents) == 0:
        # At the source level, just deploy the ddl / dml statements
        to_process= table_list_to_process.pop()
        return _deploy_ddl_dml(to_process, compute_pool_id, dml_only, config)
    else:
        logger.info(f"Process parents of {current_pipeline_def.table_name}")
        for parent in current_pipeline_def.parents:
            logger.info(f"\nVerify DML: {parent.dml_ref} is running")
            if not get_table_structure(parent.table_name, compute_pool_id):
                # not existing parent table so run DDL and DML
                table_list_to_process.append((parent, ddl_statement_name, dml_statement_name))
                _deploy_current_with_parents_when_needed(table_list_to_process, parent, compute_pool_id, False, config)
            else:
                logger.info(f"Parent table: {parent.table_name} already exists, so not recreating it")
                product_name = extract_product_name(parent.path)
                ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_table(parent.table_name, 
                                                            config['kafka']['cluster_type'], 
                                                            product_name)
                statement = get_statement(dml_statement_name)
                # if parent is running do not redeploy it.
                if not statement:
                    table_list_to_process.append((parent, ddl_statement_name, dml_statement_name))
                    _deploy_current_with_parents_when_needed(table_list_to_process, parent, compute_pool_id, True, config)
                elif statement.status.phase == 'FAILED':
                    delete_flink_statement(dml_statement_name)
                    table_list_to_process.append((parent, ddl_statement_name, dml_statement_name))
                    _deploy_current_with_parents_when_needed(table_list_to_process, parent, compute_pool_id, True, config)
                else:
                    logger.info(f"Parent {dml_statement_name} already running, so no change")
        to_process= table_list_to_process.pop()
        return _deploy_ddl_dml(to_process, compute_pool_id, dml_only, config)
        


def _deploy_ddl_dml(to_process: Tuple[FlinkTablePipelineDefinition, str, str], compute_pool_id: str, dml_only: bool, config: dict):
    current_statement = to_process[0]
    ddl_statement_name =  to_process[1]
    dml_statement_name =  to_process[2]
    cpool_id = compute_pool_id
    if current_statement.compute_pool_id or len(current_statement.compute_pool_id) > 0:
        cpool_id = current_statement.compute_pool_id
    if not dml_only:
        logger.info(f"_deploy_ddl_dml() - Deploy ddl: {ddl_statement_name} for {current_statement.table_name}")
        drop_table(current_statement.table_name)
        statement=deploy_flink_statement(current_statement.ddl_ref, 
                                    cpool_id, 
                                    ddl_statement_name, 
                                    config)
        delete_flink_statement(ddl_statement_name)
    logger.info(f"_deploy_ddl_dml() - Deploy  dml: {dml_statement_name}")
    statement=deploy_flink_statement(current_statement.dml_ref, 
                                    cpool_id, 
                                    dml_statement_name, 
                                    config)
    # TO DO assess if we need to persist execution timing 
    #_save_compute_pool_info_in_metadata(current_statement, cpool_id)
    return statement   

def _save_compute_pool_info_in_metadata(current_definition: FlinkTablePipelineDefinition, compute_pool_id: str):
    current_definition.compute_pool_id = compute_pool_id
    fname = current_definition.path  + "/" + PIPELINE_JSON_FILE_NAME
    update_pipeline_definition_file(fname,  current_definition)

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


            