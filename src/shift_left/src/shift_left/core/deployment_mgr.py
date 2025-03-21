import subprocess
import logging, os
import time
from logging.handlers import RotatingFileHandler
from pydantic import BaseModel
from shift_left.core.pipeline_mgr import (
    ReportInfoNode,
    read_pipeline_metadata,
    walk_the_hierarchy_for_report_from_table)  

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config
from shift_left.core.pipeline_mgr import ReportInfoNode, PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.file_search import (
    FlinkTableReference, 
    get_ddl_dml_names_from_table, 
    extract_product_name,
    load_existing_inventory
)
from shift_left.core.flink_statement_model import *

import json

log_dir = os.path.join(os.getcwd(), 'logs')
logger = logging.getLogger("deployment")
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(log_dir, "cc-client.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=1024*1024,  # 1MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
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
        raise Exception(f"found more tFan one statement for the expected statement {statement_name}")
    if len(statements) == 1:
        return Statement(**statements[0])
    return None
    

def deploy_pipeline_from_table(table_name: str, 
                               inventory_path: str, 
                               compute_pool_id: str,
                               dml_only: bool = False,
                               force: bool = False ) -> DeploymentReport:
    """
    Given the table name, executes the dml and ddl to deploy a pipeline.
    If the compute pool id is present it will use it. If not it will 
    get the existing pool_id from the table already deployed.
    """    
    pipeline_def: ReportInfoNode = walk_the_hierarchy_for_report_from_table(table_name, inventory_path )
    if pipeline_def is None:
        raise Exception(f"Table {table_name} not found in inventory")
    result: DeploymentReport = None
    compute_pool_id= get_or_build_compute_pool(compute_pool_id, pipeline_def)
    if dml_only:
        statement_name = _deploy_dml_statements(pipeline_def, compute_pool_id, force)
        flink_statement_deployed=[pipeline_def.dml]
        result = DeploymentReport(table_name, compute_pool_id, statement_name,flink_statement_deployed)
    else:
        _deploy_ddl_statements(pipeline_def, compute_pool_id, force)
        statement_name= _deploy_dml_statements(pipeline_def, compute_pool_id, force)
        flink_statement_deployed=[pipeline_def.ddl_path, pipeline_def.dml]
        result = DeploymentReport(table_name, compute_pool_id, statement_name,flink_statement_deployed)
    return result
 
def get_or_build_compute_pool(compute_pool_id: str, pipeline_def: ReportInfoNode):
    """
    if the compute pool is given, use it, else assess if there is a statement
    for this table already running and reuse the compute pool, if not
    reuse the compute pool persisted in the table's pipeline definition.
    else create a new pool.
    """
    client = ConfluentCloudClient(get_config())
    if compute_pool_id and _validate_a_pool(client, compute_pool_id):
        return compute_pool_id
    else:
        statement = search_existing_flink_statement(pipeline_def.dml_path)
        pool_id= statement.spec.compute_pool_id
        if pool_id:
            return pool_id
        else:
            pool_id= read_pipeline_metadata(pipeline_def.base_path).compute_pool_id
            if pool_id and _validate_a_pool(client, pool_id):
                return pool_id
            else:
                pool_spec = _build_compute_pool_spec(pipeline_def.table_name)
                return _create_compute_pool(pool_spec)


# ---- private API

def _garbage():
   
    
    statement = search_existing_flink_statement(pipeline_def.dml_path)
    logger.info(f"* Delete the current table DML statement to stop processing")
    if statement:
        _delete_flink_statement(statement)
    logger.info(f"* Stop children dml statements - recursively")
    _stop_child_dmls(pipeline_def, inventory_path)
    logger.info("recreate the table using the DDL")
    
    logger.info(f"* Recreate the new DML for this table")
   
    logger.info(f"* Re-start the child DMLs")
    _start_child_dmls(pipeline_def, inventory_path)


def _deploy_ddl_statements(pipeline_def: ReportInfoNode, 
                           compute_pool_id: str, 
                           force: bool = False) -> Statement:
    config = get_config()
    product_name = extract_product_name(pipeline_def.base_path)
    ddl_statement_name, _ = get_ddl_dml_names_from_table(pipeline_def.table_name, 
                                                      config['kafka']['cluster_type'], 
                                                      product_name)
    logger.info(f"Deploying DDL statements from {pipeline_def.ddl_path} named {ddl_statement_name}")
    statement = search_existing_flink_statement(ddl_statement_name)
    if statement:
        _delete_flink_statement(ddl_statement_name)
    _deploy_flink_statement(pipeline_def.ddl_path, compute_pool_id, ddl_statement_name, config)

def _deploy_dml_statements(dml_path: str):
    logger.info(f"Deploying DML statements from {dml_path}")
    # TODO: implement

def _deploy_flink_statement(content_path: str, compute_pool_id: str, statement_name: str, config: dict):
    with open(content_path, "r") as f:
        sql_content = f.read()
        client = ConfluentCloudClient(config)
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
        return client.post_flink_statement(compute_pool_id, statement_name, sql_content,  properties )

def _delete_flink_statement(statement_name: str) -> str:
    logger.info(f"Deleting Flink statement: {statement_name}")
    client = ConfluentCloudClient(get_config())
    result = client.delete_flink_statement(statement_name).json()
    logger.info(result)
    if "404" in result['errors'][0]['status']:
        return result['errors'][0]['detail']
    elif "202" in result:
        return "statement deleted"
    else:
        return result['errors'][0]

    
def _stop_dml_statement(table_name: str, statement):
    logger.info(f"Stopping DML statements for {table_name} with {statement}")
    client = ConfluentCloudClient(get_config())
    rep = client.update_flink_statement(statement, True)
    logger.info(rep)

def _resume_dml_statements(table_name: str):
    logger.info(f"Resume DML statements for {table_name}")
    

def _stop_child_dmls(pipeline_def, inventory_path: str):
    if not pipeline_def.children or len(pipeline_def.children) == 0:
        return
    for node in pipeline_def.children:
        logger.info(node)
        node_ref=walk_the_hierarchy_for_report_from_table(node['table_name'], inventory_path )
        _stop_dml_statement(node['table_name']) # stop or delete and recreate
        _stop_child_dmls(node_ref)

def _start_child_dmls(pipeline_def):
    if not pipeline_def.children or len(pipeline_def.children) == 0:
        return
    for node in pipeline_def.children:
        _resume_dml_statements(node['table_name']) # stop or delete and recreate
        _pipeline_def = ReportInfoNode.model_validate(node)
        _start_child_dmls(_pipeline_def)

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

def _verify_compute_pool_provisioned(client, pool_id: str):
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
    return (current / max) * 100

def _validate_a_pool(client: ConfluentCloudClient, compute_pool_id: str):
    """
    Validate a pool exist or not and with enough resources
    """
    pool_info=client.get_compute_pool_info(compute_pool_id)
    if pool_info == None:
        logger.info(f"Compute Pool not found")
        raise Exception(f"The given compute pool {compute_pool_id} is not, prefer to stop")
    logger.info(f"Using compute pool {compute_pool_id} with {pool_info['status']['current_cfu']} CFUs for a max: {pool_info['spec']['max_cfu']} CFUs")
    ratio = _get_pool_usage(pool_info) 
    if ratio <= 0.3:
        raise Exception(f"The CFU usage at {ratio} % is too high for {compute_pool_id}")
    return True


            