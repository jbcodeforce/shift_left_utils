import subprocess
import logging, os
from shift_left.core.pipeline_mgr import (
    ReportInfoNode,
    walk_the_hierarchy_for_report_from_table)  

from shift_left.core.utils.ccloud_client import verify_compute_pool_exists, ConfluentCloudClient
from shift_left.core.utils.app_config import get_config
from shift_left.core.pipeline_mgr import ReportInfoNode
from shift_left.core.utils.file_search import FlinkTableReference, get_ddl_dml_names_from_table, extract_product_name
import json

log_path = os.path.join(os.getcwd(), '..', 'logs')
if not os.path.exists(log_path):
    os.mkdir(log_path)

logging.basicConfig(
    filename=os.path.join(log_path, 'deployment.log'),
    filemode='w',
    level=get_config()["app"]["logging"],
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def get_pool_usage(compute_pool_id: str) -> json:
    result=subprocess.run(["confluent", "flink", "compute-pool", "describe", compute_pool_id, "-o", "json"],capture_output=True, text=True)
    return json.loads(result.stdout)

def search_existing_flink_statement(statement_name: str):
    """
    Given a table name the Flink SQL statement for dml includes the name of the table changing '_' to '-'
    """
    logging.debug(f"Searching statement {statement_name} in deployed statements")
    print(f"Searching statement {statement_name} in deployed statements")
    statements = []
    client = ConfluentCloudClient(get_config())
    for statement in client.get_flink_statement_list()["data"]:
        if statement_name in statement["name"]:
            statements.append(statement)
    logging.info(f"Found those statements {statements}")
    print(f"Found those statements {statements}")
    return statements
    

def deploy_pipeline_from_table(table_name: str, inventory_path: str, compute_pool_id: str) -> None:
    """
    Given the table name, executes the dml and ddl to deploy a pipeline.
    """
    pipeline_def = walk_the_hierarchy_for_report_from_table(table_name, inventory_path )
    if pipeline_def is None:
        raise Exception(f"Table {table_name} not found in inventory")
    config = get_config()
    if compute_pool_id:
        #usage=verify_compute_pool_exists(compute_pool_id)
        #usage=get_pool_usage(compute_pool_id)
        #print(f"Using compute pool {compute_pool_id} with {usage['currrent_cfu']} CFUs for a max: {usage['max_cfu']} CFUs")
        pass
    product_name = extract_product_name(pipeline_def.base_path)
    ddl_name, dml_name = get_ddl_dml_names_from_table(table_name, config["kafka"]["cluster_type"], product_name)
    statements = search_existing_flink_statement(dml_name)
    print(f"* Delete the current table DML statement to stop processing")
    _stop_dml_statements(table_name, statements)
    print(f"* Stop children dml statements - recursively")
    _stop_child_dmls(pipeline_def, inventory_path)
    print(f"* Recreate the new DML for this table")
    _deploy_table_statements(pipeline_def.dml_path)
    print(f"* Re-start the child DMLs")
    _start_child_dmls(pipeline_def, inventory_path)


def _deploy_ddl_statements(ddl_path: str):
    print(f"Deploying DDL statements from {ddl_path}")
    # TODO: implement

def _deploy_table_statements(dml_path: str):
    print(f"Deploying DML statements from {dml_path}")
    # TODO: implement


def _stop_dml_statements(table_name: str, statements):
    print(f"Stopping DML statements for {table_name}")
    pass

def _resume_dml_statements(table_name: str):
    print(f"Resume DML statements for {table_name}")
    pass

def _stop_child_dmls(pipeline_def, inventory_path: str):
    if not pipeline_def.children or len(pipeline_def.children) == 0:
        return
    for node in pipeline_def.children:
        print(node)
        node_ref=walk_the_hierarchy_for_report_from_table(node['table_name'], inventory_path )
        _stop_dml_statements(node['table_name']) # stop or delete and recreate
        _stop_child_dmls(node_ref)

def _start_child_dmls(pipeline_def):
    if not pipeline_def.children or len(pipeline_def.children) == 0:
        return
    for node in pipeline_def.children:
        _resume_dml_statements(node['table_name']) # stop or delete and recreate
        _pipeline_def = ReportInfoNode.model_validate(node)
        _start_child_dmls(_pipeline_def)