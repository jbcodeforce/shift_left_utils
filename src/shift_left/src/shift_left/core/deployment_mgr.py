import subprocess
from shift_left.core.pipeline_mgr import (
    ReportInfoNode,
    walk_the_hierarchy_for_report_from_table)  

from shift_left.core.utils.ccloud_client import verify_compute_pool_exists, ConfluentCloudClient
from shift_left.core.utils.app_config import get_config
from shift_left.core.pipeline_mgr import ReportInfoNode
from shift_left.core.utils.file_search import FlinkTableReference
import json

def get_pool_usage(compute_pool_id: str) -> json:
    result=subprocess.run(["confluent", "flink", "compute-pool", "describe", compute_pool_id, "-o", "json"],capture_output=True, text=True)
    return json.loads(result.stdout)

def search_existing_flink_statement(table_name: str):
    client = ConfluentCloudClient(get_config())
    statement_name = table_name.replace("_","-")
    for statement in client.get_flink_statement_list()["data"]:
        print(statement["name"])
        if statement_name in statement["name"]:
            print(f"--> {statement}")
    

def deploy_pipeline_from_table(table_name: str, inventory_path: str, compute_pool_id: str) -> None:
    """
    """
    pipeline_def=walk_the_hierarchy_for_report_from_table(table_name,inventory_path )
    if pipeline_def is None:
        raise Exception(f"Table {table_name} not found in inventory")
    config = get_config()
    if compute_pool_id:
        #usage=verify_compute_pool_exists(compute_pool_id)
        #usage=get_pool_usage(compute_pool_id)
        #print(f"Using compute pool {compute_pool_id} with {usage['currrent_cfu']} CFUs for a max: {usage['max_cfu']} CFUs")
        pass
    search_existing_flink_statement(table_name)
    # delete the statement
    # recreate the new one with a new table
    for node in pipeline_def.children:
        print(f" Stop {node['table_name']}")
        _stop_dml_statements(node['dml_path']) # stop or delete and recreate

def _deploy_ddl_statements(ddl_path: str):
    print(f"Deploying DDL statements from {ddl_path}")
    # TODO: implement
def _deploy_dml_statements(dml_path: str):
    print(f"Deploying DML statements from {dml_path}")
    # TODO: implement
