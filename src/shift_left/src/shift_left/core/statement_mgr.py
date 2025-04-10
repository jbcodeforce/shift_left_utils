""""
Copyright 2024-2025 Confluent, Inc.
"""
from typing import List
import os
from importlib import import_module
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.pipeline_mgr import (
    FlinkTablePipelineDefinition,
    get_or_build_inventory,
    PIPELINE_JSON_FILE_NAME
)
from shift_left.core.utils.file_search import ( 
    read_pipeline_definition_from_file,
    get_table_ref_from_inventory,
    get_ddl_dml_names_from_pipe_def,
    from_pipeline_to_absolute
)
from shift_left.core.flink_statement_model import Statement, StatementResult
from shift_left.core.utils.file_search import (
    FlinkTableReference
)


def deploy_flink_statement(flink_statement_file_path: str, 
                           compute_pool_id: str, 
                           statement_name: str, 
                           config: dict) -> StatementResult:
    """
    Read the SQL content for the flink_statement file name, and deploy to
    the assigned compute pool. If the statement fails, propagate the exception to higher level.
    """
    logger.debug(f"{statement_name} with content: {flink_statement_file_path} deploy to {compute_pool_id}")
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    if not statement_name:
        statement_name = os.path.basename(flink_statement_file_path).replace('.sql','').replace('_','-')
    full_file_path = from_pipeline_to_absolute(flink_statement_file_path)
    with open(full_file_path, "r") as f:
        sql_content = f.read()
        transformer = _get_or_build_sql_content_transformer()
        _, sql_out= transformer.update_sql_content(sql_content)
        client = ConfluentCloudClient(config)
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
        statement= client.post_flink_statement(compute_pool_id, 
                                           statement_name, 
                                           sql_out,  
                                           properties )
        if statement and "king" in statement and statement["king"] == "Statement":
            return StatementResult(results={"data" : statement})
        else:
            return statement
        
def report_running_flink_statements(table_name: str, inventory_path: str):
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    config = get_config()
    client = ConfluentCloudClient(config)
    statement_list = client.get_flink_statement_list()
    results = {}
    return results

def delete_statement_if_exists(statement_name):
    logger.info(f"{statement_name}")
    statement_list = get_statement_list()
    if statement_name in statement_list:
        logger.info(f"{statement_name} in the cached statement list")
        config = get_config()
        client = ConfluentCloudClient(config)
        client.delete_flink_statement(statement_name)
        statement_list.pop(statement_name)
        return 
    else: # not found in cache, do remote API call
        logger.info(f"{statement_name} not found in cache trying REST api call")
        config = get_config()
        client = ConfluentCloudClient(config)
        client.delete_flink_statement(statement_name)


def get_statement(statement_name: str) -> None | Statement:
    """
    Get the statement given the statement name
    """
    logger.info(f"Verify {statement_name} statement's status")
    client = ConfluentCloudClient(get_config())
    statement = client.get_statement_info(statement_name)
    if statement and isinstance(statement, Statement):
        logger.info(f"Retrieved statement is {statement.model_dump_json(indent=3)}")
        if statement.status and statement.status.phase:
            get_statement_list()[statement_name] = statement.status.phase
    return statement

_statement_list = None  # cache the statment loaded to limit the number of call to CC API

def get_statement_list() -> List[Statement]:
    global _statement_list
    if _statement_list == None:
        logger.info("Load the current list of Flink statements from REST API")
        config = get_config()
        client = ConfluentCloudClient(config)
        _statement_list = client.get_flink_statement_list()
        if not _statement_list:
            logger.debug(f"statement list seems empty.")
            return []
    return _statement_list

def _update_results_from_node(node: FlinkTablePipelineDefinition, statement_list, results, table_inventory, config: dict):
    for parent in node.parents:
        results= _search_statement_status(parent, statement_list, results, table_inventory, config)
    ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(node, config['kafka']['cluster_type'])
    if dml_statement_name in statement_list:
        status = statement_list[dml_statement_name]
        results[dml_statement_name]=status
    return results

def _search_statement_status(node: FlinkTablePipelineDefinition, statement_list, results, table_inventory, config: dict):
    ddl_statement_name, statement_name = get_ddl_dml_names_from_pipe_def(node, config['kafka']['cluster_type'])
    if statement_name in get_statement_list():
        status = get_statement_list()[statement_name]
        results[statement_name]=status
        table_ref: FlinkTableReference = get_table_ref_from_inventory(node.table_name, table_inventory)
        pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
        results = _update_results_from_node(pipeline_def, statement_list, results, table_inventory, config)
    return results

_runner_class = None
def _get_or_build_sql_content_transformer():
    global _runner_class
    if not _runner_class:
        class_to_use = get_config().get('app').get('sql_content_modifier')
        module_path, class_name = class_to_use.rsplit('.',1)
        mod = import_module(module_path)
        _runner_class = getattr(mod, class_name)()
    return _runner_class