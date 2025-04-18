""""
Copyright 2024-2025 Confluent, Inc.
A set of operations to manage a flink statement
"""
from typing import List, Optional
import os
import time
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
from shift_left.core.flink_statement_model import Statement, StatementResult, StatementInfo
from shift_left.core.utils.file_search import (
    FlinkTableReference
)
from shift_left.core.utils.table_worker import ReplaceEnvInSqlContent

def build_and_deploy_flink_statement_from_sql_content(flink_statement_file_path: str, 
                           compute_pool_id: str = None, 
                           statement_name: str= None, 
                           config: dict = None) -> str:
    """
    Read the SQL content for the flink_statement file name, and deploy to
    the assigned compute pool. If the statement fails, propagate the exception to higher level.
    """
    
    if not config:
        config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    if not statement_name:
        statement_name = (config['kafka']['cluster_type'] + "-" + os.path.basename(flink_statement_file_path).replace('.sql','')).replace('_','-').replace('.','-')
    logger.info(f"{statement_name} with content: {flink_statement_file_path} deploy to {compute_pool_id}")
    full_file_path = from_pipeline_to_absolute(flink_statement_file_path)
    try:
        with open(full_file_path, "r") as f:
            sql_content = f.read()
            transformer = _get_or_build_sql_content_transformer()
            _, sql_out= transformer.update_sql_content(sql_content)

            statement= post_flink_statement(compute_pool_id, 
                                            statement_name, 
                                            sql_out)
            logger.debug(f"Statement: {statement_name} -> {statement}")
            if statement and statement.status:
                logger.debug(f"Statement: {statement_name} status is: {statement.status.phase}")
                get_statement_list()[statement_name]=statement.status.phase   # important to avoid doing an api call
                return { "name": statement_name, "status": "success"}
            else:
                return { "name": statement_name, "status": "not_sure"}
    except Exception as e:
        logger.error(e)
        return { "name": statement_name, "status": "failed"}


def get_statement_status(statement_name: str) -> StatementInfo:
    statement_list = get_statement_list()
    if statement_list and statement_name in statement_list:
        return statement_list[statement_name]
    statement_info = StatementInfo(name=statement_name,
                                   status_phase="UNKNOWN",
                                   status_detail="Statement not found int the existing deployed Statements"
                                )
    return statement_info  

def post_flink_statement(compute_pool_id: str,  
                             statement_name: str, 
                             sql_content: str,
                             stopped: bool = False) -> Statement: 
        """
        POST to the statements API to execute a SQL statement.
        """
        config = get_config()
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
        client = ConfluentCloudClient(config)
        url = client.build_flink_url_and_auth_header()
        statement_data = {
                "name": statement_name,
                "organization_id": config["confluent_cloud"]["organization_id"],
                "environment_id": config["confluent_cloud"]["environment_id"],
                "spec": {
                    "statement": sql_content,
                    "properties": properties,
                    "compute_pool_id":  compute_pool_id,
                    "stopped": stopped
                }
            }
        try:
            logger.debug(f"> Send POST request to Flink statement api with {statement_data}")
            start_time = time.perf_counter()
            response = client.make_request("POST", url + "/statements", statement_data)
            logger.debug(f"> POST response= {response}")
            if response.get('errors'):
                logger.error(f"Error executing rest call: {response['errors']}")
                return  None
                #raise Exception(response['errors'][0]['detail'])
            elif response["status"]["phase"] == "PENDING":
                return client.wait_response(url, statement_name, start_time)
            return  None
        except Exception as e:
            logger.error(f"Error executing rest call: {e}")
            raise e
                
def report_running_flink_statements(table_name: str, inventory_path: str):
    """
    Report running flink statements from a table
    """
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    config = get_config()
    client = ConfluentCloudClient(config)
    statement_list = client.get_flink_statement_list()
    results = {}
    # TO DO to terminate
    return results

def delete_statement_if_exists(statement_name) -> str | None:
    logger.info(f"Enter with {statement_name}")
    statement_list = get_statement_list()
    if statement_name in statement_list:
        logger.info(f"{statement_name} in the cached statement list")
        config = get_config()
        client = ConfluentCloudClient(config)
        result = client.delete_flink_statement(statement_name)
        if result == "deleted":
            statement_list.pop(statement_name)
            return "deleted"
        return None
    else: # not found in cache, do remote API call
        logger.info(f"{statement_name} not found in cache, trying REST api call")
        config = get_config()
        client = ConfluentCloudClient(config)
        return client.delete_flink_statement(statement_name)


def get_statement_info(statement_name: str) -> None | StatementInfo:
    """
    Get the statement given the statement name
    """
    logger.info(f"Verify {statement_name} statement's status")
    if statement_name in get_statement_list():
        return get_statement_list()[statement_name]
    client = ConfluentCloudClient(get_config())
    statement = client.get_flink_statement(statement_name)
    if statement and isinstance(statement, Statement):
        statement_info = _map_to_statement_info(statement.data)
        get_statement_list()[statement_name] = statement_info
        return statement_info
    return None


        
_statement_list = None  # cache the statement list loaded to limit the number of call to CC API
def get_statement_list() -> dict[str, StatementInfo]:
    """
    Get the statement list from the CC API - the list is <statement_name, statement_info>
    """
    global _statement_list
    if _statement_list == None:
        _statement_list = {}
        logger.info("Load the current list of Flink statements from REST API")
        config = get_config()
        page_size = config["confluent_cloud"].get("page_size", 100)
        client = ConfluentCloudClient(config)
        url=client.build_flink_url_and_auth_header()+"/statements?page_size="+str(page_size)
        next_page_token = None
        while True:
            if next_page_token:
                resp=client.make_request("GET", next_page_token)
            else:
                resp=client.make_request("GET", url)
            logger.debug("Statement execution result:", resp)
            if resp and 'data' in resp:
                for info in resp.get('data'):
                    statement_info = _map_to_statement_info(info)
                    _statement_list[info['name']] = statement_info
            if "metadata" in resp and "next" in resp["metadata"]:
                next_page_token = resp["metadata"]["next"]
                if not next_page_token:
                    break
            else:
                break
    return _statement_list


def show_flink_table_structure(table_name: str, compute_pool_id: Optional[str] = None) -> str | None:
    """
    Retrieves the DDL structure of a Flink SQL table by executing a SHOW CREATE TABLE statement.

    This function connects to a Confluent Cloud Flink compute pool and executes a SHOW CREATE TABLE
    statement to get the full table definition, including columns, properties and other attributes.

    Args:
        table_name: The name of the table to get the structure for
        compute_pool_id: Optional ID of the Flink compute pool to use. If not provided, uses the default
                        from the configuration.

    Returns:
        str | None: The CREATE TABLE statement as a string if successful, None if the table doesn't exist
                   or there was an error.

    Raises:
        No exceptions are raised - errors are logged and None is returned.

    Example:
        >>> structure = get_table_structure("my_table")
        >>> print(structure)
        'CREATE TABLE my_table (...) WITH (...)'
    """
    logger.debug(f"{table_name}")
    statement_name = "show-" + table_name.replace('_','-')
    result_str = None
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    client = ConfluentCloudClient(config)
    sql_content = f"show create table {table_name};"
    delete_statement_if_exists(statement_name)
    try:
        statement = post_flink_statement(compute_pool_id, statement_name, sql_content)
        if statement and isinstance(statement, Statement) and statement.status.phase in ("RUNNING", "COMPLETED"):
            get_statement_list()[statement_name] = _map_to_statement_info(statement.data)
            statement_result = client.get_statement_results(statement_name)
            if statement_result and isinstance(statement_result, StatementResult):
                if statement_result.results and len(statement_result.results.data) > 0:
                    result_str = str(statement_result.results.data[0].row[0])
                    logger.debug(f"Run show create table:\n {result_str}")

    except Exception as e:
        logger.error(f"get_table_structure {e}")
    finally:
        delete_statement_if_exists(statement_name)
        return result_str



def drop_table(table_name: str, compute_pool_id: Optional[str] = None):
    """
    Drops a Flink SQL table if it exists.

    This function connects to a Confluent Cloud Flink compute pool and executes a DROP TABLE
    statement to remove the table from the database.

    Args:
        table_name: The name of the table to drop
        compute_pool_id: Optional ID of the Flink compute pool to use. If not provided, uses the default
                        from the configuration.

    Returns:
        str: A message indicating the table was dropped successfully
    """
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    logger.debug(f"Run drop table {table_name}")
    sql_content = f"drop table if exists {table_name};"
    drop_statement_name = "drop-" + table_name.replace('_','-')
    try:
        delete_statement_if_exists(drop_statement_name)
        result= post_flink_statement(compute_pool_id, 
                                            drop_statement_name, 
                                            sql_content)
        logger.debug(f"Run drop table {result}")
        delete_statement_if_exists(drop_statement_name)
    except Exception as e:
        logger.error(f"drop_table {e}")
    return f"{table_name} dropped"

# ------------- private methods -------------
def _map_to_statement_info(info: dict) -> StatementInfo:
    if 'properties' in info.get('spec') and info.get('spec').get('properties'):
        catalog = info.get('spec',{}).get('properties',{}).get('sql.current-catalog','UNKNOWN')
        database = info.get('spec',{}).get('properties',{}).get('sql.current-database','UNKNOWN')
    else:
        catalog = 'UNKNOWN' 
        database = 'UNKNOWN'
    return StatementInfo(name=info['name'],
                                    status_phase= info.get('status').get('phase', 'UNKNOWN'),
                                    status_detail= info.get('status').get('detail', 'UNKNOWN'),
                                    sql_content= info.get('spec').get('statement', 'UNKNOWN'),
                                    compute_pool_id= info.get('spec').get('compute_pool_id'),
                                    principal= info.get('spec').get('principal', 'UNKNOWN'),
                                    sql_catalog=catalog,
                                    sql_database=database)


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
        if get_config().get('app').get('sql_content_modifier'):
        
            class_to_use = get_config().get('app').get('sql_content_modifier')
            module_path, class_name = class_to_use.rsplit('.',1)
            mod = import_module(module_path)
            _runner_class = getattr(mod, class_name)()
        else:
            _runner_class = ReplaceEnvInSqlContent()
    return _runner_class