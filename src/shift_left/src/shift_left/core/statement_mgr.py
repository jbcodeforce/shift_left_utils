""""
Copyright 2024-2025 Confluent, Inc.
A set of operations to manage a flink statement
"""
from re import S
from typing import List, Optional
import os
import time
import json
import threading
import shutil
from datetime import datetime
from importlib import import_module
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger, session_log_dir, shift_left_dir
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
from shift_left.core.models.flink_statement_model import (
    Statement,
    StatementResult,
    StatementInfo,
    StatementListCache,
    StatementError,
    ErrorData,
    FlinkStatementNode
)
from shift_left.core.utils.file_search import (
    FlinkTableReference
)
from shift_left.core.utils.table_worker import NoChangeDoneToSqlContent, TableWorker

STATEMENT_LIST_FILE=session_log_dir + "/statement_list.json"

def build_and_deploy_flink_statement_from_sql_content(flinkStatement_to_process: FlinkStatementNode,
                                                      flink_statement_file_path: str = "",
                                                      statement_name: str = ""
) -> Statement | StatementError:
    """
    Read the SQL content for the flink_statement file name, and deploy to
    the assigned compute pool. If the statement fails, propagate the exception to higher level.
    """
    config = get_config()
    compute_pool_id = flinkStatement_to_process.compute_pool_id or config['flink']['compute_pool_id']
    if not statement_name:
        statement_name = (config['kafka']['cluster_type'] + "-" + os.path.basename(flink_statement_file_path).replace('.sql','')).replace('_','-').replace('.','-')
    logger.info(f"{statement_name} with content: {flink_statement_file_path} deploy to {compute_pool_id}")
    full_file_path = from_pipeline_to_absolute(flink_statement_file_path)
    try:
        with open(full_file_path, "r") as f:
            sql_content = f.read()
            column_to_search = config.get('app', {}).get('data_limit_column_name_to_select_from', None)
            transformer = get_or_build_sql_content_transformer()
            _, sql_out= transformer.update_sql_content(
                                                sql_content=sql_content,
                                                column_to_search=column_to_search or "",
                                                product_name=flinkStatement_to_process.product_name)

            statement= post_flink_statement(compute_pool_id,
                                            statement_name,
                                            sql_out)
            logger.debug(f"Statement: {statement_name} -> {statement}")
            if statement and isinstance(statement, Statement) and statement.status:
                logger.info(f"Statement: {statement_name} status is: {statement.status.phase}")
                get_statement_list()[statement_name]=map_to_statement_info(statement)   # important to avoid doing an api call
            return statement
    except Exception as e:
        logger.error(e)
        return StatementError(errors=[ErrorData(id=statement_name, status="FAILED", detail=str(e))])


def get_statement_status_with_cache(statement_name: str) -> StatementInfo:
    statement_list = get_statement_list()
    if statement_list and statement_name in statement_list:
        return statement_list[statement_name]
    statement_info = StatementInfo(name=statement_name,
                                   status_phase="UNKNOWN",
                                   status_detail="Statement not found int the existing deployed Statements",
                                   compute_pool_id=None,
                                   compute_pool_name=None
                                )
    return statement_info

def get_statement(statement_name: str) -> Statement | StatementError:
    config = get_config()
    client = ConfluentCloudClient(config)
    url, auth_header = client.build_flink_url_and_auth_header()
    response = client.make_request(method="GET", url=url + "/statements/" + statement_name, auth_header=auth_header)
    if response and response.get('errors'):
        return StatementError(**response)
    return Statement(**response)

def post_flink_statement(compute_pool_id: str,
                             statement_name: str,
                             sql_content: str,
                             stopped: bool = False) -> Statement | StatementError:
        """
        POST to the statements API to execute a SQL statement.
        """
        config = get_config()
        properties = {'sql.current-catalog' : config['flink']['catalog_name'] , 'sql.current-database' : config['flink']['database_name']}
        client = ConfluentCloudClient(config)
        url, auth_header = client.build_flink_url_and_auth_header()
        statement_data = {
                "name": statement_name,
                "organization_id": config["confluent_cloud"]["organization_id"],
                "environment_id": config["confluent_cloud"]["environment_id"],
                "spec": {
                    "statement": sql_content,
                    "properties": properties,
                    "compute_pool_id": compute_pool_id,
                    "stopped": stopped
                }
            }
        try:
            logger.debug(f"> Send POST request to Flink statement api with {statement_data}")
            start_time = time.perf_counter()
            auth_header = client._get_flink_auth()
            response = client.make_request(method="POST", url=f"{url}/statements", data=statement_data, auth_header=auth_header)
            logger.info(f"> POST response= {response}")
            if isinstance(response, dict):
                if response.get('errors'):
                    logger.error(f"Error executing rest call: {response['errors']}")
                    if response.get("errors")[0].get("status") == "409":
                        delete_statement_if_exists(statement_name)
                    return  "Exists but deleted so retry"
                #raise Exception(response['errors'][0]['detail'])
                elif response["status"]["phase"] == "PENDING":
                    return client.wait_response(url, statement_name, start_time)
                return  Statement(**response)
            else:
                return StatementError(errors=[ErrorData(id=statement_name, status="FAILED", detail=str(response))])
        except Exception as e:
            logger.error(f"Error executing rest call: {e}")
            raise e


def delete_statement_if_exists(statement_name) -> str | None:
    logger.info(f"Enter with {statement_name}")
    statement_list = get_statement_list()
    config = get_config()
    client = ConfluentCloudClient(config)
    # 05/27 the following call is not really needed as there is most likely no creation of the same statement outside of the tool.
    #  so return None
    result=client.delete_flink_statement(statement_name)
    if result == "deleted" and statement_name in statement_list:
        statement_list.pop(statement_name)
    return result

def patch_statement_if_exists(statement_name: str, stopped: bool) -> str | None:
    logger.info(f"Enter with {statement_name}")
    config = get_config()
    client = ConfluentCloudClient(config)
    result=client.patch_flink_statement(statement_name, stopped)
    return result

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
        statement_info = map_to_statement_info(statement)
        get_statement_list()[statement_name] = statement_info
        return statement_info
    return None


def get_statement_results(statement_name: str)-> StatementResult:
        client = ConfluentCloudClient(get_config())
        url, auth_header = client.build_flink_url_and_auth_header()
        try:
            next_page_token = None
            previous_step = None
            while True:
                if next_page_token and previous_step != next_page_token:
                    logger.info(f"Get next page token: {next_page_token} for {statement_name}")
                    resp=client.make_request(method="GET", url=next_page_token, auth_header=auth_header)
                else:
                    logger.info(f"Get results from {url}/statements/{statement_name}/results")
                    resp=client.make_request(method="GET", url=f"{url}/statements/{statement_name}/results", auth_header=auth_header)
                logger.info(f"response: {resp} same tokens: {previous_step == next_page_token}")
                if (resp and "metadata" in resp and "next" in resp["metadata"] and resp["metadata"]["next"]):
                    previous_step = next_page_token
                    next_page_token = resp["metadata"]["next"]
                else:
                    logger.info(f"Data received for {statement_name}: data: {resp.get("results").get("data")}")
                    break
            return StatementResult(**resp)
        except Exception as e:
            logger.error(f"Error executing GET statement call for {statement_name}: {e}")
            return None

def get_next_statement_results(next_token_page: str) -> StatementResult:
    config = get_config()
    client = ConfluentCloudClient(config)
    auth_header = client._get_flink_auth()
    resp=client.make_request(method="GET", url=next_token_page, auth_header=auth_header)
    return StatementResult(**resp)

_cache_lock = threading.RLock()
_statement_list_cache = None  # cache the statement list loaded to limit the number of call to CC API
def get_statement_list(compute_pool_id: Optional[str] = None) -> dict[str, StatementInfo]:
    """
    Get the statement list from the CC API - the list is <statement_name, statement_info>
    """
    global _statement_list_cache
    with _cache_lock:
        if _statement_list_cache == None:
            reload = True
            if os.path.exists(STATEMENT_LIST_FILE):
                try:
                    with open(STATEMENT_LIST_FILE, "r") as f:
                        _statement_list_cache = StatementListCache.model_validate(json.load(f))
                    if _statement_list_cache.created_at and (datetime.now() - datetime.strptime(str(_statement_list_cache.created_at), "%Y-%m-%d %H:%M:%S")).total_seconds() < get_config()['app']['cache_ttl']:
                        reload = False
                except Exception as e:
                    logger.warning(f"Loading statement list cache file failed: {e} -> delete the cache file")
                    reload = True
                    os.remove(STATEMENT_LIST_FILE)
            if reload:
                _statement_list_cache = StatementListCache(created_at=datetime.now())
                config = get_config()
                logger.info("Load the current list of Flink statements using REST API")
                print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Load current flink statements using REST API {config['confluent_cloud']['organization_id']}")
                start_time = time.perf_counter()
                page_size = config["confluent_cloud"].get("page_size", 100)
                client = ConfluentCloudClient(config)
                url, auth_header = client.build_flink_url_and_auth_header()
                url=url+"/statements?page_size="+str(page_size)
                next_page_token = None
                while True:
                    if next_page_token:
                        resp=client.make_request(method="GET", url=next_page_token, auth_header=auth_header)
                    else:
                        resp=client.make_request(method="GET", url=url, auth_header=auth_header)
                    logger.debug("Statement execution result:", resp)
                    if resp and 'data' in resp:
                        for info in resp.get('data'):
                            statement_info = map_to_statement_info(info)
                            _statement_list_cache.statement_list[info['name']] = statement_info
                    if resp and "metadata" in resp and "next" in resp["metadata"]:
                        next_page_token = resp["metadata"]["next"]
                        if not next_page_token:
                            break
                    else:
                        logger.warning(f"resp is not valid: {resp}")
                        break
                _save_statement_list(_statement_list_cache)
                stop_time = time.perf_counter()
                logger.info(f"Statement list has {len(_statement_list_cache.statement_list)} statements, read in {int(stop_time - start_time)} seconds")
                print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Statement list has {len(_statement_list_cache.statement_list)} statements")
        elif (_statement_list_cache.created_at
            and (datetime.now() - _statement_list_cache.created_at).total_seconds() > get_config()['app']['cache_ttl']):
            logger.info("Statement list cache is expired, reload it")
            _statement_list_cache = None
            return get_statement_list(compute_pool_id)
        if compute_pool_id:
            return {k: v for k, v in _statement_list_cache.statement_list.items() if v.compute_pool_id == compute_pool_id}
        return _statement_list_cache.statement_list


def reset_statement_list():
    global _statement_list_cache
    with _cache_lock:
        _statement_list_cache = None
        try:
            if os.path.exists(STATEMENT_LIST_FILE):
                os.remove(STATEMENT_LIST_FILE)
        except Exception as e:
            logger.warning(f"Error resetting statement list cache: {e}")

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
    statement_name = ("show-" + table_name.replace('_', '-').replace('.', '-'))[:99]
    result_str = None
    config = get_config()
    if not compute_pool_id:
        compute_pool_id=config['flink']['compute_pool_id']
    sql_content = f"show create table `{table_name}`;"
    delete_statement_if_exists(statement_name)
    try:
        statement = post_flink_statement(compute_pool_id, statement_name, sql_content)
        if statement and isinstance(statement, Statement) and statement.status.phase in ("RUNNING", "COMPLETED"):
            get_statement_list()[statement_name] = map_to_statement_info(statement)
            statement_result = get_statement_results(statement_name)
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
    logger.info(f"Run drop table {table_name}")
    sql_content = f"drop table if exists {table_name};"
    drop_statement_name = "drop-" + table_name.replace('_','-')
    try:
        delete_statement_if_exists(drop_statement_name)
        result= post_flink_statement(compute_pool_id,
                                            drop_statement_name,
                                            sql_content)
        if result and isinstance(result, Statement) and result.status.phase not in ("COMPLETED", "FAILED"):
            while result.status.phase not in ["COMPLETED", "FAILED"]:
                time.sleep(1)
                result = get_statement(drop_statement_name)
                logger.info(f"Drop table {table_name} status is: {result.status.phase}")
            if result.status.phase == "FAILED":
                raise Exception(f"Drop table {table_name} failed")
    except Exception as e:
        logger.error(f"drop_table {e}")
    finally:
        delete_statement_if_exists(drop_statement_name)
    return f"{table_name} dropped"

_runner_class = None
def get_or_build_sql_content_transformer() -> TableWorker:
    global _runner_class
    if not _runner_class:
        if get_config().get('app').get('sql_content_modifier'):

            class_to_use = get_config().get('app').get('sql_content_modifier')
            module_path, class_name = class_to_use.rsplit('.',1)
            mod = import_module(module_path)
            _runner_class = getattr(mod, class_name)()
        else:
            _runner_class = NoChangeDoneToSqlContent()
    return _runner_class

def map_to_statement_info(info: Statement) -> StatementInfo:
    """
    Map the statement info, result of the REST call to the StatementInfo model
    """

    if info and isinstance(info, dict):
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
                                    created_at= info.get('metadata').get('created_at', 'UNKNOWN'),
                                    sql_catalog=catalog,
                                    sql_database=database)
    elif info and isinstance(info, Statement) and info.spec:
        catalog = info.spec.properties.get('sql.current-catalog','UNKNOWN')
        database = info.spec.properties.get('sql.current-database','UNKNOWN')
        if info.status:
            status_phase = info.status.phase
            status_detail = info.status.detail
        else:
            status_phase = "UNKNOWN"
            status_detail = "UNKNOWN"
        return StatementInfo(name=info.name,
                             status_phase= status_phase,
                             status_detail= status_detail,
                             sql_content= info.spec.statement,
                             compute_pool_id= info.spec.compute_pool_id,
                             principal= info.spec.principal,
                             created_at= info.metadata.created_at,
                             sql_catalog=catalog,
                             sql_database=database)
    else:
        raise Exception(f"Invalid statement info: {info}")

# ------------- private methods -------------
def _save_statement_list(statement_list: dict[str, StatementInfo]):
    """
    Save the statement list to the cache file
    """

    # Write to temporary file first, then atomic rename
    temp_file = STATEMENT_LIST_FILE + ".tmp"
    try:
        with open(temp_file, "w") as f:
            f.write(statement_list.model_dump_json(indent=2, warnings=False))
            f.flush()  # Ensure data is written
            os.fsync(f.fileno())  # Force write to disk

        # Atomic operation - either succeeds completely or fails
        shutil.move(temp_file, STATEMENT_LIST_FILE)
    except Exception as e:
        logger.error(f"Failed to save statement list: {e}")
        # Clean up temp file
        try:
            os.remove(temp_file)
        except OSError:
            pass





def _update_results_from_node(node: FlinkTablePipelineDefinition, statement_list, results, table_inventory, config: dict):
    for parent in node.parents:
        results= _search_statement_status(parent, statement_list, results, table_inventory, config)
    ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(node)
    if dml_statement_name in statement_list:
        status = statement_list[dml_statement_name]
        results[dml_statement_name]=status
    return results


def _search_statement_status(node: FlinkTablePipelineDefinition,
                             statement_list, results,
                             table_inventory, config: dict):
    ddl_statement_name, statement_name = get_ddl_dml_names_from_pipe_def(node)
    if statement_name in get_statement_list():
        status = get_statement_list()[statement_name]
        results[statement_name]=status
        table_ref: FlinkTableReference = get_table_ref_from_inventory(node.table_name, table_inventory)
        pipeline_def: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
        results = _update_results_from_node(pipeline_def, statement_list, results, table_inventory, config)
    return results

