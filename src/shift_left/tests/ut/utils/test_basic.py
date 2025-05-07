import os
import pytest
from pydantic import BaseModel
from typing import List
from shift_left.core.pipeline_mgr import read_pipeline_definition_from_file
from shift_left.core.pipeline_mgr import PIPELINE_JSON_FILE_NAME
from datetime import datetime
from shift_left.core.models.flink_statement_model import FlinkStatementNode, StatementResult
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
import shift_left.core.table_mgr as table_mgr
import shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.utils.file_search import from_pipeline_to_absolute
from shift_left.core.utils.app_config import get_config
import json
import re


def _test_get_table_info():
    os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
    os.environ["PIPELINES"]= "/Users/jerome/Code/customers/master-control/data-platform-flink/pipelines"
    inventory_path=  os.environ["PIPELINES"]
    config = get_config()
    compute_pool_id = config["flink"]["compute_pool_id"]
    table_name = "int_aqem_recordexecution_element_data_unnest"
    sql = "show create table " + table_name
    statement_name = "test-show-table"
    statement_mgr.delete_statement_if_exists(statement_name)
    statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name,sql)
    print(statement.model_dump_json(indent=3))
    results = statement_mgr.get_statement_results(statement_name)
    print(results.model_dump_json(indent=3))
    sql_content=results.results.data[0].row[0]
    print(f"Primary key columns: {_get_primary_key_columns(sql_content)}")
    print(f"Distributed by columns: {_get_distributed_by_columns(sql_content)}")

def _get_primary_key_columns(sql_content: str) -> List[str]:
    pk_pattern = r"PRIMARY KEY\((.*?)\)"
    pk_match = re.search(pk_pattern, sql_content)
    print(pk_match)
    if pk_match:
        return [col.strip('`') for col in pk_match.group(1).split(',')]
    return []

def _get_distributed_by_columns(sql_content: str) -> List[str]:
    distributed_by_pattern = r"DISTRIBUTED BY HASH\((.*?)\)"
    distributed_by_match = re.search(distributed_by_pattern, sql_content)
    if distributed_by_match:
        return [col.strip('`') for col in distributed_by_match.group(1).split(',')]
    return []

def test_process_results():
    table_name = "int_aqem_recordexecution_element_data_unnest"
    inventory_path=  os.environ["PIPELINES"]
    pipeline_def = pipeline_mgr.get_pipeline_definition_for_table(table_name, inventory_path)
    ddl_file_path = from_pipeline_to_absolute(pipeline_def.ddl_ref)
    sql_content=table_mgr.load_sql_content_from_file(ddl_file_path)
    print(sql_content)
    print(_get_primary_key_columns(sql_content))
    print(_get_distributed_by_columns(sql_content))
