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
import pathlib
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.table_worker import ReplaceEnvInSqlContent
import json
import re


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

    