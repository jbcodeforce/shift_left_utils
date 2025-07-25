"""
Copyright 2024-2025 Confluent, Inc.
"""

from pydantic import BaseModel
from typing import Tuple
import os
import importlib.resources 

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement
from shift_left.core.utils.llm_code_agent_base import AnySqlToFlinkSqlAgent

