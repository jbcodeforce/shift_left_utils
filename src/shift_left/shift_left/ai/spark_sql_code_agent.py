"""
Copyright 2024-2026 Confluent, Inc.

Spark SQL to Flink SQL Translation Agent

An enhanced agentic flow to translate Spark SQL to Flink SQL with
validation and error correction.

The translation process includes multiple validation
steps and can handle both single and multiple table/stream definitions.

"""

from pydantic import BaseModel
from typing import Tuple, List
import importlib.resources


from shift_left.core.utils.app_config import logger
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent, SqlTableDetection, FlinkSqlForRefinement, ErrorCategory, FlinkSql
from shift_left.core.utils.sql_parser import SQLparser

class SparkSqlFlinkDdl(BaseModel):
    flink_ddl_output: str
    key_name: str

class SparkToFlinkSqlAgent(TranslatorToFlinkSqlAgent):
    """
    Spark SQL to Flink SQL Translation Agent
    This class implements a multi-step workflow for translating Spark SQL to Flink SQL:
    1. Input cleaning (remove DROP TABLE statements and comments)
    2. Table detection (identify multiple CREATE TABLE statements)
    3. Translation using LLM agents
    4. Mandatory validation and syntax checking
    5. Optional semantic validation against live Flink environment
    6. Iterative refinement based on error feedback
    """

    def __init__(self):
        super().__init__()
        self._load_prompts()
        try:
            from shift_left.ai.rag import rag_enabled
            self.use_rag_for_translation = rag_enabled()
        except ImportError:
            self.use_rag_for_translation = False



    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("ddl_creation.txt")
        with fname.open("r") as f:
            self.ddl_creation_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.spark_fsql").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt= f.read()

