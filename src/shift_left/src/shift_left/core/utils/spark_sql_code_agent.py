"""
Copyright 2024-2025 Confluent, Inc.

An agentic flow to translate Spark SQL to Flink SQL.
"""

from pydantic import BaseModel
from typing import Tuple
import os
import importlib.resources 

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement
from shift_left.core.utils.llm_code_agent_base import AnySqlToFlinkSqlAgent

class SparkSqlFlinkSql(BaseModel):
    flink_ddl_output: str
    flink_dml_output: str

class SparkSqlFlinkDdl(BaseModel):
    flink_ddl_output: str


class SparkToFlinkSqlAgent(AnySqlToFlinkSqlAgent):
    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.core.utils.prompts.spark_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.spark_fsql").joinpath("ddl_creation.txt")
        with fname.open("r") as f:
            self.ddl_creation_system_prompt= f.read()

    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        translator_prompt_template = "spark_sql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        response= self.llm_client.chat.completions.parse(
            model=self.model_name, 
            response_format=SparkSqlFlinkSql,
            messages=messages
        )
        obj_response = response.choices[0].message
        print(f"Response: {obj_response.parsed}")
        if obj_response.parsed:
            return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
        else:
            return "", ""

    def _mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        return ddl_sql, dml_sql

    def _ddl_generation(self, sql: str) -> str:
        translator_prompt_template = "flink_sql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        response= self.llm_client.chat.completions.parse(
            model=self.model_name, 
            response_format=SparkSqlFlinkDdl,
            messages=messages
        )
        obj_response = response.choices[0].message
        print(f"Response: {obj_response.parsed}")
        if obj_response.parsed:
            return obj_response.parsed.flink_ddl_output
        else:
            return ""

    def translate_to_flink_sql(self, sql: str, validate: bool = False) -> Tuple[str, str]:
        print(f"Start translating spark sql to flink sql with {self.model_name}")
        ddl_sql, dml_sql = self._translator_agent(sql)
        print(f"DDL: {ddl_sql}")    
        print(f"DML: {dml_sql}")
        final_ddl, final_dml = self._mandatory_validation_agent(ddl_sql, dml_sql)
        final_ddl = self._ddl_generation(final_dml)
        print(f"Final DDL: {final_ddl}")
        return final_ddl, final_dml
