"""
Copyright 2024-2026 Confluent, Inc.
KSQL to Flink SQL Translation Agent

This module provides functionality to translate KSQL (Kafka SQL) statements to Apache Flink SQL
using Large Language Model (LLM) agents. The translation process includes multiple validation
steps and can handle both single and multiple table/stream definitions.

Key Components:
- KsqlToFlinkSqlAgent: Main agent class that orchestrates the translation workflow
- Multiple Pydantic models for structured LLM responses
- Multi-step validation pipeline including syntax and semantic checks
- Support for batch processing of multiple CREATE statements

Copyright 2024-2025 Confluent, Inc.
"""

from typing import Tuple, List
import importlib.resources
from shift_left.core.utils.app_config import logger
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent, SqlTableDetection, FlinkSqlForRefinement, FlinkSql

class KsqlToFlinkSqlAgent(TranslatorToFlinkSqlAgent):
    """
    Main agent class for translating KSQL statements to Flink SQL.

    This class implements a multi-step workflow for translating KSQL to Flink SQL:
    1. Input cleaning (remove DROP statements and comments)
    2. Table detection (identify multiple CREATE statements)
    3. Translation using LLM agents
    4. Mandatory validation and syntax checking
    5. Optional semantic validation against live Flink environment
    6. Iterative refinement based on error feedback

    The agent uses structured LLM responses via Pydantic models to ensure
    consistent and parseable output from the language model.
    """
    def __init__(self):
        super().__init__()

    def _load_prompts(self):
        """
        Load system prompts from external files for different LLM agents.

        This method loads specialized prompts for each stage of the translation pipeline:
        - translator.txt: Main KSQL to Flink SQL translation prompt
        - refinement.txt: Error-based refinement prompt
        - mandatory_validation.txt: Syntax and best practices validation prompt

        Using external files allows for easier prompt engineering and maintenance
        without modifying the code.
        """
        # Load the main translation system prompt
        fname = importlib.resources.files("shift_left.ai.prompts.ksql_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()

        # Load the refinement prompt for error-based corrections
        fname = importlib.resources.files("shift_left.ai.prompts.ksql_fsql").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt= f.read()


    def translate_to_flink_sqls(self,
                table_name: str,
                sql: str,
                validate: bool = False
    ) -> Tuple[List[str], List[str]]:

        logger.info("\n0/ Cleaning KSQL input by removing DROP TABLE statements and comments...\n")
        print("\n0/ Cleaning KSQL input by removing DROP TABLE statements and comments...\n", flush=True)
        final_ddl = []
        final_dml = []
        cleaned_sql = self._clean_sql_input(sql)
        logger.info(f"Cleaned source SQL input: {cleaned_sql}...")
        print(f"Cleaned source SQL input:\n {cleaned_sql}", flush=True)
        logger.info("\n1/ Doing the translation with the agent...\n")
        print("\n1/ Doing the translation with the agent...\n", flush=True)
        ddl_sql, dml_sql = self._do_translation_with_agent(cleaned_sql)
        logger.info(f"Done with translator agent for statement DDL: {ddl_sql}..., DML: {dml_sql if dml_sql else 'empty'}...")
        print(f"\n\nTranslated DDL: {ddl_sql}\n\nTranslated DML: {dml_sql if dml_sql else 'empty'}", flush=True)
        self._snapshot_ddl_dml(table_name, ddl_sql, dml_sql)

        if ddl_sql and ddl_sql.strip():
            final_ddl.append(ddl_sql)
        if dml_sql and dml_sql.strip():
            final_dml.append(dml_sql)
        if validate:
            logger.info("\n2/ Validating the translation with the agent...\n")
            print("\n2/ Validating the translation with the agent...\n", flush=True)
            final_ddl, final_dml = self._validate_ddl_dml_on_cc(final_ddl, final_dml)
        return final_ddl, final_dml
