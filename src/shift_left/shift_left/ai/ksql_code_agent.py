"""
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

from pydantic import BaseModel
from typing import Tuple, List, Optional
import importlib.resources
from shift_left.core.utils.app_config import logger
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent, SqlTableDetection, FlinkSqlForRefinement, FlinkSql
from shift_left.core.utils.sql_parser import SQLparser
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
        self._load_prompts()

    # -------------------------
    # Public API
    # -------------------------
    def translate_to_flink_sqls(self,
                table_name: str,
                sql: str,
                validate: bool = False
    ) -> Tuple[List[str], List[str]]:
        """
        Main entry point for KSQL to Flink SQL translation workflow.

        This method orchestrates the complete translation pipeline:
        1. Input cleaning and preprocessing
        2. LLM based Multiple table detection and splitting
        3. LLM-based translation for each statement
        4. Mandatory validation and syntax checking
        5. Optional live validation against Confluent Cloud for Apache Flink
        6. Iterative refinement based on execution feedback

        Args:
            table_name (str): The name of the table to translate
            sql (str): The SQL input to translate (can contain multiple statements)
            validate (bool, optional): Whether to perform live validation against Flink.
                                     Defaults to False. When True, requires user confirmation.

        Returns:
            Tuple[List[str], List[str]]: A tuple containing:
                - List of translated DDL statements
                - List of translated DML statements

        Note:
            When validate=True, the method will prompt for user confirmation before
            executing statements against the live Flink environment.
        """

        logger.info("\n0/ Cleaning KSQL input by removing DROP TABLE statements and comments...\n")
        print("\n0/ Cleaning SQL input by removing DROP TABLE statements and comments...\n")

        ksql = self._clean_sql_input(sql)
        logger.info(f"Cleaned KSQL input: {ksql[:400]}...")

        logger.info(f"\n1/ Analyzing KSQL input for multiple CREATE TABLE statements using: {self.model_name} \n")
        print(f"\n1/ Analyzing KSQL input for multiple CREATE TABLE statements using: {self.model_name} \n")

        table_detection = self._detect_multitable_with_agent(ksql)
        logger.info(f"Table detection result: {table_detection.model_dump_json()}")
        final_ddl = []
        final_dml = []
        print(f"Starting translation using {self.model_name} with cc-validation={validate}")
        print(f"-"*40)
        if table_detection.has_multiple_tables:
            # Process multiple statements individually for better accuracy
            logger.info(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")
            for i, table_statement in enumerate(table_detection.table_statements):
                logger.info(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                print(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                # Translate individual statement
                ddl_sql, dml_sql = self._do_translation_with_agent(table_statement)
                logger.info(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql}..., DML: {dml_sql if dml_sql else 'empty'}...")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Validate and refine the translation
                ddl_sql, dml_sql = self._run_mandatory_validation_agent(ddl_sql, dml_sql)
                logger.info(f"Done with mandatory validation agent for statement {i+1}")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Collect non-empty results
                if ddl_sql.strip():
                    final_ddl.append(ddl_sql)
                if dml_sql and dml_sql.strip():
                    final_dml.append(dml_sql)
        else:
            # Process as single statement
            logger.info("2/ Processing single KSQL statement...")
            print("\n2/: Processing single Spark SQL to Flink SQL...")
            ddl_sql, dml_sql = self._do_translation_with_agent(ksql)

            self._snapshot_ddl_dml(table_name, ddl_sql, dml_sql)
            logger.info(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}\n3/ Start mandatory validation agent...")
            ddl, dml = self._run_mandatory_validation_agent(ddl_sql, dml_sql)
            self._snapshot_ddl_dml(table_name, ddl, dml)
            logger.info(f"Done with mandatory validation agent, updated flink DDL sql is:\n {ddl}\nand DML: {dml if dml else 'empty'}")
            final_ddl = [ddl]
            final_dml = [dml]
        # Optional live validation against Confluent Cloud for Apache Flink
        if validate:
            final_ddl, final_dml = self._validate_ddl_dml_on_cc(final_ddl, final_dml)
        return final_ddl, final_dml

    def _load_prompts(self):
        """
        Load system prompts from external files for different LLM agents.

        This method loads specialized prompts for each stage of the translation pipeline:
        - translator.txt: Main KSQL to Flink SQL translation prompt
        - refinement.txt: Error-based refinement prompt
        - mandatory_validation.txt: Syntax and best practices validation prompt
        - table_detection.txt: Multiple table/stream detection prompt

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

