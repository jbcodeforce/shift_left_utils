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
from typing import Tuple, List, Optional, Dict
import importlib.resources
from shift_left.core.utils.app_config import logger
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent, SqlTableDetection, FlinkSqlForRefinement


class KsqlFlinkSql(BaseModel):
    """
    Structured response model for KSQL to Flink SQL translation.

    This model captures the LLM's translation output, separating DDL (Data Definition Language)
    and DML (Data Manipulation Language) statements for better organization.

    Attributes:
        ksql_input: The original KSQL statement provided as input
        flink_ddl_output: Generated Flink SQL DDL statements (CREATE TABLE, etc.)
        flink_dml_output: Generated Flink SQL DML statements (INSERT, SELECT, etc.)
    """
    sql_input: str
    flink_ddl_output:  Optional[str] = ""
    flink_dml_output:  Optional [str] = ""


class FlinkSql(BaseModel):
    """
    Model for Flink SQL validation and refinement process.

    Used during the mandatory validation step to ensure the generated Flink SQL
    is syntactically correct and follows best practices.

    Attributes:
        ddl_sql_input: Input DDL statements for validation (optional)
        dml_sql_input: Input DML statements for validation (optional)
        flink_ddl_output: Validated and potentially corrected DDL output (optional)
        flink_dml_output: Validated and potentially corrected DML output (optional)
    """
    ddl_sql_input: Optional[str] = None
    dml_sql_input: Optional[str] = None
    flink_ddl_output: Optional[str] = None
    flink_dml_output: Optional[str] = None




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
        ksql = self._clean_sql_input(sql)
        logger.info(f"Cleaned KSQL input: {ksql[:400]}...")

        logger.info(f"\n1/ Analyzing KSQL input for multiple CREATE TABLE statements using: {self.model_name} \n")
        table_detection = self._table_detection_agent(ksql)
        logger.info(f"Table detection result: {table_detection.model_dump_json()}")
        final_ddl = []
        final_dml = []
        if table_detection.has_multiple_tables:
            # Process multiple statements individually for better accuracy
            logger.info(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")
            for i, table_statement in enumerate(table_detection.table_statements):
                logger.info(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                # Translate individual statement
                ddl_sql, dml_sql = self._translator_agent(table_statement)
                logger.info(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql[:100]}..., DML: {dml_sql[:50] if dml_sql else 'empty'}...")
                self._snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Validate and refine the translation
                ddl_sql, dml_sql = self._mandatory_validation_agent(ddl_sql, dml_sql)
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
            ddl_sql, dml_sql = self._translator_agent(ksql)

            self._snapshot_ddl_dml(table_name, ddl_sql, dml_sql)
            logger.info(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}\n3/ Start mandatory validation agent...")
            ddl, dml = self._mandatory_validation_agent(ddl_sql, dml_sql)
            self._snapshot_ddl_dml(table_name, ddl, dml)
            logger.info(f"Done with mandatory validation agent, updated flink DDL sql is:\n {ddl}\nand DML: {dml if dml else 'empty'}")
            final_ddl = [ddl]
            final_dml = [dml]
        # Optional live validation against Confluent Cloud for Apache Flink
        if validate:
            print("3/ Start validating the flink SQLs using Confluent Cloud for Flink, do you want to continue? (y/n)")
            answer = input()
            if answer == "y":
                validated_ddls = []
                validated_dmls = []

                # Validate each DDL/DML pair against live environment
                for ddl, dml in zip(final_ddl, final_dml):
                    # Validate DDL first (tables must exist before queries)
                    ddl, validated = self._iterate_on_validation(ddl)
                    if validated:
                        ddl = self._process_syntax_validation(ddl)
                        if dml:
                            dml, validated = self._iterate_on_validation(dml)
                            if validated:
                                dml = self._process_syntax_validation(dml)
                    validated_dmls.append(dml)
                    validated_ddls.append(ddl)
                    self._snapshot_ddl_dml(table_name, ddl, dml)
                # Use validated results
                final_ddl = validated_ddls
                final_dml = validated_dmls

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
        fname = importlib.resources.files("shift_left.ai.prompts.ksql_fsql").joinpath("refinement.txt")
        with fname.open("r") as f:
            self.refinement_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.ksql_fsql").joinpath("mandatory_validation.txt")
        with fname.open("r") as f:
            self.mandatory_validation_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.ai.prompts.ksql_fsql").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt= f.read()



    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        """
        Translate a single KSQL statement to Flink SQL using LLM.

        This is the core translation agent that converts KSQL syntax to equivalent
        Flink SQL statements. It separates the output into DDL and DML components
        for better organization and downstream processing.

        Args:
            sql (str): A single KSQL statement or group of related statements

        Returns:
            Tuple[str, str]: A tuple containing:
                - flink_ddl_output: Flink SQL DDL statements (CREATE TABLE, etc.)
                - flink_dml_output: Flink SQL DML statements (INSERT, SELECT, etc.)
        """
        translator_prompt_template = "ksql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        print(f"Messages: {messages}")
        try:
            # Use structured output to separate DDL and DML components
            response= self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=KsqlFlinkSql,
                messages=messages
            )

            obj_response = response.choices[0].message
            logger.info(f"\n\nResponse: {obj_response.parsed}")
            print(f"Response: {obj_response.parsed}")
            if obj_response.parsed:
                return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
            else:
                # Return empty strings if parsing fails
                return "", ""
        except Exception as e:
            logger.error(f"Error in translator agent: {e}")
            return "", ""

    def _mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        """
        Perform mandatory validation and correction of generated Flink SQL.

        This agent reviews the initially translated SQL for:
        - Syntax correctness according to Flink SQL standards
        - Best practices and common patterns
        - Structural improvements and optimizations
        - Compliance with Confluent Cloud for Apache Flink requirements

        Args:
            ddl_sql (str): The DDL statements to validate and potentially correct
            dml_sql (str): The DML statements to validate and potentially correct

        Returns:
            Tuple[str, str]: A tuple containing:
                - Validated and potentially corrected DDL statements
                - Validated and potentially corrected DML statements
        """
        syntax_checker_prompt_template = "ddl_sql_input: {ddl_sql_input}\ndml_sql_input: {dml_sql_input}"
        messages=[
            {"role": "system", "content": self.mandatory_validation_system_prompt},
            {"role": "user", "content": syntax_checker_prompt_template.format(ddl_sql_input=ddl_sql, dml_sql_input=dml_sql)}
        ]

        try:
            # Use structured output to maintain DDL/DML separation
            response= self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=FlinkSql,
                messages=messages
            )

            obj_response = response.choices[0].message
            if obj_response.parsed:
                return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
            else:
                # Return original inputs if validation fails
                return ddl_sql, dml_sql
        except Exception as e:
            logger.error(f"Error in mandatory validation agent: {e}")
            return ddl_sql, dml_sql

    def _refinement_agent(self, sql: str,
                              history: str,
                              error_message: str) -> str:
        """
        Refine SQL statements based on execution errors from Flink environment.

        When SQL statements fail during live validation against Confluent Cloud for Apache Flink,
        this agent analyzes the error message and conversation history to generate
        corrected SQL that should resolve the specific issues encountered.

        Args:
            sql (str): The SQL statement that failed validation
            history (str): Conversation history and previous attempts for context
            error_message (str): Specific error message from Flink execution

        Returns:
            str: Refined SQL statement that addresses the reported error
        """
        refinement_prompt_template = "flink_sql_input: {sql_input}\nhistory of the conversation: {history}\nreported error: {error_message}"

        messages=[
            {"role": "system", "content": self.refinement_system_prompt},
            {"role": "user", "content": refinement_prompt_template.format(sql_input=sql, history=history, error_message=error_message)}
        ]

        try:
            # Use structured output to get clean refined SQL
            response= self.llm_client.chat.completions.parse(
                model=self.model_name,
                response_format=FlinkSqlForRefinement,
                messages=messages
            )

            obj_response = response.choices[0].message
            if obj_response.parsed:
                return obj_response.parsed.flink_output
            else:
                # Return empty string if refinement fails
                return sql
        except Exception as e:
            logger.error(f"Error in refinement agent: {e}")
            return sql


