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
import os
import importlib.resources 

from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.translator_to_flink_sql import TranslatorToFlinkSqlAgent


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
    ksql_input: str
    flink_ddl_output:  Optional[str] = None
    flink_dml_output:  Optional[str] = None


class KsqlTableDetection(BaseModel):
    """
    Model for detecting and splitting multiple CREATE TABLE/STREAM statements in KSQL input.
    
    This model helps identify when the input contains multiple table or stream definitions
    that should be processed separately for better translation accuracy.
    
    Attributes:
        has_multiple_tables: Whether the input contains multiple CREATE statements
        table_statements: List of individual CREATE statements if multiple were found
        description: Human-readable description of the detection result
    """
    has_multiple_tables: bool
    table_statements: list[str]
    description: str


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


class FlinkSqlForRefinement(BaseModel):
    """
    Model for SQL refinement when errors are encountered during validation.
    
    This model is used when the initial translation fails validation and needs
    to be refined based on error feedback from the Flink execution environment.
    
    Attributes:
        sql_input: The SQL statement that failed validation
        error_message: Error message received from Flink
        flink_output: Refined SQL statement that should resolve the error
    """
    sql_input: Optional[str] = None
    error_message: Optional[str] = None
    flink_output: Optional[str] = None


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

    def _clean_ksql_input(self, ksql: str) -> str:
        """
        Clean KSQL input by removing problematic statements and comments.
        
        This preprocessing step removes:
        - DROP TABLE and DROP STREAM statements (not needed for translation)
        - Comment lines starting with '--' (can confuse the LLM)
        - Preserves empty lines for formatting consistency
        
        Args:
            ksql (str): The raw KSQL string to clean
            
        Returns:
            str: Cleaned KSQL string with unwanted statements and comments removed
        """
        lines = ksql.split('\n')
        cleaned_lines = []
        
        for line in lines:
            stripped_line = line.strip()
            
            # Preserve empty lines for formatting
            if not stripped_line:
                cleaned_lines.append(stripped_line)
                continue
                
            if stripped_line.startswith('--'):
                continue
                
            # These are not needed for translation and can confuse the LLM
            if stripped_line.upper().startswith('DROP TABLE'):
                continue
            
            if stripped_line.upper().startswith('DROP STREAM'):
                continue
            
            # Keep all other lines including the original indentation
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
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
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
        
        # Load the refinement prompt for error-based corrections
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("refinement.txt")
        with fname.open("r") as f:
            self.refinement_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("mandatory_validation.txt")
        with fname.open("r") as f:
            self.mandatory_validation_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt= f.read()

    def _table_detection_agent(self, ksql: str) -> KsqlTableDetection:
        """
        Analyze KSQL input to detect multiple CREATE TABLE/STREAM statements.
        
        This agent determines whether the input contains multiple table or stream
        definitions that should be processed separately. Processing multiple statements
        individually often yields better translation results than processing them together.
        
        Args:
            ksql (str): The cleaned KSQL input to analyze
            
        Returns:
            KsqlTableDetection: Analysis result containing:
                - Whether multiple tables were detected
                - List of individual statements if multiple were found
                - Description of the detection result
        """
        table_detection_prompt_template = "ksql_input: {ksql_input}"
        messages=[
            {"role": "system", "content": self.table_detection_system_prompt},
            {"role": "user", "content": table_detection_prompt_template.format(ksql_input=ksql)}
        ]
        
        # Use structured output to ensure consistent response format
        response = self.llm_client.chat.completions.parse(
            model=self.model_name,
            response_format=KsqlTableDetection,
            messages=messages
        )
        
        obj_response = response.choices[0].message
        if obj_response.parsed:
            return obj_response.parsed
        else:
            # Fallback if parsing fails - treat as single statement
            return KsqlTableDetection(
                has_multiple_tables=False,
                table_statements=[ksql],
                description="Error in detection, treating as single statement"
            )
        

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
        
        # Use structured output to separate DDL and DML components
        response= self.llm_client.chat.completions.parse(
            model=self.model_name, 
            response_format=KsqlFlinkSql,
            messages=messages
        )
        
        obj_response = response.choices[0].message
        print(f"Response: {obj_response.parsed}")
        
        if obj_response.parsed:
            return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
        else:
            # Return empty strings if parsing fails
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
            return "", ""

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
            return ""

    def _process_semantic_validation(self, sql: str) -> str:
        """
        Process semantic validation of SQL statements.
        
        This method is a placeholder for future semantic validation logic.
        Semantic validation would check for logical correctness, data flow consistency,
        and business rule compliance beyond basic syntax checking.
        
        Args:
            sql (str): The SQL statement to validate semantically
            
        Returns:
            str: The SQL statement (currently unchanged, future enhancement point)
        """
        # TODO: Implement semantic validation logic
        # This could include checks for:
        # - Data type compatibility
        # - Join condition validity
        # - Temporal semantics for streaming queries
        # - Resource usage optimization
        return sql

    def translate_from_ksql_to_flink_sql(self, table_name: str, ksql: str, validate: bool = False) -> Tuple[List[str], List[str]]:
        """
        Main entry point for KSQL to Flink SQL translation workflow.
        
        This method orchestrates the complete translation pipeline:
        1. Input cleaning and preprocessing
        2. Multiple table detection and splitting
        3. LLM-based translation for each statement
        4. Mandatory validation and syntax checking
        5. Optional live validation against Confluent Cloud for Apache Flink
        6. Iterative refinement based on execution feedback
        
        Args:
            table_name (str): The name of the table to translate
            ksql (str): The KSQL input to translate (can contain multiple statements)
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
        
        print("0/ Cleaning KSQL input by removing DROP TABLE statements and comment lines...")
        logger.info("Starting KSQL input cleaning")
        ksql = self._clean_ksql_input(ksql)
        print(f"Cleaned KSQL input: {ksql[:400]}...")
        logger.info("KSQL input cleaning completed")
        

        print(f"1/ Analyzing KSQL input for multiple CREATE TABLE statements using: {self.model_name} ")
        logger.info("Starting table detection analysis")
        table_detection = self._table_detection_agent(ksql)
        print(f"Table detection result: {table_detection.description}")
        logger.info(f"Table detection result: {table_detection.description}")
        
        if table_detection.has_multiple_tables:
            # Process multiple statements individually for better accuracy
            print(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")          
            all_ddl_statements = []
            all_dml_statements = []
            
            for i, table_statement in enumerate(table_detection.table_statements):
                print(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                logger.info(f"Processing statement {i+1}")
                
                # Translate individual statement
                ddl_sql, dml_sql = self._translator_agent(table_statement)
                print(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql[:100]}..., DML: {dml_sql[:50] if dml_sql else 'empty'}...")
                logger.info(f"Translator agent completed for statement {i+1}")
                _snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Validate and refine the translation
                ddl_sql, dml_sql = self._mandatory_validation_agent(ddl_sql, dml_sql)
                print(f"Done with mandatory validation agent for statement {i+1}")
                logger.info(f"Mandatory validation completed for statement {i+1}")
                _snapshot_ddl_dml(table_name + "_" + str(i), ddl_sql, dml_sql)
                # Collect non-empty results
                if ddl_sql.strip():
                    all_ddl_statements.append(ddl_sql)
                if dml_sql and dml_sql.strip():
                    all_dml_statements.append(dml_sql)
            
            # Combine results from all statements
            final_ddl = all_ddl_statements
            final_dml = all_dml_statements
            
        else:
            # Process as single statement
            print("2/ Processing single KSQL statement...")
            ddl_sql, dml_sql = self._translator_agent(ksql)
            _snapshot_ddl_dml(table_name, ddl_sql, dml_sql)
            print(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}\n3/ Start mandatory validation agent...")
            logger.info(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}")
            ddl, dml = self._mandatory_validation_agent(ddl_sql, dml_sql)
            _snapshot_ddl_dml(table_name, ddl, dml)
            print(f"Done with mandatory validation agent updated flink DDL sql is:\n {ddl}\nand DML: {dml if dml else 'empty'}")
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
                        ddl = self._process_semantic_validation(ddl)
                        if dml:
                            dml, validated = self._iterate_on_validation(dml)
                            if validated:
                                dml = self._process_semantic_validation(dml)
                    validated_dmls.append(dml)
                    validated_ddls.append(ddl)
                    _snapshot_ddl_dml(table_name, ddl, dml)
                # Use validated results
                final_ddl = validated_ddls
                final_dml = validated_dmls
        
        return final_ddl, final_dml


def _snapshot_ddl_dml(table_name: str, ddl: str, dml: str) -> Tuple[str, str]:
    """
    Snapshot the DDL and DML statements to a file.
    """
    if ddl and len(ddl) > 0:
        with open(os.path.join(shift_left_dir, f"ddl.{table_name}.sql"), "w") as f:
            f.write(ddl)
    if dml and len(dml) > 0:
        with open(os.path.join(shift_left_dir, f"dml.{table_name}.sql"), "w") as f:
            f.write(dml)
    return ddl, dml