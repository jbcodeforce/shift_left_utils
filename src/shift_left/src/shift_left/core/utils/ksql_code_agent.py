"""
Copyright 2024-2025 Confluent, Inc.
"""

from pydantic import BaseModel
from typing import Tuple, List  
import os
import importlib.resources 

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement
from shift_left.core.utils.llm_code_agent_base import AnySqlToFlinkSqlAgent

# use structured output to get the sql from LLM output
class KsqlFlinkSql(BaseModel):
    ksql_input: str
    flink_ddl_output: str
    flink_dml_output: str

# New model for detecting multiple CREATE TABLE statements
class KsqlTableDetection(BaseModel):
    has_multiple_tables: bool
    table_statements: list[str]
    description: str


class FlinkSql(BaseModel):
    ddl_sql_input: str
    dml_sql_input: str
    flink_ddl_output: str
    flink_dml_output: str

class FlinkSqlForRefinement(BaseModel):
    sql_input: str
    error_message: str
    flink_output: str


class KsqlToFlinkSqlAgent(AnySqlToFlinkSqlAgent):

    def _clean_ksql_input(self, ksql: str) -> str:
        """
        Clean KSQL input by removing DROP TABLE statements and comment lines starting with '--'
        
        Args:
            ksql (str): The raw KSQL string to clean
            
        Returns:
            str: Cleaned KSQL string with DROP TABLE statements and comments removed
        """
        lines = ksql.split('\n')
        cleaned_lines = []
        
        for line in lines:
            stripped_line = line.strip()
            
            # Skip empty lines
            if not stripped_line:
                cleaned_lines.append(stripped_line)
                continue
                
            # Skip comment lines starting with --
            if stripped_line.startswith('--'):
                continue
                
            # Skip DROP TABLE statements (case insensitive)
            if stripped_line.upper().startswith('DROP TABLE'):
                continue
            # Skip DROP STREAM statements (case insensitive)
            if stripped_line.upper().startswith('DROP STREAM'):
                continue
            cleaned_lines.append(line)
        
        return '\n'.join(cleaned_lines)
    
    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt= f.read()
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
        Analyze KSQL input to detect multiple CREATE TABLE/STREAM statements and split them if needed
        """
        table_detection_prompt_template = "ksql_input: {ksql_input}"
        messages=[
            {"role": "system", "content": self.table_detection_system_prompt},
            {"role": "user", "content": table_detection_prompt_template.format(ksql_input=ksql)}
        ]
        response = self.llm_client.chat.completions.parse(
            model=self.model_name,
            response_format=KsqlTableDetection,
            messages=messages
        )
        obj_response = response.choices[0].message
        if obj_response.parsed:
            return obj_response.parsed
        else:
            return KsqlTableDetection(
                has_multiple_tables=False,
                table_statements=[ksql],
                description="Error in detection, treating as single statement"
            )
        

    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        translator_prompt_template = "ksql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
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
            return "", ""

    def _mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        """
        Process the mandatory validation of the Flink sql
        """
        syntax_checker_prompt_template = "ddl_sql_input: {ddl_sql_input}\ndml_sql_input: {dml_sql_input}"
        messages=[
            {"role": "system", "content": self.mandatory_validation_system_prompt},
            {"role": "user", "content": syntax_checker_prompt_template.format(ddl_sql_input=ddl_sql, dml_sql_input=dml_sql)}
        ]
        response= self.llm_client.chat.completions.parse(
            model=self.model_name, 
            response_format=FlinkSql,
            messages=messages
        )
        obj_response = response.choices[0].message
        if obj_response.parsed:
            return obj_response.parsed.flink_ddl_output, obj_response.parsed.flink_dml_output
        else:
            return "", ""

    def _refinement_agent(self, sql: str, 
                              history: str,
                              error_message: str) -> str:
        refinement_prompt_template = "flink_sql_input: {sql_input}\nhistory of the conversation: {history}\nreported error: {error_message}"
        
        messages=[
            {"role": "system", "content": self.refinement_system_prompt},
            {"role": "user", "content": refinement_prompt_template.format(sql_input=sql, history=history, error_message=error_message)}
        ]
        response= self.llm_client.chat.completions.parse(
            model=self.model_name, 
            response_format=FlinkSqlForRefinement,
            messages=messages
        )
        obj_response = response.choices[0].message
        if obj_response.parsed:
            return obj_response.parsed.flink_output
        else:
            return ""


    def _process_semantic_validation(self, sql: str) -> str:
        """
        Process the semantic validation of the sql
        """
        return sql

    def translate_from_ksql_to_flink_sql(self, ksql: str, validate: bool = False) -> Tuple[List[str], List[str]]:
        """
        Entry point to translate ksql to flink sql using LLM Agents. This is the workflow implementation.
        """
        
        # Step 0: Clean the KSQL input by removing DROP TABLE statements and comments
        print("0/ Cleaning KSQL input by removing DROP TABLE statements and comment lines...")
        logger.info("Starting KSQL input cleaning")
        ksql = self._clean_ksql_input(ksql)
        print(f"Cleaned KSQL input: {ksql[:400]}...")
        logger.info("KSQL input cleaning completed")
        
        # Step 1: Detect if there are multiple CREATE TABLE statements
        print(f"1/ Analyzing KSQL input for multiple CREATE TABLE statements using: {self.model_name} ")
        logger.info("Starting table detection analysis")
        table_detection = self._table_detection_agent(ksql)
        print(f"Table detection result: {table_detection.description}")
        logger.info(f"Table detection result: {table_detection.description}")
        
        if table_detection.has_multiple_tables:
            print(f"Found {len(table_detection.table_statements)} separate CREATE statements. Processing each separately...")          
            all_ddl_statements = []
            all_dml_statements = []
            
            for i, table_statement in enumerate(table_detection.table_statements):
                print(f"\n2.{i+1}/ Processing statement {i+1}: {table_statement[:100]}...")
                logger.info(f"Processing statement {i+1}")
                
                ddl_sql, dml_sql = self._translator_agent(table_statement)
                print(f"Done with translator agent for statement {i+1}, DDL: {ddl_sql[:100]}..., DML: {dml_sql[:50] if dml_sql else 'empty'}...")
                logger.info(f"Translator agent completed for statement {i+1}")
                
                ddl_sql, dml_sql = self._mandatory_validation_agent(ddl_sql, dml_sql)
                print(f"Done with mandatory validation agent for statement {i+1}")
                logger.info(f"Mandatory validation completed for statement {i+1}")
                
                if ddl_sql.strip():
                    all_ddl_statements.append(ddl_sql)
                if dml_sql and dml_sql.strip():
                    all_dml_statements.append(dml_sql)
            
            # Combine all statements
            final_ddl = all_ddl_statements
            final_dml = all_dml_statements
            
        else:
            # Single statement processing
            print("2/ Processing single KSQL statement...")
            ddl_sql, dml_sql = self._translator_agent(ksql)
            print(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}\n3/ Start mandatory validation agent...")
            logger.info(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}")
            ddl, dml = self._mandatory_validation_agent(ddl_sql, dml_sql)
            print(f"Done with mandatory validation agent updated flink DDL sql is:\n {ddl}\nand DML: {dml if dml else 'empty'}")
            logger.info(f"Done with mandatory validation agent, updated flink DDL sql is:\n {ddl}\nand DML: {dml if dml else 'empty'}")
            final_ddl = [ddl]
            final_dml = [dml]
        
        if validate:
            print("3/ Start validating the flink SQLs using Confluent Cloud for Flink, do you want to continue? (y/n)")
            answer = input()
            if answer == "y":
                validated_ddls = []
                validated_dmls = []
                for ddl, dml in zip(final_ddl, final_dml):
                    ddl, validated = self._iterate_on_validation(ddl)
                    if validated:
                        ddl = self._process_semantic_validation(ddl)
                        dml, validated = self._iterate_on_validation(dml)
                        if validated:
                            dml = self._process_semantic_validation(dml)
                    validated_ddls.append(ddl)
                    validated_dmls.append(dml)
                final_ddl = validated_ddls
                final_dml = validated_dmls
        return final_ddl, final_dml


