"""
Copyright 2024-2025 Confluent, Inc.
"""

from pydantic import BaseModel
from typing import Tuple, List
import os
import importlib.resources
import requests
import json

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement


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


class KsqlToFlinkSqlVllmAgent:
    """
    KSQL to Flink SQL translation agent using vLLM with cogito model
    """

    def __init__(self):
        self.model_name = os.getenv("VLLM_MODEL", "cogito:32b")
        self.vllm_base_url = os.getenv("VLLM_BASE_URL", "http://localhost:8000")
        self.vllm_api_key = os.getenv("VLLM_API_KEY", "token")
        self._load_prompts()

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
        """Load prompt templates for vLLM cogito model"""
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_vllm").joinpath("translator.txt")
        with fname.open("r") as f:
            self.translator_system_prompt = f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_vllm").joinpath("refinement.txt")
        with fname.open("r") as f:
            self.refinement_system_prompt = f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_vllm").joinpath("mandatory_validation.txt")
        with fname.open("r") as f:
            self.mandatory_validation_system_prompt = f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_vllm").joinpath("table_detection.txt")
        with fname.open("r") as f:
            self.table_detection_system_prompt = f.read()

    def _make_vllm_request(self, prompt: str, response_format=None) -> dict:
        """
        Make a request to vLLM API endpoint
        
        Args:
            prompt (str): The prompt to send to the model
            response_format (type): Expected Pydantic model for structured output
            
        Returns:
            dict: Response from vLLM
        """
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.vllm_api_key}"
        }
        
        data = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.1,
            "max_tokens": 2048
        }
        
        # Add structured output if response format is specified
        if response_format:
            data["response_format"] = {
                "type": "json_object",
                "schema": response_format.model_json_schema()
            }
        
        try:
            response = requests.post(
                f"{self.vllm_base_url}/v1/chat/completions",
                headers=headers,
                json=data,
                timeout=60
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"vLLM request failed: {e}")
            raise

    def _parse_vllm_response(self, response: dict, response_format=None):
        """
        Parse vLLM response and convert to structured output if needed
        
        Args:
            response (dict): Raw response from vLLM
            response_format (type): Expected Pydantic model
            
        Returns:
            Parsed response object or string
        """
        try:
            content = response["choices"][0]["message"]["content"]
            
            if response_format:
                # Try to parse JSON response into Pydantic model
                if content.strip().startswith("{"):
                    json_data = json.loads(content)
                    return response_format(**json_data)
                else:
                    # Extract JSON from text response if needed
                    import re
                    json_match = re.search(r'\{.*\}', content, re.DOTALL)
                    if json_match:
                        json_data = json.loads(json_match.group())
                        return response_format(**json_data)
            
            return content
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.error(f"Failed to parse vLLM response: {e}")
            if response_format:
                # Return default instance with error indicators
                if response_format == KsqlTableDetection:
                    return KsqlTableDetection(
                        has_multiple_tables=False,
                        table_statements=[],
                        description="Error in parsing response"
                    )
                elif response_format == KsqlFlinkSql:
                    return KsqlFlinkSql(
                        ksql_input="",
                        flink_ddl_output="",
                        flink_dml_output=""
                    )
            return ""

    def _table_detection_agent(self, ksql: str) -> KsqlTableDetection:
        """
        Analyze KSQL input to detect multiple CREATE TABLE/STREAM statements and split them if needed
        """
        prompt = f"{self.table_detection_system_prompt}\n\nksql_input: {ksql}"
        
        response = self._make_vllm_request(prompt, KsqlTableDetection)
        parsed_response = self._parse_vllm_response(response, KsqlTableDetection)
        
        if isinstance(parsed_response, KsqlTableDetection):
            return parsed_response
        else:
            return KsqlTableDetection(
                has_multiple_tables=False,
                table_statements=[ksql],
                description="Error in detection, treating as single statement"
            )

    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        """
        Translate KSQL to Flink SQL using vLLM cogito model
        """
        prompt = f"{self.translator_system_prompt}\n\nksql_input: {sql}"
        
        response = self._make_vllm_request(prompt, KsqlFlinkSql)
        parsed_response = self._parse_vllm_response(response, KsqlFlinkSql)
        
        if isinstance(parsed_response, KsqlFlinkSql):
            print(f"Response: {parsed_response}")
            return parsed_response.flink_ddl_output, parsed_response.flink_dml_output
        else:
            return "", ""

    def _mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        """
        Process the mandatory validation of the Flink sql
        """
        prompt = f"{self.mandatory_validation_system_prompt}\n\nddl_sql_input: {ddl_sql}\ndml_sql_input: {dml_sql}"
        
        response = self._make_vllm_request(prompt, FlinkSql)
        parsed_response = self._parse_vllm_response(response, FlinkSql)
        
        if isinstance(parsed_response, FlinkSql):
            return parsed_response.flink_ddl_output, parsed_response.flink_dml_output
        else:
            return "", ""

    def _refinement_agent(self, sql: str, history: str, error_message: str) -> str:
        """
        Refine Flink SQL based on error feedback
        """
        prompt = f"{self.refinement_system_prompt}\n\nflink_sql_input: {sql}\nhistory of the conversation: {history}\nreported error: {error_message}"
        
        response = self._make_vllm_request(prompt, FlinkSqlForRefinement)
        parsed_response = self._parse_vllm_response(response, FlinkSqlForRefinement)
        
        if isinstance(parsed_response, FlinkSqlForRefinement):
            return parsed_response.flink_output
        else:
            return ""

    def _validate_flink_sql_on_cc(self, sql_to_validate: str) -> Tuple[bool, str]:
        """
        Validate Flink SQL on Confluent Cloud
        """
        config = get_config()
        compute_pool_id = config.get('flink').get('compute_pool_id')
        statement = None
        if sql_to_validate:
            statement_name = "syntax-check"
            delete_statement_if_exists(statement_name)
            statement = post_flink_statement(compute_pool_id, statement_name, sql_to_validate)
            print(f"CC Flink Statement: {statement}")
            logger.info(f"CC Flink Statement: {statement}")
            if statement and statement.status:
                if statement.status.phase == "RUNNING":
                    # Stop the statement as not needed anymore
                    delete_statement_if_exists(statement_name)
                    return True, ""
                elif statement.status.phase == "COMPLETED":
                    return True, statement.status.detail
                else:
                    return False, statement.status.detail
            else:
                return False, "No statement found"
        else:
            return False, "No sql to validate"

    def _iterate_on_validation(self, translated_sql: str) -> Tuple[str, bool]:
        """
        Iterate on validation with refinement
        """
        sql_validated = False
        iteration_count = 0
        agent_history = [{"agent": "refinement", "sql": translated_sql}]
        while not sql_validated and iteration_count < 3:
            iteration_count += 1
            sql_validated, status_detail = self._validate_flink_sql_on_cc(translated_sql)
            if not sql_validated:
                print(f"\n\n--> Error: {status_detail}")
                print(f"Process with refinement agent {iteration_count}")
                translated_sql = self._refinement_agent(translated_sql, str(agent_history), status_detail)
                print(f"Refined sql:\n {translated_sql}")
                agent_history.append({"agent": "refinement", "sql": translated_sql})
                print("do you want to continue? (y/n) or you continue with the generated sql?")
                answer = input()
                if answer != "y":
                    return translated_sql, sql_validated
        return translated_sql, sql_validated

    def _process_semantic_validation(self, sql: str) -> str:
        """
        Process the semantic validation of the sql
        """
        return sql

    def translate_from_ksql_to_flink_sql(self, ksql: str, validate: bool = False) -> Tuple[List[str], List[str]]:
        """
        Entry point to translate ksql to flink sql using vLLM Agents. This is the workflow implementation.
        """
        
        # Step 0: Clean the KSQL input by removing DROP TABLE statements and comments
        print("0/ Cleaning KSQL input by removing DROP TABLE statements and comment lines...")
        logger.info("Starting KSQL input cleaning")
        ksql = self._clean_ksql_input(ksql)
        print(f"Cleaned KSQL input: {ksql[:200]}...")
        logger.info("KSQL input cleaning completed")
        
        # Step 1: Detect if there are multiple CREATE TABLE statements
        print(f"1/ Analyzing KSQL input for multiple CREATE TABLE statements using vLLM: {self.model_name}")
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