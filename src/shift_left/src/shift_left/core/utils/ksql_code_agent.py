"""
Copyright 2024-2025 Confluent, Inc.
"""
from ollama import chat
from pydantic import BaseModel
from typing import Tuple
import os
import importlib.resources 

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement

# use structured output to get the sql from LLM output
class KsqlFlinkSql(BaseModel):
    ksql_input: str
    flink_ddl_output: str
    flink_dml_output: str


class FlinkSql(BaseModel):
    ddl_sql_input: str
    dml_sql_input: str
    flink_ddl_output: str
    flink_dml_output: str

class FlinkSqlForRefinement(BaseModel):
    sql_input: str
    error_message: str
    flink_output: str


class KsqlToFlinkSqlAgent:

    def __init__(self):
        self.qwen_model_name=os.getenv("LLM_MODEL","qwen2.5-coder:32b")
        self.mistral_model_name=os.getenv("LLM_MODEL","mistral-small:latest")
        self.cogito_model_name=os.getenv("LLM_MODEL","cogito:32b")
        self.model_name=self.cogito_model_name
        self.llm_base_url=os.getenv("LLM_BASE_URL","http://localhost:11434")
        self._load_prompts()


    def _validate_flink_sql_on_cc(self, sql_to_validate: str) -> Tuple[bool, str]:
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

    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        translator_prompt_template = "ksql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        response= chat(model=self.model_name, 
                        format=KsqlFlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = KsqlFlinkSql.model_validate_json(response.message.content)
        return obj_response.flink_ddl_output,  obj_response.flink_dml_output

    def _mandatory_validation_agent(self, ddl_sql: str, dml_sql: str) -> Tuple[str, str]:
        """
        Process the mandatory validation of the Flink sql
        """
        syntax_checker_prompt_template = "ddl_sql_input: {ddl_sql_input}\ndml_sql_input: {dml_sql_input}"
        messages=[
            {"role": "system", "content": self.mandatory_validation_system_prompt},
            {"role": "user", "content": syntax_checker_prompt_template.format(ddl_sql_input=ddl_sql, dml_sql_input=dml_sql)}
        ]
        response= chat(model= self.model_name, # self.mistral_model_name, 
                        format=FlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = FlinkSql.model_validate_json(response.message.content)
        return obj_response.flink_ddl_output, obj_response.flink_dml_output

    def _refinement_agent(self, sql: str, 
                              history: str,
                              error_message: str) -> str:
        refinement_prompt_template = "flink_sql_input: {sql_input}\nhistory of the conversation: {history}\nreported error: {error_message}"
        
        messages=[
            {"role": "system", "content": self.refinement_system_prompt},
            {"role": "user", "content": refinement_prompt_template.format(sql_input=sql, history=history, error_message=error_message)}
        ]
        response= chat(model=self.model_name, 
                        format=FlinkSqlForRefinement.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = FlinkSqlForRefinement.model_validate_json(response.message.content)
        return obj_response.flink_output


    def _process_semantic_validation(self, sql: str) -> str:
        """
        Process the semantic validation of the sql
        """
        return sql

    def translate_from_ksql_to_flink_sql(self, ksql: str, validate: bool = False) -> Tuple[str, str]:
        """
        Entry point to translate ksql to flink sql using LLM Agents. This is the workflow implementation.
        """
        
        ddl_sql, dml_sql = self._translator_agent(ksql)
        print(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}\n2/ Start mandatory validation agent...")
        logger.info(f"Done with translator agent, the flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}")
        ddl_sql, dml_sql = self._mandatory_validation_agent(ddl_sql, dml_sql)
        print(f"Done with mandatory validation agent updated flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}")
        logger.info(f"Done with mandatory validation agent, updated flink DDL sql is:\n {ddl_sql}\nand DML: {dml_sql if dml_sql else 'empty'}")
        if validate:
            print("3/ Start validating the flink SQLs using Confluent Cloud for Flink, do you want to continue? (y/n)")
            answer = input()
            if answer != "y":
                return ddl_sql, dml_sql
            ddl_sql, validated = _iterate_on_validation(self, ddl_sql)
            if validated:
                final_ddl = self._process_semantic_validation(ddl_sql)
                dml_sql, validated = _iterate_on_validation(self, dml_sql)
                if validated:
                    final_dml = self._process_semantic_validation(dml_sql)
                    return final_ddl, final_dml
                else:
                    return final_ddl, dml_sql   
        return ddl_sql, dml_sql


def _iterate_on_validation(self, translated_sql: str) -> Tuple[str, bool]:
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


