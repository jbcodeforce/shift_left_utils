
import json
from ollama import chat, AsyncClient
from pydantic import BaseModel
from typing import Tuple
import os
import importlib.resources 

from shift_left.core.utils.app_config import get_config
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement

# use structured output to get the sql from LLM output
class KsqlFlinkSql(BaseModel):
    ksql_input: str
    flink_sql_output: str


class FlinkSql(BaseModel):
    flink_sql_input: str
    error_message: str
    flink_sql_output: str


class KsqlToFlinkSqlAgent:

    def __init__(self):
        self.qwen_model_name=os.getenv("LLM_MODEL","qwen2.5-coder:32b")
        self.mistral_model_name=os.getenv("LLM_MODEL","mistral-small:latest")
        self.llm_base_url=os.getenv("LLM_BASE_URL","http://localhost:11434")
        self._load_prompts()


    def _validate_flink_sql_on_cc(self, sql: str) -> Tuple[bool, str]:
        config = get_config()
        compute_pool_id = config['flink']['compute_pool_id']
        statement_name = "syntax-check-flink-sql"
        delete_statement_if_exists(statement_name)
        statement = post_flink_statement(compute_pool_id, statement_name, sql)
        print(f"CC Flink Statement: {statement}")
        if statement and isinstance(statement, Statement) and statement.status:
            if statement.status.phase == "RUNNING":
                return True, ""
            else:
                return False, statement.status.detail
        else:
            return False, statement
        
    def _load_prompts(self):
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("translator.txt")
        with open(fname, "r") as f:
            self.translator_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("refinement.txt")
        with open(fname, "r") as f:
            self.refinement_system_prompt= f.read()
        fname = importlib.resources.files("shift_left.core.utils.prompts.ksql_fsql").joinpath("mandatory_validation.txt")
        with open(fname, "r") as f:
            self.mandatory_validation_system_prompt= f.read()

    def _translator_agent(self, sql: str) -> Tuple[str, str]:
        translator_prompt_template = "ksql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.translator_system_prompt},
            {"role": "user", "content": translator_prompt_template.format(sql_input=sql)}
        ]
        response= chat(model=self.qwen_model_name, 
                        format=KsqlFlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = KsqlFlinkSql.model_validate_json(response.message.content)
        return obj_response.flink_sql_output, ""

    def _mandatory_validation_agent(self, sql: str) -> str:
        """
        Process the mandatory validation of the Flink sql
        """
        syntax_checker_prompt_template = "flink_sql_input: {sql_input}"
        messages=[
            {"role": "system", "content": self.mandatory_validation_system_prompt},
            {"role": "user", "content": syntax_checker_prompt_template.format(sql_input=sql)}
        ]
        response= chat(model= self.qwen_model_name, # self.mistral_model_name, 
                        format=FlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = FlinkSql.model_validate_json(response.message.content)
        return obj_response.flink_sql_output

    def _refinement_agent(self, sql: str, 
                              history: str,
                              error_message: str) -> str:
        refinement_prompt_template = "flink_sql_input: {sql_input}\nhistory of the conversation: {history}\nreported error: {error_message}"
        
        messages=[
            {"role": "system", "content": self.refinement_system_prompt},
            {"role": "user", "content": refinement_prompt_template.format(sql_input=sql, history=history, error_message=error_message)}
        ]
        response= chat(model=self.qwen_model_name, 
                        format=FlinkSql.model_json_schema(), 
                        messages=messages, 
                        options={'temperature': 0}
                        )
        obj_response = FlinkSql.model_validate_json(response.message.content)
        return obj_response.flink_sql_output


    def _process_semantic_validation(self, sql: str) -> str:
        """
        Process the semantic validation of the sql
        """
        return sql

    def translate_from_ksql_to_flink_sql(self, ksql: str, validate: bool = False) -> Tuple[str, str]:
        """
        Entry point to translate ksql to flink sql
        """
        agent_history = []
        translated_sql, ddl_sql = self._translator_agent(ksql)
        print(f"Done with translator agent, the flink sql is:\n {translated_sql}\n{ddl_sql}")
        translated_sql = self._mandatory_validation_agent(translated_sql)
        print(f"Done with mandatory validation agent updated flink sql:\n {translated_sql}\n")
        ddl_sql = "" # as of now
        if validate:
            print("Start validating the flink sql, do you want to continue? (y/n)")
            answer = input()
            if answer != "y":
                return translated_sql, ddl_sql
            error_message = ""
            final_sql_created = False
            iteration_count = 0
            while not final_sql_created and iteration_count < 3:
                iteration_count += 1
                final_sql_created, status_detail = self._validate_flink_sql_on_cc(translated_sql)
                if not final_sql_created:
                    print(f"\n\n--> Error: {status_detail}")
                    error_message = status_detail
                    print(f"Process with refinement agent {iteration_count}")
                    translated_sql = self._refinement_agent(translated_sql, agent_history, error_message)
                    print(f"Refined sql:\n {translated_sql}")
                    agent_history.append({"agent": "refinement", "sql": translated_sql})
                print("do you want to continue? (y/n) or you continue with the generated sql?")
                answer = input()
                if answer != "y":
                    return translated_sql, ddl_sql
            final_sql = self._process_semantic_validation(translated_sql)
        else:
            final_sql = translated_sql
        return final_sql, ddl_sql





