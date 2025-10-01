"""
Copyright 2024-2025 Confluent, Inc.
"""

from openai import OpenAI
from typing import Tuple
import os


from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement



class AnySqlToFlinkSqlAgent:

    def __init__(self):
        self.qwen_model_name="qwen2.5-coder:32b"
        self.qwen3_model_name="qwen3:30b"
        self.mistral_model_name="mistral-small:latest"
        self.cogito_model_name="cogito:32b"
        self.kimi_k2_model_name="moonshotai/Kimi-K2-Instruct:novita"
        #self.model_name=self.cogito_model_name
        self.model_name=os.getenv("SL_LLM_MODEL",self.qwen3_model_name) # default to qwen3
        self.llm_base_url=os.getenv("SL_LLM_BASE_URL","http://localhost:11434/v1")
        self.llm_api_key=os.getenv("SL_LLM_API_KEY","ollama_test_key")
        print(f"Using {self.model_name} with {self.llm_base_url} and {self.llm_api_key[:25]}...")
        self.llm_client = OpenAI(api_key=self.llm_api_key, base_url=self.llm_base_url)
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
                if statement.status.phase in ["RUNNING", "COMPLETED"]:
                    # Stop the statement as it is not needed anymore
                    delete_statement_if_exists(statement_name)
                    return True, statement.status.detail
                else:
                    return False, statement.status.detail
            else:
                return False, "No statement found"
        else:
            return False, "No sql to validate"
        
    def _load_prompts(self):
        print("To implement")
        pass

    def translate_from_ksql_to_flink_sql(self, ksql: str, validate: bool = False) -> Tuple[str, str]:
        """
        Entry point to translate the source SQL to flink sql using LLM Agents. 
        This is the workflow implementation.
        """
        final_ddl = ""
        final_dml = ""
        print("To implement")
        return final_ddl, final_dml

    def _refinement_agent(self, sql: str, 
                                history: str,
                                error_message: str) -> str:
            print("To implement")
            return sql

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
            else:
                print(f"SQL is valid and runs on CC after {iteration_count} iterations")
        return translated_sql, sql_validated


