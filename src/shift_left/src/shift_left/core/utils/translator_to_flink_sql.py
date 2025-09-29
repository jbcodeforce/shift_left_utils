"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
from importlib import import_module
from typing import Tuple, List
from shift_left.core.utils.app_config import get_config, logger
from openai import OpenAI
from shift_left.core.statement_mgr import post_flink_statement, delete_statement_if_exists
from shift_left.core.models.flink_statement_model import Statement

"""
Factory method to create the appropriate translator to Flink SQL agent, with two implementations:
- SparkTranslatorToFlinkSqlAgent: for Spark SQL
- KsqlTranslatorToFlinkSqlAgent: for KsqlDB SQL
"""
class TranslatorToFlinkSqlAgent():
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
        self.llm_client = OpenAI(api_key=self.llm_api_key, base_url=self.llm_base_url)
        self._load_prompts()
        print("-"*50 + f"\n\tUsing {self.model_name} with {self.llm_base_url} and {self.llm_api_key[:25]}...\n" + "-"*50)

    def _load_prompts(self):
        print("To implement")
        pass

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

    def translate_to_flink_sqls(self, sql: str, validate: bool = True) -> Tuple[List[str], List[str]]:
        return [sql], [sql]


_agent_class = None
def get_or_build_sql_translator_agent()-> TranslatorToFlinkSqlAgent:
    """
    Factory to get the SQL translator agent using external configuration file, or
    the default one: DbtTranslatorToFlinkSqlAgent
    """
    global _agent_class
    if not _agent_class:
        if get_config().get('app').get('translator_to_flink_sql_agent'):
            class_to_use = get_config().get('app').get('translator_to_flink_sql_agent')
            module_path, class_name = class_to_use.rsplit('.',1)
            logger.info(f"Using {class_to_use} for SQL translator agent")
            mod = import_module(module_path)
            _agent_class = getattr(mod, class_name)()
        else:
            logger.error("No SQL translator agent specified in config")
            raise ValueError("No SQL translator agent specified in config")
    return _agent_class