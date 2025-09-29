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


