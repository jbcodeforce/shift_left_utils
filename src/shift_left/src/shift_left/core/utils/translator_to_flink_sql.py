"""
Copyright 2024-2025 Confluent, Inc.
"""
from pydantic import BaseModel
from importlib import import_module
from typing import Tuple, List
from shift_left.core.utils.app_config import get_config, logger

from shift_left.core.utils.ksql_code_agent import KsqlToFlinkSqlAgent
from shift_left.core.utils.spark_sql_code_agent import SparkToFlinkSqlAgent
from shift_left.core.utils.flink_sql_code_agent_lg import define_flink_sql_agent

class TranslatorToFlinkSqlAgent(BaseModel):
    def __init__(self):
        pass

    def translate_to_flink_sqls(self, table_name: str,sql: str) -> Tuple[str, str]:
        return sql, ''


class DbtTranslatorToFlinkSqlAgent(TranslatorToFlinkSqlAgent):
    def  translate_to_flink_sqls(self, table_name: str, sql: str, validate: bool = False) -> Tuple[List[str], List[str]]:
        logger.info(f"Start translating dbt to flink sql for table {table_name}")
        app = define_flink_sql_agent()
        inputs = {"sql_input": sql, "table_name" : table_name}
        result=app.invoke(inputs)
        return [result['flink_sql']], [result['derived_ddl']]
        #agent = SparkToFlinkSqlAgent()
        #ddl, dml = agent.translate_to_flink_sql(sql, validate=validate)
        #return [ddl], [dml]

class KsqlTranslatorToFlinkSqlAgent(TranslatorToFlinkSqlAgent):
    """
    ksqlDB translator to Flink SQL
    """
    def translate_to_flink_sqls(self, table_name: str, ksql: str, validate: bool = False) -> Tuple[List[str], List[str]]:
        logger.info(f"Start translating ksql to flink sql for table {table_name} using KsqlToFlinkSqlAgent")
        print(f"Start translating ksql to flink sql for table {table_name} using KsqlToFlinkSqlAgent")
        agent = KsqlToFlinkSqlAgent()
        ddl, dml = agent.translate_from_ksql_to_flink_sql(table_name, ksql, validate=validate)
        return ddl, dml

_agent_class = None
def get_or_build_sql_translator_agent():
    """
    Factory to get the SQL translator agent using external configuration file, or
    the default one: DbtTranslatorToFlinkSqlAgent
    """
    global _agent_class
    if not _agent_class:
        if get_config().get('app').get('translator_to_flink_sql_agent'):
            class_to_use = get_config().get('app').get('translator_to_flink_sql_agent')
            module_path, class_name = class_to_use.rsplit('.',1)
            mod = import_module(module_path)
            _agent_class = getattr(mod, class_name)()
        else:
            _agent_class = DbtTranslatorToFlinkSqlAgent()
    return _agent_class