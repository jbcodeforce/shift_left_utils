
from shift_left.ai.spark_sql_code_agent import SparkToFlinkSqlAgent
from shift_left.ai.ksql_code_agent import KsqlToFlinkSqlAgent
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent
from shift_left.core.utils.app_config import  logger
SPARK_AGENT_TYPE = "spark"
KSQL_AGENT_TYPE = "ksql"
class AgentFactory:
    def __init__(self):
        self._agent_class: TranslatorToFlinkSqlAgent = None # type: ignore

    def get_or_build_sql_translator_agent(self, type: str = SPARK_AGENT_TYPE) -> TranslatorToFlinkSqlAgent:
        """
        Factory to get the SQL translator agent using external configuration file, or
        the default one: DbtTranslatorToFlinkSqlAgent
        """
        if not self._agent_class:
            if type == SPARK_AGENT_TYPE:
                self._agent_class = SparkToFlinkSqlAgent()
            elif type == KSQL_AGENT_TYPE:
                self._agent_class = KsqlToFlinkSqlAgent()
            else:
                logger.error(f"No translator to flink sql agent configured for type: {type}")
                raise ValueError(f"No translator to flink sql agent configured for type: {type}")
        return self._agent_class
