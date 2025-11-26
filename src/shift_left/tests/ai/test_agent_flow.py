
import unittest
from shift_left.ai.agent_factory import AgentFactory
from shift_left.ai.spark_sql_code_agent import SparkToFlinkSqlAgent
from shift_left.ai.ksql_code_agent import KsqlToFlinkSqlAgent

class TestAgentFlow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    def test_agent_factory_success(self):
        agent = AgentFactory().get_or_build_sql_translator_agent("spark")
        self.assertIsInstance(agent, SparkToFlinkSqlAgent)
        agent = AgentFactory().get_or_build_sql_translator_agent("ksql")
        self.assertIsInstance(agent, KsqlToFlinkSqlAgent)

    def test_agent_factory_failure(self):
        with self.assertRaises(ValueError):
            AgentFactory().get_or_build_sql_translator_agent("invalid")

    def test_simple_ksql
if __name__ == "__main__":
    unittest.main()
