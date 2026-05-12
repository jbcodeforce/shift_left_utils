"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import json
from it.BaseIT import _run_integration_tests, _SKIP_MSG

from shift_left.core.models.cc_environment_model import EnvironmentListResponse
from shift_left.core.models.flink_compute_pool_model import ComputePoolListResponse, ComputePoolResponse
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.flink_sql_adapter import delete_flink_statement as flink_sql_delete_statement
import  shift_left.core.statement_mgr as sm

_RUN_IT = _run_integration_tests()
@unittest.skipUnless(_RUN_IT, _SKIP_MSG)
class TestConfluentClient(unittest.TestCase):

    def test_get_environment_list(self):
        print("#"*30 + "\ntest_get_environment_list\n")
        # need another api key
        client = ConfluentCloudClient(get_config())
        environments = client.get_environment_list()
        assert environments
        assert isinstance(environments, EnvironmentListResponse)
        assert "org/v2" in environments.api_version
        assert "EnvironmentList" in environments.kind
        self.assertGreater(len(environments.data or []), 0)
        for e in environments.data or []:
            print(f"{e.display_name} - {e.id}")
            assert e.metadata
            assert e.metadata.created_at
            assert e.metadata.resource_name
            assert e.metadata.self
            assert e.metadata.updated_at
            assert e.stream_governance_config
            assert e.stream_governance_config.package



    def test_get_topic_list(self):
        print("#"*30 + "\ntest_get_topic_list\n")
        client = ConfluentCloudClient(get_config())
        resp = client.list_topics()
        assert resp
        self.assertGreater(len(resp), 0)
        print(resp['data'])


    def test_show_create_table_statement(self):
        print("\n"+"#"*30+ "\n test_show_create_table_statement\n")
        config = get_config()
        statement_name="test-statement"
        sql_content = "show create table `examples`.`marketplace`.`clicks`;"
        flink_sql_delete_statement(get_config(), statement_name)
        try:
            statement = sm.post_flink_statement(compute_pool_id=config['flink']['compute_pool_id'],
                                statement_name=statement_name,
                                sql_content=sql_content,
                                properties={},
                                stopped=False)
            print(f"\n\n---- {statement}")
            assert statement.result.results
            print( statement.result.results[0]['results']['data'][0]['row'])
            statement = sm.get_statement(statement_name)
            assert statement
            print(f"--- {statement}")
            print("#"*30 + "\n Verify get flink statement list\n")
            statements = sm.get_statement_list()
            self.assertGreater(len(statements), 0)
            print(json.dumps(statements, indent=2))
        except Exception as e:
            print(e)
        status = flink_sql_delete_statement(get_config(), statement_name)
        print(f"\n--- {status}")

    def _test_get_topic_message_count(self):
        print("#"*30 + "\ntest_get_topic_message_count\n")
        client = ConfluentCloudClient(get_config())
        topic_name = "src_aqem_tag_tag"
        message_count = client.get_topic_message_count(topic_name)
        print(f"Message count for {topic_name}: {message_count}")



if __name__ == '__main__':
    unittest.main()
