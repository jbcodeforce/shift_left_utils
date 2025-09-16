"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import json
import os, pathlib
#os.environ["CONFIG_FILE"] =  str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config

class TestConfluentClient(unittest.TestCase):

    def test_get_environment_list(self):
        print("#"*30 + "\ntest_get_environment_list\n")
        # need another api key
        client = ConfluentCloudClient(get_config())
        environments = client.get_environment_list()
        assert environments
        self.assertGreater(len(environments), 0)
        for e in environments['data']:
            print(e['display_name'])
            print(e['id'])
  

    def test_get_compute_pool_list(self):
        print("#"*30 + "\ntest_get_compute_pool_list\n")
        client = ConfluentCloudClient(get_config())
        config=get_config()
        pools = client.get_compute_pool_list(config.get('confluent_cloud').get('environment_id'), config.get('confluent_cloud').get('region'))
        self.assertGreater(len(pools.data), 0)
        print(pools.model_dump_json(indent=2))

    def test_verify_compute_exist(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        pool = client.get_compute_pool_info(config.get('flink').get('compute_pool_id'), config.get('confluent_cloud').get('environment_id'))
        assert pool
        print(pool['spec'])
        print(f"{pool['status']['current_cfu']} over {pool['spec']['max_cfu']}")

    def test_create_compute_pool(self):
        spec = {}
        config = get_config()
        spec['display_name'] = "test_pool"
        spec['cloud'] = config['confluent_cloud']['provider']
        spec['region'] = config['confluent_cloud']['region']
        spec['max_cfu'] =  config['flink']['max_cfu']
        spec['environment'] = { 'id': config['confluent_cloud']['environment_id']}
        client = ConfluentCloudClient(config)
        pool = client.create_compute_pool(spec)
        assert pool
                                            
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
        client = ConfluentCloudClient(config)
        statement_name="test-statement"
        sql_content = "show create table `examples`.`marketplace`.`clicks`;"
        properties = {'sql.current-catalog' : 'examples' , 'sql.current-database' : 'marketplace'}
        rep= client.delete_flink_statement(statement_name)
        try:
            statement = client.post_flink_statement(config['flink']['compute_pool_id'], statement_name, sql_content, properties, False)
            print(f"\n\n---- {statement}")
            assert statement.result.results
            print( statement.result.results[0]['results']['data'][0]['row'])
            statement = client.get_flink_statement(statement_name)
            assert statement
            print(f"--- {statement}")
            print("#"*30 + "\n Verify get flink statement list\n")
            statements = client.get_flink_statement_list()
            self.assertGreater(len(statements), 0)
            print(json.dumps(statements, indent=2))
        except Exception as e:
            print(e)
        status=client.delete_flink_statement(statement_name)
        print(f"\n--- {status}")

    def _test_get_topic_message_count(self):
        print("#"*30 + "\ntest_get_topic_message_count\n")
        client = ConfluentCloudClient(get_config())
        topic_name = "src_aqem_tag_tag"
        message_count = client.get_topic_message_count(topic_name)
        print(f"Message count for {topic_name}: {message_count}")
    


if __name__ == '__main__':
    unittest.main()