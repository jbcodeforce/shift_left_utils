import unittest
import json

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config

class TestConfluentClient(unittest.TestCase):


    def test_get_environment_list(self):
        # need another api key
        client = ConfluentCloudClient(get_config())
        environments = client.get_environment_list()
        assert environments
        self.assertGreater(len(environments), 0)
        for e in environments['data']:
            print(e['display_name'])

    def test_get_compute_pool_list(self):
        client = ConfluentCloudClient(get_config())
        config=get_config()
        pools = client.get_compute_pool_list(config.get('confluent_cloud').get('environment_id'))
        self.assertGreater(len(pools), 0)
        print(json.dumps(pools, indent=2))

    def test_verify_compute_exist(self):
        client = ConfluentCloudClient(get_config())
        pool = client.get_compute_pool_info(get_config()['flink']['compute_pool_id'])
        assert pool
        print(pool['spec'])
        print(f"{pool['status']['current_cfu']} over {pool['spec']['max_cfu']}")

    def _test_create_compute_pool(self):
        spec = {}
        config = get_config()
        spec['display_name'] = "test_pool"
        spec['cloud'] = config['confluent_cloud']['provider']
        spec['region'] = config['confluent_cloud']['region']
        spec['max_cfu'] =  config['flink']['max_cfu']
        spec['environment'] = { 'id': config['confluent_cloud']['environment_id']}
        client = ConfluentCloudClient(spec)
        pool = client.create_compute_pool(config)
        assert pool
                                            
    def test_get_topic_list(self):
        client = ConfluentCloudClient(get_config())
        resp = client.list_topics()
        assert resp
        self.assertGreater(len(resp), 0)
        print(resp['data'])

    def test_get_flink_statements_list(self):
        client = ConfluentCloudClient(get_config())
        statements = client.get_flink_statement_list()
        assert statements
        self.assertGreater(len(statements), 0)
        print(json.dumps(statements, indent=2))


    def test_show_create_table_statement(self):
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
            statement = client.get_statement_info(statement_name)
            assert statement
            print(f"--- {statement}")
        except Exception as e:
            print(e)
        status=client.delete_flink_statement(statement_name)
        print(f"\n--- {status}")

    def test_select_statement_with_getting_result(self):
        config = get_config()
        client = ConfluentCloudClient(config)
        statement_name="test-statement"
        sql_content = "select * from src_table_1;"
        properties = {'sql.current-catalog' : 'j9r-env' , 'sql.current-database' : 'j9r-kafka'}
        rep= client.delete_flink_statement(statement_name)
        try:
            statement = client.post_flink_statement(config['flink']['compute_pool_id'], statement_name, sql_content, properties, False)
            print(f"\n\n---- {statement}")
            assert statement
            statement = client.get_statement_results(statement_name)
            print(f"--- with result: {statement}\n\n")
            for op_row in statement.results.data:
                print(op_row)
        except Exception as e:
            print(e)
        status=client.delete_flink_statement(statement_name)
        print(f"\n--- {status}")

    def _test_update_statement(self):
        print("Not clear yet how it works")

if __name__ == '__main__':
    unittest.main()