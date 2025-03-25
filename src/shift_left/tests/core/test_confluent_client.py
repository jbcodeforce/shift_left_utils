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

    def test_get_topic_list(self):
        client = ConfluentCloudClient(get_config())
        resp = client.list_topics()
        self.assertGreater(len(resp), 0)
        print(resp['data'])

    def test_get_flink_statements_list(self):
        client = ConfluentCloudClient(get_config())
        statements = client.get_flink_statement_list()
        self.assertGreater(len(statements), 0)
        print(json.dumps(statements, indent=2))

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



if __name__ == '__main__':
    unittest.main()