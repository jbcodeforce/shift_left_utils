import unittest
import json

from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config

class TestConfluentClient(unittest.TestCase):


    def _test_get_environment_list(self):
        # need another api key
        client = ConfluentCloudClient(get_config())
        environments = client.get_environment_list()
        self.assertGreater(len(environments), 0)
        print(environments)

    def _test_get_topic_list(self):
        client = ConfluentCloudClient(get_config())
        topics = client.list_topics()
        self.assertGreater(len(topics), 0)
        print(topics)

    def _test_get_flink_statements_list(self):
        client = ConfluentCloudClient(get_config())
        statements = client.get_flink_statement_list()
        self.assertGreater(len(statements), 0)
        print(json.dumps(statements, indent=2))

    def test_get_compute_pool_list(self):
        client = ConfluentCloudClient(get_config())
        pools = client.get_compute_pool_list()
        self.assertGreater(len(pools), 0)
        print(json.dumps(pools, indent=2))


if __name__ == '__main__':
    unittest.main()