"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config
from datetime import datetime

class TestConfluentMetrics(unittest.TestCase):
    def setUp(self):
        self.config = get_config()
        self.client = ConfluentCloudClient(self.config)

    def test_get_metric_kafka_topic(self):
        view="cloud"
        qtype="query"
        cluster_id = self.config["kafka"]["cluster_id"]
        topic_name = "src_master_template"
        query = {
            "group_by": [
                "metric.topic"
            ],
            "aggregations": [
                {
                "metric": "io.confluent.kafka.server/retained_bytes",
                "agg": "SUM"
                }
            ],
            "filter": {
                "op": "AND",
                "filters": [
                    {
                        "field": "resource.kafka.id",
                        "op": "EQ",
                        "value": cluster_id
                    },
                    {
                        "filter": {
                            "field": "metric.topic",
                            "op": "EQ",
                            "value": topic_name
                        }
                    }
                ]
            },
            "group_by": [
                "metric.topic"
            ],
            "granularity": "PT30M",
            "intervals": [
               "now-1m"
            ],
            "limit": 2
        }
        metrics = self.client.get_metrics(view, qtype, query)
        print(metrics)

if __name__ == '__main__':
    unittest.main()