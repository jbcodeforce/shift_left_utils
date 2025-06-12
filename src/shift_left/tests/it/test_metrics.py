"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
#os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent /  "config-ccloud.yaml")
os.environ["CONFIG_FILE"] =  "/Users/jerome/.shift_left/config-stage-flink.yaml"
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from datetime import datetime, timedelta
import json
import shift_left.core.metric_mgr as metric_mgr 
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient
import socket
import os
import json


def get_topic_message_count(topic_name) -> int:
    """
    Gets the approximate number of messages in a Kafka topic.

    Args:
        bootstrap_servers (str or list): Comma-separated string or list of Kafka broker addresses (e.g., 'localhost:9092').
        topic_name (str): The name of the Kafka topic.

    Returns:
        int: The approximate total number of messages in the topic, or None if an error occurs.
    """
    try:
        config = get_config()
        kafka_config={"bootstrap.servers": config["kafka"]["bootstrap.servers"],
           "group.id": "grp_test",
           "enable.auto.commit": False,
           "auto.offset.reset": "latest",
           "security.protocol": config["kafka"]["security.protocol"],
           "sasl.mechanism": config["kafka"]["sasl.mechanism"],
           "sasl.username": config["kafka"]["sasl.username"],
           "sasl.password": config["kafka"]["sasl.password"],
           #"client.id": config["kafka"]["client.id"],
           "receive.message.max.bytes": 1213486160,
           "session.timeout.ms": config["kafka"]["session.timeout.ms"]
           }
        print(kafka_config)
        #admin_client = AdminClient(kafka_config)
        #metadata = admin_client.list_topics(timeout=10)
        #topics = list(metadata.topics.keys())

        consumer = Consumer(
            kafka_config
        )

        metadata = consumer.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            print(f"Topic '{topic_name}' not found.")
            return None

        total_messages = 0
        for partition_info in metadata.topics[topic_name].partitions.values():
            tp = TopicPartition(topic_name, partition_info.id)
            low, high = consumer.get_watermark_offsets(tp)
            total_messages += high

        consumer.close()
        return total_messages

    except KafkaError as e:
        print(f"Kafka error: {e}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
        
class TestConfluentMetrics(unittest.TestCase):

    

    def test_socket_connection(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        config = get_config()
        kafka_cluster_host=config["kafka"]["bootstrap.servers"].split(":")[0]
        kafka_cluster_port=int(config["kafka"]["bootstrap.servers"].split(":")[1])
        print(f"Checking connection to {kafka_cluster_host}:{kafka_cluster_port}")
        sock.settimeout(10)  # 10 second timeout
        result = sock.connect_ex((kafka_cluster_host,kafka_cluster_port))
        assert result == 0
        if result == 0:
            print ("Port is open")
        else:
            print ("Port is not open")
        sock.close()    
    
    def _test_get_metric_kafka_topic(self):
        view="cloud"
        qtype="query"
        topic_name = "src_aqem_recordconfiguration_form_element"
        q1= {"aggregations":[
                {"metric":"io.confluent.kafka.server/retained_bytes"}
                ],
            "filter":{"op":"OR",
                      "filters":[{"field":"resource.kafka.id","op":"EQ","value":"lkc-1j51r6"},
                                 {"field":"resource.kafka.id","op":"EQ","value":"lkc-3dx5pj"}]
                    },
            "granularity":"PT1H",
            "intervals":["2025-02-18T15:36:00+05:30/2025-02-18T16:36:00+05:30"],
            "limit":1000,
            "group_by":["resource.kafka.id"],
            "format":"GROUPED"
            }
        now_minus_1_hour = datetime.now() - timedelta(hours=1)
        now= datetime.now()
        interval = f"{now_minus_1_hour.strftime('%Y-%m-%dT%H:%M:%S%z')}/{now.strftime('%Y-%m-%dT%H:%M:%S%z')}"
        query = {"aggregations": [{"metric": "io.confluent.kafka.server/retained_bytes"}],  
                "filter": { "field": "resource.kafka.id",
                            "op": "EQ",
                            "value": "v"
                 },
                "group_by": ["metric.topic"],
                "granularity": "PT1M",
                "intervals": [interval],
                "limit": 10,
                "format": "GROUPED"
        }


    def test_get_retention_size(self):
        print("test_get_retention_size")
        table_name = "src_aqem_recordconfiguration_form_element"
        retention_size = metric_mgr.get_retention_size(table_name)
        print(f"retention_size: {retention_size}")
        assert retention_size > 0
    
    def test_get_total_message(self):
        print("test_get_total_messages")
        table_name = "src_aqem_recordconfiguration_form_element"
        compute_pool_id = "lfcp-79zx9j"
        nb_of_messages = metric_mgr.get_total_amount_of_messages(table_name, compute_pool_id)
        print(nb_of_messages)
        assert nb_of_messages >= 0

    def test_get_pending_records(self):
        print("test_get_pending_records")
        statement_name = "stage-aqem-dml-aqem-mv-fct-step-event"
        compute_pool_id = "lfcp-1o07pz"
        pending_records = metric_mgr.get_pending_records(statement_name, compute_pool_id)
        print(f"pending_records: {pending_records}")
        assert pending_records >= 0

    def test_get_output_records(self):
        print("test_get_output_records")
        statement_name = "stage-aqem-dml-aqem-mv-fct-step-event"
        compute_pool_id = "lfcp-1o07pz"
        statement_name = "stage-aqem-dml-src-aqem-recordexecution-participant"
        compute_pool_id = "lfcp-6xzvx6"
        output_records = metric_mgr.get_num_records_out(statement_name, compute_pool_id)
        print(f"output_records: {output_records}")
        assert output_records >= 0

if __name__ == '__main__':
    unittest.main()