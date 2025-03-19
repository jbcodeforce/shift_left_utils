from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from app_config import read_config
import os, argparse
import json
import coloredlogs, logging
coloredlogs.install()

CONFIG_FILE="config.yaml"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate records for testing given a schema subject in a schema registry'
)

parser.add_argument('-t', '--topic_name', required=True, help="The name of the topic to write message to")


def prepare_consumer(config):
    registry_client=SchemaRegistryClient({"url": config["registry"]["url"], 
                                   "basic.auth.user.info":  config["registry"]["registry_key_name"]+":"+config["registry"]["registry_key_secret"]})
    value_deserializer = AvroDeserializer(registry_client)
    key_deserializer = AvroDeserializer(registry_client)
    config["kafka"]["group.id"]="grp_test"
    config["kafka"]["session.timeout.ms"]= 6000
    config["kafka"]["auto.offset.reset"] = "earliest"
    config["key.deserializer"]=key_deserializer
    config["value.deserializer"]=value_deserializer
    return Consumer(config["kafka"])

def process_messages(consumer, topic_name):
    consumer.subscribe([topic_name])
    try:
        while True:
            # Poll for new messages
            msg = consumer.poll(timeout=30.0)
            
            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print(f"Key: {msg.key()}, Value: {msg.value()}")
                
    except KeyboardInterrupt:
        pass
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    """
    Load the data.json file, and sent each row as record to the kafka topic
    """
    args = parser.parse_args()
    config=read_config(CONFIG_FILE)
    consumer = prepare_consumer(config)
    process_messages(consumer,args.topic_name)