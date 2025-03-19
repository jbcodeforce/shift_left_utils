from confluent_kafka import  SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient

from app_config import get_config
import os, argparse
import json
import coloredlogs, logging
coloredlogs.install()


TESTS_FOLDER="tests"
DATA_FN="data.json"
CONFIG_FILE="./config.yaml"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate records for testing given a schema subject in a schema registry'
)

parser.add_argument('-t', '--topic_name', required=True, help="The name of the topic to write message to")
parser.add_argument('-s', '--src_table_name', required=True, help="The name of the source table to use to load test data from")

"""
The goal of this tool is to read test data for a given source, send them to the 
corresponding Kafka Topic in Confluent Cloud, taking into account the schema registry
"""

def specific_shema(sr_client, subject_name: str) -> str | None:
    try:
        schema_version = sr_client.get_latest_version(subject_name)
        schema_def = schema_version.schema   # instance of confluent_kafka.schema_registry.schema_registry_client.Schema
        logging.info(f"\n\t@@@@ Found {subject_name} in schema registry --> {schema_def.schema_str}")
        return schema_def.schema_str
    except Exception as e:
        logging.error(f"@@@@ - Schema {subject_name} not found returning None\n")
        return None
        
    
def load_schema_definitions_from_schema_registry(config: dict, topic_name: str) -> tuple[str,str]:
    """
    :param: config includes the loaded configuration of the app from the config.yaml, it includes schema registry URL and credentials
    """
    sr_client= SchemaRegistryClient({"url": config["registry"]["url"], 
                                   "basic.auth.user.info":  config["registry"]["registry_key_name"]+":"+config["registry"]["registry_key_secret"]})
    
    schema_value = specific_shema(sr_client, f"{topic_name}-value")
    schema_key = specific_shema(sr_client, f"{topic_name}-key")
    return schema_key, schema_value


def load_schema_definitions(sr_client: dict, topic_name: str) -> tuple[str,str]:
    """
    :param: Schema registry client
    :param: topic name, subject
    """
    schema_value = specific_shema(sr_client, f"{topic_name}-value")
    schema_key = specific_shema(sr_client, f"{topic_name}-key")
    return schema_key, schema_value

def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
 

def process_test_data(kafka_producer, src_table_name: str, topic_name: str):
    with open(f"{src_table_name}/{TESTS_FOLDER}/{DATA_FN}", "r") as f:
        data=json.load(f)
        for record in data:
            kafka_producer.produce(topic_name, 
                   key=json.dumps(record["key"]), 
                   value=json.dumps(record["value"]),
                   on_delivery=delivery_report)
            logging.info(f"Produced message to topic {topic_name}: key = {record["key"]} value = {record["value"]}")
            kafka_producer.flush()

def prepare_producer(config: dict, topic_name: str):   

    registry_client=SchemaRegistryClient({"url": config["registry"]["url"], 
                                   "basic.auth.user.info":  config["registry"]["registry_key_name"]+":"+config["registry"]["registry_key_secret"]})
    key_sch, value_sch=load_schema_definitions(registry_client,topic_name)
    value_serializer = AvroSerializer(registry_client, 
                                     value_sch, 
                                     conf= {"auto.register.schemas": False })
    if key_sch:
        key_serializer = AvroSerializer(registry_client, 
                                     key_sch, 
                                     conf= {"auto.register.schemas": False })
        config["key.serializer"]=key_serializer
    config["value.serializer"]=value_serializer
    
    return SerializingProducer(config["kafka"])

if __name__ == "__main__":
    """
    Load the data.json file, and sent each row as record to the kafka topic
    """
    args = parser.parse_args()
    config=read_config(CONFIG_FILE)
    kafka_producer = prepare_producer(config, args.topic_name)
    process_test_data(kafka_producer, args.src_table_name, args.topic_name)
    print("Done !")
    