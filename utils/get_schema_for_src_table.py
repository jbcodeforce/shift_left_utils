"""
Set of functions to get table information, mostly for source topics.

* Function to get the list of topics within a Kafka Cluster
* Function to get the matching topic name given a table name.
* Get the SQL schema given a topic name, using the `show create table ` and 
the Confluent Cloud Flink REST API.
* ConfluentCloudRestAPI client class
"""

import os, time
import argparse
import requests, json
from base64 import b64encode
from kafka.app_config import get_config
import logging
from typing import List
import asyncio

logging.basicConfig(level=get_config()["app"]["logging"], format='%(levelname)s: %(message)s')

TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Set of basic function to query Confluent Cloud'
)

parser.add_argument('-tl', '--topic_list', required=False, help="List topics from the Kafka cluster as defined in CONFIG_FILE and save to the given filename")


class ConfluentFlinkClient:
    """
    Confluent Cloud client to connect to CC and to execute queries using REST API.
    """
    def __init__(self, api_key, api_secret, cloud_api_endpoint):
        self.api_key = api_key
        self.api_secret = api_secret
        self.cloud_api_endpoint = cloud_api_endpoint
        self.auth_header = self._generate_auth_header()
        
    def _generate_auth_header(self):
        """Generate the Basic Auth header using API key and secret"""
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"
    
    def make_request(self, method, endpoint, data=None):
        """Make HTTP request to Confluent Cloud API"""
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        
        url = f"{self.cloud_api_endpoint}{endpoint}"
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data
        )
        if response.status_code in [200,202]:
            logging.debug(response.request.body)
            logging.debug("Response headers:", response.headers)
        else:
            logging.error("Request failed:", response.status_code)
        response.raise_for_status()
        return response.json()
    
    def get_statement_status(self, endpoint, statement_id):
        """Get the status of a Flink SQL statement"""
        endpoint = f"{endpoint}/{statement_id}"
        return self.make_request("GET", endpoint)["status"]
    


def _find_sub_string(table_name, topic_name) -> bool:
    """
    Topic name may includes words separated by ., and table may have words
    separated by _, so try to find all the words defining the name of the table
    to be in the topic name
    """
    words=table_name.split("_")
    subparts=topic_name.split(".")
    all_present = True
    for w in words:
        if w not in subparts:
            all_present=False
            break
    return all_present

def search_matching_topic(table_name: str, rejected_prefixes: List[str]) -> str:
    """
    Given the table name search in the list of topics the potential matching topic.
    return the topic name if found otherwise return the table name
    """
    potential_matches=[]
    logging.debug(f"Search {table_name} in the list of topics, avoiding the ones starting by {rejected_prefixes}")
    with open(TOPIC_LIST_FILE,"r") as f:
        for line in f:
            line=line.strip()
            if ',' in line:
                keyname=line.split(',')[0]
                line=line.split(',')[1].strip()
            else:
                keyname=line
            if table_name == keyname:
                potential_matches.append(line)
            elif table_name in keyname:
                potential_matches.append(line)
            elif _find_sub_string(table_name, keyname):
                potential_matches.append(line)
    if len(potential_matches) == 1:
        return potential_matches[0]
    else:
        logging.warning(f"Found multiple potential matching topics: {potential_matches}, removing the ones that may be not start with {rejected_prefixes}")
        narrow_list=[]
        for topic in potential_matches:
            found = False
            for prefix in rejected_prefixes:
                if topic.startswith(prefix):
                    found = True
            if not found:
                narrow_list.append(topic)
        if len(narrow_list) > 1:
            logging.error(f"Still found more topic than expected {narrow_list}\n\t--> Need to abort")
            exit()
        elif len(narrow_list) == 0:
            logging.warning(f"Found no more topic {narrow_list}")
            return ""
        logging.debug(f"Take the following topic: {narrow_list[0]}")
        return narrow_list[0]



def get_environment_list(config):
    """
    Get the list of environments. use the CC resource api_key
    """
    url=f"https://{config["confluent_cloud"]["base_api"]}"
    client = ConfluentFlinkClient(config["confluent_cloud"]["api_key"], config["confluent_cloud"]["api_secret"], url)
    try:
        result = client.make_request("GET","/environments")
        logging.info("Statement execution result:", json.dumps(result, indent=2))
        return result
    except requests.exceptions.RequestException as e:
        logging.info(f"Error executing rest call: {e}")

def _build_flink_client(config):
    region=config["confluent_cloud"]["region"]
    cloud_provider=config["confluent_cloud"]["provider"]
    organization_id=config["confluent_cloud"]["organization_id"]
    env_id=config["confluent_cloud"]["environment_id"]
    if config["flink"]["url_scope"].lower() == "private":
        url=f"https://flink.{region}.{cloud_provider}.private.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
    else:
        url=f"https://flink.{region}.{cloud_provider}.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
    return ConfluentFlinkClient(config["flink"]["api_key"], config["flink"]["api_secret"], url), url
   
def get_topic_list(config):
    region=config["confluent_cloud"]["region"]
    cloud_provider=config["confluent_cloud"]["provider"]
    pkafka_cluster=config["kafka"]["pkafka_cluster"]
    cluster_id=config["kafka"]["cluster_id"]
    url=f"https://{pkafka_cluster}.{region}.{cloud_provider}.confluent.cloud/kafka/v3/clusters/{cluster_id}"
    client=ConfluentFlinkClient(config["kafka"]["api_key"], config["kafka"]["api_secret"], url )
    try:
        result = client.make_request("GET","/topics")
        return result
    except requests.exceptions.RequestException as e:
        logging.info(f"Error executing rest call: {e}")

def get_flink_statement_list(config): 
    client, _ = _build_flink_client(config)
    try:
        result = client.make_request("GET","/statements")
        logging.info("Statement execution result:", json.dumps(result, indent=2))
        return result
    except requests.exceptions.RequestException as e:
        logging.info(f"Error executing rest call: {e}")

def get_compute_pool_list(config): 
    env_id=config["confluent_cloud"]["environment_id"]
    url=f"https://confluent.cloud/api/fcpm/v2/compute-pools?environment={env_id}"
    client=ConfluentFlinkClient(config["confluent_cloud"]["api_key"], config["confluent_cloud"]["api_secret"], url)
    try:
        result = client.make_request("GET","")
        logging.info("Statement execution result:", json.dumps(result, indent=2))
        return result
    except requests.exceptions.RequestException as e:
        logging.info(f"Error executing rest call: {e}")

def post_flink_statement(config, statement_name: str, sql_statement: str, stopped: False): 
    """
    POST to the statements API to execute a SQL statement.
    """
    client, url = _build_flink_client(config)
    compute_pool_id = os.getenv("CPOOL_ID") if os.getenv("CPOOL_ID") else config["flink"]["compute_pool_id"]
    statement_data = {
            "name": statement_name,
            "organization_id": config["confluent_cloud"]["organization_id"],
            "environment_id": config["confluent_cloud"]["environment_id"],
            "spec": {
                "statement": sql_statement,
                "compute_pool_id":  compute_pool_id,
                "stopped": stopped
            }
        }
    try:
        logging.info(f"Send POST request to Flink statement api with {statement_data}")
        response = client.make_request("POST","/statements", statement_data)
        statement_id = response["metadata"]["uid"]
        logging.debug(f"Statement id for post request: {statement_id}")
        while True:
            status = client.get_statement_status("/statements", statement_name)
            if status["phase"] in ["COMPLETED"]:
                results=client.make_request("GET",f"/statements/{statement_name}/results")
                logging.debug(f"Response to the get statement results: {results}")
                return results
            time.sleep(5)
    except requests.exceptions.RequestException as e:
        logging.error(f"Error executing rest call: {e}")

def delete_flink_statement(config, statement_name):
    client, url = _build_flink_client(config)
    try:
        status=client.make_request("DELETE",f"/statements/{statement_name}")
        logging.info(f" delete_flink_statement: {status}")
    except requests.exceptions.RequestException as e:
        logging.info(f"Error executing rest call: {e}")

def mock_result() -> dict:
    return {'api_version': 'sql/v1', 'kind': 'StatementResult', 'metadata': {'created_at': '2025-02-03T20:18:11.285278Z', 'next': '', 'self': 'https://flink.us-west-2.aws.private.confluent.cloud/sql/v1/organizations/5329e19e-9edd-4b41-9dcf-fa5710edbd96/environments/env-p6272/statements/show-ct-1/results'}, 'results': {'data': [{'row': ["CREATE TABLE `development_non-prod-TG`.`development-us-west-2-dedicated`.`clone.prod.mc.dbo.web_business_unit` (\n  `key` VARBINARY(2147483647),\n  `unit_id` VARCHAR(2147483647) NOT NULL,\n  `unit_name` VARCHAR(2147483647) NOT NULL,\n  `__op` VARCHAR(2147483647),\n  `__db` VARCHAR(2147483647),\n  `__snapshot` VARCHAR(2147483647),\n  `__source_ts_ms` BIGINT,\n  `__ts_ms` BIGINT,\n  `__deleted` VARCHAR(2147483647),\n  `__keys` VARCHAR(2147483647) NOT NULL\n)\nDISTRIBUTED BY HASH(`key`) INTO 1 BUCKETS\nWITH (\n  'changelog.mode' = 'append',\n  'connector' = 'confluent',\n  'kafka.cleanup-policy' = 'delete',\n  'kafka.max-message-size' = '4194328 bytes',\n  'kafka.retention.size' = '0 bytes',\n  'kafka.retention.time' = '0 ms',\n  'key.format' = 'raw',\n  'scan.bounded.mode' = 'unbounded',\n  'scan.startup.mode' = 'earliest-offset',\n  'value.avro-registry.schema-context' = '.cluster_link',\n  'value.format' = 'avro-registry'\n)\n"]}]}}

def _extract_column_definitions(sql_str: str) -> tuple[str,str]:
    result=""
    fields=""
    lines = sql_str.split("\n")
    for line in lines:
        if "CREATE TABLE" in line or " `key` VARBINARY" in line:
            continue
        if "DISTRIBUTED" in line:
            break
        else:
            if line.strip() != ")":
                result+=line + "\n"
                fields+=line.strip().split(" ")[0]+",\n"
    logging.debug(f"_extract_column_definitions {result} and {fields}")
    return result[:-1]+",", fields[:-2]

def get_column_definitions(table_name: str, config) -> tuple[str,str]:
    """
    Get column definitions by using the SQL show create table of the given table name.
    """
    src_topic_name = search_matching_topic(table_name, config["kafka"]["reject_topics_prefixes"])
    logging.info(f"Found topic: {src_topic_name}, in the list of topics ")
    if src_topic_name:
        try:
            response= post_flink_statement(config,"show-ct-1", f"show create table `{config["flink"]['catalog_name']}`.`{config["flink"]['database_name']}`.`{src_topic_name}`;", True)
            if response and response["results"]:
                delete_flink_statement(config,"show-ct-1")
                return _extract_column_definitions(response["results"]["data"][0]["row"][0])
            else:
                logging.info(f"Schema from table {src_topic_name} not found in {config["flink"]['catalog_name']}/{config["flink"]['database_name']}")
                return "",""
        except Exception as e:
            logging.info(e)
            return "",""
    else:
        return "",""
    
if __name__ == "__main__":
    config=get_config()
    args = parser.parse_args()
    if args.topic_list:
        with open( args.topic_list, "w") as f:
            for topic in get_topic_list(config)["data"]:
                f.write(topic["topic_name"]+"\n")
            

    #columns,fields = get_column_definitions("training_unit",config)


    #get_environment_list(config)
    #get_flink_statement_list(config)
    #schema=post_flink_statement(config,"show-ct-1","show create table `development_non-prod-TG`.`development-us-west-2-dedicated`.`clone.prod.mc.dbo.web_business_unit`;", True)
    #schema=mock_result()
    #sql_str=schema["results"]["data"][0]["row"][0]
    #columns,fields=extract_column_definitions(sql_str)
   # logging.info(columns)
    #logging.info(fields)
    # delete_flink_statement(config,"show-ct-1")
    #logging.info(search_matching_topic("recordexecution_task"))
    #logging.info(search_matching_topic("tdc_doc_document_type"))
