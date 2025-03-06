import logging
import subprocess
import requests
import json
import os
from base64 import b64encode
from typing import List, Dict

from shift_left.core.utils.app_config import get_config

TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')

class ConfluentFlinkClient:
    """
    Client to connect to Confluent Cloud and execute Flink SQL queries using the REST API.
    """
    def __init__(self, config: dict):
        self.config = config
        self.api_key = config["confluent_cloud"]["api_key"]
        self.api_secret = config["confluent_cloud"]["api_secret"]
        self.cloud_api_endpoint = config["confluent_cloud"]["base_api"]
        self.auth_header = self._generate_auth_header()
        
    def _generate_auth_header(self):
        """Generate the Basic Auth header using API key and secret"""
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"
    
    def make_request(self, method, url, data=None):
        """Make HTTP request to Confluent Cloud API"""
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        print(headers)
        print(url)
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data
        )
        print(response)
        if response.status_code in [200,202]:
            logging.debug(response.request.body)
            logging.debug("Response headers:", response.headers)
        else:
            logging.error("Request failed:", response.status_code)
            logging.debug(response.request.body)
        response.raise_for_status()
        return response.json()
    
    def get_statement_status(self, endpoint, statement_id):
        """Get the status of a Flink SQL statement"""
        endpoint = f"{endpoint}/{statement_id}"
        return self.make_request("GET", endpoint)["status"]

    def get_environment_list(self):
        """Get the list of environments"""
        url = f"https://{self.cloud_api_endpoint}/environments"
        try:
            result = self.make_request("GET", url)
            logging.info("Statement execution result: %s", json.dumps(result, indent=2))
            return result
        except requests.exceptions.RequestException as e:
            logging.info(f"Error executing rest call: {e}")
            return None
    
    def get_compute_pool_list(self):
        """Get the list of compute pools"""
        env_id=self.config["confluent_cloud"]["environment_id"]
        url=f"https://confluent.cloud/api/fcpm/v2/compute-pools?environment={env_id}"
        return self.make_request("GET", url)

    def list_topics(self):
        """List the topics in the environment"""
        region=self.config["confluent_cloud"]["region"]
        cloud_provider=self.config["confluent_cloud"]["provider"]
        pkafka_cluster=self.config["kafka"]["pkafka_cluster"]
        cluster_id=self.config["kafka"]["cluster_id"]
        url=f"https://{pkafka_cluster}.{region}.{cloud_provider}.confluent.cloud/kafka/v3/clusters/{cluster_id}/topics"
        
        print(url)
        try:
            result= self.make_request("GET", url)
            return result
        except requests.exceptions.RequestException as e:
            logging.error(e)

    def _build_flink_client(self):
        region=self.config["confluent_cloud"]["region"]
        cloud_provider=self.config["confluent_cloud"]["provider"]
        organization_id=self.config["confluent_cloud"]["organization_id"]
        env_id=self.config["confluent_cloud"]["environment_id"]
        self.api_key = self.config["flink"]["api_key"]
        self.api_secret = self.config["flink"]["api_secret"]
        self.auth_header = self._generate_auth_header()
        if self.config["flink"]["url_scope"].lower() == "private":
            url=f"https://flink.{region}.{cloud_provider}.private.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
        else:
            url=f"https://flink.{region}.{cloud_provider}.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
        return url

    def get_flink_statement_list(self): 
        url = self._build_flink_client()
        try:
            result = self.make_request("GET", url+"/statements")
            logging.info("Statement execution result:", json.dumps(result, indent=2))
            return result
        except requests.exceptions.RequestException as e:
            logging.info(f"Error executing rest call: {e}")

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