"""
Set of function to get table information, mostly for source topics.

* Function to get the matching topic name given a table name.
* Get the SQL schema given a topic name, using the `show create table ` and 
the Confluent Cloud Flink REST API.
"""

import os, time
import requests, json
from base64 import b64encode
from kafka.app_config import get_config

TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')
BEARER_TOKEN=os.getenv("API_SECRET")

class ConfluentFlinkClient:
    """
    Confluent Cloud client to connect to CC and execute queries using REST API.
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
            print(response.request.body)
            print("Response headers:", response.headers)
            print("Response content:", response.content)
        else:
            print("Request failed:", response.status_code)
        response.raise_for_status()
        return response.json()
    
    def get_statement_status(self, endpoint, statement_id):
        """Get the status of a Flink SQL statement"""
        endpoint = f"{endpoint}/{statement_id}"
        return self.make_request("GET", endpoint)["status"]
    


def find_sub_string(table_name, topic_name) -> bool:
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

def search_matching_topic(table_name: str) -> str:
    """
    Given the table name search in the list of topics the potential matching topic.
    """
    with open(TOPIC_LIST_FILE,"r") as f:
        for line in f:
            if table_name == line:
                return line
            elif table_name in line:
                return line
            elif find_sub_string(table_name,line):
                return line
            else:
                print(f"{table_name} not found in topic list")
    return table_name


def get_environment_list(config):
    """
    Get the list of environments. use the CC resource api_key
    """
    url=f"https://{config["confluent_cloud"]["base_api"]}"
    client = ConfluentFlinkClient(config["confluent_cloud"]["api_key"], config["confluent_cloud"]["api_secret"], url)
    try:
        result = client.make_request("GET","/environments")
        print("Statement execution result:", json.dumps(result, indent=2))
        return result
    except requests.exceptions.RequestException as e:
        print(f"Error executing rest call: {e}")

def _build_flink_client(config):
    region=config["confluent_cloud"]["region"]
    cloud_provider=config["confluent_cloud"]["provider"]
    organization_id=config["confluent_cloud"]["organization_id"]
    env_id=config["confluent_cloud"]["environment_id"]
    url=f"https://flink.{region}.{cloud_provider}.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
    return ConfluentFlinkClient(config["flink"]["api_key"], config["flink"]["api_secret"], url), url
   

def get_flink_statement_list(config): 
    client, _ = _build_flink_client(config)
    try:
        result = client.make_request("GET","/statements")
        print("Statement execution result:", json.dumps(result, indent=2))
        return result
    except requests.exceptions.RequestException as e:
        print(f"Error executing rest call: {e}")

def get_compute_pool_list(config): 
    env_id=config["confluent_cloud"]["environment_id"]
    url=f"https://confluent.cloud/api/fcpm/v2/compute-pools?environment={env_id}"
    client=ConfluentFlinkClient(config["confluent_cloud"]["api_key"], config["confluent_cloud"]["api_secret"], url)
    try:
        result = client.make_request("GET","")
        print("Statement execution result:", json.dumps(result, indent=2))
        return result
    except requests.exceptions.RequestException as e:
        print(f"Error executing rest call: {e}")

def post_flink_statement(config, statement_name: str, sql_statement: str, stopped: False): 
    client, url = _build_flink_client(config)
    statement_data = {
            "name": statement_name,
            "organization_id": config["confluent_cloud"]["organization_id"],
            "environment_id": config["confluent_cloud"]["environment_id"],
            "spec": {
                "statement": sql_statement,
                "compute_pool_id":  config["flink"]["compute_pool_id"],
                "stopped": stopped
            }
        }
    try:
        response = client.make_request("POST","/statements", statement_data)
        print(response)
        statement_id = response["metadata"]["uid"]
        while True:
            status = client.get_statement_status("/statements", statement_name)
            if status["phase"] in ["COMPLETED"]:
                results=client.make_request("GET",f"/statements/{statement_name}/results")
                return results.data[0]["row"]
            time.sleep(5)
    except requests.exceptions.RequestException as e:
        print(f"Error executing rest call: {e}")

def delete_flink_statement(config, statement_name):
    client, url = _build_flink_client(config)
    try:
        status=client.make_request("DELETE",f"/statements/{statement_name}")
        print(status)
    except requests.exceptions.RequestException as e:
        print(f"Error executing rest call: {e}")

if __name__ == "__main__":
    config=get_config()
    #get_environment_list(config)
    #get_compute_pool_list(config)
    #get_flink_statement_list(config)
    schema=post_flink_statement(config,"show-ct-1","show create table `j9r-env`.`j9r-kafka`.`order_enriched`;", True)
    print(schema)
    delete_flink_statement(config,"show-ct-1")
    #print(search_matching_topic("production_record_status"))
    #print(search_matching_topic("tdc_doc_document_type"))
