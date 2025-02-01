"""
Get the SQL schema given a topic name, using the `show create table ` and the REST API
"""

import os, time
import requests, json
from base64 import b64encode
from kafka.app_config import get_config

TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')
BEARER_TOKEN=os.getenv("API_SECRET")

class ConfluentFlinkClient:
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
    
    def _make_request(self, method, endpoint, data=None):
        """Make HTTP request to Confluent Cloud API"""
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        
        url = f"{self.cloud_api_endpoint}{endpoint}"
        print(headers)
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            json=data
        )
        
        response.raise_for_status()
        return response.json()
    
    def execute_flink_sql(self, organization_id, environment_id, statement_name, sql_statement):
        """Execute a Flink SQL statement"""
        
        # Create the SQL statement
        statement_data = {
            "name": statement_name,
            "statement": sql_statement,
            "properties": {
                "execution.runtime-mode": "STREAMING"
            }
        }
        
        # Submit the statement
        endpoint = f"/sql/v1/organizations/{organization_id}/environments/{environment_id}/statements"
        response = self._make_request("POST", endpoint, statement_data)
        statement_id = response["id"]
        
        # Poll for statement completion
        while True:
            status = self.get_statement_status(endpoint, statement_id)
            print(status)
            if status["phase"] in ["COMPLETED", "FAILED"]:
                return status
            time.sleep(5)
    
    def get_statement_status(self, endpoint, statement_id):
        """Get the status of a Flink SQL statement"""
        endpoint = f"{endpoint}/{statement_id}"
        return self._make_request("GET", endpoint)
    


def find_sub_string(table_name, topic_name) -> bool:
    words=table_name.split("_")
    subparts=topic_name.split(".")
    all_present = True
    for w in words:
        if w not in subparts:
            all_present=False
            break
    return all_present

def search_matching_topic(table_name: str) -> str:
    with open(TOPIC_LIST_FILE,"r") as f:
        for line in f:
            if table_name == line:
                return line
            elif table_name in line:
                return line
            elif find_sub_string(table_name,line):
                return line
    return table_name


def post_show_create_table(config):
    url=f"https://flink.{config["confluent_cloud"]["region"]}.{config["confluent_cloud"]["provider"]}.confluent.prod"
    print(url)
    client = ConfluentFlinkClient(config["flink"]["api_key"], config["flink"]["api_secret"], url)
    sql_statement = """
    SHOW CREATE TABLE `clone.prod.mc.dbo.training_competency`
    """
    
    try:
        # Execute the SQL statement
        result = client.execute_flink_sql(
            organization_id=config["confluent_cloud"]["organization_id"],
            environment_id=config["confluent_cloud"]["environment_id"],
            statement_name="example_query",
            sql_statement=sql_statement
        )
        
        print("Statement execution result:", json.dumps(result, indent=2))
        
    except requests.exceptions.RequestException as e:
        print(f"Error executing Flink SQL: {e}")

def get_environment_list(config):
    url=f"https://{config["confluent_cloud"]["base_api"]}"
    client = ConfluentFlinkClient(config["confluent_cloud"]["api_key"], config["confluent_cloud"]["api_secret"], url)
    try:
        # Execute the SQL statement
        result = client._make_request("GET","/environments")
        
        print("Statement execution result:", json.dumps(result, indent=2))
    except requests.exceptions.RequestException as e:
        print(f"Error executing rest call: {e}")


if __name__ == "__main__":
    config=get_config()
    get_environment_list(config)
    #print(search_matching_topic("production_record_status"))
    #print(search_matching_topic("tdc_doc_document_type"))
