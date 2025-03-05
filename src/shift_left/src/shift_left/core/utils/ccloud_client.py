import logging
import subprocess
import requests
from base64 import b64encode
from typing import List, Dict

from shift_left.core.utils.app_config import get_config

class ConfluentFlinkClient:
    """
    Client to connect to Confluent Cloud and execute Flink SQL queries using the REST API.
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

    def get_environment_list(self):
        """Get the list of environments"""
        return self.make_request("GET", "/environments")
    
    def get_compute_pool_list(self):
        """Get the list of compute pools"""
        return self.make_request("GET", "/compute-pools")


def search_matching_topic(table_name):
    return None