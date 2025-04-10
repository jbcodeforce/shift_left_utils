"""
Copyright 2024-2025 Confluent, Inc.
"""
import time
import requests
from urllib.parse import urlparse
import json
from base64 import b64encode
from typing import List
from shift_left.core.utils.app_config import logger
from shift_left.core.flink_statement_model import *


class ConfluentCloudClient:
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
        response = None
        logger.debug(f">>> Make request {method} to {url}")
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data
            )
            response.raise_for_status()
            try:
                    json_response = response.json()
                    logger.debug(f">>>> Successful {method} request to {url}. \n\tResponse: {json_response}")
                    return json_response
            except ValueError:
                    logger.debug(f">>>> Successful {method} request to {url}. \n\tResponse text: {response.text}")
                    return response.text
        except requests.exceptions.RequestException as e:
            if response is not None:
                if response.status_code == 404:
                    logger.warning(f"Request to {url} has reported error: {e}, it may be fine when looking at non present element.")
                    result = json.loads(response.text)
                    logger.debug(f">>>> Response text: {result['errors'][0]['detail']}")
                    return result
                else:
                    logger.error(f">>>> Response status code: {response.status_code}, Response text: {response.text}")
                    return result
            else:
                raise 
    
    def get_environment_list(self):
        """Get the list of environments"""
        url = f"https://{self.cloud_api_endpoint}/environments?page_size=50"
        try:
            result = self.make_request("GET", url)
            logger.info("Statement execution result: %s", json.dumps(result, indent=2))
            return result
        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing rest call: {e}")
            return None
    
    def get_compute_pool_list(self, env_id: str) -> List[str]:
        """Get the list of compute pools"""
        if not env_id:
            env_id=self.config["confluent_cloud"]["environment_id"]
        region=self.config["confluent_cloud"]["region"]
        results=[]
        next_page_token = None
        page_size = self.config["confluent_cloud"].get("page_size", 100)
        url=f"https://api.confluent.cloud/fcpm/v2/compute-pools?spec.region={region}&environment={env_id}&page_size={page_size}"
        
        while True:
            if next_page_token:
                #parsed_url = urlparse(next_page_token)
                #path = "https://api.confluent.cloud" + parsed_url.path + '?' + parsed_url.query if parsed_url.query else parsed_url.path
                resp=self.make_request("GET", next_page_token + f"?page_size={page_size}")
            else:
                resp=self.make_request("GET", url)
            logger.debug(f"compute pool response= {resp}")
            if resp and "data" in resp and resp.get('data'):
                for info in resp.get('data'):
                    results.append({'id' : info["id"], 'name':  info.get('spec').get('display_name')} )
            if resp and "metadata" in resp and "next" in resp.get('metadata'):
                next_page_token = resp.get('metadata').get('next')
                if not next_page_token:
                    break
            else:
                break
        return results
        

    def get_compute_pool_info(self, compute_pool_id: str):
        """Get the info of a compute pool"""
        self.api_key = self.config["flink"]["api_key"]
        self.api_secret = self.config["flink"]["api_secret"]
        self.auth_header = self._generate_auth_header()
        env_id=self.config["confluent_cloud"]["environment_id"]
        url=f"https://api.confluent.cloud/fcpm/v2/compute-pools/{compute_pool_id}?environment={env_id}"
        return self.make_request("GET", url)

    def create_compute_pool(self, spec: dict):
        self.api_key = self.config["confluent_cloud"]["api_key"]
        self.api_secret = self.config["confluent_cloud"]["api_secret"]
        self.auth_header = self._generate_auth_header()
        data={'spec': spec}
        url=f"https://api.confluent.cloud/fcpm/v2/compute-pools"
        return self.make_request("POST", url, data)
        

    def list_topics(self):
        """List the topics in the environment 
        example of url https://pkc-00000.region.provider.confluent.cloud/kafka/v3/clusters/cluster-1/topics \
 
        """
        region=self.config["confluent_cloud"]["region"]
        cloud_provider=self.config["confluent_cloud"]["provider"]
        pkafka_cluster=self.config["kafka"]["pkafka_cluster"]
        cluster_id=self.config["kafka"]["cluster_id"]
        self.api_key = self.config["kafka"]["api_key"]
        self.api_secret = self.config["kafka"]["api_secret"]
        self.auth_header = self._generate_auth_header()
        url=f"https://{pkafka_cluster}.{region}.{cloud_provider}.confluent.cloud/kafka/v3/clusters/{cluster_id}/topics"
        logger.info(f"List topic from {url}")
        try:
            result= self.make_request("GET", url)
            logger.info(result)
            return result
        except requests.exceptions.RequestException as e:
            logger.error(e)
            return None

    def _build_flink_url_and_auth_header(self):
        region=self.config["confluent_cloud"]["region"]
        cloud_provider=self.config["confluent_cloud"]["provider"]
        organization_id=self.config["confluent_cloud"]["organization_id"]
        env_id=self.config["confluent_cloud"]["environment_id"]
        self.api_key = self.config["flink"]["api_key"]
        self.api_secret = self.config["flink"]["api_secret"]
        self.auth_header = self._generate_auth_header()
        if self.config["confluent_cloud"]["url_scope"].lower() == "private":
            url=f"https://flink.{region}.{cloud_provider}.private.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
        else:
            url=f"https://flink.{region}.{cloud_provider}.confluent.cloud/sql/v1/organizations/{organization_id}/environments/{env_id}"
        return url

    def get_flink_statement_list(self): 
        page_size = self.config["confluent_cloud"].get("page_size", 100)
        url = self._build_flink_url_and_auth_header()
        results={}
        next_page_token = None
        url+=f"/statements?page_size={page_size}"
        while True:
            if next_page_token:
                resp=self.make_request("GET", next_page_token)
            else:
                resp=self.make_request("GET", url)
            #logger.debug("Statement execution result:", resp)
            if resp and 'data' in resp:
                for info in resp.get('data'):
                    if 'properties' in info.get('spec') and info.get('spec').get('properties'):
                        catalog = info.get('spec',{}).get('properties',{}).get('sql.current-catalog','UNKNOWN')
                        database = info.get('spec',{}).get('properties',{}).get('sql.current-database','UNKNOWN')
                    else:
                        catalog = 'UNKNOWN'
                        database = 'UNKNOWN'
                    statement_info = StatementInfo(name=info['name'],
                                                   status_phase= info.get('status').get('phase', 'UNKNOWN'),
                                                   status_detail= info.get('status').get('detail', 'UNKNOWN'),
                                                   sql_content= info.get('spec').get('statement', 'UNKNOWN'),
                                                   compute_pool_id= info.get('spec').get('compute_pool_id'),
                                                   principal= info.get('spec').get('principal', 'UNKNOWN'),
                                                   sql_catalog=catalog,
                                                   sql_database=database)
                    results[info['name']] = statement_info
            if "metadata" in resp and "next" in resp["metadata"]:
                next_page_token = resp["metadata"]["next"]
                if not next_page_token:
                    break
            else:
                break
        return results

    def _wait_response(self, url: str, statement_name: str, start_time ) -> StatementResult:
        """
        wait to get a non pending state
        """
        timer= self.config['flink'].get("poll_timer", 10)
        logger.info(f"Start polling response for {statement_name}")
        counter = 0
        try:
            while True:
                statement = self.get_statement_info(statement_name)
                if statement.status.phase in ["PENDING"]:
                    logger.debug(f"{statement_name} still pending.... sleep and poll again")
                    time.sleep(timer)
                    counter+=1
                    if counter == 6:
                        timer = 30
                else:
                    execution_time = time.perf_counter() - start_time
                    statement.loop_counter= counter
                    statement.execution_time= execution_time
                    logger.info(f"Done waiting with response= {statement.model_dump_json(indent=3)}") 
                    return statement    
        except Exception as e:
            logger.error(f">>>> wait_response() error waiting for response {e}")
            execution_time = time.perf_counter() - start_time
            statement_result = Statement.model_validate({"loop_counter": counter, "execution_time": execution_time, "result" : None})
            return statement_result


    def post_flink_statement(self, compute_pool_id: str,  
                             statement_name: str, 
                             sql_statement: str, 
                             properties: str, 
                             stopped: bool = False) -> Statement: 
        """
        POST to the statements API to execute a SQL statement.
        """
        url = self._build_flink_url_and_auth_header()
        if not compute_pool_id:
            compute_pool_id = self.config['flink']['compute_pool_id']
        statement_data = {
                "name": statement_name,
                "organization_id": self.config["confluent_cloud"]["organization_id"],
                "environment_id": self.config["confluent_cloud"]["environment_id"],
                "spec": {
                    "statement": sql_statement,
                    "properties": properties,
                    "compute_pool_id":  compute_pool_id,
                    "stopped": stopped
                }
            }
        try:
            logger.debug(f"> Send POST request to Flink statement api with {statement_data}")
            start_time = time.perf_counter()
            response = self.make_request("POST", url + "/statements", statement_data)
            logger.debug(f"> POST response= {response}") 
            if response["status"]["phase"] == "PENDING":
                return self._wait_response(url, statement_name, start_time)
            execution_time = time.perf_counter() - start_time
            return  StatementResult.model_validate({"loop_counter": 0, "execution_time": execution_time, "results" : [response]})
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing rest call: {e}")

    def get_statement_info(self, statement_name: str)-> Statement:
        url = self._build_flink_url_and_auth_header()
        try:
            resp=self.make_request("GET",f"{url}/statements/{statement_name}")
            if resp:
                try:
                    s: Statement = Statement.model_validate(resp)
                    return s 
                except Exception as e:
                    return Statement(name=statement_name,status = Status(detail=resp['errors'][0]['detail'], phase="FAILED"))
        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing GET statement call for {statement_name}: {e}")
            return Statement(name=statement_name)
        
    def get_statement_results(self, statement_name: str)-> Statement:
        url = self._build_flink_url_and_auth_header()
        try:
            resp=self.make_request("GET",f"{url}/statements/{statement_name}/results")
            logger.debug(resp)

            if resp["metadata"]["next"]:
                resp=self.make_request("GET", resp["metadata"]["next"])
                logger.debug(f"After next called: {resp}")
                return StatementResult(**resp)
            elif resp['results'] and resp['results']['data']:
                return StatementResult(**resp)

        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing GET statement call for {statement_name}: {e}")
            return None

    def delete_flink_statement(self, statement_name: str):
        url = self._build_flink_url_and_auth_header()
        timer= self.config['flink'].get("poll_timer", 10)
        try:
            resp = self.make_request("DELETE",f"{url}/statements/{statement_name}")
            if resp:
                return "deleted"
            counter=0
            while True:
                statement = self.get_statement_info(statement_name)
                if statement and statement.status and statement.status.phase in ("FAILED", "FAILING", "DELETED"):
                    break
                else:
                    logger.debug(statement)
                    counter+=1
                    if counter == 6:
                        timer = 30
                    if counter == 10:
                        return "failed to delete"
                time.sleep(timer)
            return "deleted"
        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing delete statement call for {statement_name}: {e}")
            return "unknown - mostly not removed"
        
    def update_flink_statement(self, statement_name: str,  statement: Statement, stopped: bool):
        url = self._build_flink_url_and_auth_header()
        try:
            statement.spec.stopped = stopped
            statement_data = {
                "name": statement_name,
                "organization_id": self.config["confluent_cloud"]["organization_id"],
                "environment_id": self.config["confluent_cloud"]["environment_id"],
                "spec": statement.spec.model_dump()
            }
            logger.info(f" update_flink_statement payload: {statement_data}")
            start_time = time.perf_counter()
            statement=self.make_request("PUT",f"{url}/statements/{statement_name}", statement_data)
            logger.info(f" update_flink_statement: {statement}")
            rep = self._wait_response(url, statement_name, start_time)
            logger.info(f" update_flink_statement: {rep}")
            return rep
        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing rest call: {e}")
     

