"""
Copyright 2024-2025 Confluent, Inc.
"""
from importlib.metadata import version, PackageNotFoundError
import time
import os
import requests
import json
from base64 import b64encode
from typing import Tuple

from shift_left.core.utils.app_config import logger, BASE_CC_API
from shift_left.core.models.flink_statement_model import *
from shift_left.core.models.flink_compute_pool_model import *

COMPUTE_POOL_URL = "https://api.confluent.cloud/fcpm/v2/compute-pools"

class VersionInfo:
    """
    This class is needed for CC control plane to track the user agent.
    """
    @staticmethod
    def get_version():
        try:
            return version("shift-left")
        except PackageNotFoundError:
            logger.warning("Package 'shift-left' not found, using 'unknown' version")
            return "unknown"

class ConfluentCloudClient:
    """
    Client to connect to Confluent Cloud and execute Flink SQL queries using the REST API.
    """
    def __init__(self, config: dict):
        self.config = config
        cluster_info = self._extract_cluster_info_from_bootstrap(self.config.get("kafka").get("bootstrap.servers"))
        self.cluster_id=cluster_info["cluster_id"]
        self.base_url=cluster_info["base_url"]

    def _get_ccloud_auth(self):
        api_key = os.getenv("SL_CONFLUENT_CLOUD_API_KEY") or self.config["confluent_cloud"]["api_key"]
        api_secret = os.getenv("SL_CONFLUENT_CLOUD_API_SECRET") or self.config["confluent_cloud"]["api_secret"]
        self.cloud_api_endpoint = BASE_CC_API
        return  self._generate_auth_header(api_key, api_secret)

    def _get_kafka_auth(self):
        api_key = os.getenv("SL_KAFKA_API_KEY") or self.config["kafka"]["api_key"]
        api_secret = os.getenv("SL_KAFKA_API_SECRET") or self.config["kafka"]["api_secret"]
        return self._generate_auth_header(api_key, api_secret)

    def _get_flink_auth(self):
        api_key = os.getenv("SL_FLINK_API_KEY") or self.config["flink"]["api_key"]
        api_secret = os.getenv("SL_FLINK_API_SECRET") or self.config["flink"]["api_secret"]
        return self._generate_auth_header(api_key, api_secret)

    def _generate_auth_header(self, api_key, api_secret):
        """Generate the Basic Auth header using API key and secret"""
        credentials = f"{api_key}:{api_secret}"
        encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"
    
    def make_request(self, method, url, auth_header=None, data=None) -> str:
        """Make HTTP request to Confluent Cloud API"""
        version_str = VersionInfo.get_version()
        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "User-Agent": f"python-shift-left-utils/{version_str}"
        }
        response = None
        logger.info(f">>> Make request {method} to {url} with headers: {headers} and data: {data}")
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data
            )
            response.raise_for_status()
            try:
                if response.status_code == 202 and method == "DELETE":
                    return 'deleted'
                json_response = response.json()
                logger.debug(f">>>> Successful {method} request to {url}. \n\tResponse: {json_response}")
                return json_response
            except ValueError:
                logger.debug(f">>>> Mostly successful {method} request to {url}. \n\tResponse: {response}")
                return response.text
        except requests.exceptions.RequestException as e:
            if response is not None:
                if response.status_code == 404:
                    logger.debug(f"Request to {url} has reported error: {e}, it may be fine when looking at non present element.")
                    result = json.loads(response.text)
                    logger.info(f">>>> Exception with 404 {result} response text: {result['errors'][0]['detail']}")
                    return result
                elif response.status_code == 409:
                    logger.info(f">>>> Response to {method} at {url} has reported error: {e}, status code: {response.status_code}, Response text: {response.text}")
                    return json.loads(response.text)
                else:
                    logger.error(f">>>> Response to {method} at {url} has reported error: {e}, status code: {response.status_code}, Response text: {response.text}")
                    return response.text
            else:
                logger.error(f">>>> Response to {method} at {url} has reported error: {e}")
                raise e
    
    # ------------- CCloud related methods ----
    def get_environment_list(self):
        """Get the list of environments"""
        auth_header = self._get_ccloud_auth()
        url = f"https://{self.cloud_api_endpoint}/environments?page_size=50"
        try:
            result = self.make_request(method="GET", url=url, auth_header=auth_header)
            logger.info("Statement execution result: %s", json.dumps(result, indent=2))
            return result
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing rest call: {e}")
            return None
    
    def get_compute_pool_list(self, env_id: str, region: str) -> ComputePoolListResponse:
        """Get the list of compute pools"""
        if not env_id:
            env_id=self.config["confluent_cloud"]["environment_id"]
        if not region:
            region=self.config["confluent_cloud"]["region"]
        compute_pool_list = ComputePoolListResponse()
        next_page_token = None
        page_size = self.config["confluent_cloud"].get("page_size", 100)
        auth_header = self._get_ccloud_auth()
        #auth_header = self._get_flink_auth()
        url=f"{COMPUTE_POOL_URL}?spec.region={region}&environment={env_id}&page_size={page_size}"
        logger.info(f"compute pool url= {url}")
        previous_token=None
        while True:
            if next_page_token:
                resp=self.make_request(method="GET", url=next_page_token+"&page_size="+str(page_size), auth_header=auth_header)
            else:
                resp=self.make_request(method="GET", url=url, auth_header=auth_header)
            logger.debug(f"compute pool response= {resp}")
            try:
                resp_obj = ComputePoolListResponse.model_validate(resp)
                if resp_obj.data:
                    compute_pool_list.data.extend(resp_obj.data)
                if "metadata" in resp and "next" in resp["metadata"]:
                    next_page_token = resp.get('metadata').get('next')
                    if not next_page_token or next_page_token == previous_token:
                        break
                    previous_token = next_page_token
                else:
                    break
            except Exception as e:
                logger.error(f"Error parsing compute pool response: {e}")
                logger.error(f"Response: {resp}")
                break
        return compute_pool_list
        

    def get_compute_pool_info(self, compute_pool_id: str, env_id: str = None):
        """Get the info of a compute pool"""
        if not env_id:
            env_id=self.config["confluent_cloud"]["environment_id"]
        auth_header = self._get_ccloud_auth()
        url=f"{COMPUTE_POOL_URL}/{compute_pool_id}?environment={env_id}"
        return self.make_request(method="GET", url=url, auth_header=auth_header)

    def create_compute_pool(self, spec: dict):
        auth_header = self._get_ccloud_auth()
        data={'spec': spec}
        url=f"{COMPUTE_POOL_URL}"
        return self.make_request(method="POST", url=url, auth_header=auth_header, data=data)

    def delete_compute_pool(self, compute_pool_id: str, env_id: str = None):
        if not env_id:
            env_id=self.config["confluent_cloud"]["environment_id"]
        auth_header = self._get_ccloud_auth()
        url=f"{COMPUTE_POOL_URL}/{compute_pool_id}?environment={env_id}"
        return self.make_request(method="DELETE", url=url, auth_header=auth_header)

    def wait_response(self, url: str, statement_name: str, start_time ) -> StatementResult:
        """
        wait to get a non pending state
        TODO: Provide an option to wait for a specific status. e.g when stopping, wait for STOPPED status, when resuming, wait for RUNNING status.
        """
        timer= self.config['flink'].get("poll_timer", 10)
        logger.info(f"As status is PENDING, start polling response for {statement_name}")
        pending_counter = 0
        error_counter = 0
        statement = None
        while True:
            try:
                statement = self.get_flink_statement(statement_name)
            except Exception as e:
                if error_counter > 5:
                    logger.error(f">>>> wait_response() there is an error waiting for response {e}")
                    raise Exception(f"Done waiting with response because of error {e}")
                else:
                    logger.warning(f">>>> wait_response() current response {e}")
                    time.sleep(timer)
                    error_counter+=1
            if statement and statement.status and statement.status.phase in ["PENDING"]:
                logger.debug(f"{statement_name} still pending.... sleep and poll again")
                time.sleep(timer)
                pending_counter+=1
                if pending_counter % 3 == 0:
                    timer+= 10
                    print(f"Wait {statement_name} deployment, increase wait response timer to {timer} seconds")
                if pending_counter >= 23:
                    logger.error(f"Too long waiting with response= {statement.model_dump_json(indent=3)}") 
                    execution_time = time.perf_counter() - start_time
                    error_statement = Statement.model_validate({"name": statement_name, 
                                                                "spec": statement.spec,
                                                                "status": {"phase": "FAILED", "detail": "Done waiting with response"},
                                                                "loop_counter": pending_counter, 
                                                                "execution_time": execution_time, 
                                                                "result" : statement.result})
                    raise Exception(f"Too long waiting with response= {error_statement.model_dump_json(indent=3)}")   
            else:
                execution_time = time.perf_counter() - start_time
                statement.loop_counter= pending_counter
                statement.execution_time= execution_time
                logger.info(f"Done waiting, got {statement.status.phase} with response= {statement.model_dump_json(indent=3)}") 
                return statement    

                
    def _extract_cluster_info_from_bootstrap(self, bootstrap_servers):
            """
            Extract cluster_id and base_url from bootstrap.servers value.
            
            Args:
                bootstrap_servers (str): Bootstrap servers string like 'lkc-7...g3p-dm8me7.us-west-2.aws.glb.confluent.cloud:9092'
                                      or 'pkc-n9..pk.us-west-2.aws.confluent.cloud:9092'
            
            Returns:
                dict: Contains 'cluster_id', 'base_url'
            """
            if not bootstrap_servers:
                bootstrap_servers = self.config["kafka"]["bootstrap.servers"]
            
            # Remove port if present
            server_without_port = bootstrap_servers.split(':')[0]
            
            # Extract cluster_id and base_url
            if server_without_port.startswith('lkc-') or server_without_port.startswith('pkc-'):
                # Handle format like: lkc-7..p-..us-west-2.aws.glb.confluent.cloud
                # The key difference is lkc- has a third component after the cluster ID
                if server_without_port.startswith('lkc-') and server_without_port.count('-') >= 3:
                    parts = server_without_port.split('-', 2)  # Split into at most 3 parts
                    cluster_id = f"{parts[0]}-{parts[1]}"  # e.g., 'lkc-79kg3p'
                    base_url = parts[2]  # e.g., 'dm8me7.us-west-2.aws.glb.confluent.cloud'
                # Handle format like: pkc-n9..n.us-west-2.aws.confluent.cloud  
                else:
                    # Find the first dot to separate cluster from domain
                    dot_index = server_without_port.find('.')
                    if dot_index != -1:
                        cluster_part = server_without_port[:dot_index]  # e.g., 'pkc-n...pk'
                        base_url = server_without_port[dot_index+1:]  # e.g., 'us-west-2.aws.confluent.cloud'
                        cluster_id = cluster_part
                    else:
                        return {"cluster_id": None, "base_url": None}
                
                return {
                    "cluster_id": cluster_id,
                    "base_url": base_url,
                }
            
            return {"cluster_id": None, "base_url": None}

    # ---- Topic related methods ----
 
    def get_topic_message_count(self, topic_name: str) -> int:
        """
        Get the number of messages in a Kafka topic.
        
        Args:
            topic_name (str): The name of the topic to get message count for
            
        Returns:
            int: The total number of messages in the topic
        """
        url=self._build_confluent_cloud_kafka_url()
        url=f"{url}/{topic_name}/partitions"
        auth_header = self._get_kafka_auth()
        response = self.make_request(method="GET", url=url, auth_header=auth_header)
        partitions = response["data"]
        print(f"partitions: {partitions}")
        total_messages = 0
        for partition in partitions:
            partition_id = partition["partition_id"]
            url = f"{url}/{partition_id}"
            response = self.make_request(method="GET", url=url, auth_header=auth_header)
            logger.debug(response)
            
        return total_messages

    def list_topics(self) -> dict | None:
        """List the topics in the environment 
        example of url https://lkc-23456-doqmp5.us-west-2.aws.confluent.cloud/kafka/v3/clusters/lkc-23456/topics \
 
        """
        url=self._build_confluent_cloud_kafka_url()
        logger.info(f"List topic from {url}")
        auth_header = self._get_kafka_auth()
        try:
            result= self.make_request(method="GET", url=url, auth_header=auth_header)
            logger.debug(result)
            return result
        except requests.exceptions.RequestException as e:
            logger.error(e)
            return None
        

    # ---- Flink related methods ----
    def build_flink_url_and_auth_header(self) -> Tuple[str, str]:
        organization_id=self.config["confluent_cloud"]["organization_id"]
        env_id=self.config["confluent_cloud"]["environment_id"]
        api_key = os.getenv("SL_FLINK_API_KEY") or self.config["flink"]["api_key"]
        api_secret = os.getenv("SL_FLINK_API_SECRET") or self.config["flink"]["api_secret"]
        auth_header = self._generate_auth_header(api_key, api_secret)
        if self.cluster_id and self.cluster_id.startswith("lkc-"):
            url=f"https://flink-{self.base_url}/sql/v1/organizations/{organization_id}/environments/{env_id}"
        else:
            url=f"https://flink.{self.base_url}/sql/v1/organizations/{organization_id}/environments/{env_id}"
        return url, auth_header
    
    def get_flink_statement(self, statement_name: str)-> Statement | None:
        url, auth_header = self.build_flink_url_and_auth_header()
        try:
            resp=self.make_request("GET",f"{url}/statements/{statement_name}", auth_header=auth_header)
            if resp and not resp.get("errors"):
                try:
                    s: Statement = Statement.model_validate(resp)
                    return s 
                except Exception as e:
                    logger.error(f"Error parsing statement response: {resp} with error {e}")
                    return None
            elif resp and resp.get("errors") and resp.get("errors")[0].get("status") == "404":
                logger.warning(f"Statement {statement_name} not found") 
                return None
            else:
                logger.error(f"Error getting statement {statement_name}: {resp}")
                return None
        except Exception as e:
            logger.error(f"Error executing GET statement call for {statement_name}: {e}")
            raise e

    def delete_flink_statement(self, statement_name: str) -> str:
        url, auth_header = self.build_flink_url_and_auth_header()
        timer= self.config['flink'].get("poll_timer", 10)
        try:
            resp = self.make_request("DELETE",f"{url}/statements/{statement_name}", auth_header=auth_header)
            if resp and isinstance(resp, dict) and resp.get("errors") and "does not exist" in resp.get("errors")[0].get("detail"):
                logger.info(f"Statement {statement_name} not found")
                return "deleted"
            if resp == '' or resp == 'deleted':
                return "deleted"
            counter=0
            while True:
                statement = self.get_flink_statement(statement_name)
                if statement and statement.status and statement.status.phase in ("FAILED", "FAILING", "DELETED"):
                    logger.info(f"Statement {statement_name} is {statement.status.phase}, break")
                    break
                else:
                    logger.info(f"Statement {statement_name} is {statement.status.phase}, continue")
                    counter+=1
                    if counter == 6:
                        timer = 30
                    if counter == 10:
                        logger.error(f"Statement {statement_name} is still running after {counter} times")
                        return "failed to delete"
                time.sleep(timer)
            return "deleted"
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing delete statement call for {statement_name}: {e}")
            return "unknown - mostly not removed"
        
    def update_flink_statement(self, statement_name: str,  statement: Statement, stopped: bool):
        url, auth_header = self.build_flink_url_and_auth_header()
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
            statement=self.make_request(method="PUT", url=f"{url}/statements/{statement_name}", data=statement_data, auth_header=auth_header )
            logger.info(f" update_flink_statement: {statement}")
            rep = self.wait_response(url, statement_name, start_time)
            logger.info(f" update_flink_statement: {rep}")
            return rep
        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing rest call: {e}")

    def patch_flink_statement(self, statement_name: str,  stopped: bool):
        url, auth_header = self.build_flink_url_and_auth_header()
        try:
            statement_data = [ {
                "path": "/spec/stopped",
                "op": "replace",
                "value": stopped
            } ]

            logger.info(f" patch_flink_statement payload: {statement_data}")
            start_time = time.perf_counter()
            statement=self.make_request(method="PATCH", url=f"{url}/statements/{statement_name}", data=statement_data, auth_header=auth_header )
            logger.info(f" patch_flink_statement: {statement_name}")
            rep = self.wait_response(url, statement_name, start_time)
            logger.info(f" patch_flink_statement: {rep}")
            return rep
        except requests.exceptions.RequestException as e:
            logger.info(f"Error executing rest call: {e}")


    # ---- Metrics related methods ----
    def get_metrics(self, view: str, qtype: str, query: str) -> dict:
        url=f"https://api.telemetry.confluent.cloud/v2/metrics/{view}/{qtype}"
        version_str = VersionInfo.get_version()
        auth_header = self._get_ccloud_auth()
        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "User-Agent": f"python-shift-left-utils/{version_str}"
        }
        response = None
        try:
            response = requests.request(
                method="POST",
                url=url,
                headers=headers,
                data=query
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing rest call: {e}")
            logger.error(f"Response: {response.text}")
            return None    



    def _build_confluent_cloud_kafka_url(self) -> str:
        cluster_info = self._extract_cluster_info_from_bootstrap(self.config.get("kafka",{}).get("bootstrap.servers",""))
        cluster_url_id  = cluster_info.get("cluster_id")
        base_url=cluster_info.get("base_url")
        config_cluster_id=self.config.get("kafka").get("cluster_id","")
        if config_cluster_id and config_cluster_id != cluster_url_id:
            cluster_id = config_cluster_id
        else:
            cluster_id = cluster_url_id
        
        # For lkc- format with multiple components, use dash; for pkc- format, use dot
        # lkc-7...3p-dm8me7.us-west-2.aws  -> cluster_id is lkc-7...3p and base_url is dm8me7.us-west-2.aws
        # pkc-n9..k.us-west-2.aws  -> cluster_id is pkc-n9..k and base_url is us-west-2.aws
        if cluster_url_id and cluster_url_id.startswith("lkc-"):
            url=f"https://{cluster_url_id}-{base_url}/kafka/v3/clusters/{cluster_id}/topics"
        else:
            url=f"https://{cluster_url_id}.{base_url}/kafka/v3/clusters/{cluster_id}/topics"
        return url
    
