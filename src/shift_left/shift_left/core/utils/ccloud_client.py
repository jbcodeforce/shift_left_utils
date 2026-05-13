"""
Copyright 2024-2025 Confluent, Inc.
"""
from importlib.metadata import version, PackageNotFoundError
import os
import requests
import json
from base64 import b64encode
from typing import Tuple

from pydantic import ValidationError

from shift_left.core.utils.app_config import logger, BASE_CC_API
from shift_left.core.models.flink_compute_pool_model import ComputePoolListResponse, ComputePoolResponse
from shift_left.core.models.cc_environment_model import EnvironmentListResponse

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
    REST client for Confluent Cloud APIs (Kafka, Flink control-plane URLs, telemetry).
    Flink statement execution uses confluent-sql (see shift_left.core.utils.flink_sql_adapter).
    """
    def __init__(self, config: dict):
        self.config = config
        cluster_info = self._extract_cluster_info_from_bootstrap(self.config.get("kafka").get("bootstrap.servers"))
        self.cluster_id=cluster_info["cluster_id"]
        self.base_url=cluster_info["base_url"]

    def get_ccloud_auth(self) ->str:
        api_key = os.getenv("SL_CONFLUENT_CLOUD_API_KEY") or self.config["confluent_cloud"]["api_key"]
        api_secret = os.getenv("SL_CONFLUENT_CLOUD_API_SECRET") or self.config["confluent_cloud"]["api_secret"]
        self.cloud_api_endpoint = BASE_CC_API
        return  self._generate_auth_header(api_key, api_secret)

    def _get_kafka_auth(self) -> str:
        api_key = os.getenv("SL_KAFKA_API_KEY") or self.config["kafka"]["api_key"]
        api_secret = os.getenv("SL_KAFKA_API_SECRET") or self.config["kafka"]["api_secret"]
        return self._generate_auth_header(api_key, api_secret)

    def _get_flink_auth(self) -> str:
        api_key = os.getenv("SL_FLINK_API_KEY") or self.config["flink"]["api_key"]
        api_secret = os.getenv("SL_FLINK_API_SECRET") or self.config["flink"]["api_secret"]
        return self._generate_auth_header(api_key, api_secret)

    def _generate_auth_header(self, api_key, api_secret) -> str:
        """Generate the Basic Auth header using API key and secret"""
        credentials = f"{api_key}:{api_secret}"
        encoded_credentials = b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded_credentials}"

    def make_request(self, method, url, auth_header=None, data=None):
        """Make HTTP request to Confluent Cloud API.
        When data is provided, the body is serialized explicitly as JSON (utf-8)
        so that statement strings containing quotes are never broken by
        request-layer handling.
        """
        version_str = VersionInfo.get_version()
        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json",
            "User-Agent": f"python-shift-left-utils/{version_str}"
        }
        response = None
        logger.info(f">>> Make request {method} to {url} with headers: {headers} and data: {data}")
        try:
            if data is not None:
                body = json.dumps(data, ensure_ascii=False)
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers,
                    data=body.encode("utf-8")
                )
            else:
                response = requests.request(
                    method=method,
                    url=url,
                    headers=headers
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
                if response.status_code in [401, 404, 409, 403]:
                    result = json.loads(response.text)
                    logger.info(f">>>> Exception with {response.status_code} response: {result}")
                    return result
                else:
                    logger.error(f">>>> Response to {method} at {url} has reported error: {e}, status code: {response.status_code}, Response text: {response.text}")
                    return response.text
            else:
                logger.error(f">>>> Response to {method} at {url} has reported error: {e}")
                raise e

    # ------------- CCloud related methods ----
    def get_environment_list(self) -> EnvironmentListResponse | None:
        """Get the list of environments"""
        auth_header = self.get_ccloud_auth()
        url = f"https://{self.cloud_api_endpoint}/environments?page_size=50"
        try:
            result = self.make_request(method="GET", url=url, auth_header=auth_header)
            logger.info(f"Environment list response: {json.dumps(result, indent=2)}")
            if not isinstance(result, dict):
                logger.error("Unexpected environment list response type: %s", type(result).__name__)
                return None
            parsed = EnvironmentListResponse.model_validate(result)
            logger.info("Environment list response: %s", parsed.model_dump_json(indent=2))
            return parsed
        except ValidationError as e:
            logger.error("Error parsing environment list response: %s", e)
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error executing rest call: {e}")
            return None


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


    # ---- Metrics related methods ----
    def get_metrics(self, view: str, qtype: str, query: str) -> dict:
        url=f"https://api.telemetry.confluent.cloud/v2/metrics/{view}/{qtype}"
        version_str = VersionInfo.get_version()
        auth_header = self.get_ccloud_auth()
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

