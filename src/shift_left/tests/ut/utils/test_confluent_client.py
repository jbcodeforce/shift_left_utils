import unittest

"""
unit test confluent client methods not need integration with confluent cloud.
"""
import os
from base64 import b64encode
import pathlib
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.ccloud_client import ConfluentCloudClient


class TestConfluentClient(unittest.TestCase):
    """
    Test Confluent client methods not need integration with confluent cloud.
    """

    def setUp(self):
        pass
    

    def test_extract_cluster_info_from_bootstrap(self):
        """Test extraction of cluster_id and base_url from bootstrap.servers value"""

        # Test case 1: Standard Confluent Cloud bootstrap format
        bootstrap_servers = "lkc-79kg3p-dm8me7.us-west-2.aws.glb.confluent.cloud:9092"
        cclient = ConfluentCloudClient(get_config())
        result = cclient._extract_cluster_info_from_bootstrap(bootstrap_servers)
        
        expected_cluster_id = "lkc-79kg3p"
        expected_base_url = "dm8me7.us-west-2.aws.glb.confluent.cloud"
        
        self.assertEqual(result["cluster_id"], expected_cluster_id)
        self.assertEqual(result["base_url"], expected_base_url)

        
        # Test case 2: Bootstrap without port
        bootstrap_servers_no_port = "lkc-abc123-xyz789.us-east-1.aws.glb.confluent.cloud"
        result_no_port = cclient._extract_cluster_info_from_bootstrap(bootstrap_servers_no_port)
        
        self.assertEqual(result_no_port["cluster_id"], "lkc-abc123")
        self.assertEqual(result_no_port["base_url"], "xyz789.us-east-1.aws.glb.confluent.cloud")
        
        # Test case 3: Different region
        bootstrap_servers_eu = "lkc-def456-uvw321.eu-central-1.aws.glb.confluent.cloud:9092"
        result_eu = cclient._extract_cluster_info_from_bootstrap(bootstrap_servers_eu)
        
        self.assertEqual(result_eu["cluster_id"], "lkc-def456")
        self.assertEqual(result_eu["base_url"], "uvw321.eu-central-1.aws.glb.confluent.cloud")

        # Test case 4: Empty string
        config = get_config()
        config["kafka"]["bootstrap.servers"] = ""
        cclient = ConfluentCloudClient(get_config())
        result_empty = cclient._extract_cluster_info_from_bootstrap("")
        self.assertIsNone(result_empty["cluster_id"])
        self.assertIsNone(result_empty["base_url"])

        # Test case 5: Invalid format (not starting with lkc-)
        result_invalid = cclient._extract_cluster_info_from_bootstrap("localhost:9092")
        self.assertIsNone(result_invalid["cluster_id"])
        self.assertIsNone(result_invalid["base_url"])

    def test_build_confluent_cloud_url(self):
        """Test building of confluent cloud url"""
        config = get_config()
        config["kafka"]["bootstrap.servers"] = "lkc-79kg3p-dm8me7.us-west-2.aws.glb.confluent.cloud:9092"
        config["kafka"]["cluster_id"] = "clusterid1"
        cclient = ConfluentCloudClient(config)
        url = cclient._build_confluent_cloud_kafka_url()
        self.assertEqual(url, "https://lkc-79kg3p-dm8me7.us-west-2.aws.glb.confluent.cloud/kafka/v3/clusters/clusterid1/topics")

        config["kafka"]["bootstrap.servers"] = "pkc-n98pk.us-west-2.aws.confluent.cloud:9092"
        cclient = ConfluentCloudClient(config)
        url = cclient._build_confluent_cloud_kafka_url()    
        self.assertEqual(url, "https://pkc-n98pk.us-west-2.aws.confluent.cloud/kafka/v3/clusters/clusterid1/topics")

    def test_auth_header(self):
        """Test authentication header"""
        config = get_config()
        config["kafka"]["bootstrap.servers"] = "lkc-79kg3p-dm8me7.us-west-2.aws.glb.confluent.cloud:9092"
        os.environ["SL_KAFKA_API_KEY"] = "test-api-key"
        os.environ["SL_KAFKA_API_SECRET"] = "test-api-secret"
        cclient = ConfluentCloudClient(get_config())
        key=b64encode("test-api-key:test-api-secret".encode('utf-8')).decode('utf-8')
        auth_header = cclient._get_kafka_auth()
        self.assertEqual(auth_header, "Basic " + key)
        
    def test_flink_url(self):
        """Test building of flink url"""
        config = get_config()
        config["kafka"]["bootstrap.servers"] = "lkc-79kg3p-dm8me7.us-west-2.aws.glb.confluent.cloud:9092"
        cclient = ConfluentCloudClient(config)
        url, auth_header = cclient.build_flink_url_and_auth_header()
        self.assertTrue(url.startswith("https://flink-dm8me7.us-west-2.aws.glb.confluent.cloud/sql/v1/organizations"))
        self.assertTrue(auth_header.startswith("Basic "))
        
if __name__ == "__main__":
    unittest.main()