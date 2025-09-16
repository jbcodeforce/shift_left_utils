import copy
import os
import unittest
import pytest
import pathlib
from unittest.mock import patch

# Set up config file path for testing
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")

from shift_left.core.utils.app_config import validate_config, get_config
"""
test app configuration management.
"""

class TestValidateConfig(unittest.TestCase):
    """Test cases for the _validate_config function"""

    def setUp(self):
        """Set up a valid configuration for testing"""
        self.valid_config = {
            "kafka": {
                "bootstrap.servers": "localhost:9092",
                "sasl.username": "test-username",
                "sasl.password": "test-password",
                "src_topic_prefix": "test-src-topic-prefix",
                "cluster_id": "test-cluster-id",
                "pkafka_cluster": "test-pkafka-cluster",
                "cluster_type": "test-cluster-type"
            },
            "confluent_cloud": {
                "environment_id": "env-12345",
                "base_api": "https://api.confluent.cloud",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-12345",
                "api_key": "cc-api-key",
                "api_secret": "cc-api-secret",
                "url_scope": "private"
            },
            "flink": {
                "flink_url": "test.confluent.cloud",
                "api_key": "flink-api-key",
                "api_secret": "flink-api-secret",
                "compute_pool_id": "lfcp-12345",
                "catalog_name": "test-catalog",
                "database_name": "test-database",
                "max_cfu": 10,
                "max_cfu_percent_before_allocation": 0.7
            },
            "app": {
                "delta_max_time_in_min": 15,
                "timezone": "America/Los_Angeles",
                "logging": "INFO",
                "data_limit_column_name_to_select_from": "tenant_id",
                "products": ["p1", "p2", "p3"],
                "accepted_common_products": ["common", "seeds"],
                "sql_content_modifier": "shift_left.core.utils.table_worker.ReplaceEnvInSqlContent",
                "dml_naming_convention_modifier": "shift_left.core.utils.naming_convention.DmlNameModifier",
                "compute_pool_naming_convention_modifier": "shift_left.core.utils.naming_convention.ComputePoolNameModifier",
                "data_limit_where_condition": "tenant_id = 'test'",
                "data_limit_replace_from_reg_ex": "src_",
                "data_limit_table_type": "source"
            }
        }

    def test_get_config_default_values(self):   
        """Test that get_config returns default values"""
        config = get_config()
        assert config.get("app").get("logging") == "INFO"
        assert "lkc" in config.get("kafka").get("cluster_id")
    
   


if __name__ == '__main__':
    unittest.main()