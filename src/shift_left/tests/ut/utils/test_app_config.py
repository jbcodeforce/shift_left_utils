import copy
import os
import unittest
import pytest
import pathlib
from unittest.mock import patch, mock_open, MagicMock

from io import StringIO

# Set up config file path for testing
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")

from shift_left.core.utils.app_config import _validate_config


class TestValidateConfig(unittest.TestCase):
    """Test cases for the _validate_config function"""

    def setUp(self):
        """Set up a valid configuration for testing"""
        self.valid_config = {
            "kafka": {
                "bootstrap.servers": "localhost:9092",
                "api_key": "test-api-key",
                "api_secret": "test-api-secret",
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

    def test_valid_config_passes(self):
        """Test that a valid configuration passes validation"""
        # Should not call exit() or print error messages
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(self.valid_config)
            mock_print.assert_not_called()
            mock_exit.assert_not_called()

    def test_empty_config_fails(self):
        """Test that empty configuration fails"""
        with pytest.raises(ValueError, match="Configuration is empty"):
            _validate_config({})

    def test_none_config_fails(self):
        """Test that None configuration fails"""
        with pytest.raises(ValueError, match="Configuration is empty"):
            _validate_config(None)

    def test_missing_main_sections_fail(self):
        """Test that missing main sections cause validation to fail"""
        required_sections = ["kafka",  "confluent_cloud", "flink", "app"]
        
        for section in required_sections:
            config = self.valid_config.copy()
            del config[section]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                _validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert f"Configuration is missing {section} section" in error_message

    def test_multiple_missing_sections_reported_together(self):
        """Test that multiple missing sections are reported together"""
        config = {"kafka": self.valid_config["kafka"]}  # Only kafka section present
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration validation failed with the following errors:" in error_message
            assert "Configuration is missing confluent_cloud section" in error_message
            assert "Configuration is missing flink section" in error_message
            assert "Configuration is missing app section" in error_message

    def test_missing_kafka_fields_fail(self):
        """Test that missing kafka required fields cause validation to fail"""
        kafka_required = ["src_topic_prefix", "cluster_id", "pkafka_cluster", "cluster_type"]
        
        for field in kafka_required:
            config = self.valid_config.copy()
            del config["kafka"][field]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                _validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert f"Configuration is missing kafka.{field}" in error_message

    def test_missing_registry_fields_fail(self):
        """Test that missing registry required fields cause validation to fail (commented out since registry is optional)"""
        # Registry validation is currently commented out in the main function
        # This test is kept for future use when registry validation is re-enabled
        pass

    def test_missing_confluent_cloud_fields_fail(self):
        """Test that missing confluent_cloud required fields cause validation to fail"""
        cc_required = ["environment_id", "region", "provider", "organization_id", "api_key", "api_secret", "url_scope"]
        
        for field in cc_required:
            config = self.valid_config.copy()
            del config["confluent_cloud"][field]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                _validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert f"Configuration is missing confluent_cloud.{field}" in error_message

    def test_missing_flink_fields_fail(self):
        """Test that missing flink required fields cause validation to fail"""
        flink_required = ["flink_url", "api_key", "api_secret", "compute_pool_id", "catalog_name", "database_name", "max_cfu"]
        
        for field in flink_required:
            config = self.valid_config.copy()
            del config["flink"][field]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                _validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert f"Configuration is missing flink.{field}" in error_message

    def test_missing_app_fields_fail(self):
        """Test that missing app required fields cause validation to fail"""
        app_required = [
            "delta_max_time_in_min", 
            "timezone", "logging", 
            "data_limit_column_name_to_select_from",
            "products", "accepted_common_products", 
            "sql_content_modifier", 
            "dml_naming_convention_modifier",
            "compute_pool_naming_convention_modifier", 
            "data_limit_where_condition", 
            "data_limit_replace_from_reg_ex", 
            "data_limit_table_type"
        ]
        
        for field in app_required:
            config = copy.deepcopy(self.valid_config)
            del config["app"][field]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                _validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert f"Configuration is missing app.{field}" in error_message
            config["app"][field] = self.valid_config["app"][field]

    def test_invalid_delta_max_time_type_fails(self):
        """Test that invalid delta_max_time_in_min type fails validation"""
        config = self.valid_config.copy()
        config["app"]["delta_max_time_in_min"] = "not-a-number"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.delta_max_time_in_min must be a number" in error_message

    def test_invalid_logging_level_fails(self):
        """Test that invalid logging level fails validation"""
        config = self.valid_config.copy()
        config["app"]["logging"] = "INVALID_LEVEL"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.logging must be a valid log level" in error_message

    def test_valid_logging_levels_pass(self):
        """Test that all valid logging levels pass validation"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        
        for level in valid_levels:
            config = self.valid_config.copy()
            config["app"]["logging"] = level
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                _validate_config(config)
                mock_print.assert_not_called()
                mock_exit.assert_not_called()

    def test_optional_app_fields_type_validation(self):
        """Test that optional app fields are validated for correct types when present"""
        # Test max_cfu - should be numeric
        config = self.valid_config.copy()
        config["app"]["max_cfu"] = "not-a-number"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.max_cfu must be a number" in error_message
        
        # Test max_cfu_percent_before_allocation - should be numeric
        config = self.valid_config.copy()
        config["app"]["max_cfu_percent_before_allocation"] = "not-a-number"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.max_cfu_percent_before_allocation must be a number" in error_message


    def test_nested_placeholder_values_fail(self):
        """Test that nested placeholder values are detected"""
        config = self.valid_config.copy()
        config["confluent_cloud"]["environment_id"] = "<TO_FILL>"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration contains placeholder value '<TO_FILL>' at confluent_cloud.environment_id" in error_message

    def test_numeric_delta_max_time_passes(self):
        """Test that numeric values for delta_max_time_in_min pass validation"""
        config = self.valid_config.copy()
        
        # Test integer
        config["app"]["delta_max_time_in_min"] = 10
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_not_called()
            mock_exit.assert_not_called()
        
        # Test float
        config["app"]["delta_max_time_in_min"] = 10.5
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_not_called()
            mock_exit.assert_not_called()

    def test_list_type_fields_validation(self):
        """Test that list type fields are properly validated"""
        # Test products - should be list
        config = self.valid_config.copy()
        config["app"]["products"] = "not-a-list"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.products must be a list" in error_message
        
        # Test accepted_common_products - should be list
        config = self.valid_config.copy()
        config["app"]["accepted_common_products"] = "not-a-list"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.accepted_common_products must be a list" in error_message

    def test_empty_string_fields_fail(self):
        """Test that empty string fields are treated as missing"""
        config = self.valid_config.copy()
        config["kafka"]["src_topic_prefix"] = ""
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration is missing kafka.src_topic_prefix" in error_message

    def test_none_fields_fail(self):
        """Test that None fields are treated as missing"""
        config = self.valid_config.copy()
        config["kafka"]["src_topic_prefix"] = None
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration is missing kafka.src_topic_prefix" in error_message

    def test_multiple_validation_errors_reported_together(self):
        """Test that multiple validation errors from different categories are reported together"""
        config = self.valid_config.copy()
        
        # Create multiple types of errors
        del config["kafka"]["src_topic_prefix"]  # Missing required field
        del config["confluent_cloud"]["region"]  # Missing required field
        config["app"]["delta_max_time_in_min"] = "not-a-number"  # Type error
        config["app"]["logging"] = "INVALID_LEVEL"  # Invalid value
        config["flink"]["api_secret"] = "<TO_FILL>"  # Placeholder value
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration validation failed with the following errors:" in error_message
            assert "Configuration is missing kafka.src_topic_prefix" in error_message
            assert "Configuration is missing confluent_cloud.region" in error_message
            assert "Configuration app.delta_max_time_in_min must be a number" in error_message
            assert "Configuration app.logging must be a valid log level" in error_message
            assert "Configuration contains placeholder value '<TO_FILL>' at flink.api_secret" in error_message

    def test_comprehensive_validation_with_all_errors(self):
        """Test comprehensive validation showing all possible error types"""
        # Create a config with multiple issues
        bad_config = {
            "kafka": {
                "bootstrap.servers": "<TO_FILL>",  # Placeholder
                # Missing: api_key, api_secret, sasl.username, sasl.password
            },
            "confluent_cloud": {
                "environment_id": "env-12345",
                # Missing: region, provider, organization_id, api_key, api_secret, url_scope
            },
            "flink": {
                "flink_url": "test.confluent.cloud",
                "api_key": "<kafka-api-key>",  # Placeholder
                # Missing: api_secret, compute_pool_id, catalog_name, database_name, max_cfu
            },
            "app": {
                "delta_max_time_in_min": "invalid",  # Type error
                "logging": "INVALID",  # Invalid value
                "products": "not-a-list",  # Type error
                # Missing many required fields
            }
        }
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            _validate_config(bad_config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            
            # Should contain header
            assert "Configuration validation failed with the following errors:" in error_message
            
            # Should contain missing field errors
            assert "Configuration is missing kafka.src_topic_prefix" in error_message
            assert "Configuration is missing confluent_cloud.region" in error_message
            assert "Configuration is missing confluent_cloud.url_scope" in error_message
            assert "Configuration is missing flink.api_secret" in error_message
            assert "Configuration is missing flink.max_cfu" in error_message
            assert "Configuration is missing app.data_limit_table_type" in error_message
            
            # Should contain type errors
            assert "Configuration app.delta_max_time_in_min must be a number" in error_message
            assert "Configuration app.logging must be a valid log level" in error_message
            assert "Configuration app.products must be a list" in error_message
            


if __name__ == '__main__':
    unittest.main()