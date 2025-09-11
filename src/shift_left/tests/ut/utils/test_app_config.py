import copy
import os
import unittest
import pytest
import pathlib
from unittest.mock import patch

# Set up config file path for testing
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config-ccloud.yaml")

from shift_left.core.utils.app_config import validate_config, get_config, _apply_default_overrides, _apply_env_overrides, get_missing_env_vars
"""
test app configuration management.
"""

class TestValidateConfig(unittest.TestCase):
    """Test cases for the _validate_config function"""

    def setUp(self):
        """Set up a valid configuration for testing"""
        os.environ["SL_KAFKA_API_KEY"]="test-api-key" 
        os.environ["SL_KAFKA_API_SECRET"]="test-api-secret"
        os.environ["SL_CONFLUENT_CLOUD_API_KEY"]="test-api-key"
        os.environ["SL_CONFLUENT_CLOUD_API_SECRET"]="test-api-secret"
        os.environ["SL_FLINK_API_KEY"]="test-api-key"
        os.environ["SL_FLINK_API_SECRET"]="test-api-secret"
        
        self.valid_config = {
            "kafka": {
                "bootstrap.servers": "localhost:9092",
                "src_topic_prefix": "test-src-topic-prefix",
                "cluster_id": "test-cluster-id",
                "cluster_type": "dev"
            },
            "confluent_cloud": {
                "environment_id": "env-12345",
                "region": "us-west-2",
                "provider": "aws",
                "organization_id": "org-12345"
            },
            "flink": {
                "compute_pool_id": "lfcp-12345",
                "catalog_name": "test-catalog",
                "database_name": "test-database",
            },
            "app": {
                "logging": "INFO",
            }
        }

    def extract_messages_from_mock_print(self, mock_print):
        """Helper method to extract error and warning messages from mock print calls"""
        all_print_calls = [str(call[0][0]) for call in mock_print.call_args_list]
        
        error_message = None
        warning_message = None
        
        for call_message in all_print_calls:
            if "Configuration validation failed with the following errors:" in call_message:
                error_message = call_message
            elif "Configuration validation has the following warnings:" in call_message:
                warning_message = call_message
        
        return error_message, warning_message, all_print_calls

    def test_valid_config_passes(self):
        """Test that a valid configuration passes validation"""
        # Should not call exit() or print error messages
        config = _apply_default_overrides(self.valid_config)
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_not_called()

    def test_empty_config_fails(self):
        """Test that empty configuration fails"""
        with pytest.raises(ValueError, match="Configuration is empty"):
            validate_config({})

    def test_none_config_fails(self):
        """Test that None configuration fails"""
        with pytest.raises(ValueError, match="Configuration is empty"):
            validate_config(None)

    def test_missing_main_sections_fail(self):
        """Test that missing main sections cause validation to fail"""
        required_sections = ["kafka",  "confluent_cloud", "flink", "app"]
        
        for section in required_sections:
            config = self.valid_config.copy()
            del config[section]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_called_once()
                error_message = mock_print.call_args[0][0]
                assert f"Configuration is missing {section} section" in error_message

    def test_multiple_missing_sections_reported_together(self):
        """Test that multiple missing sections are reported together"""
        config = {"kafka": self.valid_config["kafka"]}  # Only kafka section present
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration validation failed with the following errors:" in error_message
            assert "Configuration is missing confluent_cloud section" in error_message
            assert "Configuration is missing flink section" in error_message
            assert "Configuration is missing app section" in error_message

    def test_missing_kafka_fields_fail(self):
        """Test that missing kafka required fields cause validation to fail"""
        kafka_required = [ "cluster_id",  "cluster_type"]
        
        for field in kafka_required:
            config = self.valid_config.copy()
            del config["kafka"][field]
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                validate_config(config)
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
        cc_required = ["environment_id", "region", "provider", "organization_id"]
        config = copy.deepcopy(self.valid_config)
        config = _apply_default_overrides(config)
        for field in cc_required:
            del config["confluent_cloud"][field]
            
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            assert mock_print.call_count >= 1
            mock_exit.assert_called_once()
            all_print_calls = [str(call[0][0]) for call in mock_print.call_args_list]
            
            # Find the error message (the one with "Configuration validation failed")
            error_message = None
            for call_message in all_print_calls:
                if "Configuration validation failed with the following errors:" in call_message:
                    error_message = call_message
                    break
            
            assert error_message is not None, f"Error message not found in print calls: {all_print_calls}"
            
            # Check that all required fields are mentioned in the error message
            for field in cc_required:
                assert f"Configuration is missing confluent_cloud.{field}" in error_message, f"Missing field {field} not found in error message"
            
    def test_missing_flink_fields_fail(self):
        """Test that missing flink required fields cause validation to fail"""
        flink_required = ["compute_pool_id", "catalog_name", "database_name", "max_cfu"]
        config = copy.deepcopy(self.valid_config)
        config = _apply_default_overrides(config)
        for field in flink_required:
            del config["flink"][field]
            
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            assert mock_print.call_count >= 1
            mock_exit.assert_called_once()
            all_print_calls = [str(call[0][0]) for call in mock_print.call_args_list]
            
            # Find the error message (the one with "Configuration validation failed")
            error_message = None
            for call_message in all_print_calls:
                if "Configuration validation failed with the following errors:" in call_message:
                    error_message = call_message
                    break
            
            assert error_message is not None, f"Error message not found in print calls: {all_print_calls}"
            
            # Check that all required fields are mentioned in the error message
            for field in flink_required:
                assert f"Configuration is missing flink.{field}" in error_message, f"Missing field {field} not found in error message"
            
    def test_missing_app_fields_fail(self):
        """Test that missing app required fields cause validation to fail"""
        app_required = [
            "delta_max_time_in_min", 
            "timezone", 
            "logging", 
            "data_limit_column_name_to_select_from",
            "products", 
            "accepted_common_products", 
            "sql_content_modifier", 
            "dml_naming_convention_modifier",
            "compute_pool_naming_convention_modifier", 
            "data_limit_where_condition", 
            "data_limit_replace_from_reg_ex", 
            "data_limit_table_type"
        ]
        config = copy.deepcopy(self.valid_config)
        config = _apply_default_overrides(config)
        for field in app_required:  
            if config["app"].get(field):
                del config["app"][field]
            
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            
            # Check that print was called and exit was called
            assert mock_print.call_count >= 1
            mock_exit.assert_called_once()
            
            # Get all print calls - error message should be in the first call
            all_print_calls = [str(call[0][0]) for call in mock_print.call_args_list]
            
            # Find the error message (the one with "Configuration validation failed")
            error_message = None
            for call_message in all_print_calls:
                if "Configuration validation failed with the following errors:" in call_message:
                    error_message = call_message
                    break
            
            assert error_message is not None, f"Error message not found in print calls: {all_print_calls}"
            
            # Check that all required fields are mentioned in the error message
            for field in app_required:
                assert f"Configuration is missing app.{field}" in error_message, f"Missing field {field} not found in error message"
            

    def test_invalid_delta_max_time_type_fails(self):
        """Test that invalid delta_max_time_in_min type fails validation"""
        config = self.valid_config.copy()
        config["app"]["delta_max_time_in_min"] = "not-a-number"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.delta_max_time_in_min must be a number" in error_message

    def test_invalid_logging_level_fails(self):
        """Test that invalid logging level fails validation"""
        config = self.valid_config.copy()
        config["app"]["logging"] = "INVALID_LEVEL"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.logging must be a valid log level" in error_message

    def test_valid_logging_levels_pass(self):
        """Test that all valid logging levels pass validation"""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        
        for level in valid_levels:
            config = self.valid_config.copy()
            config = _apply_default_overrides(config)
            config["app"]["logging"] = level
            
            with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
                validate_config(config)
                mock_print.assert_called_once()
                mock_exit.assert_not_called()

    def test_optional_app_fields_type_validation(self):
        """Test that optional app fields are validated for correct types when present"""
        # Test max_cfu - should be numeric
        config = self.valid_config.copy()
        config["app"]["max_cfu"] = "not-a-number"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.max_cfu must be a number" in error_message
        
        # Test max_cfu_percent_before_allocation - should be numeric
        config = self.valid_config.copy()
        config["app"]["max_cfu_percent_before_allocation"] = "not-a-number"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.max_cfu_percent_before_allocation must be a number" in error_message


    def test_nested_placeholder_values_fail(self):
        """Test that nested placeholder values are detected"""
        config = self.valid_config.copy()
        config["confluent_cloud"]["environment_id"] = "<TO_FILL>"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration contains placeholder value '<TO_FILL>' at confluent_cloud.environment_id" in error_message

    def test_numeric_delta_max_time_passes(self):
        """Test that numeric values for delta_max_time_in_min pass validation"""
        config = self.valid_config.copy()
        config = _apply_default_overrides(config)
        
        # Test integer
        config["app"]["delta_max_time_in_min"] = 10
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_not_called()
        
        # Test float
        config["app"]["delta_max_time_in_min"] = 10.5
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_not_called()

    def test_list_type_fields_validation(self):
        """Test that list type fields are properly validated"""
        # Test accepted_common_products - should be list
        config = self.valid_config.copy()
        config["app"]["accepted_common_products"] = "not-a-list"
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration app.accepted_common_products must be a list" in error_message

    def test_empty_string_fields_fail(self):
        """Test that empty string fields are treated as missing"""
        config = self.valid_config.copy()
        config["flink"]["compute_pool_id"] = ""
        #config["app"]["accepted_common_products"] = ["common", "seeds"]
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            assert mock_print.call_count == 1
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration is missing flink.compute_pool_id" in error_message

    def test_none_fields_fail(self):
        """Test that None fields are treated as missing"""
        config = self.valid_config.copy()
        config["flink"]["compute_pool_id"] = None
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            error_message = mock_print.call_args[0][0]
            assert "Configuration is missing flink.compute_pool_id" in error_message

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
            validate_config(config)
            mock_exit.assert_called_once()
            
            # Extract messages using helper method
            error_message, warning_message, all_print_calls = self.extract_messages_from_mock_print(mock_print)
            
            # Should have error message
            assert error_message is not None, f"Error message not found in print calls: {all_print_calls}"
            
            assert "Configuration validation failed with the following errors:" in error_message
            assert "Configuration is missing kafka.src_topic_prefix" in error_message
            assert "Configuration is missing confluent_cloud.region" in error_message
            assert "Configuration app.delta_max_time_in_min must be a number" in error_message
            assert "Configuration app.logging must be a valid log level" in error_message
            
            # May also have warnings if deprecated fields are present  
            if warning_message:
                assert "Configuration validation has the following warnings:" in warning_message


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
                "catalog_name": "test.cloud.env",
            },
            "app": {
                "logging": "INVALID",  # Invalid value
                # Missing many required fields
            }
        }
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(bad_config)
            mock_print.assert_called_once()
            mock_exit.assert_called_once()
            
            # Extract messages using helper method
            error_message, warning_message, all_print_calls = self.extract_messages_from_mock_print(mock_print)
            
            # Should have error message
            assert error_message is not None, f"Error message not found in print calls: {all_print_calls}"
            
            # Should contain header
            assert "Configuration validation failed with the following errors:" in error_message
            # Should contain missing field errors
            assert "Configuration is missing confluent_cloud.region" in error_message   
            # Should contain type errors
            assert "Configuration app.logging must be a valid log level" in error_message
            

    def test_get_config(self):
        """Test that get_config returns a dictionary"""
        config = get_config()
        assert isinstance(config, dict)
        assert config.get("app") is not None
        assert config.get("app").get("logging") is not None
        assert config.get("app").get("logging") == "INFO"
        assert "lkc" in config.get("kafka").get("cluster_id")

    def test_deprecated_fields(self):
        """Test that deprecated fields are detected"""
        # Start with a valid config and apply defaults to avoid missing field errors  
        config = copy.deepcopy(self.valid_config)
        config = _apply_default_overrides(config)
        
        # Add deprecated fields
        config["kafka"]["pkafka_cluster"] = "test-pkafka-cluster"
        config["confluent_cloud"]["url_scope"] = "private"
        config["confluent_cloud"]["base_api"] = "https://api.confluent.cloud"
        config["flink"]["api_key"] = "flink-api-key"
        config["flink"]["api_secret"] = "flink-api-secret"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            
            # Extract messages using helper method
            error_message, warning_message, all_print_calls = self.extract_messages_from_mock_print(mock_print)
            
            # For deprecated fields with a complete config, we expect only warnings (no errors)
            assert error_message is None, f"Unexpected error message: {error_message}"
            assert warning_message is not None, f"Warning message not found in print calls: {all_print_calls}"
            assert mock_print.call_count == 1, f"Expected 1 call (warnings only), got {mock_print.call_count}: {all_print_calls}"
            
            assert "Warning: kafka.pkafka_cluster is deprecated - can be removed from config file" in warning_message
            assert "Warning: confluent_cloud.url_scope is deprecated - can be removed from config file" in warning_message
            assert "Warning: confluent_cloud.base_api is deprecated - can be removed from config file" in warning_message
            assert "Warning: flink.api_key is deprecated use environment variables instead" in warning_message
            assert "Warning: flink.api_secret is deprecated use environment variables instead" in warning_message

    def test_error_and_warning_messages_together(self):
        """Test that both error and warning messages can be captured when both occur"""
        # Create a config with missing fields (errors) and deprecated fields (warnings)
        config = copy.deepcopy(self.valid_config)
        config = _apply_default_overrides(config)
        
        # Remove some required fields to generate errors
        del config["app"]["sql_content_modifier"]
        del config["app"]["products"]
        
        # Add deprecated fields to generate warnings  
        config["flink"]["api_key"] = "deprecated-api-key"
        config["kafka"]["pkafka_cluster"] = "deprecated-cluster"
        
        with patch('builtins.print') as mock_print, patch('builtins.exit') as mock_exit:
            validate_config(config)
            
            # Should have both error and warning calls
            assert mock_print.call_count == 2, f"Expected 2 calls (errors + warnings), got {mock_print.call_count}"
            mock_exit.assert_called_once()
            
            # Extract messages using helper method
            error_message, warning_message, all_print_calls = self.extract_messages_from_mock_print(mock_print)
            
            # Should have both error and warning messages
            assert error_message is not None, f"Error message not found in print calls: {all_print_calls}"
            assert warning_message is not None, f"Warning message not found in print calls: {all_print_calls}"
            
            # Check error content
            assert "Configuration validation failed with the following errors:" in error_message
            assert "Configuration is missing app.sql_content_modifier" in error_message
            assert "Configuration is missing app.products" in error_message
            
            # Check warning content
            assert "Configuration validation has the following warnings:" in warning_message
            assert "Warning: flink.api_key is deprecated use environment variables instead" in warning_message
            assert "Warning: kafka.pkafka_cluster is deprecated - can be removed from config file" in warning_message
    
    def test_apply_env_overrides(self):
        """Test that _apply_env_overrides applies environment variable overrides"""
        config = self.valid_config.copy()
        config = _apply_default_overrides(config)
        config = _apply_env_overrides(config)
        assert config["kafka"]["api_secret"] == "test-api-secret"
        assert config["confluent_cloud"]["api_secret"] == "test-api-secret"
        assert config["flink"]["api_secret"] == "test-api-secret"
        assert config["kafka"]["api_key"] == "test-api-key"
        assert config["confluent_cloud"]["api_key"] == "test-api-key"
        assert config["flink"]["api_key"] == "test-api-key"

    def test_get_missing_env_vars(self):
        """Test that get_missing_env_vars returns the correct missing environment variables"""
        del os.environ["SL_KAFKA_API_KEY"]
        config = self.valid_config.copy()
        config = _apply_default_overrides(config)
        missing_env_vars = get_missing_env_vars(config)
        assert missing_env_vars == {"SL_KAFKA_API_KEY"}

if __name__ == '__main__':
    unittest.main()