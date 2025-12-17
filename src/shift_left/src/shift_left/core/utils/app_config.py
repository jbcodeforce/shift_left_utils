"""
Copyright 2024-2025 Confluent, Inc.
"""
import yaml
import os
from functools import lru_cache
import logging
from logging.handlers import RotatingFileHandler
import datetime
import random
import string
from typing import Dict, Any, Optional
from .error_sanitizer import sanitize_error_message

_config: dict[str, dict[str,str]] = {}

BASE_CC_API = "api.confluent.cloud/org/v2"
__version__ = "0.1.47"

# Environment variable mapping for sensitive values
ENV_VAR_MAPPING = {
    # Kafka API credentials
    "kafka.api_key": "SL_KAFKA_API_KEY",
    "kafka.api_secret": "SL_KAFKA_API_SECRET",
    "kafka.sasl.username": "SL_KAFKA_API_KEY",
    "kafka.sasl.password": "SL_KAFKA_API_SECRET",

    # Confluent Cloud API credentials
    "confluent_cloud.api_key": "SL_CONFLUENT_CLOUD_API_KEY",
    "confluent_cloud.api_secret": "SL_CONFLUENT_CLOUD_API_SECRET",

    # Flink API credentials
    "flink.api_key": "SL_FLINK_API_KEY",
    "flink.api_secret": "SL_FLINK_API_SECRET"
}


def get_env_value(config_path: str) -> Optional[str]:
    """
    Get environment variable value for a given config path.

    Args:
        config_path: Dot-separated config path like 'kafka.api_key'

    Returns:
        Environment variable value if set, None otherwise
    """
    env_var_name = ENV_VAR_MAPPING.get(config_path)
    if env_var_name:
        return os.getenv(env_var_name)
    return None



def get_missing_env_vars(config: Dict[str, Any]) -> set[str]:
    """
    Check which environment variables are missing for required API keys/secrets.

    Args:
        config: Configuration dictionary

    Returns:
        List of missing environment variable names
    """
    logger.info(f"version {__version__}")
    missing_env_vars = set()

    for config_path, env_var_name in ENV_VAR_MAPPING.items():
        path_parts = config_path.split('.')
        section = path_parts[0]
        field = path_parts[1]

        # Check if the config value exists and is a placeholder
        value=config.get(section, {}).get(field)
        if (not value or value in ["", "<TO_FILL>", "<kafka-api-key>", "<kafka-api-key_secret>", "<no-api-key>", "<no-key"]) and not os.getenv(env_var_name):
            missing_env_vars.add(env_var_name)

    return missing_env_vars


def print_env_var_help():
    """
    Print helpful information about supported environment variables.
    """
    print("\n" + "="*80 + f" version {__version__}")
    print("SHIFT_LEFT ENVIRONMENT VARIABLES")
    print("="*80)
    print("You can set the following environment variables to provide API keys and secrets")
    print("instead of storing them in config.yaml files:\n")

    # Group by section for better readability
    sections = {}
    for config_path, env_var_name in ENV_VAR_MAPPING.items():
        section = config_path.split('.')[0]
        if section not in sections:
            sections[section] = []
        sections[section].append((config_path, env_var_name))

    for section, vars_list in sections.items():
        print(f"{section.upper().replace('_', ' ')} SECTION:")
        for config_path, env_var_name in vars_list:
            config_field = config_path.split('.')[1]
            print(f"  {env_var_name:<35} -> {config_path}")
        print()

    print("USAGE EXAMPLES:")
    print("  export SL_KAFKA_API_KEY='your-kafka-api-key'")
    print("  export SL_KAFKA_API_SECRET='your-kafka-api-secret'")
    print("  export SL_CONFLUENT_CLOUD_API_KEY='your-confluent-cloud-api-key'")
    print("  export SL_CONFLUENT_CLOUD_API_SECRET='your-confluent-cloud-api-secret'")
    print("  export SL_FLINK_API_KEY='your-flink-api-key'")
    print("  export SL_FLINK_API_SECRET='your-flink-api-secret'")
    print("\nNOTE: Environment variables take precedence over config.yaml values")
    print("="*80 + "\n")


class SecureFormatter(logging.Formatter):
    """
    Custom logging formatter that sanitizes sensitive information from log messages.

    This formatter ensures that API keys, passwords, tokens, and other sensitive data
    are automatically masked in all log outputs, preventing accidental exposure
    of secrets in log files.
    """

    def format(self, record):
        """Format the log record and sanitize any sensitive information."""
        # First get the formatted message using the parent formatter
        formatted_message = super().format(record)

        # Sanitize the entire formatted message
        sanitized_message = sanitize_error_message(formatted_message)

        return sanitized_message


def generate_session_id() -> tuple[str, str]:
    """Generate a session ID in format mm-dd-yy-XXXX where XXXX is random alphanumeric"""
    date_str = datetime.datetime.now().strftime("%m-%d-%y-%H-%M-%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
    return f"{date_str}-{random_str}", random_str



shift_left_dir = os.path.join(os.path.expanduser("~"), '.shift_left')
log_dir = os.path.join(shift_left_dir, 'logs')
log_name, session_id = generate_session_id()
session_log_dir = os.path.join(log_dir, log_name)

# Configure secure logging with automatic sensitive data sanitization
# The SecureFormatter ensures that API keys, passwords, tokens, and other
# sensitive information are automatically masked in all log outputs
logger = logging.getLogger("shift_left")
logger.propagate = False  # Prevent propagation to root logger
os.makedirs(session_log_dir, exist_ok=True)

log_file_path = os.path.join(session_log_dir, "shift_left_cli.log")
file_handler = RotatingFileHandler(
    log_file_path,
    maxBytes=5*1024*1024,  # 5MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(SecureFormatter('%(asctime)s - %(name)s - %(levelname)s %(pathname)s:%(lineno)d - %(funcName)s() - %(message)s'))
logger.addHandler(file_handler)
print("-" * 40 + " SHIFT_LEFT " + __version__ + " " + "-" * 40)
header_line=f"""| CONFIG_FILE     : {os.getenv('CONFIG_FILE')}
| LOGS folder     : {session_log_dir}
| Session started : {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
print(header_line)
logger.info(header_line)
print("-" * 92)
#console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.INFO)
#logger.addHandler(console_handler)


def validate_config(config: dict[str,dict[str,str]]) -> None:
  """Validate the configuration"""
  errors = []
  warnings = []
  if not config:
    errors.append("Configuration is empty")
    raise ValueError("\n".join(errors))

  # Validate main sections exist
  required_sections = ["kafka", "confluent_cloud", "flink", "app"]
  for section in required_sections:
    if not config.get(section):
      errors.append(f"Configuration is missing {section} section")
  warnings = _check_deprecated_fields(config)

  # Only proceed with detailed validation if main sections exist
  if not errors:
    # Validate kafka section
    if config.get("kafka"):
      kafka_required = ["bootstrap.servers", "cluster_type", "cluster_id"]
      for field in kafka_required:
        if not config["kafka"].get(field):
          errors.append(f"Configuration is missing kafka.{field}")


    # Validate confluent_cloud section
    if config.get("confluent_cloud"):
      cc_required = ["environment_id", "region", "provider", "organization_id"]
      for field in cc_required:
        if not config["confluent_cloud"].get(field):
          errors.append(f"Configuration is missing confluent_cloud.{field}")

    # Validate flink section
    if config.get("flink"):
      flink_required = ["catalog_name", "database_name", "compute_pool_id"]
      for field in flink_required:
        if not config["flink"].get(field):
          errors.append(f"Configuration is missing flink.{field}")

    # Validate app section
    if config.get("app"):
      app_required = [
                      "accepted_common_products",
                      "sql_content_modifier",
                      "dml_naming_convention_modifier",
                     "compute_pool_naming_convention_modifier"
                  ]
      for field in app_required:
        if not config["app"].get(field):
          errors.append(f"Configuration is missing app.{field}")
      # Validate specific app configuration types only if the fields exist
      numeric_fields = ["delta_max_time_in_min", "max_cfu", "max_cfu_percent_before_allocation", "cache_ttl"]
      for field in numeric_fields:
        if config["app"].get(field) is not None:
          if not isinstance(config["app"][field], (int, float)):
            errors.append(f"Configuration app.{field} must be a number")

      if config["app"].get("accepted_common_products"):
        if not isinstance(config["app"]["accepted_common_products"], list):
          errors.append("Configuration app.accepted_common_products must be a list")

      if config["app"].get("logging"):
        if config["app"]["logging"] not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
          errors.append("Configuration app.logging must be a valid log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")

      if config["app"].get("post_fix_unit_test"):
        pfut = config["app"]["post_fix_unit_test"].lstrip('_')
        if not config["app"]["post_fix_unit_test"].startswith("_") or not (2<= len(pfut) <= 3) or not pfut.isalnum():
          errors.append("post_fix_unit_test must start with _, be 2 or 3 characters and be alpha numeric")


  # Check for placeholder values that need to be filled
  placeholders = ["<TO_FILL>", "<kafka-api-key>", "<kafka-api-key_secret>", "<no-api-key>", "<no-key"]
  def check_placeholders(obj, path=""):
    if isinstance(obj, dict):
      for key, value in obj.items():
        check_placeholders(value, f"{path}.{key}" if path else key)
    elif isinstance(obj, str) and obj in placeholders:
      # Check if there's a corresponding environment variable
      env_var_name = ENV_VAR_MAPPING.get(path)
      if env_var_name and os.getenv(env_var_name):
        # Environment variable is set, so placeholder is acceptable
        return
      else:
        if env_var_name:
          errors.append(f"Configuration contains placeholder value '{obj}' at {path} - please set environment variable {env_var_name} or replace with actual value in config file")
        else:
          errors.append(f"Configuration contains placeholder value '{obj}' at {path} - please replace with actual value")

  check_placeholders(config)

  # Check for missing environment variables when config values are placeholders
  missing_env_vars = get_missing_env_vars(config)
  if missing_env_vars:
    errors.append(f"Missing environment variables for API keys/secrets: {', '.join(sorted(missing_env_vars))}. Please set these environment variables or update the config file with actual values.")

  # If there are any errors, raise them all at once
  if len(errors) > 0:
    error_message = "Configuration validation failed with the following errors:\n" + "\n".join(f"  - {error}" for error in errors)
    # Use safe error display to ensure no sensitive config values are exposed
    sanitized_message = sanitize_error_message(error_message)
    print(sanitized_message)
    logger.error(sanitized_message)
    exit()
  if len(warnings) > 0:
    warning_message = "Configuration validation has the following warnings:\n" + "\n".join(f"  - {warning}" for warning in warnings)
    sanitized_message = sanitize_error_message(warning_message)
    print(sanitized_message)
    logger.warning(sanitized_message)



@lru_cache
def get_config() -> dict[str,dict[str,str]]:
  """
  Read configuration from config.yaml and apply environment variable overrides.

  Environment variables take precedence over config file values for sensitive data.
  Supported environment variables:
  - SL_KAFKA_API_KEY / SL_KAFKA_API_SECRET
  - SL_CONFLUENT_CLOUD_API_KEY / SL_CONFLUENT_CLOUD_API_SECRET
  - SL_FLINK_API_KEY / SL_FLINK_API_SECRET

  Args:
      fn (str, optional): Config file path. Defaults to "config.yaml".

  Returns:
      dict: Configuration dictionary with environment variable overrides applied
  """
  global _config
  if _config.__len__() == 0:
      CONFIG_FILE = os.getenv("CONFIG_FILE",  "./config.yaml")
      if CONFIG_FILE:
        try:
          config = {}
          for section in ["kafka", "confluent_cloud", "flink", "app"]:
            config[section] = {}
          config= _apply_default_overrides(config)
          with open(CONFIG_FILE) as f:
            _config = yaml.load(f, Loader=yaml.FullLoader)
            _merged_config = _merge_config(_config, config)
            # Apply environment variable overrides for sensitive values
            _config = _apply_env_overrides(_merged_config)
            validate_config(_merged_config)
        except FileNotFoundError:
          print(f"Warning: Configuration file {CONFIG_FILE} not found. Using environment variables only.")
          # Create minimal config structure and apply environment variables
          _config = {
            "kafka": {},
            "confluent_cloud": {},
            "flink": {},
            "registry": {},
            "app": {
            }
          }
          _config = _apply_default_overrides(_config)
          _config = _apply_env_overrides(_config)
          validate_config(_config)

  return _config


def reset_config_cache():
  """Reset the configuration cache for testing purposes."""
  global _config
  _config = {}
  # Clear the LRU cache for get_config function
  get_config.cache_clear()


def reset_all_caches() -> None:
  """Reset all module-level caches for testing purposes."""
  reset_config_cache()

  # Reset statement manager caches
  try:
    import shift_left.core.statement_mgr as statement_mgr
    statement_mgr._statement_list_cache = None
    statement_mgr._runner_class = None
  except (ImportError, AttributeError):
    pass

  # Reset compute pool manager caches
  try:
    import shift_left.core.compute_pool_mgr as compute_pool_mgr
    compute_pool_mgr._compute_pool_list = None
    compute_pool_mgr._compute_pool_name_modifier = None
  except (ImportError, AttributeError):
    pass

  # Reset file search caches
  try:
    import shift_left.core.utils.file_search as file_search
    file_search._statement_name_modifier = None
  except (ImportError, AttributeError):
    pass


try:
    config = get_config()
    if config and config.get("app"):
        logger.setLevel(config.get("app",{}).get("logging", logging.INFO))
except Exception:
    # If config loading fails during module import, use default level
    logger.setLevel(logging.INFO)


# --- private functions ---
def _merge_config(loaded_config: dict[str,dict[str,str]], default_config: dict[str,dict[str,str]]) -> dict[str,dict[str,str]]:
  """Merge the loaded config with the default config, with loaded_config taking precedence"""
  merged_config = {}

  # Start with all sections from default_config
  for section, section_values in default_config.items():
    merged_config[section] = section_values.copy()

  # Merge sections from loaded_config, preserving default values for missing keys
  # Handle case where loaded_config might be None
  if loaded_config is not None:
    for section, section_values in loaded_config.items():
      if section in merged_config:
        # Section exists in defaults, merge the fields
        merged_config[section].update(section_values)
      else:
        # New section not in defaults, add it completely
        merged_config[section] = section_values.copy()

  return merged_config


def _apply_default_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
  """
  Apply default overrides to configuration.
  """
  config["kafka"]["security.protocol"]="SASL_SSL"
  config["kafka"]["sasl.mechanism"]="PLAIN"
  config["kafka"]["session.timeout.ms"]=5000
  config["kafka"]["cluster_type"]="dev"
  config["kafka"]["src_topic_prefix"]="clone"
  config["confluent_cloud"]["base_api"]="api.confluent.cloud/org/v2"
  config["confluent_cloud"]["page_size"]=100
  config["confluent_cloud"]["glb_name"]="glb"
  config["confluent_cloud"]["url_scope"]="private"
  config["flink"]["max_cfu"]=10
  config["flink"]["max_cfu_percent_before_allocation"]=0.8
  config["flink"]["statement_name_post_fix"]="None"
  config["app"]["default_PK"]="__db"
  config["app"]["delta_max_time_in_min"]= 15
  config["app"]["timezone"]="America/Los_Angeles"
  config["app"]["src_table_name_prefix"]="src_"
  config["app"]["logging"]="INFO"
  config["app"]["products"]=["p1", "p2", "p3"]
  config["app"]["accepted_common_products"]=["common", "seeds"]
  config["app"]["cache_ttl"]=120
  config["app"]["sql_content_modifier"]= "shift_left.core.utils.table_worker.ReplaceEnvInSqlContent"
  config["app"]["translator_to_flink_sql_agent"]= "shift_left.core.utils.translator_to_flink_sql.DbtTranslatorToFlinkSqlAgent"
  config["app"]["dml_naming_convention_modifier"]= "shift_left.core.utils.naming_convention.DmlNameModifier"
  config["app"]["compute_pool_naming_convention_modifier"]= "shift_left.core.utils.naming_convention.ComputePoolNameModifier"
  config["app"]["data_limit_where_condition"]= "rf\"where tenant_id in ( SELECT tenant_id FROM tenant_filter_pipeline WHERE product = {product_name})\""
  config["app"]["data_limit_replace_from_reg_ex"]= "r\"\\s*select\\s+\\*\\s+from\\s+final\\s*;?\""
  config["app"]["data_limit_table_type"]= "source"
  config["app"]["data_limit_column_name_to_select_from"]= "tenant_id"
  config["app"]["post_fix_unit_test"]= "_ut"
  config["app"]["post_fix_integration_test"]= "_it"
  return config


def _apply_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Apply environment variable overrides to configuration.
    Environment variables take precedence over config file values.

    Args:
        config: Configuration dictionary loaded from YAML

    Returns:
        Updated configuration with environment variable overrides
    """
    # Use print instead of logger since logger may not be initialized yet
    env_overrides_applied = 0

    for config_path, env_var_name in ENV_VAR_MAPPING.items():
        env_value = os.getenv(env_var_name)
        if env_value:
            # Split the path to navigate the config structure
            path_parts = config_path.split('.')
            section = path_parts[0]
            field = path_parts[1]

            # Initialize section if it doesn't exist
            if section not in config:
                config[section] = {}

            # Set the environment variable value
            config[section][field] = env_value
            env_overrides_applied += 1

    if env_overrides_applied > 0:
        print(f"Applied {env_overrides_applied} environment variable overrides for sensitive values")

    return config

def _check_deprecated_fields(config: dict[str,dict[str,str]]) -> list[str]:
  """Check for deprecated fields in the configuration"""
  warnings = []
  if config.get("registry"):
    warnings.append(f"Warning: registry section is deprecated - may be removed from config file")

  # deprecated fields
  deprecated_fields = ["api_key", "api_secret"]
  for section in ["kafka", "confluent_cloud", "flink"]:
    for field in deprecated_fields:
      if config.get(section) and config.get(section).get(field):
        matched_env_var = get_env_value(f"{section}.{field}")
        if not matched_env_var:
          warnings.append(f"Warning: {section}.{field} is deprecated use environment variables instead")
        else:
          warnings.append(f"{section}.{field} is set via environment variable")
  deprecated_fields = ["security.protocol", "sasl.mechanism", "sasl.username", "sasl.password", "session.timeout.ms", "glb_name", "pkafka_cluster", "url_scope", "base_api", "page_size"]
  warnings.extend(_check_fields_in_sections(config, ["kafka", "confluent_cloud"], deprecated_fields))
  deprecated_fields = ["flink_url", "max_cfu", "max_cfu_percent_before_allocation", "statement_name_post_fix"]
  warnings.extend(_check_fields_in_sections(config, ["flink"], deprecated_fields))
  deprecated_fields = ["delta_max_time_in_min", "default_PK", "timezone", "logging", "products", "cache_ttl",   "data_limit_where_condition", "data_limit_replace_from_reg_ex", "data_limit_table_type", "data_limit_column_name_to_select_from"]
  warnings.extend(_check_fields_in_sections(config, ["app"], deprecated_fields))
  return warnings

def _check_fields_in_sections(config: dict[str,dict[str,str]], sections:list[str], deprecated_fields: list[str]) -> list[str]:
  """Check for deprecated fields in the configuration"""
  warnings = []
  for section in sections:
    for field in deprecated_fields:
      if config.get(section) and config.get(section).get(field):
        warnings.append(f"{section}.{field} is set to overide default value, or may be removed from config file if not needed")
  return warnings
