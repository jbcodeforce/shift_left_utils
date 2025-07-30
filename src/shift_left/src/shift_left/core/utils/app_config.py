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

_config: dict[str, dict[str,str]] = {}

def generate_session_id() -> tuple[str, str]:
    """Generate a session ID in format mm-dd-yy-XXXX where XXXX is random alphanumeric"""
    date_str = datetime.datetime.now().strftime("%m-%d-%y-%H-%M-%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
    return f"{date_str}-{random_str}", random_str



shift_left_dir = os.path.join(os.path.expanduser("~"), '.shift_left') 
log_dir = os.path.join(shift_left_dir, 'logs')
log_name, session_id = generate_session_id()
session_log_dir = os.path.join(log_dir, log_name)

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
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s %(pathname)s:%(lineno)d - %(funcName)s() - %(message)s'))
logger.addHandler(file_handler)
print("-" * 80)
print(f"| SHIFT_LEFT Session started at {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} LOGS folder is : {session_log_dir} |")
print("-" * 80)
#console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.INFO)
#logger.addHandler(console_handler)


def validate_config(config: dict[str,dict[str,str]]) -> None:
  """Validate the configuration"""
  errors = []
  
  if not config:
    errors.append("Configuration is empty")
    raise ValueError("\n".join(errors))
  
  # Validate main sections exist
  required_sections = ["kafka", "confluent_cloud", "flink", "app"]
  for section in required_sections:
    if not config.get(section):
      errors.append(f"Configuration is missing {section} section")
  
  # Only proceed with detailed validation if main sections exist
  if not errors:
    # Validate kafka section
    if config.get("kafka"):
      #kafka_required = ["bootstrap.servers", "api_key", "api_secret", "sasl.username", "sasl.password"]
      kafka_required = ["src_topic_prefix", "cluster_id", "pkafka_cluster", "cluster_type"]
      for field in kafka_required:
        if not config["kafka"].get(field):
          errors.append(f"Configuration is missing kafka.{field}")
    
    # Validate registry section (commented out as it's optional)
    #if config.get("registry"):
    #  registry_required = ["url", "registry_key_name", "registry_key_secret"]
    #  for field in registry_required:
    #    if not config["registry"].get(field):
    #      errors.append(f"Configuration is missing registry.{field}")
    
    # Validate confluent_cloud section
    if config.get("confluent_cloud"):
      cc_required = ["base_api", "environment_id", "region", "provider", "organization_id", "api_key", "api_secret", "url_scope"]
      for field in cc_required:
        if not config["confluent_cloud"].get(field):
          errors.append(f"Configuration is missing confluent_cloud.{field}")
    
    # Validate flink section
    if config.get("flink"):
      flink_required = ["flink_url", "api_key", "api_secret", "compute_pool_id", "catalog_name", "database_name", "max_cfu", "max_cfu_percent_before_allocation"]
      for field in flink_required:
        if not config["flink"].get(field):
          errors.append(f"Configuration is missing flink.{field}")
    
    # Validate app section
    if config.get("app"):
      app_required = ["delta_max_time_in_min", 
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
                     "data_limit_table_type"]
      for field in app_required:
        if not config["app"].get(field):
          errors.append(f"Configuration is missing app.{field}")
      
      # Validate specific app configuration types only if the fields exist
      numeric_fields = ["delta_max_time_in_min", "max_cfu", "max_cfu_percent_before_allocation", "cache_ttl"]
      for field in numeric_fields:
        if config["app"].get(field) is not None:
          if not isinstance(config["app"][field], (int, float)):
            errors.append(f"Configuration app.{field} must be a number")
      
      if config["app"].get("products"):
        if not isinstance(config["app"]["products"], list):
          errors.append("Configuration app.products must be a list")
      
      if config["app"].get("accepted_common_products"):
        if not isinstance(config["app"]["accepted_common_products"], list):
          errors.append("Configuration app.accepted_common_products must be a list")
      
      if config["app"].get("logging"):
        if config["app"]["logging"] not in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
          errors.append("Configuration app.logging must be a valid log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)")
  
  # Check for placeholder values that need to be filled
  placeholders = ["<TO_FILL>", "<kafka-api-key>", "<kafka-api-key_secret>"]
  def check_placeholders(obj, path=""):
    if isinstance(obj, dict):
      for key, value in obj.items():
        check_placeholders(value, f"{path}.{key}" if path else key)
    elif isinstance(obj, str) and obj in placeholders:
      errors.append(f"Configuration contains placeholder value '{obj}' at {path} - please replace with actual value")
  
  check_placeholders(config)
  
  # If there are any errors, raise them all at once
  if len(errors) > 0:
    error_message = "Configuration validation failed with the following errors:\n" + "\n".join(f"  - {error}" for error in errors)
    print(error_message)
    logger.error(error_message)
    exit()

@lru_cache
def get_config() -> dict[str,dict[str,str]]:
  """_summary_
  reads the client configuration from config.yaml
  Args:
      fn (str, optional): _description_. Defaults to "config.yaml".
  return: a key-value map
  """
  global _config
  if _config.__len__() == 0:
      CONFIG_FILE = os.getenv("CONFIG_FILE",  "./config.yaml")
      if CONFIG_FILE:
        with open(CONFIG_FILE) as f:
          _config=yaml.load(f,Loader=yaml.FullLoader)
          validate_config(_config)

  return _config


def reset_config_cache():
  """Reset the configuration cache for testing purposes."""
  global _config
  _config = {}


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
  
  # Reset translator caches
  try:
    import shift_left.core.utils.translator_to_flink_sql as translator
    translator._agent_class = None
  except (ImportError, AttributeError):
    pass


try:
    config = get_config()
    if config and config.get("app"):
        logger.setLevel(config.get("app",{}).get("logging", logging.INFO))
except Exception:
    # If config loading fails during module import, use default level
    logger.setLevel(logging.INFO)