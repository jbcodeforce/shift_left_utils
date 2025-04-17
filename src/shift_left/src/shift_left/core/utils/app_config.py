"""
Copyright 2024-2025 Confluent, Inc.
"""
import yaml
import os
from functools import lru_cache
import logging
import logging
from logging.handlers import RotatingFileHandler

_config = None



@lru_cache
def get_config() -> dict[str, str] | None:
  """_summary_
  reads the client configuration from config.yaml
  Args:
      fn (str, optional): _description_. Defaults to "config.yaml".
  return: a key-value map
  """
  global _config
  if _config is None:
      CONFIG_FILE = os.getenv("CONFIG_FILE",  "./config.yaml")
      if CONFIG_FILE:
        with open(CONFIG_FILE) as f:
          _config=yaml.load(f,Loader=yaml.FullLoader)
  return _config

shift_left_dir = os.path.join(os.path.expanduser("~"), '.shift_left') 
log_dir = os.path.join(shift_left_dir, 'logs')
logger = logging.getLogger("shift_left")
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(log_dir, "shift_left_cli.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=1024*1024,  # 1MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s %(pathname)s:%(lineno)d - %(funcName)s() - %(message)s'))
logger.addHandler(file_handler)
#console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.INFO)
#logger.addHandler(console_handler)