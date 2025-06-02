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

_config = None

def generate_session_id() -> str:
    """Generate a session ID in format mm-dd-yy-XXXX where XXXX is random alphanumeric"""
    date_str = datetime.datetime.now().strftime("%m-%d-%y-%H-%M-%S")
    random_str = ''.join(random.choices(string.ascii_letters + string.digits, k=4))
    return f"{date_str}-{random_str}", random_str

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
log_name, session_id = generate_session_id()
session_log_dir = os.path.join(log_dir, log_name)

logger = logging.getLogger("shift_left")
logger.propagate = False  # Prevent propagation to root logger
os.makedirs(session_log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(session_log_dir, "shift_left_cli.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=10*1024*1024,  # 10MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s %(pathname)s:%(lineno)d - %(funcName)s() - %(message)s'))
logger.addHandler(file_handler)
print("-" * 80)
print(f"| SHIFT_LEFT SESSION LOGS FOLDER: {session_log_dir} |")
print("-" * 80)
#console_handler = logging.StreamHandler()
#console_handler.setLevel(logging.INFO)
#logger.addHandler(console_handler)