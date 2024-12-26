import yaml
import os
from functools import lru_cache

_config = {}

@lru_cache
def get_config(fn: str = "config.yaml") -> dict[str, str]:
  """_summary_
  reads the client configuration from config.yaml
  Args:
      fn (str, optional): _description_. Defaults to "config.yaml".
  return: a key-value map
  """
  global _config
  if _config is None:
      CONFIG_FILE = os.getenv("CONFIG_FILE")
      if CONFIG_FILE:
        with open(fn) as f:
          _config=yaml.load(f,Loader=yaml.FullLoader)
  return _config
