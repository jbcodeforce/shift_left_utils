import yaml
import os
from functools import lru_cache

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
      CONFIG_FILE = os.getenv("CONFIG_FILE",  "/app/config.yaml")
      if CONFIG_FILE:
        with open(CONFIG_FILE) as f:
          _config=yaml.load(f,Loader=yaml.FullLoader)
  return _config
