import yaml

def read_config(fn: str = "config.yaml"):
  # reads the client configuration from config.yaml
  # and returns it as a key-value map
  configuration = None
  with open(fn) as f:
    configuration=yaml.load(f,Loader=yaml.FullLoader)
  return configuration
