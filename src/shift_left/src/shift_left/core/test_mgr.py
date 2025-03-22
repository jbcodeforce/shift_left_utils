"""
A test manager processes the test definition and executes all or a specific test.
The test definition is done in yaml
"""
from pydantic import BaseModel
from typing import List, Final
import yaml
import logging
import os
from logging.handlers import RotatingFileHandler
from shift_left.core.utils.app_config import get_config

SCRIPTS_DIR: Final[str] = "sql-scripts"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"

log_dir = os.path.join(os.getcwd(), 'logs')
logger = logging.getLogger("test_harness")
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(log_dir, "test_harness.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=1024*1024,  # 1MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
logger.addHandler(console_handler)

class SLTestData(BaseModel):
    table_name: str
    sql_file_name: str

class SLTestCase(BaseModel):
    name: str
    inputs: List[SLTestData]
    outputs: List[SLTestData]

class Foundation(BaseModel):
    table_name: str
    ddl_for_test: str

class SLTestSuite(BaseModel):
    foundations: List[Foundation]
    test_suite: List[SLTestCase]

# --- public api
def excute_one_test(table_name: str, test_case_name: str, compute_pool_id :str):
    """
    From the definition of all tests, select the test to run
    """

    pass

def excute_all_tests(table_name: str, test_case_name: str, compute_pool_id :str):
    """
    From the definition of all tests, run all of them
    """

    pass


def load_test_definition(table_folder: str) -> SLTestSuite:
    test_definitions = table_folder + "/tests/test_definitions.yaml"
    with open(test_definitions) as f:
          cfg_as_dict=yaml.load(f,Loader=yaml.FullLoader)
          print(cfg_as_dict)
          definition= SLTestSuite.model_validate(cfg_as_dict)
          return definition
