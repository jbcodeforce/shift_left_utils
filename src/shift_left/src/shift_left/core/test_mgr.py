"""
A test manager processes the test definition and executes all or a specific test.
The test definition is done in yaml
"""
from pydantic import BaseModel
from typing import List
import yaml

class SLTestData(BaseModel):
     table_name: str
     sql_file_name: str

class SLTestCase(BaseModel):
    name: str
    inputs: List[SLTestData]

class SLTestSuite(BaseModel):
     testcases: List[SLTestCase]

# --- public api
def excute_one_test(table_name: str, test_case_name: str):
    """
    From the definition of all tests, select the test to run
    """

    pass


def load_test_definition(table_folder: str) -> SLTestSuite:
    test_definitions = table_folder + "/tests/test_definitions.yaml"
    with open(test_definitions) as f:
          cfg_as_dict=yaml.load(f,Loader=yaml.FullLoader)
          print(cfg_as_dict)
          definition= SLTestSuite.model_validate(cfg_as_dict)
          return definition
