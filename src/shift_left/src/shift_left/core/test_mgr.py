"""
A test manager processes the test definition and executes all or a specific test.
The test definition is done in yaml
"""
from pydantic import BaseModel
from typing import List, Final
import yaml
import logging,time
import os
from logging.handlers import RotatingFileHandler
from shift_left.core.utils.app_config import get_config
from shift_left.core.deployment_mgr import *
from shift_left.core.utils.ccloud_client import ConfluentCloudClient

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

def log( table_folder,message):
    with open(f"{table_folder}/tests/result.txt", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")

def drop_validation_tables(table_folder):
    ddl_dir = os.path.join(table_folder, SCRIPTS_DIR)
    print(f"Dropping tables at: {ddl_dir}")
    # Loop through files
    for filename in os.listdir(ddl_dir):
        if filename.startswith("ddl.") and filename.endswith(".sql"):
            # Extract table name
            table_name = filename[len("ddl."):-len(".sql")]
            print(table_name)
            drop_table(table_name)
            print(f"{table_name} table dropped")

def drop_validation_statements(statement_names):
    for stmt_name in statement_names:       
        delete_statement_if_exists(stmt_name)


def execute_one_test(table_folder: str, test_case_name: str):
    definition = load_test_definition(table_folder)
    config = get_config()
    client = ConfluentCloudClient(config)
    statement_names = []
    print(f"**********************-Starting Validation Test Case-********************** ")

    for foundation in definition.foundations:
        ddl_path = os.path.join(table_folder, SCRIPTS_DIR, foundation.ddl_for_test)
        logger.info(f"Deploying foundation DDL: {foundation.ddl_for_test}")

        create_statement_name=f"{test_case_name.replace('_', '-')}-create-{foundation.table_name.replace('_', '-')}"
        statement_names.append(create_statement_name)
        # Create a new create statement if doesn't exist already
        if get_statement(create_statement_name) is None:
            deploy_flink_statement(ddl_path, None, create_statement_name , config)
            print( f"{create_statement_name} statement and table created")
        else:
            print( f"{create_statement_name} statement already exists")

    # Parse through test case suite in yaml file, run the test case provided in parameter
    for test_case in definition.test_suite:
        if test_case.name == test_case_name:
            logger.info(f"Running test case: {test_case.name}")

            # Create a new input statement if doesn't exist already
            for input_step in test_case.inputs:
                sql_path = os.path.join(table_folder, SCRIPTS_DIR, input_step.sql_file_name)
                logger.info(f"Executing input SQL: {input_step.sql_file_name}")
                insert_statement_name = f"{test_case_name.replace('_', '-')}-insert-{input_step.table_name.replace('_', '-')}"
                statement_names.append(insert_statement_name)

                if get_statement(insert_statement_name) is None:
                    deploy_flink_statement(sql_path, None,insert_statement_name ,config)
                    print( f"{insert_statement_name} statement created and source data inserted")
                else:
                    print(
                        f"{insert_statement_name} statement already exists")

            # Create validation statement if doesn't exist already
            for output_step in test_case.outputs:
                sql_path = os.path.join(table_folder, SCRIPTS_DIR, output_step.sql_file_name)
                logger.info(f"Executing validation SQL: {output_step.sql_file_name}")
                validate_statement_name = f"{test_case_name.replace('_', '-')}-validate"
                statement_names.append(validate_statement_name)

                if get_statement(validate_statement_name) is None:
                    deploy_flink_statement(sql_path, None, validate_statement_name, config)
                    print(f"{validate_statement_name} statement created")
                else:
                    print(f"{validate_statement_name} statement already exists")

                #Get result from the validation query
                print(statement_names)
                resp = None
                max_retries = 9
                retry_delay = 10  # seconds

                for poll in range(max_retries + 1):
                    try:
                        resp = client.get_statement_results(validate_statement_name)

                        # Check if results and data are non-empty
                        if resp and resp.results and resp.results.data:
                            print(f"Received results on poll {poll + 1}")
                            print(resp.results.data)
                            break  # success, exit retry loop
                        else:
                            print(f"Attempt {poll + 1}: Empty results, retrying in {retry_delay}s...")
                            time.sleep(retry_delay)

                    except Exception as e:
                        print(f"Attempt {poll + 1} failed with error: {e}")
                        time.sleep(retry_delay)

                #Check and print the result of the validation query
                if resp.results.data:
                    final_row = resp.results.data[-1].row[0]
                    print("Final Result:", final_row)
                    print(f"Final Result : {final_row}")
                else:
                    final_row= 'FAIL'
                    print( f"Final Result : FAIL")

                if final_row == "PASS" :
                    print(f"VALIDATION PASSED for {test_case_name}")
                    print( f"====================================VALIDATION PASSED for {test_case_name}====================================")
                else:
                    print(f"VALIDATION FAILED for {test_case_name}")
                    print( f"====================================VALIDATION FAILED for {test_case_name}====================================")


            #Drop validation statements and topics
            drop_validation_statements(statement_names)
            drop_validation_tables(table_folder)
            print(
                f"**********************-Validation Test Case Finished-********************** ")
            return True
    return False

def load_test_definition(table_folder: str) -> SLTestSuite:
    test_definitions = table_folder + "/tests/test_definitions.yaml"
    with open(test_definitions) as f:
          cfg_as_dict=yaml.load(f,Loader=yaml.FullLoader)
          print(cfg_as_dict)
          definition= SLTestSuite.model_validate(cfg_as_dict)
          return definition
