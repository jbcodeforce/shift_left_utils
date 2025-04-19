"""
Copyright 2024-2025 Confluent, Inc.
"""
from pydantic import BaseModel
from typing import List, Final
import yaml
import logging, time
import os
from logging.handlers import RotatingFileHandler
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.deployment_mgr import *
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
import shift_left.core.statement_mgr as statement_mgr
SCRIPTS_DIR: Final[str] = "sql-scripts"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"
TEST_DEFINITION_FILE_NAME: Final[str] = "test_definitions.yaml"

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

"""
Test manager defines what are test cases and test suites.
It also defines a set of functions to run test on Confluent Cloud
"""

class SLTestData(BaseModel):
    table_name: str
    sql_file_name: str

class SLTestCase(BaseModel):
    name: str
    inputs: List[SLTestData]
    outputs: List[SLTestData]

class Foundation(BaseModel):
    """
    represent the table to test and the ddl for the input tables to be created during tests.
    Those tables will be deleted after the tests are run.
    """
    table_name: str
    ddl_for_test: str

class SLTestDefinition(BaseModel):
    foundations: List[Foundation]
    test_suite: List[SLTestCase]

def log( table_folder,message):
    with open(f"{table_folder}/tests/result.txt", "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[{timestamp}] {message}\n")

# ----------- Public APIs  ------------------------------------------------------------
def execute_one_test(table_folder: str, test_case_name: str):
    """
    Execute a single test case from the test suite definition.
    
    Args:
        table_folder (str): Path to the folder containing the table definition and test files
        test_case_name (str): Name of the test case to execute from the test suite
        
    Returns:
        bool: True if test executes successfully
        
    The function:
    1. Loads test suite definition from yaml file
    2. Creates foundation tables using DDL
    3. Executes input SQL statements to populate test data
    4. Runs validation SQL to verify results
    5. Polls validation results with retries
    """
    test_suite_def = _load_test_suite_definition(table_folder)
    client = ConfluentCloudClient(get_config())
    statement_names = []

    
        #print(sql_content)

    # Parse through test case suite in yaml file, run the test case provided in parameter
    for test_case in test_suite_def.test_suite:
        if test_case.name == test_case_name:
            logger.info(f"Running test case: {test_case.name}")

            # Create a new input statement if doesn't exist already
            for input_step in test_case.inputs:
                sql_path = os.path.join(table_folder, SCRIPTS_DIR, input_step.sql_file_name)
                logger.info(f"Executing input SQL: {input_step.sql_file_name}")
                insert_statement_name = f"{test_case_name.replace('_', '-')}-{input_step.table_name.replace('_', '-').replace('.', '-')}-ut"
                statement_names.append(insert_statement_name)
                _create_test_tables(sql_path, insert_statement_name)

            # Create validation statement if doesn't exist already
            for output_step in test_case.outputs:
                sql_path = os.path.join(table_folder, SCRIPTS_DIR, output_step.sql_file_name)
                logger.info(f"Executing validation SQL: {output_step.sql_file_name}")
                validate_statement_name = f"{test_case_name.replace('_', '-')}-validate-ut"
                statement_names.append(validate_statement_name)
                _create_test_tables(sql_path, validate_statement_name)

                #Get result from the validation query
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
                           break
                       else:
                           print(f"Attempt {poll + 1}: Empty results, retrying in {retry_delay}s...")
                           time.sleep(retry_delay)
                   except Exception as e:
                       print(f"Attempt {poll + 1} failed with error: {e}")
                       #time.sleep(retry_delay)
                       break

               #Check and print the result of the validation query
                if resp and resp.results and resp.results.data:
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
            drop_validation_statements(client,statement_names)
            drop_validation_tables(table_folder)
            print(
                f"**********************-Validation Test Case Finished-********************** ")
            return True
    return False

def execute_all_tests(table_folder: str, test_case_name: str, compute_pool_id: Optional[str] = None):
    """
    Execute all test cases defined in the test suite definition for a given table.
    """
    print(f"**********************-Starting Validation Test Case-********************** ")
    statement_names=[]
    for foundation in test_suite_def.foundations:
        ddl_path = os.path.join(table_folder, SCRIPTS_DIR, foundation.ddl_for_test)
        logger.info(f"Deploying foundation DDL: {foundation.ddl_for_test}")

        # Create a new create statement if doesn't exist already
        statement_name = f"{test_case_name.replace('_', '-')}-create-{foundation.table_name.replace('_', '-').replace('.', '-')}-ut"
        _create_test_tables(ddl_path, statement_name)
        

    if not test_case_name:
        _run_foundations(table_folder, compute_pool_id, statement_names)
        test_suite_def = _load_test_suite_definition(table_folder)
        for test_case in test_suite_def.test_suite:
            execute_one_test(table_folder, test_case.name, compute_pool_id)
    else:
        _run_foundations(table_folder, compute_pool_id, statement_names)
        execute_one_test(table_folder, test_case_name, compute_pool_id)   

# ----------- Private APIs  ------------------------------------------------------------
def _run_foundations(table_folder: str, compute_pool_id: Optional[str] = None, statement_names: List[str] = []):
    """
    Run the foundation tables for a given table.
    """
    test_suite_def = _load_test_suite_definition(table_folder)
    for foundation in test_suite_def.foundations:
        statement_name = f"{foundation.table_name.replace('_', '-').replace('.', '-')}-ut"
        testfile_path = os.path.join(table_folder, foundation.ddl_for_test)
        _create_test_tables(testfile_path, statement_name, compute_pool_id)
        statement_names.append(statement_name)
         
def drop_validation_tables(table_folder):
    ddl_dir = os.path.join(table_folder, SCRIPTS_DIR)
    ddl_dir1 = os.path.join(table_folder, "tests")

    # Loop through files
    for directory in [ddl_dir, ddl_dir1]:
        full_dir = os.path.abspath(directory)
        for filename in os.listdir(full_dir):
            if filename.startswith(("ddl.", "ddl_")) and filename.endswith(".sql"):
                if filename.startswith("ddl."):
                    table_name = filename[len("ddl."): -len(".sql")] + "_ut"
                else:
                    table_name = filename[len("ddl_"): -len(".sql")] + "_ut"
                statement_mgr.drop_table(table_name)
                print(f"{table_name} table dropped")

def drop_validation_statements(client,statement_names):
    for stmt_name in statement_names:

        if client.get_statement_info(stmt_name):
            client.delete_flink_statement(stmt_name)
            print(f"Deleted statement {stmt_name}")
        else:
            print(f"Statement {stmt_name} doesn't exist")


def _create_test_tables(sql_path: str, statement_name: str, compute_pool_id: Optional[str] = None) -> str:
    """
    Creates test tables in Flink by executing SQL DDL statements.
    
    Args:
        sql_path (str): Path to the SQL file containing DDL statements
        statement_name (str): Name to give the Flink statement
        compute_pool_id (Optional[str]): ID of Flink compute pool to use. If not provided, uses config default.
    
    Returns:
        str: The SQL content that was executed
        
    The function:
    1. Reads SQL from file
    2. Extracts table names and appends "_ut" suffix for test tables
    3. Creates Flink statement to execute the DDL if it doesn't exist
    4. Returns the modified SQL content
    """
    logger.debug(f"Run create table {sql_path} {statement_name}")
    sql_content = _change_table_names_for_test_in_sql_content(sql_path)

    if statement_mgr.get_statement_info(statement_name) is None:
      try:
        statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_content)
        logger.debug(f"Run create table {statement}")
        logger.debug(f"Run create table {sql_content}")
        if statement:
            print(f"{statement_name} statement and table created")
      except Exception as e:
        logger.error(e)
    else:
      print(f"{statement_name} statement already exists")
    return sql_content




def _load_test_suite_definition(table_folder: str) -> SLTestDefinition:
    test_definitions = table_folder + "/tests/" + TEST_DEFINITION_FILE_NAME
    with open(test_definitions) as f:
          cfg_as_dict=yaml.load(f,Loader=yaml.FullLoader)
          definition= SLTestDefinition.model_validate(cfg_as_dict)
          return definition


def _change_table_names_for_test_in_sql_content(sql_file_path: str) -> str:
    with open(sql_file_path) as f:
        sql_content = f.read()

    parser = SQLparser()
    table_names = parser.extract_table_references(sql_content)
    print(f"Tables found {table_names}")

    for table in table_names:
        sql_content = sql_content.replace(table, f"{table}_ut")
    return sql_content