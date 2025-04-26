"""
Copyright 2024-2025 Confluent, Inc.
"""
from pydantic import BaseModel
from typing import List, Final, Optional, Dict, Tuple
import yaml
import time
import os

from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.flink_statement_model import Statement
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.utils.file_search import (
    FlinkTableReference,
    get_table_ref_from_inventory, 
    get_or_build_inventory,
    create_folder_if_not_exist,
    from_pipeline_to_absolute
)

SCRIPTS_DIR: Final[str] = "sql-scripts"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"
TEST_DEFINITION_FILE_NAME: Final[str] = "test_definitions.yaml"

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



# ----------- Public APIs  ------------------------------------------------------------
def init_unit_test_for_table(table_name: str):
    """
    Initialize the unit test folder and template files for a given table. It will parse the SQL statemnts to create the insert statements for the unit tests.
    """
    inventory_path = os.path.join(os.getenv("PIPELINES"),)
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    table_folder = table_ref.table_folder_name
    create_folder_if_not_exist(f"{table_folder}/tests")
    _add_test_files(table_ref, f"{table_folder}/tests", table_inventory)


def execute_one_test(table_name: str, test_case_name: str, compute_pool_id: Optional[str] = None):
    """
    Execute a single test case from the test suite definition.
    
    The function:
    1. Loads test suite definition from yaml file
    2. Creates foundation tables using DDL
    3. Executes input SQL statements to populate test data
    4. Runs validation SQL to verify results
    5. Polls validation results with retries
    """
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    statement_names = _execute_foundation_statements(test_suite_def, table_ref, prefix, compute_pool_id)
    statement_names.extend(_execute_statements_under_test(table_name, table_ref, prefix, compute_pool_id))

    client = ConfluentCloudClient(get_config())

    # Parse through test case suite in yaml file, run the test case provided in parameter
    for test_case in test_suite_def.test_suite:
        if test_case.name == test_case_name:
            logger.info(f"Running test case: {test_case.name}")
            statement_names.extend(_execute_one_testcase(test_case, table_ref, prefix, compute_pool_id))
            

                 #Drop validation statements and topics
            drop_validation_statements(client,statement_names)
            drop_validation_tables(table_folder)
            print(
                f"**********************-Validation Test Case Finished-********************** ")
            return True
    return False

def execute_all_tests(table_name: str, compute_pool_id: Optional[str] = None) -> List[Statement]:
    """
    Execute all test cases defined in the test suite definition for a given table.
    """
    print(f"**********************-Starting Validation Test Case-********************** ")
    
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    statements= _execute_foundation_statements(test_suite_def, table_ref, prefix, compute_pool_id)
      
    for test_case in test_suite_def.test_suite:
        statements.extend(_execute_one_testcase(test_case, table_ref, prefix, compute_pool_id))
    return statements


# ----------- Private APIs  ------------------------------------------------------------

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


def _execute_flink_test_statement(sql_content: str, statement_name: str, compute_pool_id: Optional[str] = None) -> Statement:
    """
    Executed the Flink statement and return the statement object.
    """
    logger.info(f"Run flink statement {statement_name}")
    statement = statement_mgr.get_statement_info(statement_name)
    if  not statement:
        try:
            statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_content)
            logger.debug(f"Executed flink test statement table {statement}")
            if statement:
                print(f"{statement_name} statement and table created")
            return statement
        except Exception as e:
            logger.error(e)
    else:
        print(f"{statement_name} statement already exists")
        return statement


    
def _load_test_suite_definition(table_name: str) -> Tuple[SLTestDefinition, FlinkTableReference]:
    inventory_path = os.path.join(os.getenv("PIPELINES"),)
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    table_folder = table_ref.table_folder_name
    # Load test suite definition from tests folder
    table_folder = from_pipeline_to_absolute(table_folder)
    test_definitions = table_folder + "/tests/" + TEST_DEFINITION_FILE_NAME
    try:
        with open(test_definitions) as f:
            cfg_as_dict=yaml.load(f,Loader=yaml.FullLoader)
            definition= SLTestDefinition.model_validate(cfg_as_dict)
            return definition, table_ref
    except FileNotFoundError:
            print(f"No test suite definition found in {table_folder}")
            raise ValueError(f"No test suite definition found in {table_folder}/tests")
 

def _execute_foundation_statements(test_suite_def: SLTestDefinition, 
                                   table_ref: FlinkTableReference, 
                                   prefix: str = 'dev',
                                   compute_pool_id: Optional[str] = None):

    statement_names = []
    table_folder = from_pipeline_to_absolute(table_ref.table_folder_name)
    for foundation in test_suite_def.foundations:
        testfile_path = os.path.join(table_folder, foundation.ddl_for_test)
        sql_content = _read_and_treat_sql_content(testfile_path, lambda x: x)
        statement_name = f"{prefix}-{table_ref.product_name}-ddl-{foundation.table_name.replace('_', '-').replace('.', '-')}-ut"
        statement = _execute_flink_test_statement(sql_content, statement_name, compute_pool_id)
        statement_names.append(statement)
    return statement_names

def _read_and_treat_sql_content(sql_path: str, fct) -> str:
    sql_path = from_pipeline_to_absolute(sql_path)
    with open(sql_path, "r") as f:
        return fct(f.read())
    
def _execute_statements_under_test(table_name: str, 
                                   table_ref: FlinkTableReference, 
                                   prefix: str = 'dev',
                                   compute_pool_id: Optional[str] = None) -> List[Statement]:
    
    def replace_table_name(sql_content: str) -> str:
        parser = SQLparser()
        table_names = parser.extract_table_references(sql_content)
        for table in table_names:
            print(f"table: {table}")
            sql_content = sql_content.replace(table, f"{table}_ut")
        return sql_content

    statement_names = []
    ddl_sql_content = _read_and_treat_sql_content(table_ref.ddl_ref, replace_table_name)
    statement_name = f"{prefix}-{table_ref.product_name}-ddl-{table_name.replace('_', '-').replace('.', '-')}-ut"
    statement = _execute_flink_test_statement(ddl_sql_content, statement_name, compute_pool_id)
    statement_names.append(statement)
    
    dml_sql_content = _read_and_treat_sql_content(table_ref.dml_ref, replace_table_name)
    statement_name = f"{prefix}-{table_ref.product_name}-dml-{table_name.replace('_', '-').replace('.', '-')}-ut"
    statement = _execute_flink_test_statement(dml_sql_content, statement_name, compute_pool_id)
    statement_names.append(statement)
    return statement_names

def _execute_one_testcase(test_case: SLTestCase, 
                          table_ref: FlinkTableReference, 
                          prefix: str = 'dev', 
                          compute_pool_id: Optional[str] = None):
    statement_names = []
    logger.info(f"Running test case: {test_case.name}")
    for input_step in test_case.inputs:
        sql_path = os.path.join(table_ref.table_folder_name, input_step.sql_file_name)
        logger.info(f"Executing input SQL: {input_step.sql_file_name}")
        sql_content = _read_and_treat_sql_content(sql_path, lambda x: x)
        statement_name = f"{prefix}-{table_ref.product_name}-{test_case.name.replace('_', '-')}-{input_step.table_name.replace('_', '-').replace('.', '-')}-ut"
        statement = _execute_flink_test_statement(sql_content, statement_name, compute_pool_id)
        statement_names.append(statement)
    # Create validation statement if doesn't exist already
    for output_step in test_case.outputs:
        sql_path = os.path.join(table_ref.table_folder_name, output_step.sql_file_name)
        logger.info(f"Executing validation SQL: {output_step.sql_file_name}")
        sql_content = _read_and_treat_sql_content(sql_path, lambda x: x)
        statement_name = f"{prefix}-{table_ref.product_name}-{test_case.name.replace('_', '-')}-validate-ut"
        statement = _execute_flink_test_statement(sql_content, statement_name, compute_pool_id)
        statement_names.append(statement)
        _poll_response(statement, test_case.name)
    return statement_names

def _poll_response(statement: Statement, test_case_name: str):
    #Get result from the validation query
    resp = None
    max_retries = 9
    retry_delay = 10  # seconds

    for poll in range(max_retries + 1):
        try:
            resp = statement_mgr.get_statement_results(statement.name)
            # Check if results and data are non-empty
            if resp and resp.result and resp.result.results and resp.result.results.data:
                print(f"Received results on poll {poll + 1}")
                print(resp.result.results.data)
                break
            else:
                print(f"Attempt {poll + 1}: Empty results, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
        except Exception as e:
            print(f"Attempt {poll + 1} failed with error: {e}")
            #time.sleep(retry_delay)
            break

    #Check and print the result of the validation query
    if resp and resp.result and resp.result.results and resp.result.results.data:
        final_row = resp.result.results.data[-1].row[0]
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
           
def _add_test_files(table_ref: FlinkTableReference, tests_folder: str, table_inventory: Dict[str, FlinkTableReference]):
    """
    Add the test files to the table folder.
    """
    file_path = from_pipeline_to_absolute(table_ref.dml_ref)
    referenced_table_names = None
    with open(file_path) as f:
        sql_content = f.read()
        parser = SQLparser()
        referenced_table_names = parser.extract_table_references(sql_content)
    if not referenced_table_names:
        logger.error(f"No referenced table names found in the sql_content of {file_path}")
        raise ValueError(f"No referenced table names found in the sql_content of {file_path}")

    tests_folder_path = from_pipeline_to_absolute(tests_folder)
    test_definition = _build_save_test_definition_json_file(tests_folder_path, table_ref.table_name, referenced_table_names)
    table_struct = _process_foundation_ddl_from_test_definitions(test_definition, tests_folder_path, table_inventory)
    
    # Create template files for each test case
    for test_case in test_definition.test_suite:
        # Create input files
        for input_data in test_case.inputs:
            input_file = os.path.join(tests_folder_path, '..', input_data.sql_file_name)
            columns_names, rows = _build_data_sample(table_struct[input_data.table_name])
            with open(input_file, "w") as f:
                f.write(f"insert into {input_data.table_name}_ut\n({columns_names})\nvalues\n{rows}\n")
            logger.info(f"Input file {input_file} created")

        # Create output validation files 
        for output_data in test_case.outputs:
            output_file = os.path.join(tests_folder_path, '..', output_data.sql_file_name) 
            with open(output_file, "w") as f:
                f.write(f"select * from {output_data.table_name}_ut")
    return test_definition

        
            
def _build_save_test_definition_json_file(file_path: str, table_name: str, referenced_table_names: List[str]) -> SLTestDefinition:
    test_definition :SLTestDefinition = SLTestDefinition(foundations=[], test_suite=[])
    for table in referenced_table_names:
        if table not in table_name:
            foundation_table_name = Foundation(table_name=table, ddl_for_test=f"./tests/ddl_{table}.sql")
            test_definition.foundations.append(foundation_table_name)
    for i in range(1, 3):
        test_case = SLTestCase(name=f"test_{table_name}_{i}", inputs=[], outputs=[])
        output = SLTestData(table_name=table_name, sql_file_name=f"./tests/validate_{table_name}_{i}.sql")
        for table in referenced_table_names:
            if table not in table_name: 
                input = SLTestData(table_name=table, sql_file_name=f"./tests/insert_{table}_{i}.sql")
                test_case.inputs.append(input)
        test_case.outputs.append(output)
        test_definition.test_suite.append(test_case)
    
    with open(f"{file_path}/{TEST_DEFINITION_FILE_NAME}", "w") as f:
        yaml.dump(test_definition.model_dump(), f, sort_keys=False)
    logger.info(f"Test definition file {file_path}/{TEST_DEFINITION_FILE_NAME} created")
    return test_definition

def _process_foundation_ddl_from_test_definitions(test_definition: SLTestDefinition, 
                             tests_folder_path: str, 
                             table_inventory: Dict[str, FlinkTableReference]) -> Dict[str, Dict[str, str]]:
    """
    Create a matching DDL statement for the referenced tables. Get the table columns structure.
    save the DDL for unit test to the tests folder.
    """
    table_struct = {}  # table_name -> {column_name -> column_metadata}
    for foundation in test_definition.foundations:
        input_table_ref: FlinkTableReference = FlinkTableReference.model_validate(table_inventory[foundation.table_name])
        ddl_sql_content = f"create table if not exists {foundation.table_name}_ut (\n\n)"
        file_path = from_pipeline_to_absolute(input_table_ref.ddl_ref)
        parser = SQLparser()
        with open(file_path, "r") as f:
            ddl_sql_content = f.read()
            columns = parser.build_column_metadata_from_sql_content(ddl_sql_content)  # column_name -> column_metadata
            ddl_sql_content = ddl_sql_content.replace(input_table_ref.table_name, f"{input_table_ref.table_name}_ut")
            table_struct[foundation.table_name] = columns
        ddl_file = os.path.join(tests_folder_path, '..', foundation.ddl_for_test)
        with open(ddl_file, "w") as f:
            f.write(ddl_sql_content)
    return table_struct

def _build_data_sample(columns: Dict[str, str]) -> Tuple[str, str]:
    """
    Returns a string of all columns names separated bu ',' so it can be used
    in the insert statement and a string of 5 rows of data sample.
    """
    columns_names = ""
    for column in columns:
        columns_names += f"{column}, "
    columns_names = columns_names[:-2]
    rows = ""
    for values in range(1,6):
        rows += "("
        for column in columns:
            if columns[column]['type'] == "BIGINT":
                rows += f"0, "
            else:
                rows += f"'{column}_{values}', "
        rows = rows[:-2]+ '),\n'
    rows = rows[:-2]+ ';\n'
    return columns_names, rows
