"""
Copyright 2024-2025 Confluent, Inc.

Test Manager - Automated testing framework for Flink SQL statements on Confluent Cloud.

Provides testing capabilities including:
- Unit test initialization and scaffolding for Flink tables
- Test execution with foundation tables, input data, and validation SQL statements
- Test artifact cleanup and management
- Integration with Confluent Cloud Flink REST API
- YAML-based test suite definitions and CSV test data support
"""
from pydantic import BaseModel, Field
from typing import List, Final, Optional, Dict, Tuple, Union, Any, Callable
import yaml
import time
import os
import json
import re
from jinja2 import Environment, PackageLoader
from datetime import datetime
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.models.flink_statement_model import Statement, StatementError, StatementResult
from shift_left.core.models.flink_test_model import SLTestDefinition, SLTestCase, TestResult, TestSuiteResult, Foundation, SLTestData
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.utils.file_search import (
    FlinkTableReference,
    get_table_ref_from_inventory, 
    get_or_build_inventory,
    create_folder_if_not_exist,
    from_pipeline_to_absolute
)
from shift_left.core.utils.ut_ai_data_tuning import AIBasedDataTuning

SCRIPTS_DIR: Final[str] = "sql-scripts"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"
TEST_DEFINITION_FILE_NAME: Final[str] = "test_definitions.yaml"
DEFAULT_POST_FIX_UNIT_TEST="_ut"
CONFIGURED_POST_FIX_UNIT_TEST: Final[str] = get_config().get('app').get('post_fix_unit_test',DEFAULT_POST_FIX_UNIT_TEST)
TOPIC_LIST_FILE: Final[str] = shift_left_dir + "/topic_list.json"

# Polling and retry configuration constants
MAX_POLLING_RETRIES: Final[int] = 7
POLLING_RETRY_DELAY_SECONDS: Final[int] = 10

# SQL generation limits
MAX_SQL_CONTENT_SIZE_BYTES: Final[int] = 4_000_000  # 4MB limit for SQL content

# Statement naming constraints
MAX_STATEMENT_NAME_LENGTH: Final[int] = 52  # Maximum characters for statement name

# Test data generation constants
DEFAULT_TEST_DATA_ROWS: Final[int] = 3 # Number of sample rows to generate
DEFAULT_TEST_CASES_COUNT: Final[int] = 2  # Number of test cases to create by default



class TopicListCache(BaseModel):
    created_at: Optional[datetime] = Field(default=datetime.now())
    topic_list: Optional[dict[str, str]] = Field(default={})

# ----------- Public APIs  ------------------------------------------------------------
def init_unit_test_for_table(table_name: str, 
        create_csv: bool = False, 
        nb_test_cases: int = DEFAULT_TEST_CASES_COUNT,
        use_ai: bool = False) -> None:
    """
    Initialize the unit test folder and template files for a given table. It will parse the SQL statemnts to create the insert statements for the unit tests.
    """
    inventory_path = os.path.join(os.getenv("PIPELINES"),)
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    table_folder = table_ref.table_folder_name
    test_folder_path = from_pipeline_to_absolute(table_folder)
    create_folder_if_not_exist(f"{test_folder_path}/tests")
    _add_test_files(table_to_test_ref=table_ref, 
                    tests_folder=f"{table_folder}/tests", 
                    table_inventory=table_inventory,
                    create_csv=create_csv,
                    nb_test_cases=nb_test_cases,
                    use_ai=use_ai)
    logger.info(f"Unit test for {table_name} created")
    print(f"Unit test for {table_name} created")

def execute_one_or_all_tests(table_name: str, 
                      test_case_name: str = None, 
                      compute_pool_id: Optional[str] = None,
                      run_validation: bool = False
) -> TestSuiteResult:
    """
    Execute all test cases defined in the test suite definition for a given table.
    1. Loads test suite definition from yaml file
    2. Creates foundation tables using DDL
    3. Executes input SQL statements to populate test data
    4. Runs validation SQL to verify results
    5. Polls validation results with retries
    """
    statement_mgr.reset_statement_list()
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    test_suite_def, table_ref, prefix, test_result = _init_test_foundations(table_name, "", compute_pool_id, prefix)
    
    test_suite_result = TestSuiteResult(foundation_statements=test_result.foundation_statements, test_results={})

    for idx, test_case in enumerate(test_suite_def.test_suite):
        # loop over all the test cases of the test suite
        if test_case_name and test_case.name != test_case_name:
            continue
        statements = _execute_test_inputs(test_case=test_case,
                                        table_ref=table_ref,
                                        prefix=prefix+"-ins-"+str(idx + 1),
                                        compute_pool_id=compute_pool_id)
        test_result = TestResult(test_case_name=test_case.name, result="insertion done")
        test_result.statements.extend(statements)
        if run_validation:
            statements, result_text, statement_result = _execute_test_validation(test_case=test_case,
                                                                table_ref=table_ref,
                                                                prefix=prefix+"-val-"+str(idx + 1),
                                                                compute_pool_id=compute_pool_id)
            test_result.result = result_text
            test_result.statements.extend(statements)
            test_result.validation_result = statement_result
        test_suite_result.test_results[test_case.name] = test_result
        if test_case_name and test_case.name == test_case_name:
            break
    return test_suite_result


def execute_validation_tests(table_name: str, 
                    test_case_name: str = None,
                    compute_pool_id: Optional[str] = None
) -> TestSuiteResult:
    """
    Execute all validation tests defined in the test suite definition for a given table.
    """
    logger.info(f"Execute validation tests for {table_name}")
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    test_suite_result = TestSuiteResult(foundation_statements=[], test_results={})
    for idx, test_case in enumerate(test_suite_def.test_suite):
        if test_case_name and test_case.name != test_case_name:
            continue
        statements, result_text, statement_result = _execute_test_validation(test_case=test_case,
                                                                    table_ref=table_ref,
                                                                    prefix=prefix+"-val-"+str(idx + 1),
                                                                    compute_pool_id=compute_pool_id)
        test_suite_result.test_results[test_case.name] = TestResult(test_case_name=test_case.name, result=result_text, validation_result=statement_result)
        if test_case_name and test_case.name == test_case_name:
            break
    return test_suite_result

def delete_test_artifacts(table_name: str, 
                          compute_pool_id: Optional[str] = None) -> None:
    """
    Delete the test artifacts (foundations, inserts, validations and statements) for a given table.
    """
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    statement_mgr.get_statement_list()

    config = get_config()
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    prefix = config['kafka']['cluster_type']
    for idx, test_case in enumerate(test_suite_def.test_suite):
        logger.info(f"Deleting test artifacts for {test_case.name}")
        print(f"Deleting test artifacts for {test_case.name}")
        for output in test_case.outputs:
            statement_name = _build_statement_name(output.table_name, prefix+"-val-"+str(idx + 1))
            statement_mgr.delete_statement_if_exists(statement_name)
            logger.info(f"Deleted statement {statement_name}")
            print(f"Deleted statement {statement_name}")
        for input in test_case.inputs:
            statement_name = _build_statement_name(input.table_name, prefix+"-ins-"+str(idx + 1))
            statement_mgr.delete_statement_if_exists(statement_name)
            logger.info(f"Deleted statement {statement_name}")
            print(f"Deleted statement {statement_name}")
    logger.info(f"Deleting ddl and dml artifacts for {table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
    print(f"Deleting ddl and dml artifacts for {table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
    statement_name = _build_statement_name(table_name, prefix+"-dml")
    statement_mgr.delete_statement_if_exists(statement_name)
    statement_name = _build_statement_name(table_name, prefix+"-ddl")
    statement_mgr.delete_statement_if_exists(statement_name)
    statement_mgr.drop_table(table_name+CONFIGURED_POST_FIX_UNIT_TEST, compute_pool_id)
    for foundation in test_suite_def.foundations:
        logger.info(f"Deleting ddl and dml artifacts for {foundation.table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
        statement_name = _build_statement_name(foundation.table_name, prefix+"-ddl")
        statement_mgr.delete_statement_if_exists(statement_name)
        statement_mgr.drop_table(foundation.table_name+CONFIGURED_POST_FIX_UNIT_TEST, compute_pool_id)
    logger.info(f"Test artifacts for {table_name} deleted")



# ----------- Private APIs  ------------------------------------------------------------

def _init_test_foundations(table_name: str, 
        test_case_name: str, 
        compute_pool_id: Optional[str] = None,
        prefix: str = "dev"
) -> Tuple[SLTestDefinition, FlinkTableReference, str, TestResult]:
    """
    Create input tables as defined in the test suite foundations for the given table.
    And modify the dml of the given table to use the input tables for the unit tests.
    """
    print("-"*60)
    print(f"1. Create table foundations for unit tests for {table_name}")
    print("-"*60)
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    test_result = TestResult(test_case_name=test_case_name, result="")
    test_result.foundation_statements = _execute_foundation_statements(test_suite_def, 
                                            table_ref, 
                                            prefix, 
                                            compute_pool_id)
    test_result.foundation_statements=_start_ddl_dml_for_flink_under_test(table_name, 
                                            table_ref, 
                                            prefix, 
                                            compute_pool_id, 
                                            statements=test_result.foundation_statements)
    return test_suite_def, table_ref, prefix, test_result


def _execute_flink_test_statement(
        sql_content: str, 
        statement_name: str, 
        compute_pool_id: Optional[str] = None,
        product_name: Optional[str] = None  
) -> Tuple[Optional[Statement], bool]:
    """
    Execute the Flink statement and return the statement object and whether it was newly created.
    Returns (statement, is_new) where is_new indicates if this was a newly executed statement.
    """
    logger.info(f"Run flink statement {statement_name}")
    statement = statement_mgr.get_statement(statement_name)
    
    # Check if we need to create/execute the statement
    should_execute = False
    if statement is None:
        should_execute = True
    elif isinstance(statement, StatementError):
        # Handle StatementError - check if it's a 404 or similar
        if hasattr(statement, 'errors') and statement.errors and len(statement.errors) > 0:
            # Statement exists but has errors, might need to recreate
            if statement.errors[0].status == '404':
                should_execute = True
        else:
            # StatementError without errors attribute or empty errors
            should_execute = True
    elif isinstance(statement, Statement):
        # Statement exists - check if it's running
        if statement.status and statement.status.phase != "RUNNING":
            should_execute = True
        else:
            print(f"{statement_name} statement already exists -> do nothing")
            return statement, False  # Return existing statement, not new
    
    if should_execute:
        try:
            config = get_config()
            column_name_to_select_from = config['app']['data_limit_column_name_to_select_from']
            transformer = statement_mgr.get_or_build_sql_content_transformer()
            _, sql_out= transformer.update_sql_content(sql_content, column_name_to_select_from, product_name)
            post_statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_out)
            logger.debug(f"Executed flink test statement table {post_statement}")
            return post_statement, True  # Return new statement
        except Exception as e:
            logger.error(e)
            raise e
    else:
        # Return existing statement if available but it needs processing
        if isinstance(statement, Statement):
            return statement, False
        else:
            logger.error(f"Error executing test statement {statement_name}")
            raise ValueError(f"Error executing test statement {statement_name}")
    
def _load_test_suite_definition(table_name: str) -> Tuple[SLTestDefinition, FlinkTableReference]:
    inventory_path = os.path.join(os.getenv("PIPELINES"),)
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    if not table_ref:
        raise ValueError(f"Table {table_name} not found in inventory")  

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
    except Exception as e:
        logger.error(f"Error loading test suite definition: {e}")
        raise e
 

def _execute_foundation_statements(
    test_suite_def: SLTestDefinition, 
    table_ref: FlinkTableReference, 
    prefix: str = 'dev',
    compute_pool_id: Optional[str] = None
) -> List[Statement]:
    """
    Execute the DDL statements for the foundation tables for the unit tests.
    """

    statements = []
    table_folder = from_pipeline_to_absolute(table_ref.table_folder_name)
    for foundation in test_suite_def.foundations:
        testfile_path = os.path.join(table_folder, foundation.ddl_for_test)
        statements = _load_sql_and_execute_statement(table_name=foundation.table_name,
                                    sql_path=testfile_path,
                                    prefix=prefix+"-ddl",
                                    compute_pool_id=compute_pool_id,
                                    fct=_replace_table_name_ut_with_configured_postfix,
                                    product_name=table_ref.product_name,
                                    statements=statements)
    return statements

def _read_and_treat_sql_content_for_ut(sql_path: str, 
                                    fct: Callable[[str], str],
                                    table_name: str) -> str:
    """
    Read the SQL content from the file and apply the given function fct() to the content.
    """
    sql_path = from_pipeline_to_absolute(sql_path)
    with open(sql_path, "r") as f:
        return fct(f.read(), table_name)


    
def _start_ddl_dml_for_flink_under_test(table_name: str, 
                                   table_ref: FlinkTableReference, 
                                   prefix: str = 'dev',
                                   compute_pool_id: Optional[str] = None,
                                   statements: Optional[List[Statement]] = None
) -> List[Statement]:
    """
    Run DDL and DML statements for the given tested table
    """
    def replace_table_name(sql_content: str, table_name: str) -> str:
        sql_parser = SQLparser()
        table_names =  sql_parser.extract_table_references(sql_content)
    
        # Sort table names by length (descending) to avoid substring replacement issues
        # This ensures longer table names are replaced first, preventing partial matches
        sorted_table_names = sorted(table_names, key=len, reverse=True)
        
        for table in sorted_table_names:
            # Use regex with capturing groups to preserve backticks
            # This pattern specifically handles backticks vs word boundaries separately
            escaped_table = re.escape(table)
            
            # Handle backticked table names
            backtick_pattern = r'`(' + escaped_table + r')`'
            if table+DEFAULT_POST_FIX_UNIT_TEST in sql_content:
                sql_content = re.sub(backtick_pattern, f'`{table}{CONFIGURED_POST_FIX_UNIT_TEST}`', sql_content, flags=re.IGNORECASE)
            else:
                sql_content = re.sub(backtick_pattern, f'`{table}`', sql_content, flags=re.IGNORECASE)
            # Handle non-backticked table names with word boundaries
            word_pattern = r'\b(' + escaped_table + r')\b'
            
            def replacement_func(match):
                table_name = match.group(1)
                return f"{table_name}{CONFIGURED_POST_FIX_UNIT_TEST}"
            
            sql_content = re.sub(word_pattern, replacement_func, sql_content, flags=re.IGNORECASE)
        logger.info(f"Replaced table names: {sorted_table_names} in SQL content: {sql_content}")
        return sql_content

    # Initialize statements list if None
    if statements is None:
        statements = []
    
    # Process DDL
    ddl_result = _load_sql_and_execute_statement(table_name=table_name,
                                sql_path=table_ref.ddl_ref,
                                prefix=prefix+"-ddl",
                                compute_pool_id=compute_pool_id,
                                fct=replace_table_name,
                                product_name=table_ref.product_name,
                                statements=statements)
    if ddl_result is not None:
        statements = ddl_result
    
    # Process DML
    dml_result = _load_sql_and_execute_statement(table_name=table_name,
                                sql_path=table_ref.dml_ref,
                                prefix=prefix+"-dml",
                                compute_pool_id=compute_pool_id,
                                fct=replace_table_name,
                                product_name=table_ref.product_name,
                                statements=statements)
    if dml_result is not None:
        statements = dml_result
        
    return statements

def _load_sql_and_execute_statement(table_name: str, 
                                sql_path: str, 
                                prefix: str = 'dev-ddl', 
                                compute_pool_id: Optional[str] = None,
                                fct: Callable[[str], str] = lambda x: x,
                                product_name: Optional[str] = None,
                                statements: Optional[List[Statement]] = None) -> Optional[List[Statement]]:
 
    # Initialize statements list if None
    if statements is None:
        statements = []
    
    statement_name = _build_statement_name(table_name, prefix, CONFIGURED_POST_FIX_UNIT_TEST)
    table_under_test_exists = _table_exists(table_name+CONFIGURED_POST_FIX_UNIT_TEST)
    if "ddl" in prefix and table_under_test_exists:
        return statements  # Return same list when DDL table already exists

    # Check if statement already exists and is running
    statement_info = statement_mgr.get_statement_info(statement_name)
    if statement_info and statement_info.status_phase in ["RUNNING", "COMPLETED"]:
        print(f"Statement {statement_name} already exists and is running -> do nothing")
        return statements  # Return same list when statement is already running

    sql_content = _read_and_treat_sql_content_for_ut(sql_path, fct, table_name)
    logger.info(f"Execute statement {statement_name} table: {table_name}{CONFIGURED_POST_FIX_UNIT_TEST} using {sql_path} on: {compute_pool_id}")
    print(f"Execute statement {statement_name} table: {table_name} using {sql_path} on: {compute_pool_id}")
    statement, is_new = _execute_flink_test_statement(sql_content=sql_content   , 
                                                      statement_name=statement_name, 
                                                      compute_pool_id=compute_pool_id, 
                                                      product_name=product_name)
    if statement and is_new:
        statements.append(statement)
        if statement.status and statement.status.phase == "FAILED":
            logger.error(f"{statement.status}")
            print(f"Failed to create test foundations for {table_name}.. {statement.status.detail}")
            raise ValueError(f"Failed to create test foundations for {table_name}.. {statement.status.detail}")
        else:
            print(f"Executed test foundations for {table_name}{CONFIGURED_POST_FIX_UNIT_TEST}.. {statement.status.phase}\n")
    return statements

def _execute_test_inputs(test_case: SLTestCase, 
                        table_ref: FlinkTableReference, 
                        prefix: str = 'dev', 
                        compute_pool_id: Optional[str] = None
) -> List[Statement]:
    """
    Execute the input and validation SQL statements for a given test case.
    """
    logger.info(f"Run insert statements for: {test_case.name}")
    print("-"*40)
    print(f"2. Create insert into statements for unit test {test_case.name}")
    print("-"*40)
    statements = []
    for input_step in test_case.inputs:
        statement = None
        print(f"Run insert test data for {input_step.table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
        if input_step.file_type == "sql":
            sql_path = os.path.join(table_ref.table_folder_name, input_step.file_name)
            statements = _load_sql_and_execute_statement(table_name=input_step.table_name,
                                        sql_path=sql_path,
                                        prefix=prefix,
                                        compute_pool_id=compute_pool_id,
                                        fct=_replace_table_name_ut_with_configured_postfix,
                                        product_name=table_ref.product_name,
                                        statements=statements)
        elif input_step.file_type == "csv":
            sql_path = os.path.join(table_ref.table_folder_name, input_step.file_name)
            sql_path = from_pipeline_to_absolute(sql_path)
            headers, rows = _read_csv_file(sql_path)
            sql = _transform_csv_to_sql(input_step.table_name+CONFIGURED_POST_FIX_UNIT_TEST, headers, rows)
            print(f"Execute test input {sql}")
            statement_name = _build_statement_name(input_step.table_name, prefix)
            
            statement, is_new = _execute_flink_test_statement(sql_content=sql, 
                                                      statement_name=statement_name,
                                                      product_name=table_ref.product_name,
                                                      compute_pool_id=compute_pool_id)
            if statement and isinstance(statement, Statement) and is_new:
                print(f"Executed test input {statement.status.phase}")
                statements.append(statement)
            elif not statement:
                logger.error(f"Error executing test input for {input_step.table_name}")
                raise ValueError(f"Error executing test input for {input_step.table_name}")
        else:
            logger.error(f"Error in test input file type for {input_step.table_name}")
            raise ValueError(f"Error in test input file type for {input_step.table_name}")
    return statements

def _execute_test_validation(test_case: SLTestCase, 
                          table_ref: FlinkTableReference, 
                          prefix: str = 'dev', 
                          compute_pool_id: Optional[str] = None
) -> Tuple[List[Statement], str, Optional[StatementResult]]:
    print("-"*40)
    print(f"3. Run validation SQL statements for unit test {test_case.name}")
    print("-"*40)
    statements = []
    result_text = ""
    for output_step in test_case.outputs:
        sql_path = os.path.join(table_ref.table_folder_name, output_step.file_name)
        statement_name = _build_statement_name(output_step.table_name, prefix)
        statement_mgr.delete_statement_if_exists(statement_name)
        statements = _load_sql_and_execute_statement(table_name=output_step.table_name,
                                    sql_path=sql_path,
                                    prefix=prefix,
                                    compute_pool_id=compute_pool_id,
                                    fct=_replace_table_name_ut_with_configured_postfix,
                                    statements=statements)
        result, statement_result=_poll_response(statement_name)
        result_text+=result
    return statements,result_text, statement_result
    
def _poll_response(statement_name: str) -> Tuple[str, Optional[StatementResult]]:
    #Get result from the validation query
    resp = None
    max_retries = MAX_POLLING_RETRIES
    retry_delay = POLLING_RETRY_DELAY_SECONDS

    for poll in range(1, max_retries):
        try:
            # Check statement status first
            statement = statement_mgr.get_statement(statement_name)
            if (statement and hasattr(statement, 'status') and statement.status and 
                statement.status.phase == "FAILED"):
                logger.info(f"Statement {statement_name} failed, stopping polling")
                print(f"Statement {statement_name} failed: {statement.status.detail if statement.status.detail else 'No details available'}")
                break
                
            resp = statement_mgr.get_statement_results(statement_name)
            # Check if results and data are non-empty
            if resp and resp.results and resp.results.data:
                logger.info(f"Received results on poll {poll}")
                logger.info(f"resp: {resp}")
                logger.info(resp.results.data)
                break
            elif resp:
                logger.info(f"Attempt {poll}: Empty results, retrying in {retry_delay}s...")
                print(f"... wait for result to {statement_name}")
                time.sleep(retry_delay)
        except Exception as e:
            logger.info(f"Attempt {poll} failed with error: {e}")
            print(f"Attempt {poll} failed with error: {e}")
            #time.sleep(retry_delay)
            break

    #Check and print the result of the validation query
    final_row= 'FAIL'
    if resp and resp.results and resp.results.data:
        # Take the last record in the list
        final_row = resp.results.data[len(resp.results.data) - 1].row[0]
    logger.info(f"Final Result for {statement_name}: {final_row}")
    print(f"Final Result for {statement_name}: {final_row}")
    return final_row, resp

def _add_test_files(table_to_test_ref: FlinkTableReference, 
                    tests_folder: str, 
                    table_inventory: Dict[str, FlinkTableReference],
                    create_csv: bool = False,
                    nb_test_cases: int = DEFAULT_TEST_CASES_COUNT,
                    use_ai: bool = False) -> SLTestDefinition:
    """
    Add the test files to the table/tests folder by looking at the referenced tables in the DML SQL content.
    """
    dml_file_path = from_pipeline_to_absolute(table_to_test_ref.dml_ref)
    referenced_table_names: Optional[List[str]] = None
    primary_keys: List[str] = []
    sql_content = ""
    with open(dml_file_path) as f:
        sql_content = f.read()
        parser = SQLparser()
        referenced_table_names = parser.extract_table_references(sql_content)
        primary_keys = parser.extract_primary_key_from_sql_content(sql_content)
        # keep as print for user feedback
        print(f"From the DML under test the referenced table names are: {referenced_table_names}")
    if not referenced_table_names:
        logger.error(f"No referenced table names found in the sql_content of {dml_file_path}")
        raise ValueError(f"No referenced table names found in the sql_content of {dml_file_path}")

    tests_folder_path = from_pipeline_to_absolute(tests_folder)
    test_definition = _build_save_test_definition_json_file(tests_folder_path, table_to_test_ref.table_name, referenced_table_names, create_csv, nb_test_cases)
    table_struct = _process_foundation_ddl_from_test_definitions(test_definition, tests_folder_path, table_inventory)
    
    # Create template files for each test case
    for test_case in test_definition.test_suite:
        # Create input files
        for input_data in test_case.inputs:
            if input_data.file_type == "sql":
                input_file = os.path.join(tests_folder_path, '..', input_data.file_name)
                columns_names, rows = _build_data_sample(table_struct[input_data.table_name])
                with open(input_file, "w") as f:
                    f.write(f"insert into {input_data.table_name}{DEFAULT_POST_FIX_UNIT_TEST}\n({columns_names})\nvalues\n{rows}\n")
            if input_data.file_type == "csv":
                input_file = os.path.join(tests_folder_path, '..', input_data.file_name)
                columns_names, rows = _build_data_sample(table_struct[input_data.table_name], DEFAULT_TEST_DATA_ROWS)
                rows=rows[:-2].replace("),", "").replace("(", "").replace(")", "")
                with open(input_file, "w") as f:
                    f.write(columns_names+"\n")
                    f.write(rows)
            logger.info(f"Input file {input_file} created")
            print(f"Input file {input_file} created")

        # Create output validation files 
        for output_data in test_case.outputs:
            output_file = os.path.join(tests_folder_path, '..', output_data.file_name)
            validation_sql_content = _build_validation_sql_content(output_data.table_name, test_definition)
            with open(output_file, "w") as f:
                f.write(validation_sql_content)
        if use_ai:
            _add_data_consistency_with_ai(table_to_test_ref.table_folder_name, test_definition, sql_content, test_case.name)

    _generate_test_readme(table_to_test_ref, test_definition, primary_keys, tests_folder_path)
    return test_definition


def _generate_test_readme(table_ref: FlinkTableReference, 
    test_definition: SLTestDefinition, 
    primary_keys: List[str],
    tests_folder_path: str):        
 
    config = get_config()
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    template = env.get_template("basic_test_readme_template.md")
    source_tables = []
    for foundation in test_definition.foundations:
        source_tables.append(foundation.table_name)
    context = {
        'table_name': table_ref.table_name,
        'num_input_tables': len(test_definition.foundations),
        'primary_keys': primary_keys,
        'has_unbounded_joins': True,
        'source_tables': source_tables,
        'environment': config['confluent_cloud']['environment_id'],
    }
    rendered_readme_md = template.render(context)
    with open(tests_folder_path + '/README.md', 'w') as f:
        f.write(rendered_readme_md)

def _build_validation_sql_content(table_name: str,  test_definition: SLTestDefinition) -> str:
    """
    Build the validation SQL content for a given table.
    """
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    template = env.get_template("validate_test.jinja")
    context = {
        'table_name': table_name + DEFAULT_POST_FIX_UNIT_TEST
    }
    sql_content = template.render(context)
    return sql_content

def _build_save_test_definition_json_file(
        file_path: str, 
        table_name: str, 
        referenced_table_names: List[str],
        create_csv: bool = False,
        nb_test_cases: int = DEFAULT_TEST_CASES_COUNT
) -> SLTestDefinition:
    """
    Build the test definition file for the unit tests.
    When create csv then the second test has csv based input.
    for n input there is only on output per test case.
    table_name is the table under test.
    file_path is the path to the test definition file.
    referenced_table_names is the list of tables referenced in the dml under test.
    """
    test_definition :SLTestDefinition = SLTestDefinition(foundations=[], test_suite=[])
    for input_table in referenced_table_names:
        if input_table not in table_name:
            foundation_table_name = Foundation(table_name=input_table, ddl_for_test=f"./tests/ddl_{input_table}.sql")
            test_definition.foundations.append(foundation_table_name)
    for i in range(1, nb_test_cases + 1):
        test_case = SLTestCase(name=f"test_{table_name}_{i}", inputs=[], outputs=[])
        for input_table in referenced_table_names:
            if input_table not in table_name:
                if i % 2 == 1 or not create_csv and i%2 == 0:  
                    input = SLTestData(table_name=input_table, file_name=f"./tests/insert_{input_table}_{i}.sql",file_type="sql")
                elif create_csv:
                    input = SLTestData(table_name=input_table, file_name=f"./tests/insert_{input_table}_{i}.csv",file_type="csv")
                test_case.inputs.append(input)
        output = SLTestData(table_name=table_name, file_name=f"./tests/validate_{table_name}_{i}.sql",file_type="sql")
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
    Create a matching DDL statement for the referenced tables. Get the table columns structure for future 
    data generation step.
    save the DDL for unit test to the tests folder.
    """
    table_struct = {}  # table_name -> {column_name -> column_metadata}
    for foundation in test_definition.foundations:
        input_table_ref: FlinkTableReference = FlinkTableReference.model_validate(table_inventory[foundation.table_name])
        ddl_sql_content = f"create table if not exists {foundation.table_name}{DEFAULT_POST_FIX_UNIT_TEST} (\n\n)"
        file_path = from_pipeline_to_absolute(input_table_ref.ddl_ref)
        parser = SQLparser()
        with open(file_path, "r") as f:
            ddl_sql_content = f.read()
            columns = parser.build_column_metadata_from_sql_content(ddl_sql_content)  # column_name -> column_metadata
            ddl_sql_content = ddl_sql_content.replace(input_table_ref.table_name, f"{input_table_ref.table_name}{DEFAULT_POST_FIX_UNIT_TEST}")
            table_struct[foundation.table_name] = columns
        ddl_file = os.path.join(tests_folder_path, '..', foundation.ddl_for_test)
        with open(ddl_file, "w") as f:
            f.write(ddl_sql_content)
    return table_struct

def _build_data_sample(columns: Dict[str, Dict[str, Any]], idx_offset: int = 0) -> Tuple[str, str]:
    """
    Returns a string of all columns names separated by ',' so it can be used
    in the insert statement and a string of DEFAULT_TEST_DATA_ROWS rows of data sample
    matching the column order and types.
    """
    columns_names = ""
    for column in columns:
        columns_names += f"`{column}`, "
    columns_names = columns_names[:-2]
    rows = ""
    for idx in range(1+idx_offset, DEFAULT_TEST_DATA_ROWS + 1 + idx_offset):
        rows += "("
        for column in columns:
            col_type = columns[column]['type']
            if col_type == "BIGINT" or "INT" in col_type or col_type == "FLOAT" or col_type == "DOUBLE":
                rows += f"0, "
            elif col_type == 'BOOLEAN':
                if idx % 2 == 0:
                    rows += 'true, '
                else:
                    rows += 'false, '
            elif col_type == ' ARRAY<STRING>':
                rows += f"['{column}_{idx}'], "
            elif "TIMESTAMP" in col_type:
                rows += f"TIMESTAMP '2021-01-01 00:00:00', "
            else: # string
                rows += f"'{column}_{idx}', "
            
        rows = rows[:-2]+ '),\n'
    rows = rows[:-2]+ ';\n'
    return columns_names, rows


_topic_list_cache: Optional[TopicListCache] = None
def _table_exists(table_name: str) -> bool:
    """
    Check if the table/topic exists in the cluster.
    """
    global _topic_list_cache
    if _topic_list_cache == None:
        reload = True
        if os.path.exists(TOPIC_LIST_FILE):
            try:
                with open(TOPIC_LIST_FILE, "r") as f:
                    _topic_list_cache = TopicListCache.model_validate(json.load(f))
                if _topic_list_cache.created_at and (datetime.now() - datetime.strptime(_topic_list_cache.created_at, "%Y-%m-%d %H:%M:%S")).total_seconds() < get_config()['app']['cache_ttl']:
                    reload = False
            except Exception as e:
                logger.error(f"Error loading topic list: {e}")
                reload = True
                os.remove(TOPIC_LIST_FILE)
        if reload:
            _topic_list_cache = TopicListCache(created_at=datetime.now())
            ccloud = ConfluentCloudClient(get_config())
            topics = ccloud.list_topics()
            if topics and topics.get('data'):
                _topic_list_cache.topic_list = [topic['topic_name'] for topic in topics['data']]
            else:
                _topic_list_cache.topic_list = []
            logger.debug(f"Topic list: {_topic_list_cache}")
            with open(TOPIC_LIST_FILE, "w") as f:
                f.write(_topic_list_cache.model_dump_json(indent=2, warnings=False))
    return table_name in _topic_list_cache.topic_list


def _read_csv_file(file_path: str) -> Tuple[str, List[str]]:
    """
    Read the CSV file and return the content as a string.
    """
    rows = []
    with open(file_path, "r") as f:
        for line in f:
            rows.append(line.rstrip('\n'))
    return rows[0], rows[1:]

def _transform_csv_to_sql(table_name: str, 
                          headers: str, 
                          rows: List[str]) -> str:
    """
    Transform the CSV data to a SQL insert statement.
    """
    sql_content =f"insert into {table_name} ({headers}) values\n"
    current_size = len(sql_content)
    for row in rows:
        sql_content += f"({row}),\n"
        current_size += len(sql_content)
        if current_size > MAX_SQL_CONTENT_SIZE_BYTES:
            sql_content = sql_content[:-2] + ";\n"
            current_size = len(sql_content)
            return sql_content
    sql_content = sql_content[:-2] + ";\n"
    return sql_content

def _build_statement_name(table_name: str, prefix: str, post_fix_ut: str = CONFIGURED_POST_FIX_UNIT_TEST) -> str:
    _table_name_for_statement = table_name
    if len(_table_name_for_statement) > MAX_STATEMENT_NAME_LENGTH:
        _table_name_for_statement = _table_name_for_statement[:MAX_STATEMENT_NAME_LENGTH]    
    statement_name = f"{prefix}-{_table_name_for_statement}{post_fix_ut}"
    return statement_name.replace('_', '-').replace('.', '-')

def _replace_table_name_ut_with_configured_postfix(sql_content: str, table_name: str) -> str:
    
    return sql_content.replace(table_name+DEFAULT_POST_FIX_UNIT_TEST, f"{table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")

def _add_data_consistency_with_ai(table_folder_name: str, 
    test_definition: SLTestDefinition, 
    dml_sql_content: str, 
    test_case_name: str = None
):
    """
    Add data consistency with AI:
    1/ from the dml content ask to keep integrity of the data for the joins logic and primary keys
    2/ search for the insert sql statment and the validate sql statement and ask to keep the data consistency
    """

    agent = AIBasedDataTuning()
    output_data_list = agent.enhance_test_data(table_folder_name, dml_sql_content, test_definition, test_case_name)
    for output_data in output_data_list:
        if output_data.file_name:
            logger.info(f"Update insert sql content for {output_data.file_name}")
            with open(output_data.file_name, "w") as f:
                f.write(output_data.output_sql_content)

