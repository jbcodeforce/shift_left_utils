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
from typing import List, Final, Optional, Dict, Tuple, Any, Callable, Set
import yaml
import time
import os
import json
import re
from jinja2 import Environment, PackageLoader
from datetime import datetime
import uuid
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
    test_suite_def, table_ref, prefix, test_result = _init_test_foundations(table_name, test_case_name, compute_pool_id, prefix)
    
    test_suite_result = TestSuiteResult(foundation_statements=test_result.foundation_statements, test_results={})

    for idx, test_case in enumerate(test_suite_def.test_suite):
        # loop over all the test cases of the test suite
        if test_case_name and test_case.name != test_case_name:
            continue
        logger.info(f"Execute test inputs for {test_case.name}")
        statements = _execute_test_inputs(test_case=test_case,
                                        table_ref=table_ref,
                                        prefix=prefix+"-ins-"+str(idx + 1),
                                        compute_pool_id=compute_pool_id)
        test_result = TestResult(test_case_name=test_case.name, result="insertion done")
        test_result.statements.update(statements)
        if run_validation:
            statements, result_text, statement_result = _execute_test_validation(test_case=test_case,
                                                                table_ref=table_ref,
                                                                prefix=prefix+"-val-"+str(idx + 1),
                                                                compute_pool_id=compute_pool_id)
            test_result.result = result_text
            test_result.statements.update(statements)
            test_result.validation_result = statement_result
        test_suite_result.test_results[test_case.name] = test_result
        if test_case_name and test_case.name == test_case_name:
            break
    return test_suite_result


def execute_validation_tests(table_name: str, 
                    test_case_name: str = None,
                    compute_pool_id: Optional[str] = None,
                    run_all: bool = False
) -> TestSuiteResult:
    """
    Execute all validation tests defined in the test suite definition for a given table.
    This function is designed to be reentrant and idempotent - multiple calls will produce
    consistent results and clean up properly.
    
    Args:
        table_name: Name of the table to validate
        test_case_name: Optional specific test case to run
        compute_pool_id: Optional compute pool ID
    """
    logger.info(f"Execute validation tests for {table_name}")
    
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    
   
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    test_suite_result = TestSuiteResult(
        foundation_statements=set(), 
        test_results={}
    )
    
    try:
        for idx, test_case in enumerate(test_suite_def.test_suite):
            if not run_all and test_case_name and test_case.name != test_case_name:
                continue
                
            logger.info(f"Executing validation for test case: {test_case.name}")
            try:
                statements, result_text, statement_result = _execute_test_validation(
                    test_case=test_case,
                    table_ref=table_ref,
                    prefix=f"{prefix}-val-{str(idx + 1)}",
                    compute_pool_id=compute_pool_id
                )
                
                test_suite_result.test_results[test_case.name] = TestResult(
                    test_case_name=test_case.name,
                    result=result_text,
                    validation_result=statement_result,
                    status="completed"
                )
                
            except Exception as case_error:
                logger.error(f"Error executing test case {test_case.name}: {case_error}")
                test_suite_result.test_results[test_case.name] = TestResult(
                    test_case_name=test_case.name,
                    result=f"Error: {str(case_error)}",
                    status="error"
                )
                
            if test_case_name and test_case.name == test_case_name:
                break
                

    except Exception as error:
        logger.error(f"Error during validation tests: {error}")
        test_suite_result.cleanup_errors = str(error)
    
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
            statement_name = _build_statement_name(output.table_name, prefix+"-val-"+str(idx + 1), CONFIGURED_POST_FIX_UNIT_TEST)  
            statement_mgr.delete_statement_if_exists(statement_name)
            logger.info(f"Deleted statement {statement_name}")
            print(f"Deleted statement {statement_name}")
        for input in test_case.inputs:
            statement_name = _build_statement_name(input.table_name, prefix+"-ins-"+str(idx + 1), CONFIGURED_POST_FIX_UNIT_TEST)  
            statement_mgr.delete_statement_if_exists(statement_name)
            logger.info(f"Deleted statement {statement_name}")
            print(f"Deleted statement {statement_name}")
    logger.info(f"Deleting ddl and dml artifacts for {table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
    print(f"Deleting ddl and dml artifacts for {table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
    statement_name = _build_statement_name(table_name, prefix+"-dml", CONFIGURED_POST_FIX_UNIT_TEST)  
    statement_mgr.delete_statement_if_exists(statement_name)
    statement_name = _build_statement_name(table_name, prefix+"-ddl", CONFIGURED_POST_FIX_UNIT_TEST)  
    statement_mgr.delete_statement_if_exists(statement_name)
    statement_mgr.drop_table(table_name+CONFIGURED_POST_FIX_UNIT_TEST, compute_pool_id)
    for foundation in test_suite_def.foundations:
        logger.info(f"Deleting ddl and dml artifacts for {foundation.table_name}{CONFIGURED_POST_FIX_UNIT_TEST}")
        statement_name = _build_statement_name(foundation.table_name, prefix+"-ddl", CONFIGURED_POST_FIX_UNIT_TEST)  
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
    For each input table as defined in the test suite foundations:
    run the ddl of the input table.
    the ddl of the table under test.
    and modify the dml 
    modify the dml of the given table to use the input tables for the unit tests.
    return the test suite definition, the table reference, the prefix and the test result.
    """
    print("-"*60)
    print(f"1. Create foundation tables for unit tests for {table_name}")
    print("-"*60)
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    test_result = TestResult(test_case_name="" if test_case_name is None else test_case_name, result="")
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
        product_name: Optional[str] = None ,
        existing_statement: Optional[Statement] = None
) -> Tuple[Optional[Statement], bool]:
    """
    Execute the Flink statement and return the statement object and whether it was newly created.
    Returns (statement, is_new) where is_new indicates if this was a newly executed statement.
    """
    logger.info(f"Run flink statement {statement_name}")
    if existing_statement:
        statement = existing_statement
    else:
        statement = statement_mgr.get_statement(statement_name)
    
    # Check if we need to create/execute the statement
    should_execute = False
    is_new = False
    if statement is None:
        should_execute = True
        is_new = True
    elif isinstance(statement, StatementError):
        if hasattr(statement, 'errors') and statement.errors and len(statement.errors) > 0:
            if statement.errors[0].status == "404":
                is_new = True
        should_execute = True
    elif isinstance(statement, Statement):
        # Statement exists - check if it's running
        if statement.status and statement.status.phase != "RUNNING":
            should_execute = True
            statement_mgr.delete_statement_if_exists(statement_name)
        else:
            logger.info(f"{statement_name} statement already exists -> do nothing")
            return statement, is_new  # Return existing statement, not new
    
    if should_execute:
        try:
            config = get_config()
            column_name_to_select_from = config['app']['data_limit_column_name_to_select_from']
            transformer = statement_mgr.get_or_build_sql_content_transformer()
            _, sql_out= transformer.update_sql_content(sql_content, column_name_to_select_from, product_name)
            logger.info(f"Execute statement {statement_name} on: {compute_pool_id}")
            print(f"Execute statement {statement_name}  on: {compute_pool_id}")
            post_statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_out)
            if "Exists but deleted so retry" in post_statement:
                logger.info(f"Statement {statement_name} exists but deleted so retry")
                post_statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_out)
            return post_statement, is_new  # Return new statement
        except Exception as e:
            logger.error(e)
            raise e
    else:
        # Return existing statement if available but it needs processing
        if isinstance(statement, Statement):
            logger.info(f"{statement.name} statement already exists -> {statement.status.phase}")
            print(f"{statement_name} statement already exists -> {statement.status.phase}")
            return statement, is_new
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
) -> Set[Statement]:
    """
    Execute the DDL statements for the foundation tables for the unit tests.
    """

    statements = set()
    table_folder = from_pipeline_to_absolute(table_ref.table_folder_name)
    for foundation in test_suite_def.foundations:
        testfile_path = os.path.join(table_folder, foundation.ddl_for_test)
        print(f"May execute foundation statement for {foundation.table_name} {testfile_path} on {compute_pool_id}")
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
                                   statements: Optional[Set[Statement]] = None
) -> Set[Statement]:
    """
    Run DDL and DML statements for the given tested table
    """
    def replace_table_name(sql_content: str, table_name: str) -> str:
        """
        Replace the table names in the SQL content with the configured postfix for the unit test.
        Change the scan.bounded.mode for DDL to stop dml.
        """
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
            if "CREATE TABLE" in sql_content:
                if "scan.bounded.mode" in sql_content:
                    sql_content = sql_content.replace("'scan.bounded.mode' = 'unbounded'", "'scan.bounded.mode' = 'latest-offset'")
                else:
                    # Find the last occurrence of ')' or ');' and replace it with ", 'scan.bounded.mode'= 'lastest-offset')" or ", 'scan.bounded.mode'= 'lastest-offset');"
                    pattern = r"\)(\s*;?)\s*$"
                    replacement = r", 'scan.bounded.mode'= 'lastest-offset')\1"
                    sql_content = re.sub(pattern, replacement, sql_content)
            logger.info(f"Replaced table names: {sorted_table_names} in SQL content: {sql_content}")
        return sql_content

    # Initialize statements list if None
    if statements is None:
        statements = set()
    
    # Process DDL
    ddl_result = _load_sql_and_execute_statement(table_name=table_name,
                                sql_path=table_ref.ddl_ref,
                                prefix=prefix+"-ddl",
                                compute_pool_id=compute_pool_id,
                                fct=replace_table_name,
                                product_name=table_ref.product_name,
                                statements=statements)
    if ddl_result is not None:
        statements.update(ddl_result)
    
    # Process DML
    dml_result = _load_sql_and_execute_statement(table_name=table_name,
                                sql_path=table_ref.dml_ref,
                                prefix=prefix+"-dml",
                                compute_pool_id=compute_pool_id,
                                fct=replace_table_name,
                                product_name=table_ref.product_name,
                                statements=statements)
    if dml_result is not None:
        statements.update(dml_result)
        
    return statements

def _load_sql_and_execute_statement(table_name: str, 
                                sql_path: str, 
                                prefix: str = 'dev-ddl', 
                                compute_pool_id: Optional[str] = None,
                                fct: Callable[[str], str] = lambda x: x,
                                product_name: Optional[str] = None,
                                statements: Optional[Set[Statement]] = None) -> Optional[Set[Statement]]:
 
    # Initialize statements list if None
    if statements is None:
        statements = set()
    
    statement_name = _build_statement_name(table_name, prefix, CONFIGURED_POST_FIX_UNIT_TEST)
    
    # For DDL statements, check if table already exists
    if "ddl" in prefix:
        try:
            table_under_test_exists = _table_exists(table_name+CONFIGURED_POST_FIX_UNIT_TEST)
            if table_under_test_exists:
                logger.info(f"Table {table_name}{CONFIGURED_POST_FIX_UNIT_TEST} already exists, skipping DDL")
                print(f"Table {table_name}{CONFIGURED_POST_FIX_UNIT_TEST} already exists, skipping DDL")
                return statements
        except Exception as e:
            logger.warning(f"Error checking if table exists: {e}")
            # Continue execution - we'll try to create the table anyway

    # Check existing statement status
    try:
        statement_info = statement_mgr.get_statement(statement_name)
        if statement_info and isinstance(statement_info, Statement):
            if statement_info.status.phase in ["RUNNING", "COMPLETED"]:
                logger.info(f"Statement {statement_name} already exists and is {statement_info.status.phase}")
                print(f"Statement {statement_name} already exists and is {statement_info.status.phase}")
                statements.add(statement_info)
                return statements
            elif statement_info.status.phase == "FAILED":
                logger.warning(f"Found failed statement {statement_name}, will try to recreate")
                print(f"Found failed statement {statement_name}, will try to recreate")
                try:
                    statement_mgr.delete_statement_if_exists(statement_name)
                except Exception as e:
                    logger.warning(f"Error deleting failed statement: {e}")
        # at this point it is possible statement_info is a StatementError because of 404 for statement not found. we can then continue
        sql_content = _read_and_treat_sql_content_for_ut(sql_path, fct, table_name)
          
        statement, is_new = _execute_flink_test_statement(
            sql_content=sql_content, 
            statement_name=statement_name, 
            compute_pool_id=compute_pool_id, 
            product_name=product_name,
            existing_statement=statement_info
        )
        
        if statement and is_new:
            statements.add(statement)
            
            # Handle statement status
            if statement.status and statement.status.phase == "FAILED":
                error_msg = f"Failed to create {table_name}: {statement.status.detail}"
                logger.error(error_msg)
                print(error_msg)
                raise ValueError(error_msg)
            
            # For DDL statements, wait for completion
            if "-ddl-" in statement_name:
                max_wait = 30  # Maximum wait time in seconds
                wait_time = 0
                while statement.status.phase not in ["COMPLETED", "FAILED"] and wait_time < max_wait:
                    time.sleep(2)
                    wait_time += 2
                    statement = statement_mgr.get_statement(statement_name)
                    logger.info(f"DDL deployment status is: {statement.status.phase}")
                
                if statement.status.phase == "FAILED":
                    error_msg = f"Deployment failed for {table_name} {statement_name}: {statement.status.detail}"
                    logger.error(error_msg)
                    raise RuntimeError(error_msg)
                elif wait_time >= max_wait:
                    logger.warning(f"DDL deployment taking longer than expected for {statement_name}")
            
            print(f"Executed statement for table: {table_name}{CONFIGURED_POST_FIX_UNIT_TEST} status: {statement.status.phase}\n")
            
    except Exception as e:
        logger.warning(f"Error checking statement status: {e}")
        # Continue execution - we'll try to create the statement    
    return statements

def _execute_test_inputs(test_case: SLTestCase, 
                        table_ref: FlinkTableReference, 
                        prefix: str = 'dev', 
                        compute_pool_id: Optional[str] = None
) -> Set[Statement]:
    """
    Execute the input and validation SQL statements for a given test case.
    """
    logger.info(f"Run insert statements for: {test_case.name}")
    print("-"*40)
    print(f"2. Deploy insert into statements for unit test {test_case.name}")
    print("-"*40)
    statements = set()
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
                statements.add(statement)
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
) -> Tuple[Set[Statement], str, Optional[StatementResult]]:
    """
    Execute the validation SQL statements for a given test case.
    It is possible that the validation SQL statements are not immediately available after the execution of the insert statements.
    So we need to poll for the results.
    We poll for the results until the statement is completed or failed.

    """
    print("-"*40)
    print(f"3. Run validation SQL statements for unit test {test_case.name}")
    print("-"*40)
    statements = set()
    result_text = ""
 
    for output_step in test_case.outputs:
        sql_path = os.path.join(table_ref.table_folder_name, output_step.file_name)
        statement_name = _build_statement_name(output_step.table_name, prefix, CONFIGURED_POST_FIX_UNIT_TEST)  
        # First try to delete any existing statement
        try:
            delete_result = statement_mgr.delete_statement_if_exists(statement_name)
            if delete_result is None:
                logger.info(f"No existing statement found for {statement_name}")
            elif "deleted" not in delete_result:
                logger.warning(f"Unexpected delete result for {statement_name}: {delete_result}")
        except Exception as e:
            logger.warning(f"Error deleting statement {statement_name}: {e}")
            # Continue execution - the statement will be recreated
        
        try:
            # Execute the validation statement
            statements = _load_sql_and_execute_statement(
                table_name=output_step.table_name,
                sql_path=sql_path,
                prefix=prefix,
                compute_pool_id=compute_pool_id,
                fct=_replace_table_name_ut_with_configured_postfix,
                product_name=table_ref.product_name,
                statements=statements
            )

            time.sleep(2)
            # Poll for results
            result, statement_result = _poll_response(statement_name)
            if result:
                result_text += result
            
            # Clean up after getting results
            try:
                delete_result = statement_mgr.delete_statement_if_exists(statement_name)
                if delete_result is None:
                    logger.info(f"Statement {statement_name} already deleted")
                elif "deleted" not in delete_result:
                    logger.warning(f"Unexpected final delete result for {statement_name}: {delete_result}")
            except Exception as e:
                logger.warning(f"Error in final cleanup of statement {statement_name}: {e}")
            
            return statements, result_text, statement_result
            
        except Exception as e:
            logger.error(f"Error executing validation for {statement_name}: {e}")
            # Try to clean up on error
            try:
                statement_mgr.delete_statement_if_exists(statement_name)
            except Exception as cleanup_error:
                logger.warning(f"Error cleaning up after failed validation {statement_name}: {cleanup_error}")
            raise  # Re-raise the original error
            
    return statements, result_text, None

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
                logger.info(f"data: {resp.results.data}")
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
                yaml_file = os.path.join(tests_folder_path, '..', input_data.file_name.replace(".sql", ".yaml"))
                with open(yaml_file, "w") as f:
                    f.write(f"{input_data.table_name}:\n")
                    for column in table_struct[input_data.table_name]:
                        f.write(f"  - {column}: ['enum_test_data']\n")

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
            validation_sql_content = _build_validation_sql_content(output_data.table_name, table_inventory)
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

def _build_validation_sql_content(table_name: str, table_inventory: Dict[str, FlinkTableReference]) -> str:
    """
    Build the validation SQL content for a given table.
    It is possible that the SQL under test has multiple output tables, but most likely one: itself.
    The inventory argument is used to get the table reference of the output table.
    """
    output_table_ref: FlinkTableReference = FlinkTableReference.model_validate(table_inventory[table_name])
    file_path = from_pipeline_to_absolute(output_table_ref.ddl_ref)
    parser = SQLparser()
    sql_content = ""
    with open(file_path, "r") as f:
        ddl_sql_content = f.read()
        columns = parser.build_column_metadata_from_sql_content(ddl_sql_content)  # column_name -> column_metadata
        column_names = [name for name in columns]
        env = Environment(loader=PackageLoader("shift_left.core","templates"))
        template = env.get_template("validate_test.jinja")
        context = {
            'table_name': table_name + DEFAULT_POST_FIX_UNIT_TEST,
            'column_names': column_names,
        }
        sql_content = template.render(context)
        sql_content = sql_content.replace("AND then 1", "then 1")
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
                logger.warning(f"Error loading topic list from file {TOPIC_LIST_FILE}: {e}")
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

