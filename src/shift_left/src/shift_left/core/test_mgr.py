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
from shift_left.core.models.flink_statement_model import Statement, StatementError, StatementResult
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
    file_name: str
    file_type: str = "sql"

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

class TestResult(BaseModel):
    test_case_name: str
    result: str
    validation_result: StatementResult = None
    foundation_statements: List[Statement] = []
    statements: List[Statement] = []

class TestSuiteResult(BaseModel):
    foundation_statements: List[Statement] = []
    test_results: Dict[str, TestResult] = {}

# ----------- Public APIs  ------------------------------------------------------------
def init_unit_test_for_table(table_name: str):
    """
    Initialize the unit test folder and template files for a given table. It will parse the SQL statemnts to create the insert statements for the unit tests.
    """
    inventory_path = os.path.join(os.getenv("PIPELINES"),)
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    table_folder = table_ref.table_folder_name
    test_folder_path = from_pipeline_to_absolute(table_folder)
    create_folder_if_not_exist(f"{test_folder_path}/tests")
    _add_test_files(table_ref=table_ref, 
                    tests_folder=f"{table_folder}/tests", 
                    table_inventory=table_inventory)


def execute_one_test(
        table_name: str, 
        test_case_name: str, 
        compute_pool_id: Optional[str] = None
) -> TestResult:
    """
    Execute a single test case from the test suite definition.
    
    The function:
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

    try:
        print(f"1. Create table foundations for unit tests")
        test_suite_def, table_ref, prefix, test_result = _init_test_foundations(table_name, test_case_name, compute_pool_id)
    
        # Parse through test case suite in yaml file, run the test case provided in parameter
        for idx, test_case in enumerate(test_suite_def.test_suite):
            if test_case.name == test_case_name:
                logger.info(f"Running test case: {test_case.name}")
                print(f"2. Create input statements for unit test {test_case.name}")
                statements = _execute_test_inputs(test_case=test_case,
                                                table_ref=table_ref,
                                                prefix=prefix+"-ins-"+str(idx + 1),
                                                compute_pool_id=compute_pool_id)
                test_result.statements.extend(statements)
                print(f"3. Run validation SQL statements for unit test {test_case.name}")
                statements, result_text, statement_result = _execute_test_validation(test_case=test_case,
                                                                table_ref=table_ref,
                                                                prefix=prefix+"-val"+"-"+str(idx + 1),
                                                                compute_pool_id=compute_pool_id)
                test_result.result = result_text
                test_result.statements.extend(statements)
                test_result.validation_result = statement_result
        return test_result
    except Exception as e:
        logger.error(f"Error executing test case: {e}")
        raise e


def execute_all_tests(table_name: str, 
                      compute_pool_id: Optional[str] = None
) -> TestSuiteResult:
    """
    Execute all test cases defined in the test suite definition for a given table.
    """
    statement_mgr.reset_statement_list()
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    try:
        print(f"1. Create table foundations for unit tests")
        test_suite_def, table_ref, prefix, test_result = _init_test_foundations(table_name, "", compute_pool_id)
       
        test_suite_result = TestSuiteResult(foundation_statements=test_result.foundation_statements, test_results={})
        for idx, test_case in enumerate(test_suite_def.test_suite):
            print(f"2. Create input statements for unit test {test_case.name}")
            statements = _execute_test_inputs(test_case=test_case,
                                            table_ref=table_ref,
                                            prefix=prefix+"-ins-"+str(idx + 1),
                                            compute_pool_id=compute_pool_id)
            test_result = TestResult(test_case_name=test_case.name, result="")
            print(f"3. Run validation SQL statements for unit test {test_case.name}")
            statements, result_text, statement_result = _execute_test_validation(test_case=test_case,
                                                                table_ref=table_ref,
                                                                prefix=prefix+"-val-"+str(idx + 1),
                                                                compute_pool_id=compute_pool_id)
            test_result.result = result_text
            test_result.statements.extend(statements)
            test_result.validation_result = statement_result
            test_suite_result.test_results[test_case.name] = test_result

        return test_suite_result
    except Exception as e:
        logger.error(f"Error executing test suite: {e}")
        raise e

def delete_test_artifacts(table_name: str, 
                          compute_pool_id: Optional[str] = None, 
                          test_suite_result: Optional[TestSuiteResult] = None) -> None:
    """
    Delete the test artifacts for a given table.
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
        for input in test_case.inputs:
            statement_name = _build_statement_name(input.table_name, prefix+"-ins-"+str(idx + 1))
            statement_mgr.delete_statement_if_exists(statement_name)
    logger.info(f"Deleting ddl and dml artifacts for {table_name}")
    statement_name = _build_statement_name(table_name, prefix+"-dml")
    statement_mgr.delete_statement_if_exists(statement_name)
    statement_name = _build_statement_name(table_name, prefix+"-ddl")
    statement_mgr.delete_statement_if_exists(statement_name)
    statement_mgr.drop_table(table_name+"_ut", compute_pool_id)
    for foundation in test_suite_def.foundations:
        logger.info(f"Deleting ddl and dml artifacts for {foundation.table_name}")
        statement_name = _build_statement_name(foundation.table_name, prefix+"-ddl")
        statement_mgr.delete_statement_if_exists(statement_name)
        statement_mgr.drop_table(foundation.table_name+"_ut", compute_pool_id)
    logger.info(f"Test artifacts for {table_name} deleted")



# ----------- Private APIs  ------------------------------------------------------------

def _init_test_foundations(table_name: str, 
        test_case_name: str, 
        compute_pool_id: Optional[str] = None
) -> Tuple[SLTestDefinition, FlinkTableReference, str, TestResult]:
    test_suite_def, table_ref = _load_test_suite_definition(table_name)
    config = get_config()
    if compute_pool_id is None:
        compute_pool_id = config['flink']['compute_pool_id']
    prefix = config['kafka']['cluster_type']
    test_result = TestResult(test_case_name=test_case_name, result="")
    test_result.foundation_statements = _execute_foundation_statements(test_suite_def, table_ref, prefix, compute_pool_id)
    statements = _start_ddl_dml_for_flink_under_test(table_name, table_ref, prefix, compute_pool_id)
    if statements:
        for statement in statements:
            test_result.foundation_statements.append(statement)
    return test_suite_def, table_ref, prefix, test_result


def _delete_test_statements(statement_names):
    for stmt_name in statement_names:
        statement_mgr.delete_statement_if_exists(stmt_name)


def _drop_test_tables(test_suite_def, main_table_name):
    for foundation in test_suite_def.foundations:
        statement_mgr.drop_table(foundation.table_name)
    statement_mgr.drop_table(main_table_name)



def _execute_flink_test_statement(
        sql_content: str, 
        statement_name: str, 
        compute_pool_id: Optional[str] = None,
        product_name: Optional[str] = None  
) -> Statement:
    """
    Execute the Flink statement and return the statement object.
    """
    logger.info(f"Run flink statement {statement_name}")
    statement = statement_mgr.get_statement(statement_name)
    if  isinstance(statement, StatementError) or not statement:
        try:
            config = get_config()
            column_name_to_select_from = config['app']['data_limit_column_name_to_select_from']
            transformer = statement_mgr.get_or_build_sql_content_transformer()
            _, sql_out= transformer.update_sql_content(sql_content, column_name_to_select_from, product_name)
            post_statement = statement_mgr.post_flink_statement(compute_pool_id, statement_name, sql_out)
            logger.debug(f"Executed flink test statement table {post_statement}")
            return post_statement
        except Exception as e:
            logger.error(e)
            raise e
    else:
        print(f"{statement_name} statement already exists -> do nothing")
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
        print(f"Execute DDL for {foundation.table_name} from {testfile_path}")
        statement = _load_sql_and_execute_statement(table_name=foundation.table_name,
                                    sql_path=testfile_path,
                                    prefix=prefix+"-ddl",
                                    compute_pool_id=compute_pool_id)
        if statement:
            statements.append(statement)
    return statements

def _read_and_treat_sql_content_for_ut(sql_path: str, fct) -> str:
    """
    Read the SQL content from the file and apply the given function fct() to the content.
    """
    sql_path = from_pipeline_to_absolute(sql_path)
    with open(sql_path, "r") as f:
        return fct(f.read())


    
def _start_ddl_dml_for_flink_under_test(table_name: str, 
                                   table_ref: FlinkTableReference, 
                                   prefix: str = 'dev',
                                   compute_pool_id: Optional[str] = None
) -> List[Statement]:
    """
    Run DDL and DML statements for the given table
    """
    def replace_table_name(sql_content: str) -> str:
        parser = SQLparser()
        table_names = parser.extract_table_references(sql_content)
        for table in table_names:
            sql_content = sql_content.replace(table, f"{table}_ut")
        return sql_content

    statements = []
    statement = _load_sql_and_execute_statement(table_name=table_name,
                                sql_path=table_ref.ddl_ref,
                                prefix=prefix+"-ddl",
                                compute_pool_id=compute_pool_id,
                                fct=replace_table_name)
    if statement:
        statements.append(statement)
    statement = _load_sql_and_execute_statement(table_name=table_name,
                                sql_path=table_ref.dml_ref,
                                prefix=prefix+"-dml",
                                compute_pool_id=compute_pool_id,
                                fct=replace_table_name)
    if statement:
        statements.append(statement)
    
    return statements

def _load_sql_and_execute_statement(table_name: str, 
                                sql_path: str, 
                                prefix: str = 'dev-ddl', 
                                compute_pool_id: Optional[str] = None,
                                fct = lambda x: x) -> Statement:
 
    statement_name = _build_statement_name(table_name, prefix)
    table_under_test_exists = _table_exists(table_name+"_ut")
    if "ddl" in prefix and table_under_test_exists:
        return None
    
    if "dml" in prefix:
        dml_statement_info = statement_mgr.get_statement_info(statement_name)
        if dml_statement_info and dml_statement_info.status_phase == "RUNNING":
            return None
    
    sql_content = _read_and_treat_sql_content_for_ut(sql_path, fct)
    statement = _execute_flink_test_statement(sql_content, statement_name, compute_pool_id)
    return statement

def _execute_test_inputs(test_case: SLTestCase, 
                        table_ref: FlinkTableReference, 
                        prefix: str = 'dev', 
                        compute_pool_id: Optional[str] = None
) -> List[Statement]:
    """
    Execute the input and validation SQL statements for a given test case.
    """
    logger.info(f"Run insert statements for: {test_case.name}")
    statements = []
    for input_step in test_case.inputs:
        statement = None
        print(f"Execute test input for {input_step.table_name}")
        if input_step.file_type == "sql":
            sql_path = os.path.join(table_ref.table_folder_name, input_step.file_name)
            statement = _load_sql_and_execute_statement(table_name=input_step.table_name,
                                        sql_path=sql_path,
                                        prefix=prefix,
                                        compute_pool_id=compute_pool_id,
                                        fct=lambda x: x)
        elif input_step.file_type == "csv":
            sql_path = os.path.join(table_ref.table_folder_name, input_step.file_name)
            sql_path = from_pipeline_to_absolute(sql_path)
            headers, rows = _red_csv_file(sql_path)
            sql = _transform_csv_to_sql(input_step.table_name+"_ut", headers, rows)
            print(f"Execute test input {sql}")
            statement_name = _build_statement_name(input_step.table_name, prefix)
            
            statement = _execute_flink_test_statement(sql_content=sql, 
                                                      statement_name=statement_name,
                                                      product_name=table_ref.product_name,
                                                      compute_pool_id=compute_pool_id)
        if statement and isinstance(statement, Statement) and statement.status:
            print(f"Executed test input {statement.status}")
            statements.append(statement)
        else:
            logger.error(f"Error executing test input for {input_step.table_name}")
            raise ValueError(f"Error executing test input for {input_step.table_name}")
    return statements

def _execute_test_validation(test_case: SLTestCase, 
                          table_ref: FlinkTableReference, 
                          prefix: str = 'dev', 
                          compute_pool_id: Optional[str] = None
) -> Tuple[List[Statement], str, StatementResult]:
    statements = []
    result_text = ""
    for output_step in test_case.outputs:
        sql_path = os.path.join(table_ref.table_folder_name, output_step.file_name)
        statement_name = _build_statement_name(output_step.table_name, prefix)
        statement_mgr.delete_statement_if_exists(statement_name)
        statement = _load_sql_and_execute_statement(table_name=output_step.table_name,
                                    sql_path=sql_path,
                                    prefix=prefix,
                                    compute_pool_id=compute_pool_id,
                                    fct=lambda x: x)
        if statement:
            statements.append(statement)
        result, statement_result=_poll_response(statement_name)
        result_text+=result
    return statements,result_text, statement_result
    
def _poll_response(statement_name: str) -> Tuple[str, StatementResult]:
    #Get result from the validation query
    resp = None
    max_retries = 10
    retry_delay = 10  # seconds

    for poll in range(1, max_retries):
        try:
            resp = statement_mgr.get_statement_results(statement_name)
            # Check if results and data are non-empty
            if resp and resp.results and resp.results.data:
                logger.info(f"Received results on poll {poll}")
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
        final_row = resp.results.data[0].row[0]
    logger.info(f"Final Result for {statement_name}: {final_row}")
    print(f"Final Result for {statement_name}: {final_row}")
    return final_row, resp

def _add_test_files(table_ref: FlinkTableReference, 
                    tests_folder: str, 
                    table_inventory: Dict[str, FlinkTableReference]):
    """
    Add the test files to the table/tests folder by looking at the referenced tables in the DML SQL content.
    """
    file_path = from_pipeline_to_absolute(table_ref.dml_ref)
    referenced_table_names = None
    with open(file_path) as f:
        sql_content = f.read()
        parser = SQLparser()
        referenced_table_names = parser.extract_table_references(sql_content)
        # keep as print for user feedback
        print(f"From the DML under test the referenced table names are: {referenced_table_names}")
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
            if input_data.file_type == "sql":
                input_file = os.path.join(tests_folder_path, '..', input_data.file_name)
                columns_names, rows = _build_data_sample(table_struct[input_data.table_name])
                with open(input_file, "w") as f:
                    f.write(f"insert into {input_data.table_name}_ut\n({columns_names})\nvalues\n{rows}\n")
                logger.info(f"Input file {input_file} created")
            if input_data.file_type == "csv":
                input_file = os.path.join(tests_folder_path, '..', input_data.file_name)
                columns_names, rows = _build_data_sample(table_struct[input_data.table_name])
                rows=rows[:-2].replace("),", "").replace("(", "").replace(")", "")
                with open(input_file, "w") as f:
                    f.write(columns_names+"\n")
                    f.write(rows)
                logger.info(f"Input file {input_file} created")
                print(f"Input file {input_file} created")

        # Create output validation files 
        for output_data in test_case.outputs:
            output_file = os.path.join(tests_folder_path, '..', output_data.file_name)
            sql_content = "with result_table as (\n"
            sql_content += f"   select * from {output_data.table_name}_ut\n"
            sql_content += f"   where id IS NOT NULL\n"
            sql_content += f"   --- and ... add more validations here\n"
            sql_content += ")\n"
            sql_content += f"SELECT CASE WHEN count(*)=1 THEN 'PASS' ELSE 'FAIL' END as test_result from result_table"
            with open(output_file, "w") as f:
                f.write(sql_content)
    return test_definition

        
            
def _build_save_test_definition_json_file(
        file_path: str, 
        table_name: str, 
        referenced_table_names: List[str]
) -> SLTestDefinition:
    test_definition :SLTestDefinition = SLTestDefinition(foundations=[], test_suite=[])
    for table in referenced_table_names:
        if table not in table_name:
            foundation_table_name = Foundation(table_name=table, ddl_for_test=f"./tests/ddl_{table}.sql")
            test_definition.foundations.append(foundation_table_name)
    for i in range(1, 3):
        test_case = SLTestCase(name=f"test_{table_name}_{i}", inputs=[], outputs=[])
        output = SLTestData(table_name=table_name, file_name=f"./tests/validate_{table_name}_{i}.sql",file_type="sql")
        for table in referenced_table_names:
            if table not in table_name:
                if i == 1:  
                    input = SLTestData(table_name=table, file_name=f"./tests/insert_{table}_{i}.sql",file_type="sql")
                else:
                    input = SLTestData(table_name=table, file_name=f"./tests/insert_{table}_{i}.csv",file_type="csv")
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
    Returns a string of all columns names separated by ',' so it can be used
    in the insert statement and a string of 5 rows of data sample.
    """
    columns_names = ""
    for column in columns:
        columns_names += f"`{column}`, "
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


_topic_list = []
def _table_exists(table_name: str) -> bool:
    """
    Check if the table exists in the database.
    """
    global _topic_list
    if not _topic_list:
        ccloud = ConfluentCloudClient(get_config())
        topics = ccloud.list_topics()
        _topic_list = [topic['topic_name'] for topic in topics['data']]
        logger.debug(f"Topic list: {_topic_list}")
    return table_name in _topic_list


def _red_csv_file(file_path: str) -> Tuple[str, List[str]]:
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
        if current_size > 4000000:
            sql_content = sql_content[:-2] + ";\n"
            current_size = len(sql_content)
            return sql_content
    sql_content = sql_content[:-2] + ";\n"
    return sql_content

def _build_statement_name(table_name: str, prefix: str) -> str:
    _table_name_for_statement = table_name.replace('_', '-').replace('.', '-')
    if len(_table_name_for_statement) > 52:
        _table_name_for_statement = _table_name_for_statement[:52]    
    statement_name = f"{prefix}-{_table_name_for_statement}-ut"
    return statement_name