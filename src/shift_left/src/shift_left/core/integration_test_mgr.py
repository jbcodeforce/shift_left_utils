"""
Copyright 2024-2025 Confluent, Inc.

Integration Test Manager - End-to-end testing framework for Flink SQL pipelines.

Provides integration testing capabilities including:
- Project-level integration test initialization and scaffolding
- End-to-end pipeline testing with synthetic data injection
- Latency measurement from raw topics to sink tables
- Test execution across multiple tables and pipelines
- Integration test artifact cleanup and management
- Support for timestamp-based synthetic data with unique identifiers
"""

from datetime import datetime, timezone
from pydantic import Field
from typing import List
import os
import yaml
import time
import uuid
from pathlib import Path

from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.utils.file_search import (
    get_table_ref_from_inventory,
    get_or_build_inventory,
    create_folder_if_not_exist,
    from_pipeline_to_absolute,
    FlinkTableReference
)
from shift_left.core.models.flink_statement_model import Statement, StatementResult
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.pipeline_mgr as pipeline_mgr
from shift_left.core.utils.file_search import PIPELINE_JSON_FILE_NAME
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.models.flink_test_model import (IntegrationTestSuite, 
    IntegrationTestScenario, 
    IntegrationTestData, 
    IntegrationTestSuiteResult,
    IntegrationTestLatencyResult, 
    IntegrationTestResult, 
    Foundation)

# Integration Test Configuration Constants
INTEGRATION_TEST_FOLDER = "tests"
DEFAULT_POST_FIX_INTEGRATION_TEST = "_it"
CONFIGURED_POST_FIX_INTEGRATION_TEST = get_config().get('app', {}).get('post_fix_integration_test', DEFAULT_POST_FIX_INTEGRATION_TEST)
INTEGRATION_TEST_DEFINITION_FILE = "integration_test_definitions.yaml"
MAX_POLLING_RETRIES = 10
POLLING_RETRY_DELAY_SECONDS = 15


def init_integration_tests(sink_table_name: str, pipeline_path: str = None) -> IntegrationTestSuite:
    """
    Initialize integration test structure for a given sink table.
    
    Args:
        sink_table_name: Name of the sink table to create integration tests for
        project_path: Path to the project (defaults to $PIPELINES environment variable)
        
    Returns:
        Path to the created integration test folder
    """
    if not pipeline_path:
        pipeline_path = os.getenv("PIPELINES")
        if not pipeline_path:
            raise ValueError("Project path must be provided or $PIPELINES environment variable must be set")
    
    logger.info(f"Initializing integration tests for sink table: {sink_table_name}")
    
    # Get table inventory to find the sink table and its dependencies
    inventory = get_or_build_inventory(pipeline_path, pipeline_path, False)
    
    if sink_table_name not in inventory:
        raise ValueError(f"Sink table '{sink_table_name}' not found in inventory")
    
    table_ref = FlinkTableReference.model_validate(inventory[sink_table_name])
    
    # Create integration test folder structure: tests
    test_base_path = os.path.join(pipeline_path, "..", INTEGRATION_TEST_FOLDER)
    create_folder_if_not_exist(test_base_path)
    
    # Organize by product name (extracted from table path): tests/product_name
    product_test_path = os.path.join(test_base_path, table_ref.product_name)
    create_folder_if_not_exist(product_test_path)
    
    # Create sink table specific folder: tests/product_name/sink_table_name
    sink_test_path = os.path.join(product_test_path, sink_table_name)
    create_folder_if_not_exist(sink_test_path)
    
    # Get source tables by tracing back through the pipeline
    source_tables = _find_source_tables_for_sink(sink_table_name, inventory, pipeline_path)
    # Create integration test definition file
    test_definition = _create_integration_test_definition(
        sink_table_name,
        sink_test_path,
        table_ref.product_name, 
        source_tables
    )
    
    # Write test definition file
    definition_file_path = os.path.join(sink_test_path, INTEGRATION_TEST_DEFINITION_FILE)
    with open(definition_file_path, 'w') as f:
        yaml.dump(test_definition.model_dump(), f, default_flow_style=False, indent=2)
    
    # Create ddl files for each source table
    sql_contents = _create_ddl_file_for_raw_tables(sink_test_path, source_tables)
    # Create synthetic data files for each source table
    _create_synthetic_data_files(sink_test_path, sql_contents)
    
    # Create validation query templates
    _create_validation_query_templates(sink_test_path, sink_table_name)
    
    logger.info(f"Integration test structure created at: {sink_test_path}")
    return test_definition


def run_integration_tests(sink_table_name: str, 
                         scenario_name: str = None, 
                         project_path: str = None,
                         compute_pool_id: str = None,
                         measure_latency: bool = True) -> IntegrationTestSuiteResult:
    """
    Run integration tests for a given sink table.
    
    Args:
        sink_table_name: Name of the sink table
        scenario_name: Specific scenario to run (optional, runs all if not specified)
        project_path: Path to the project
        compute_pool_id: Compute pool to use for Flink statements
        measure_latency: Whether to measure end-to-end latency
        
    Returns:
        Integration test suite results
    """
    if not project_path:
        project_path = os.getenv("PIPELINES")
    
    if not compute_pool_id:
        compute_pool_id = get_config().get('flink', {}).get('compute_pool_id')
    
    logger.info(f"Running integration tests for sink table: {sink_table_name}")
    
    # Load test definition
    test_suite = _load_integration_test_definition(sink_table_name, project_path)
    
    # Filter scenarios if specific scenario requested
    scenarios_to_run = test_suite.scenarios
    if scenario_name:
        scenarios_to_run = [s for s in test_suite.scenarios if s.name == scenario_name]
        if not scenarios_to_run:
            raise ValueError(f"Scenario '{scenario_name}' not found in test suite")
    
    # Execute test scenarios
    test_results = []
    suite_start_time = datetime.now(timezone.utc)
    
    for scenario in scenarios_to_run:
        logger.info(f"Executing scenario: {scenario.name}")
        result = _execute_integration_test_scenario(
            scenario, 
            test_suite, 
            compute_pool_id,
            measure_latency
        )
        test_results.append(result)
    
    suite_end_time = datetime.now(timezone.utc)
    total_duration_ms = (suite_end_time - suite_start_time).total_seconds() * 1000
    
    # Determine overall status
    overall_status = "PASS"
    if any(r.status == "FAIL" for r in test_results):
        overall_status = "FAIL"
    elif any(r.status == "ERROR" for r in test_results):
        overall_status = "ERROR"
    
    # Create suite result
    suite_result = IntegrationTestSuiteResult(
        suite_name=f"{test_suite.product_name}_{sink_table_name}",
        product_name=test_suite.product_name,
        sink_table=sink_table_name,
        test_results=test_results,
        overall_status=overall_status,
        total_duration_ms=total_duration_ms
    )
    
    # Save results to file
    _save_integration_test_results(suite_result, project_path)
    
    logger.info(f"Integration test suite completed. Overall status: {overall_status}")
    return suite_result


def delete_integration_test_artifacts(sink_table_name: str, 
                                    project_path: str = None,
                                    compute_pool_id: str = None) -> None:
    """
    Delete all integration test artifacts (statements and topics) for a sink table.
    
    Args:
        sink_table_name: Name of the sink table
        project_path: Path to the project
        compute_pool_id: Compute pool ID where statements were created
    """
    if not project_path:
        project_path = os.getenv("PIPELINES")
    
    logger.info(f"Deleting integration test artifacts for sink table: {sink_table_name}")
    
    # Find and delete all statements with integration test postfix
    statements = statement_mgr.get_statement_list()
    deleted_count = 0
    
    for statement_name, statement in statements.items():
        if CONFIGURED_POST_FIX_INTEGRATION_TEST in statement_name:
            logger.info(f"Deleting integration test statement: {statement_name}")
            statement_mgr.delete_statement_if_exists(statement_name)
            deleted_count += 1
    
    logger.info(f"Deleted {deleted_count} integration test statements")


# Private helper functions
def _find_source_tables_for_sink(sink_table_name: str, inventory: dict, pipeline_path: str) -> List[str]:
    """Find all source tables that feed into the given sink table"""
    source_tables = []
    visited = set()

    def _find_raw_tables(src_tables: List[str]) -> List[str]:
        raw_tables = []
        parser = SQLparser()
        for table_name in src_tables:
            table_ref = FlinkTableReference.model_validate(inventory[table_name])
            fname = from_pipeline_to_absolute(table_ref.dml_ref)
            with open(fname, 'r') as file:
                dml_sql_content = file.read()
            source_topics = parser.extract_table_references(dml_sql_content, keep_topic_name=True)
            for source_topic in source_topics:
                if (source_topic != sink_table_name 
                    and source_topic != table_name 
                    and not source_topic.startswith("src_") 
                    and source_topic not in raw_tables):
                        raw_tables.append(source_topic)
        logger.info(f"raw_tables: {raw_tables}")
        print(f"raw_tables: {raw_tables}")
        return raw_tables

    def _find_sources_recursive(table_name: str):
        if table_name in visited:
            return
        visited.add(table_name)
        
        if table_name in inventory:
            table_ref = FlinkTableReference.model_validate(inventory[table_name])
            
            # If this is a source table (starts with src_), add it to the list
            if table_ref.type == 'source':
                source_tables.append(table_name)
            
            # Get pipeline definition to find parent tables
            pipeline_def = pipeline_mgr.read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
            if pipeline_def and pipeline_def.parents:
                for parent in pipeline_def.parents:
                    _find_sources_recursive(parent.table_name)
            elif not pipeline_def:
                logger.error(f"No pipeline definition found for table: {table_name} - ensure to have run build metadata first")
                raise ValueError(f"No pipeline definition found for table: {table_name} - ensure to have run build metadata first")

    _find_sources_recursive(sink_table_name)
    logger.info(f"source_tables: {source_tables}")
    source_tables = _find_raw_tables(source_tables)
    return source_tables


def _create_ddl_file_for_raw_tables(sink_test_path: str, source_tables: List[str]) -> dict[str, str]:
    """Create alter table files for each source table"""
    sql_contents = {}
    for source_table in source_tables:
        sql_content = statement_mgr.show_flink_table_structure(source_table)
        if sql_content is None:
            logger.warning(f"Could not retrieve table structure for {source_table}, skipping DDL file creation")
            continue
        sql_content = sql_content.replace(source_table, source_table + CONFIGURED_POST_FIX_INTEGRATION_TEST)
        if "METADATA" not in sql_content:
            sql_content = sql_content.replace("\n) DISTRIBUTED", ",\n\theaders MAP<STRING, STRING> METADATA)\n) DISTRIBUTED")
        ddl_file_path = os.path.join(sink_test_path, f"ddl.{source_table}.sql")
        with open(ddl_file_path, 'w') as f:
            f.write(sql_content)
        sql_contents[source_table] = sql_content
    return sql_contents
          

def _create_integration_test_definition(sink_table_name: str, 
                                        sink_test_path: str,
                                      product_name: str, 
                                      source_tables: List[str]) -> IntegrationTestSuite:
    """Create initial integration test definition"""
    
    # Create source data specifications
    source_data = []
    foundations = []
    for source_table in source_tables:
        source_data.append(IntegrationTestData(
            table_name=source_table,
            file_name=f"./insert_{source_table}_scenario_1.sql",
            unique_id=str(uuid.uuid4())
        ))
        foundations.append(Foundation(
            table_name=source_table,
            ddl_for_test=f"./ddl.{source_table}.sql"
        ))
    
    # Create validation query specification
    validation_queries = [
        IntegrationTestData(
            table_name=sink_table_name,
            file_name=f"./validate_{sink_table_name}_scenario_1.sql",
            file_type="sql"
        )
    ]
    
    # Create test scenario
    scenario = IntegrationTestScenario(
        name=f"test_{sink_table_name}_scenario_1",

        source_data=source_data,
        validation_queries=validation_queries,

    )
    
    return IntegrationTestSuite(
        product_name=product_name,
        sink_table=sink_table_name,
        description=f"End-to-end integration test for {sink_table_name}",
        sink_test_path=sink_test_path,
        measure_latency=True,
        scenarios=[scenario],
        foundations=foundations
       
    )


def _create_synthetic_data_files(test_path: str, sql_contents: dict[str, str]) -> None:
    """Create synthetic data files for source tables"""
    parser = SQLparser()
    for source_table in sql_contents.keys():
        # Create SQL insert file with timestamped synthetic data
        insert_file_path = os.path.join(test_path, f"insert_{source_table}_scenario_1.sql")
        unique_id = str(uuid.uuid4())
        current_timestamp = datetime.now(timezone.utc).isoformat()
        columns = parser.build_column_metadata_from_sql_content(sql_contents[source_table])
        sql_content = f"""-- Integration test synthetic data for {source_table}
-- Generated on: {current_timestamp}
-- Unique test ID: {unique_id}
INSERT INTO {source_table}{CONFIGURED_POST_FIX_INTEGRATION_TEST} ("""
        for column in columns.keys():
            sql_content += f"{column}, "
        sql_content=sql_content[:-2] + """
) VALUES (
    -- Add appropriate test values here
"""
        for column in columns:
            col_type = columns[column]['type']
            if col_type == "BIGINT" or "INT" in col_type or col_type == "FLOAT" or col_type == "DOUBLE":
                sql_content += f"0, "
            elif col_type == 'BOOLEAN':
                sql_content += 'false, '
            elif col_type == ' ARRAY<STRING>':
                sql_content += f"['{column}_1'], "
            elif "TIMESTAMP" in col_type:
                sql_content += f"TIMESTAMP '2021-01-01 00:00:00', "
            else: # string
                sql_content += f"'{column}_1', "
        sql_content = sql_content[:-2] + ");"
        with open(insert_file_path, 'w') as f:
            f.write(sql_content)


def _create_validation_query_templates(test_path: str, sink_table_name: str) -> None:
    """Create validation query templates"""
    validation_file_path = os.path.join(test_path, f"validate_{sink_table_name}_scenario_1.sql")
    
    sql_content = f"""-- Integration test validation for {sink_table_name}
-- This query should validate that the expected data reached the sink table

WITH validation_result AS (
    SELECT 
        COUNT(*) as record_count,
        -- Add specific validations based on your business logic
        MIN(test_timestamp) as min_timestamp,
        MAX(test_timestamp) as max_timestamp
    FROM {sink_table_name}{CONFIGURED_POST_FIX_INTEGRATION_TEST}
    WHERE test_unique_id = '{{test_unique_id}}'  -- Will be replaced at runtime
)
SELECT 
    CASE 
        WHEN record_count > 0 THEN 'PASS'
        ELSE 'FAIL'
    END as test_result,
    record_count,
    min_timestamp,
    max_timestamp
FROM validation_result;
"""
    
    with open(validation_file_path, 'w') as f:
        f.write(sql_content)


def _load_integration_test_definition(sink_table_name: str, project_path: str) -> IntegrationTestSuite:
    """Load integration test definition from file"""
    # Find the test definition file
    test_base_path = os.path.join(project_path, "..", INTEGRATION_TEST_FOLDER)
    
    # Search through product folders
    for product_dir in os.listdir(test_base_path):
        product_path = os.path.join(test_base_path, product_dir)
        if os.path.isdir(product_path):
            sink_test_path = os.path.join(product_path, sink_table_name)
            definition_file_path = os.path.join(sink_test_path, INTEGRATION_TEST_DEFINITION_FILE)
            
            if os.path.exists(definition_file_path):
                with open(definition_file_path, 'r') as f:
                    data = yaml.safe_load(f)
                    return IntegrationTestSuite(**data)
    
    raise FileNotFoundError(f"Integration test definition not found for sink table: {sink_table_name}")


def _execute_integration_test_scenario(scenario: IntegrationTestScenario,
                                     test_suite: IntegrationTestSuite,
                                     compute_pool_id: str,
                                     measure_latency: bool) -> IntegrationTestResult:
    """Execute a single integration test scenario"""
    start_time = datetime.now(timezone.utc)
    latency_results = []
    validation_results = []
    error_message = None
    status = "PASS"
    
    try:
        # Generate unique test ID for this scenario run
        test_unique_id = str(uuid.uuid4())
        
        # Step 1: Create test-specific tables (with integration test postfix)
        _create_integration_test_tables(scenario, compute_pool_id)
        
        # Step 2: Inject synthetic data with timestamps
        data_injection_time = datetime.now(timezone.utc)
        _inject_synthetic_data(scenario, test_unique_id, data_injection_time)
        
        # Step 3: Wait for data to flow through the pipeline
        _wait_for_pipeline_processing()
        
        # Step 4: Execute validation queries
        validation_results = _execute_validation_queries(scenario, test_unique_id)
        
        # Step 5: Measure latency if requested
        if measure_latency:
            latency_results = _measure_end_to_end_latency(
                scenario, 
                test_unique_id, 
                data_injection_time
            )
        
        # Determine test status based on validation results
        for validation in validation_results:
            if validation.result and "FAIL" in str(validation.result):
                status = "FAIL"
                break
    
    except Exception as e:
        status = "ERROR"
        error_message = str(e)
        logger.error(f"Error executing integration test scenario: {e}")
    
    end_time = datetime.now(timezone.utc)
    duration_ms = (end_time - start_time).total_seconds() * 1000
    
    return IntegrationTestResult(
        scenario_name=scenario.name,
        status=status,
        start_time=start_time,
        end_time=end_time,
        duration_ms=duration_ms,
        latency_results=latency_results,
        validation_results=validation_results,
        error_message=error_message
    )


def _create_integration_test_tables(scenario: IntegrationTestScenario, compute_pool_id: str) -> None:
    """Create integration test specific tables"""
    # This is a placeholder - implementation would create test tables with postfix
    logger.info("Creating integration test tables...")
    pass


def _inject_synthetic_data(scenario: IntegrationTestScenario, test_unique_id: str, timestamp: datetime) -> None:
    """Inject synthetic data into source tables"""
    # This is a placeholder - implementation would inject data with unique IDs and timestamps
    logger.info(f"Injecting synthetic data with ID: {test_unique_id}")
    pass


def _wait_for_pipeline_processing() -> None:
    """Wait for data to flow through the pipeline"""
    logger.info("Waiting for pipeline processing...")
    time.sleep(30)  # Configurable wait time


def _execute_validation_queries(scenario: IntegrationTestScenario, test_unique_id: str) -> List[StatementResult]:
    """Execute validation queries"""
    # This is a placeholder - implementation would execute validation queries
    logger.info("Executing validation queries...")
    return []


def _measure_end_to_end_latency(scenario: IntegrationTestScenario, 
                               test_unique_id: str, 
                               data_injection_time: datetime) -> List[IntegrationTestLatencyResult]:
    """Measure end-to-end latency from source to sink"""
    # This is a placeholder - implementation would measure latency
    logger.info("Measuring end-to-end latency...")
    return []


def _save_integration_test_results(results: IntegrationTestSuiteResult, project_path: str) -> None:
    """Save integration test results to file"""
    results_file = os.path.join(
        shift_left_dir, 
        f"integration_test_results_{results.suite_name}_{results.created_at.strftime('%Y%m%d_%H%M%S')}.json"
    )
    
    with open(results_file, 'w') as f:
        f.write(results.model_dump_json(indent=2))
    
    logger.info(f"Integration test results saved to: {results_file}")

