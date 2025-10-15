"""
Copyright 2024-2025 Confluent, Inc.
"""
import typer
import subprocess
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from rich import print
from shift_left.core.utils.app_config import get_config, shift_left_dir, validate_config as validate_config_impl
from shift_left.core.compute_pool_mgr import get_compute_pool_list
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
import shift_left.core.project_manager as project_manager
import shift_left.core.integration_test_mgr as integration_test_mgr
from shift_left.core.project_manager import (
        DATA_PRODUCT_PROJECT_TYPE, 
        KIMBALL_PROJECT_TYPE)
from shift_left.core.utils.secure_typer import create_secure_typer_app
from typing_extensions import Annotated


"""
Manage project foundations
"""
app = create_secure_typer_app(no_args_is_help=True, pretty_exceptions_show_locals=False)

@app.command()
def init(project_name: Annotated[str, typer.Argument(help= "Name of project to create")] = "default_data_project", 
            project_path: Annotated[str, typer.Argument(help= "")] = "./tmp", 
            project_type: Annotated[str, typer.Option()] = KIMBALL_PROJECT_TYPE):
    """
    Create a project structure with a specified name, target path, and optional project type. 
    The project type can be one of `kimball` or `data_product`. 
    Kimball will use a structure like 
    pipelines/sources
    pipelines/facts
    pipelines/dimensions
    ...
    """
    print("#" * 30 + f" Build Project {project_name} in the {project_path} folder with a structure {project_type}")
    project_manager.build_project_structure(project_name,project_path, project_type)
    print(f"Project {project_name} created in {project_path}")

#@app.command()
def update_all_makefiles(pipeline_folder_path: Annotated[str, typer.Argument(help="Pipeline folder where all the Flink statements reside")]):
        """
        Update the makefile with a new template. Not yet implemented
        """
        print("Not implemented yet")
        pass

@app.command()
def list_topics(project_path: Annotated[str, typer.Argument(help="Project path to save the topic list text file.")]):
        """
        Get the list of topics for the Kafka Cluster define in `config.yaml` and save the list in the `topic_list.txt` file under the given folder. Be sure to have a `conflig.yaml` file setup.
        """
        print("#" * 30 + f" List topic {project_path}")
        list_of_topics = project_manager.get_topic_list(project_path + "/topic_list.txt")
        print(list_of_topics)
        print(f"Topic list saved in {project_path}/topic_list.txt")

@app.command()
def list_compute_pools(environment_id: str = typer.Option(None, help="Environment_id to return all compute pool"),
                      region: str = typer.Option(None, help="Region_id to return all compute pool")):
        """
        Get the complete list and detail of the compute pools of the given environment_id. If the environment_id is not specified, it will use the conflig.yaml
        with the ['confluent_cloud']['environment_id']
        """
        if not environment_id:
               environment_id = get_config().get('confluent_cloud').get('environment_id')
        if not region:
               region = get_config().get('confluent_cloud').get('region')
        print("#" * 30 + f" List compute pools for environment {environment_id} - in region {region}")
        list_of_pools = compute_pool_mgr.get_compute_pool_list(environment_id, region)
        print(list_of_pools)

@app.command()
def delete_all_compute_pools(product_name: Annotated[str, typer.Argument(help="The product name to delete all compute pools for")]):
        """
        Delete all compute pools for the given product name
        """
        print("#" * 30 + f" Delete all compute pools for product {product_name}")
        compute_pool_mgr.delete_all_compute_pools_of_product(product_name)
        print(f"Done")

@app.command()
def housekeep_statements( starts_with: str = typer.Option(None, "--starts-with", help="Statements names starting with this string. [default: workspace]"),
                      status: str = typer.Option(None, "--status", help="Statements with this status. [default: COMPLETED, FAILED]"),
                      age: int = typer.Option(None, "--age", help="Statements with created_date >= age (days). [default: 0]")):
        """
        Delete statements in FAILED or COMPLETED state that starts with string 'workspace' in it ( default ).
        Applies optional starts-with and age filters when provided.
        """
        reserved_words = ['dev','stage','prod']
        default_status = ['COMPLETED', 'FAILED']
        allowed_status = ['COMPLETED', 'FAILED', 'STOPPED']
        statement_status = []

        current_time = datetime.now(timezone.utc)
        completed_stmnt_cnt = failed_stmnt_cnt = stopped_stmnt_cnt = 0

        if not starts_with:
            starts_with='workspace'
        elif starts_with.lower().startswith(tuple(reserved_words)):
           print(f"Search String cannot start with one of these reserved words {reserved_words}")
           sys.exit()

        if not status:
            statement_status = default_status
        elif status.upper() not in allowed_status:
           print(f"Allowed Status are {allowed_status}")
           sys.exit()
        else:
            statement_status.append(status.upper())

        if not age:
            age=0


        print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Clean statements starting with [ {starts_with} ] in {statement_status} state, with a age >= [ {age} ]")

        statement_list = statement_mgr.get_statement_list().copy()
        for statement_name in statement_list:
            statement = statement_list[statement_name]
            statement_created_time = datetime.strptime(statement.created_at.strftime('%Y-%m-%dT%H:%M:%S.%fZ'), '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            time_difference = current_time - statement_created_time
            statement_age = time_difference.days
            #print(f"statement_name: {statement_name} statement_age: {statement_age} statement_status: {statement.status_phase}")
            if statement.name.startswith(starts_with) and statement.status_phase in statement_status and statement_age >= age:
                statement_mgr.delete_statement_if_exists(statement_name)
                if statement.status_phase == 'COMPLETED':
                    completed_stmnt_cnt+=1
                elif statement.status_phase == 'FAILED':
                    failed_stmnt_cnt+=1
                elif statement.status_phase == 'STOPPED':
                    stopped_stmnt_cnt+=1
                print(f"{time.strftime('%Y%m%d_%H:%M:%S')} delete {statement_name} {statement.status_phase}")

        if completed_stmnt_cnt == 0 and failed_stmnt_cnt == 0 and stopped_stmnt_cnt == 0:
            print("No statements deleted")
        else:
            print("\n" + str(completed_stmnt_cnt) + " COMPLETED statements deleted, " + str(failed_stmnt_cnt) + " FAILED statements deleted, " + str(stopped_stmnt_cnt) + " STOPPED statements deleted")

@app.command()
def validate_config():
        """
        Validate the config.yaml file
        """
        print(f"#" * 30 + f" Validate {os.getenv('CONFIG_FILE')}")
        config = get_config()
        validate_config_impl(config)
        print("Config.yaml validated")

@app.command()
def report_table_cross_products():
        """
        Report the list of tables that are referenced in other products
        """
        print("#" * 30 + f" Report table cross products")
        result = project_manager.report_table_cross_products(os.getenv("PIPELINES"))
        print(result)
        if result:
            with open(shift_left_dir + "/table_cross_products.txt", "w") as f:
                for table in result:
                    f.write(table + "\n")
            print(f"Table cross products saved in {os.getenv('PIPELINES')}/table_cross_products.txt")
        else:
            print(f"No table cross products found")


@app.command()
def list_tables_with_one_child():
        """
        Report the list of tables that have exactly one child table
        """
        print("#" * 30 + f" List tables with one child")
        result = project_manager.list_tables_with_one_child(os.getenv("PIPELINES"))
        print(result)
        if result:
            with open(shift_left_dir + "/tables_with_one_child.txt", "w") as f:
                for table in result:
                    f.write(table + "\n")
            print(f"Tables with one child saved in {os.getenv('PIPELINES')}/tables_with_one_child.txt")
        else:
            print(f"No tables with one child found")
       

@app.command()
def list_modified_files(
    branch_name: Annotated[str, typer.Argument(help="Git branch name to compare against (e.g., 'main', 'origin/main')")],
    output_file: Annotated[str, typer.Option(help="Output file path to save the list")] = "modified_flink_files.txt",
    project_path: Annotated[str, typer.Option(help="Project path where git repository is located")] = ".",
    file_filter: Annotated[str, typer.Option(help="File extension filter (e.g., '.sql', '.py')")] = ".sql",
    since: Annotated[str, typer.Option(help="Date from which the files were modified (e.g., 'YYYY-MM-DD')")] = None
):
    """
    Get the list of files modified in the current git branch compared to the specified branch.
    Filters for Flink-related files (by default SQL files) and saves the list to a text file.
    This is useful for identifying which Flink statements need to be redeployed in a blue-green deployment.
    """
    print("#" * 30 + f" List modified files in current branch vs {branch_name}")
    result = project_manager.list_modified_files(project_path, branch_name, since, file_filter, output_file)
    
    # Display structured result summary
    print(f"\nSummary:")
    print(f"   Total modified files: {len(result.file_list)}")
    print(f"   Tables affected:")
    if result.file_list:
        
        for file in result.file_list:
           if  not file.same_sql_content:
                print(f"   {file.table_name} {file.file_modified_url} \t\t -> not same sql content")
           elif not file.running:
                print(f"   {file.table_name} {file.file_modified_url} \t\t -> not running")

    
    return result

@app.command()
def init_integration_tests(
    sink_table_name: Annotated[str, typer.Argument(help="Name of the sink table to create integration tests for")],
    project_path: Annotated[str, typer.Option(envvar=["PIPELINES"], help="Project path where pipelines are located. If not provided, uses $PIPELINES environment variable")] = None
):
    """
    Initialize integration test structure for a given sink table.
    Creates test scaffolding including synthetic data templates and validation queries.
    Integration tests validate end-to-end data flow from source tables to the specified sink table.
    """
    print("#" * 30 + f" Initialize Integration Tests for {sink_table_name}")
    
    try:
        test_path = integration_test_mgr.init_integration_tests(sink_table_name, project_path)
        print(f"✅ Integration test structure created successfully")
        print(f"Test location: {test_path}")
        print(f"\nNext steps:")
        print(f"   1. Review and update synthetic data files in the test directory")
        print(f"   2. Customize validation queries based on your business logic")
        print(f"   3. Run tests with: shift_left project run-integration-tests {sink_table_name}")
        
    except Exception as e:
        print(f"❌ Error initializing integration tests: {e}")
        raise typer.Exit(1)

@app.command()
def run_integration_tests(
    sink_table_name: Annotated[str, typer.Argument(help="Name of the sink table to run integration tests for")],
    scenario_name: str = typer.Option(None, "--scenario-name", help="Specific test scenario to run. If not specified, runs all scenarios"),
    project_path: Annotated[str, typer.Option(envvar=["PIPELINES"], help="Project path where pipelines are located")] = None,
    compute_pool_id: str = typer.Option(None, "--compute-pool-id", envvar=["CPOOL_ID"], help="Flink compute pool ID. Uses config.yaml value if not provided"),
    measure_latency: bool = typer.Option(True, "--measure-latency/--no-measure-latency", help="Whether to measure end-to-end latency from source to sink"),
    output_file: str = typer.Option(None, "--output-file", help="File path to save detailed test results")
):
    """
    Run integration tests for a given sink table.
    Executes end-to-end pipeline testing by injecting synthetic data into source tables,
    waiting for data to flow through the pipeline, and validating results in the sink table.
    Optionally measures latency from data injection to sink table arrival.
    """
    print("#" * 30 + f" Run Integration Tests for {sink_table_name}")
    
    if not compute_pool_id:
        compute_pool_id = get_config().get('flink', {}).get('compute_pool_id')
    
    if scenario_name:
        print(f"Running specific scenario: {scenario_name}")
    else:
        print(f"Running all test scenarios")
        
    print(f"Latency measurement: {'enabled' if measure_latency else 'disabled'}")
    print(f"Compute pool: {compute_pool_id}")
    
    try:
        results = integration_test_mgr.run_integration_tests(
            sink_table_name=sink_table_name,
            scenario_name=scenario_name,
            project_path=project_path,
            compute_pool_id=compute_pool_id,
            measure_latency=measure_latency
        )
        
        # Display results summary
        print(f"\n{'=' * 60}")
        print(f"Integration Test Results Summary")
        print(f"{'=' * 60}")
        print(f"Suite: {results.suite_name}")
        print(f"Overall Status: {_get_status_emoji(results.overall_status)} {results.overall_status}")
        print(f"Total Duration: {results.total_duration_ms:.2f}ms")
        print(f"Test Scenarios: {len(results.test_results)}")
        
        # Display individual scenario results
        for test_result in results.test_results:
            status_emoji = _get_status_emoji(test_result.status)
            print(f"\n  {status_emoji} {test_result.scenario_name}")
            print(f"     Status: {test_result.status}")
            print(f"     Duration: {test_result.duration_ms:.2f}ms")
            
            if test_result.latency_results:
                avg_latency = sum(lr.latency_ms for lr in test_result.latency_results) / len(test_result.latency_results)
                print(f"     Avg Latency: {avg_latency:.2f}ms")
            
            if test_result.error_message:
                print(f"     Error: {test_result.error_message}")
        
        # Save detailed results if output file specified
        if output_file:
            with open(output_file, 'w') as f:
                f.write(results.model_dump_json(indent=2))
            print(f"\nDetailed results saved to: {output_file}")
        
        print(f"\n{'=' * 60}")
        
        if results.overall_status != "PASS":
            raise typer.Exit(1)
            
    except Exception as e:
        print(f"❌ Error running integration tests: {e}")
        raise typer.Exit(1)

@app.command()
def delete_integration_tests(
    sink_table_name: Annotated[str, typer.Argument(help="Name of the sink table to delete integration test artifacts for")],
    project_path: Annotated[str, typer.Option(envvar=["PIPELINES"], help="Project path where pipelines are located")] = None,
    compute_pool_id: str = typer.Option(None, "--compute-pool-id", envvar=["CPOOL_ID"], help="Flink compute pool ID where test artifacts were created"),
    no_confirm: bool = typer.Option(False, "--no-confirm", help="Skip confirmation prompt and delete immediately")
):
    """
    Delete all integration test artifacts (Flink statements and Kafka topics) for a given sink table.
    This cleanup command removes all test-related resources created during integration test execution.
    """
    print("#" * 30 + f" Delete Integration Test Artifacts for {sink_table_name}")
    
    if not no_confirm:
        print("⚠️  This will delete all integration test artifacts including:")
        print("   - Flink statements with '_it' postfix")
        print("   - Associated Kafka topics")
        print("   - Test data and temporary resources")
        
        confirm = typer.confirm("Are you sure you want to proceed?")
        if not confirm:
            print("❌ Deletion cancelled")
            raise typer.Exit(0)
    
    try:
        integration_test_mgr.delete_integration_test_artifacts(
            sink_table_name=sink_table_name,
            project_path=project_path,
            compute_pool_id=compute_pool_id
        )
        
        print(f"✅ Integration test artifacts deleted successfully")
        print(f"All test resources for '{sink_table_name}' have been cleaned up")
        
    except Exception as e:
        print(f"❌ Error deleting integration test artifacts: {e}")
        raise typer.Exit(1)

def _get_status_emoji(status: str) -> str:
    """Get emoji representation for test status"""
    emoji_map = {
        "PASS": "✅",
        "FAIL": "❌", 
        "ERROR": "🚫"
    }
    return emoji_map.get(status, "❓")

@app.command()
def isolate_data_product(
    product_name: Annotated[str, typer.Argument(help="Product name to isolate")],
    source_folder: Annotated[str, typer.Argument(help="Source folder to isolate the data product")],
    target_folder: Annotated[str, typer.Argument(help="Target folder to isolate the data product")]
    ):
    """
    Isolate the data product from the project
    """
    print("#" * 30 + f" Isolate data product {product_name} from {source_folder} to {target_folder}")
    project_manager.isolate_data_product(product_name, source_folder, target_folder)
    print(f"Data product isolated in {target_folder}")