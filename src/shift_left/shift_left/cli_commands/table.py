"""
Copyright 2024-2025 Confluent, Inc.
"""
import typer
import os
from importlib import import_module
from rich import print
from typing_extensions import Annotated
from shift_left.core.table_mgr import (
    search_source_dependencies_for_dbt_table,
    get_short_table_name,
    update_makefile_in_folder,
    validate_table_cross_products,
    update_sql_content_for_file,
    update_all_makefiles_in_folder,
)
from shift_left.ai.process_src_tables import migrate_one_file
from shift_left.core.utils.file_search import list_src_sql_files
from shift_left.core.utils.app_config import session_log_dir, get_config
from shift_left.core.utils.secure_typer import create_secure_typer_app
import shift_left.core.table_mgr as table_mgr
import shift_left.core.test_mgr as test_mgr
from shift_left.core.utils.app_config import logger
"""
Manage the table entities.
- build an inventory of all the tables in the project with the basic metadata per table
- deploy a table taking care of the children Flink statements to stop and start
- suport commands for test harness
"""
app = create_secure_typer_app(pretty_exceptions_show_locals=False)

@app.command()
def init(table_name: Annotated[str, typer.Argument(help="Table name to build")],
         table_path: Annotated[str, typer.Argument(help="Folder Path in which the table folder structure will be created.")],
         product_name: str = typer.Option(default=None, help="Product name to use for the table. If not provided, it will use the table_path last folder as product name")):
    """
    Build a new table structure under the specified path. For example to add a source table structure use for example the command:
    `shift_left table init src_table_1 $PIPELINES/sources/p1`
    """
    print("#" * 30 + f" Build Table in {table_path}")
    table_folder, table_name= table_mgr.build_folder_structure_for_table(table_name, table_path, product_name)
    print(f"Created folder {table_folder} for the table {table_name}")

@app.command()
def build_inventory(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """
    Build the table inventory from the PIPELINES path.
    """
    print("#" * 30 + f" Build Inventory in {pipeline_path}")
    inventory= table_mgr.get_or_create_inventory(pipeline_path)
    print(inventory)
    print(f"--> Table inventory created into {pipeline_path} with {len(inventory)} entries")

@app.command()
def search_source_dependencies(table_sql_file_name: Annotated[str, typer.Argument(help="Full path to the file name of the dbt sql file")],
                                src_project_folder: Annotated[str, typer.Argument(envvar=["SRC_FOLDER"], help="Folder name for all the dbt sources (e.g. models)")]):
    """
    Search the parent for a given table from the source project (dbt, sql or ksql folders).
    Example: shift_left table search-source-dependencies $SRC_FOLDER/
    """
    if not table_sql_file_name.endswith(".sql"):
        exit(1)
    print(f"The dependencies for {table_sql_file_name} from the {src_project_folder} project are:")
    dependencies = search_source_dependencies_for_dbt_table(table_sql_file_name, src_project_folder)
    table_name = get_short_table_name(table_sql_file_name)
    print(f"Table {table_name} in the SQL {table_sql_file_name} depends on:")
    for table in dependencies:
        print(f"  - {table['table']} (in {table['src_dbt']})")
    print("#" * 80)


@app.command()
def migrate(
        table_name: Annotated[str, typer.Argument(help= "the name of the table once migrated.")],
        sql_src_file_name: Annotated[str, typer.Argument(help= "the source file name for the sql script to migrate.")],
        target_path: Annotated[str, typer.Argument(envvar=["STAGING"], help ="the target path where to store the migrated content (default is $STAGING)")],
        source_type: str = typer.Option(default="spark", help="the type of the SQL source file to migrate. It can be ksql, dbt, spark, etc."),
        validate: bool = typer.Option(False, "--validate", help="Validate the migrated sql using Confluent Cloud for Flink."),
        product_name: str = typer.Option(default="default", help="Product name to use for the table. If not provided, it will use the table_path last folder as product name"),
     ):
    """
    Migrate a source SQL Table defined in a sql file with AI Agent to a Staging area to complete the work.
    The command uses the SRC_FOLDER to access to src_path folder.
    """
    print("\n" + "#" * 30)
    if not sql_src_file_name.endswith(".sql") and not sql_src_file_name.endswith(".ksql"):
        print("[red]Error: the sql_src_file_name parameter needs to be a dml sql file or a ksql file[/red]")
        exit(1)
    if not target_path and not os.getenv("STAGING"):
        print("[red]Error:target_path need to be provided or STAGING environment variables need to be defined.[/red]")
        exit(1)
    migrate_one_file(table_name=table_name,
                    sql_src_file=sql_src_file_name,
                    staging_target_folder=target_path,
                    source_type=source_type,
                    product_name=product_name,
                    validate=validate)
    print(f"\n Migration completed for {table_name}" + "#" * 30)


@app.command()
def update_makefile(
        table_name: Annotated[str, typer.Argument(help= "Name of the table to process and update the Makefile from.")],
        pipeline_folder_name: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """ Update existing Makefile for a given table or build a new one """

    update_makefile_in_folder(pipeline_folder_name, table_name)
    print(f"Makefile updated for table {table_name}")

@app.command()
def update_all_makefiles(
        folder_name: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Folder from where all the Makefile will be updated. If not provided, it will use the $PIPELINES environment variable.")]):
    """ Update all the Makefiles for all the tables in the given folder. Example: shift_left table update-all-makefiles $PIPELINES/dimensions/product_1
    """
    count = update_all_makefiles_in_folder(folder_name)
    print(f"Updated {count} Makefiles in {folder_name}")



@app.command()
def validate_table_names(pipeline_folder_name: Annotated[str, typer.Argument(envvar=["PIPELINES"],help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """
    Go over the pipeline folder to assess if table name,  naming convention, and other development best practices are respected.
    """
    print("#" * 30 + f"\nValidate_table_names in {pipeline_folder_name}")
    validate_table_cross_products(pipeline_folder_name)

@app.command()
def update_tables(folder_to_work_from: Annotated[str, typer.Argument(help="Folder from where to do the table update. It could be the all pipelines or subfolders.")],
                  ddl: bool = typer.Option(False, "--ddl", help="Focus on DDL processing. Default is only DML"),
                  both_ddl_dml: bool = typer.Option(False, "--both-ddl-dml", help="Run both DDL and DML sql files"),
                  string_to_change_from: str = typer.Option(None, "--string-to-change-from", help="String to change in the SQL content"),
                  string_to_change_to: str = typer.Option(None, "--string-to-change-to", help="String to change in the SQL content"),
                  class_to_use = Annotated[str, typer.Argument(help= "The class to use to do the Statement processing", default="shift_left.core.utils.table_worker.ChangeLocalTimeZone")]):
    """
    Update the tables with SQL code changes defined in external python callback. It will read dml or ddl and apply the updates.
    """
    print("#" * 30 + f"\nUpdate_tables from {folder_to_work_from} using the processor: {class_to_use}")
    files = list_src_sql_files(folder_to_work_from)
    files_to_process =[]
    if both_ddl_dml or ddl: # focus on DDLs update
        for file in files:
            if file.startswith("ddl"):
                files_to_process.append(files[file])
    if not ddl:
        for file in files:
            if file.startswith("dml"):
                files_to_process.append(files[file])
    if class_to_use:
        module_path, class_name = class_to_use.rsplit('.',1)
        mod = import_module(module_path)
        runner_class = getattr(mod, class_name)
        count=0
        processed=0
        for file in files_to_process:
            print(f"Assessing file {file}")
            updated=update_sql_content_for_file(file, runner_class(), string_to_change_from, string_to_change_to)
            if updated:
                print(f"-> {file} processed ")
                processed+=1
            else:
                print(f"-> already up to date ")
            count+=1
    print(f"Done: processed: {processed} of {count} files!")


@app.command()
def init_unit_tests(
    # ✅
    table_name: Annotated[str, typer.Argument(help="Name of the table to unit tests.")],
    create_csv: bool = typer.Option(False, "--create-csv", help="If set, also create a CSV file for the unit test data."),
    nb_test_cases: int = typer.Option(default=test_mgr.DEFAULT_TEST_CASES_COUNT, help="Number of test cases to create. Default is 2."),
    ai: bool = typer.Option(False, "--ai", help="Use AI to generate test data and validate with tool calling.")):
    """
    Initialize the unit test folder and template files for a given table. It will parse the SQL statements to create the insert statements for the unit tests.
    It is using the table inventory to find the table folder for the given table name.
    Optionally, it can also create a CSV file for the unit test data if --create-csv is set.
    """
    print("#" * 30 + f" Unit tests initialization for {table_name}")
    test_mgr.init_unit_test_for_table(table_name, create_csv=create_csv, nb_test_cases=nb_test_cases, use_ai=ai)
    print("#" * 30 + f" Unit tests initialization for {table_name} completed")

@app.command()
def run_unit_tests(  table_name: Annotated[str, typer.Argument(help= "Name of the table to unit tests.")],
                test_case_name: str = typer.Option(default=None, help= "Name of the individual unit test to run. By default it will run all the tests"),
                run_all: bool = typer.Option(False, "--run-all", help="By default run insert sqls and foundations, with this flag it will also run validation sql too."),
                compute_pool_id: str = typer.Option(default=None, envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will use config.yaml one."),
                post_fix_unit_test: str = typer.Option(default=None, help="Provide a unique post fix (e.g _foo) to avoid conflicts with other UT runs. If not provided will use config.yaml, if that doesnt exist, use default _ut.")):
    """
    Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results
    """
    if not compute_pool_id:
        compute_pool_id = get_config().get('flink').get('compute_pool_id')
    if post_fix_unit_test:
        pfut = post_fix_unit_test.lstrip('_')
        if not post_fix_unit_test.startswith("_") or not (2<= len(pfut) <= 3) or not pfut.isalnum():
            print(f"[red]Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric[/red]")
            raise typer.Exit(1)
        test_mgr.CONFIGURED_POST_FIX_UNIT_TEST=post_fix_unit_test

    print("#" * 30 + f" Unit tests execution for {table_name} - {compute_pool_id}")
    print(f"Cluster name: {get_config().get('flink').get('database_name')}")
    logger.info(f"Unit tests execution for {table_name} test: {test_case_name} - {compute_pool_id} Cluster name: {get_config().get('flink').get('database_name')} post_fix_unit_test: {post_fix_unit_test}")
    test_suite_result  = test_mgr.execute_one_or_all_tests(table_name=table_name,
                                                test_case_name=test_case_name,
                                                compute_pool_id=compute_pool_id,
                                                run_validation=run_all)

    file_name = f"{session_log_dir}/{table_name}-test-suite-result.json"
    with open(file_name, "w") as f:
        f.write(test_suite_result.model_dump_json(indent=2))
    print(f"Test suite report saved into {file_name}")
    print("#" * 30 + f" Unit tests execution for {table_name} completed")

@app.command()
def run_validation_tests(table_name: Annotated[str, typer.Argument(help= "Name of the table to unit tests.")],
                test_case_name: str = typer.Option(default=None, help= "Name of the individual unit test to run. By default it will run all the tests"),
                run_all: bool = typer.Option(False, "--run-all", help="With this flag, and not test case name provided, it will run all the validation sqls."),
                compute_pool_id: str = typer.Option(default=None, envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will use config.yaml one."),
                post_fix_unit_test: str = typer.Option(default=None, help="By default it is _ut. A Unique post fix to avoid conflict between multiple UT runs. If not provided, it will use config.yaml one.")):
    """
    Run only the validation tests (1 to n validation tests) for a given table.
    """
    logger.info(f"Run valdiation test for {table_name} test: {test_case_name} - {compute_pool_id}")

    if post_fix_unit_test:
        pfut = post_fix_unit_test.lstrip('_')
        if not post_fix_unit_test.startswith("_") or not (2<= len(pfut) <= 3) or not pfut.isalnum():
            print(f"[red]Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric[/red]")
            raise typer.Exit(1)
        test_mgr.CONFIGURED_POST_FIX_UNIT_TEST=post_fix_unit_test

    test_suite_result = test_mgr.execute_validation_tests(table_name, test_case_name, compute_pool_id, run_all)
    file_name = f"{session_log_dir}/{table_name}-test-suite-result.json"
    with open(file_name, "w") as f:
        f.write(test_suite_result.model_dump_json(indent=2))
    print(f"Test suite report saved into {file_name}")
    logger.info(f"Test result: {test_suite_result.model_dump_json(indent=2)}")
    print("#" * 30 + f" Unit tests validation execution for {table_name} - {compute_pool_id}")

@app.command()
def validate_unit_tests(table_name: Annotated[str, typer.Argument(help= "Name of the table to unit tests.")],
                test_case_name: str = typer.Option(default=None, help= "Name of the individual unit test to run. By default it will run all the tests"),
                run_all: bool = typer.Option(False, "--run-all", help="With this flag, and not test case name provided, it will run all the validation sqls."),
                compute_pool_id: str = typer.Option(default=None, envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will use config.yaml one."),
                post_fix_unit_test: str = typer.Option(default=None, help="By default it is _ut. A Unique post fix to avoid conflict between multiple UT runs. If not provided, it will use config.yaml one.")):
    """
    just a synonym for run-validation-tests
    """

    if post_fix_unit_test:
        pfut = post_fix_unit_test.lstrip('_')
        if not post_fix_unit_test.startswith("_") or not (2<= len(pfut) <= 3) or not pfut.isalnum():
            print(f"[red]Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric[/red]")
            raise typer.Exit(1)

    run_validation_tests(table_name, test_case_name, run_all, compute_pool_id=compute_pool_id, post_fix_unit_test=post_fix_unit_test)

@app.command()
def delete_unit_tests(table_name: Annotated[str, typer.Argument(help= "Name of the table to unit tests.")],
                 compute_pool_id: str = typer.Option(default=None, envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will use config.yaml one."),
                post_fix_unit_test: str = typer.Option(default=None, help="By default it is _ut. A Unique post fix to avoid conflict between multiple UT runs. If not provided, it will use config.yaml one.")):
    """
    Delete the Flink statements and kafka topics used for unit tests for a given table.
    """
    if post_fix_unit_test:
        pfut = post_fix_unit_test.lstrip('_')
        if not post_fix_unit_test.startswith("_") or not (2<= len(pfut) <= 3) or not pfut.isalnum():
            print(f"[red]Error: post-fix-unit-test must start with _, be 2 or 3 characters and be alpha numeric[/red]")
            raise typer.Exit(1)
        test_mgr.CONFIGURED_POST_FIX_UNIT_TEST=post_fix_unit_test

    print("#" * 30 + f" Unit tests deletion for {table_name}")
    test_mgr.delete_test_artifacts(table_name, compute_pool_id)
    print("#" * 30 + f" Unit tests deletion for {table_name} completed")

@app.command()
def explain(table_name: str=  typer.Option(None,help= "Name of the table to get Flink execution plan explanations from."),
            product_name: str = typer.Option(None, help="The directory to run the explain on each tables found within this directory. table or dir needs to be provided."),
            table_list_file_name: str = typer.Option(None, help="The file containing the list of tables to deploy."),
            compute_pool_id: str = typer.Option(default=None, envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will use config.yaml one."),
            persist_report: bool = typer.Option(False, "--persist-report", help="Persist the report in the shift_left_dir folder.")):
    """
    Get the Flink execution plan explanations for a given table or a group of tables using the product name or a list of tables from a file.
    """

    if table_name:
        print("#" * 30 + f" Flink execution plan explanations for {table_name}")
        table_report=table_mgr.explain_table(table_name=table_name,
                                             compute_pool_id=compute_pool_id,
                                             persist_report=persist_report)
        print(f"Table: {table_report['table_name']}")
        print("-"*50)
        print(table_report['trace'])
        print("#" * 30 + f" Flink execution plan explanations for {table_name} completed")
    elif product_name:
        print("#" * 30 + f" Flink execution plan explanations for the product: {product_name}")
        tables_report=table_mgr.explain_tables_for_product(product_name=product_name,
                                                           compute_pool_id=compute_pool_id,
                                                           persist_report=persist_report)
        print(tables_report)
        print("#" * 30 + f" Flink execution plan explanations for the product {product_name} completed")
    elif table_list_file_name:
        print("#" * 30 + f" Flink execution plan explanations for the tables in {table_list_file_name}")
        tables_report=table_mgr.explain_tables_for_list_of_tables(table_list_file_name=table_list_file_name,
                                                                  compute_pool_id=compute_pool_id,
                                                                  persist_report=persist_report)
        print(tables_report)
        print("#" * 30 + f" Flink execution plan explanations for the tables in {table_list_file_name} completed")
    else:
        print("[red]Error: table or dir needs to be provided.[/red]")
        exit(1)

