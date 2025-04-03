import typer
import os
from importlib import import_module
from rich import print
from typing_extensions import Annotated
from shift_left.core.table_mgr import (
    build_folder_structure_for_table, 
    search_source_dependencies_for_dbt_table, 
    extract_table_name, 
    build_update_makefile, 
    validate_table_cross_products,
    search_users_of_table,
    update_sql_content_for_file,
    get_or_create_inventory)
from shift_left.core.process_src_tables import process_one_file
from shift_left.core.utils.file_search import list_src_sql_files

"""
Manage the table entities.
- build an inventory of all the tables in the project with the basic metadata per table
- deploy a table taking care of the children Flink statements to stop and start
- 
"""
app = typer.Typer()

@app.command()
def init(table_name: Annotated[str, typer.Argument(help="Table name to build")],
         table_path: Annotated[str, typer.Argument(help="Path in which the table folder stucture will be created under.")]):
    """
    Build a new table structure under the specified path. For example to add a source table structure use for example the command:
    `shift_left table init src_table_1 $PIPELINES/sources/p1`
    """
    print("#" * 30 + f" Build Table in {table_path}")
    table_folder, table_name= build_folder_structure_for_table(table_name, table_path)
    print(f"Created folder {table_folder} for the table {table_name}")

@app.command()
def build_inventory(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """
    Build the table inventory from the PIPELINES path.
    """
    print("#" * 30 + f" Build Inventory in {pipeline_path}")
    inventory= get_or_create_inventory(pipeline_path)
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
    table_name = extract_table_name(table_sql_file_name)
    print(f"Table {table_name} in the SQL {table_sql_file_name} depends on:")
    for table in dependencies:
        print(f"  - {table['table']} (in {table['src_dbt']})")
    print("#" * 80)
        

@app.command()
def migrate(
        table_name: Annotated[str, typer.Argument(help= "the name of the table once migrated.")],
        sql_src_file_name: Annotated[str, typer.Argument(help= "the source file name for the sql script to migrate.")],
        target_path: Annotated[str, typer.Argument(envvar=["STAGING"], help ="the target path where to store the migrated content (default is $STAGING)")],
        recursive: bool = typer.Option(False, "--recursive", help="Indicates whether to process recursively up to the sources. (default is False)")):
    """
    Migrate a source SQL Table defined in a sql file with AI Agent to a Staging area to complete the work. 
    The command uses the SRC_FOLDER to access to src_path folder.
    """
    print("#" * 30 + f" Migrate source SQL Table defined in {sql_src_file_name}")
    if not sql_src_file_name.endswith(".sql"):
        print("[red]Error: the first parameter needs to be a dml sql file[/red]")
        exit(1)
    if not os.getenv("SRC_FOLDER"):
        print("[red]Error: SRC_FOLDER environment variable needs to be defined.[/red]")
        exit(1)
    print(f"Migrate source SQL Table defined in {sql_src_file_name} to {target_path} with its pipeline: {recursive}")
    process_one_file(table_name, sql_src_file_name, target_path, os.getenv("SRC_FOLDER"), recursive)
    print(f"Migrated content to folder {target_path} for the table {sql_src_file_name}")

@app.command()
def update_makefile(
        table_name: Annotated[str, typer.Argument(help= "Name of the table to process and update the Makefile from.")],
        pipeline_folder_name: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """ Update existing Makefile for a given table or build a new one """

    build_update_makefile(pipeline_folder_name, table_name)
    print(f"Makefile updated for table {table_name}")


@app.command()
def find_table_users(table_name: Annotated[str, typer.Argument(help="The name of the table to search ")],
                     pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"], help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """ Find the Flink Statements, user of a given table """
    print("#" * 30 + f" find_table_users for  {table_name}")
    out=search_users_of_table(table_name, pipeline_path)
    print(out)

@app.command()
def validate_table_names(pipeline_folder_name: Annotated[str, typer.Argument(envvar=["PIPELINES"],help= "Pipeline folder where all the tables are defined, if not provided will use the $PIPELINES environment variable.")]):
    """
    Go over the pipeline folder to assess table name and naming convention are respected.
    """
    print("#" * 30 + f"\nValidate_table_names in {pipeline_folder_name}")
    validate_table_cross_products(pipeline_folder_name)

@app.command()
def update_tables(folder_to_work_from: Annotated[str, typer.Argument(help="Folder from where to do the table update. It could be the all pipelines or subfolders.")],
                  ddl: bool = typer.Option(False, "--ddl", help="Focus on DDL processing. Default is only DML"),
                  both_ddl_dml: bool = typer.Option(False, "--both-ddl-dml", help="Run both DDL and DML sql files"),
                  
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
        for file in files_to_process:
            print(f"Assessing file {file}")    
            updated=update_sql_content_for_file(file, runner_class)
            if updated:
                print(f"-> {file} processed ")
    print("Done !")


@app.command()
def unit_test(  table_name: Annotated[str, typer.Argument(help= "Name of the table to unit tests.")],
                test_case_name:  Annotated[str, typer.Option(help= "Name of the individual unit test to run. By default it will run all the tests")],
                compute_pool_id: Annotated[str, typer.Option(envvar=["CPOOL_ID"], help="Flink compute pool ID. If not provided, it will create a pool.")]):
    """
    Run all the unit tests or a specified test case by sending data to `_ut` topics and validating the results
    """
    print("NOT YET IMPLEMENTED")
    pass