import typer
import os
from rich import print
from typing_extensions import Annotated
from shift_left.core.table_mgr import (
    build_folder_structure_for_table, 
    search_source_dependencies_for_dbt_table, 
    extract_table_name, build_update_makefile, 
    validate_table_cross_products,
    search_users_of_table,
    get_or_create_inventory)
from shift_left.core.process_src_tables import process_one_file

"""
Manage the table entities.
- build an inventory of all the tables in the project with the basic metadata per table

"""
app = typer.Typer()

@app.command()
def init(table_name: Annotated[str, typer.Argument()],
         table_path: Annotated[str, typer.Argument()]):
    """
    Build a new table structure under the specified path. For example to add a source table structure
    use table init src_table_1 $PIPELINES/sources/p1
    """
    print("#" * 30 + f" Build Table in {table_path}")
    table_folder, table_name= build_folder_structure_for_table(table_name, table_path)
    print(f"Created folder {table_folder} for the table {table_name}")

@app.command()
def build_inventory(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"])]):
    """
    Build the table inventory from the PIPELINES path.
    """
    print("#" * 30 + f" Build Inventory in {pipeline_path}")
    inventory= get_or_create_inventory(pipeline_path)
    print(inventory)
    print(f"--> Table inventory created into {pipeline_path} with {len(inventory)} entries")

@app.command()
def search_source_dependencies(table_sql_file_name: Annotated[str, typer.Argument()],
                                src_project_folder: Annotated[str, typer.Argument(envvar=["SRC_FOLDER"])]):
    """
    Search the parent for a given table from the source project.
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
    """
    print("#" * 30 + f" Migrate source SQL Table defined in {sql_src_file_name}")
    if not sql_src_file_name.endswith(".sql"):
        print("[red]Error: the first parameter needs to be a dml sql file[/red]")
        exit(1)
    print(f"Migrate source SQL Table defined in {sql_src_file_name} to {target_path} with its pipeline: {recursive}")
    process_one_file(table_name, sql_src_file_name, os.getenv("STAGING"), target_path, recursive)
    print(f"Migrated content to folder {target_path} for the table {sql_src_file_name}")

@app.command()
def update_makefile(
        table_name: Annotated[str, typer.Argument(help= "Name of the table to process and update the Makefile from.")],
        pipeline_folder_name: Annotated[str, typer.Argument(help= "Pipeline folder where all the tables are defined")]):
    """ Update existing Makefile for a given table or build a new one """

    build_update_makefile(pipeline_folder_name, table_name)
    print(f"Makefile updated for table {table_name}")


@app.command()
def find_table_users(table_name: Annotated[str, typer.Argument()],
                     pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"])]):
    """ Find the Flink Statements user of a given table """
    print("#" * 30 + f" find_table_users for  {table_name}")
    out=search_users_of_table(table_name, pipeline_path)
    print(out)

@app.command()
def validate_table_names(pipeline_folder_name: Annotated[str, typer.Argument(help= "Pipeline folder where all the tables are defined")]):
    print("#" * 30 + f" validate_table_names in {pipeline_folder_name}")
    validate_table_cross_products(pipeline_folder_name)