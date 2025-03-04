import typer
from rich import print
import shift_left.core.pipeline_mgr as pm 
from typing_extensions import Annotated
from shift_left.core.table_mgr import build_folder_structure_for_table
#from shift_left.core.process_src_tables import process_one_file
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
    inventory= pm.build_inventory(pipeline_path)
    print(f"--> Table inventory created into {pipeline_path} with {len(inventory)} entries")

"""
@app.command()
def migrate(sql_src_file_name: Annotated[str, typer.Argument()],
         target_path: Annotated[str, typer.Argument()],
         recursive: bool = typer.Option(False, "--recursive", help="Indicates whether to process recursively up to the sources")):
    
    Build a new table structure under the specified path. For example to add a source table structure
    use table init src_table_1 $PIPELINES/sources/p1
    
    print("#" * 30 + f" Migrate source SQL Table defined in {sql_src_file_name}")
    if not sql_src_file_name.endswith(".sql"):
        exit(1)
    process_one_file(sql_src_file_name, target_path, recursive)
    print(f"Migrated content to folder {target_path} for the table {sql_src_file_name}")

"""


    