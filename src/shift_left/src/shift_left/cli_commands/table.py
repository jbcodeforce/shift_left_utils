import typer
from rich import print
import shift_left.core.pipeline_mgr as pm 
from typing_extensions import Annotated
from shift_left.core.table_mgr import build_folder_structure_for_table

"""
Manage the table entities.
- build an inventory of all the tables in the project with the basic metadata per table
"""
app = typer.Typer()


@app.command()
def build_inventory(pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"])]):
    """
    Build the table inventory from the PIPELINES path.
    """
    print("#" * 30 + f" Build Inventory in {pipeline_path}")
    inventory= pm.build_inventory(pipeline_path)
    print(f"--> Table inventory created into {pipeline_path} with {len(inventory)} entries")

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

    