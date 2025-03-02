import typer
from rich import print
from shift_left.core.pipeline_mgr import build_inventory, build_pipeline_definition_from_table
from typing_extensions import Annotated

"""
Manage a pipeline entity:
- build the inventory of tables
"""
app = typer.Typer(no_args_is_help=True)



@app.command()
def build(dml_file_name:  Annotated[str, typer.Argument()], 
             pipeline_path: Annotated[str, typer.Argument(envvar=["PIPELINES"])]):
    """
    Build a pipeline from a sink table: add or update {} each table in the pipeline
    """
    print(f"Build pipeline from sink table {dml_file_name}")
    build_pipeline_definition_from_table(dml_file_name,pipeline_path)