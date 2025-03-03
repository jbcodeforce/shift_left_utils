import typer
from rich import print
from shift_left.core.project_manager import build_project_structure, DATA_PRODUCT_PROJECT_TYPE, KIMBALL_PROJECT_TYPE
from typing_extensions import Annotated

app = typer.Typer(no_args_is_help=True)

@app.command()
def init(project_name: Annotated[str, typer.Argument()] = "data_project", 
            project_path: Annotated[str, typer.Argument()] = "./tmp", 
            project_type: Annotated[str, typer.Option()] = DATA_PRODUCT_PROJECT_TYPE):
    """
    Create a project structure with a specified name, target path, and optional project type. 
    The project type can be either 'kimball' [default].
    """
    build_project_structure(project_name,project_path, project_type)
    print(f"Project {project_name} created in {project_path}")