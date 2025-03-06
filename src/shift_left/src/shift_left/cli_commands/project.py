import typer
from rich import print
from shift_left.core.project_manager import (
        build_project_structure, 
        DATA_PRODUCT_PROJECT_TYPE, 
        KIMBALL_PROJECT_TYPE,
        get_topic_list)
from typing_extensions import Annotated

app = typer.Typer(no_args_is_help=True)

@app.command()
def init(project_name: Annotated[str, typer.Argument()] = "default_data_project", 
            project_path: Annotated[str, typer.Argument()] = "./tmp", 
            project_type: Annotated[str, typer.Option()] = DATA_PRODUCT_PROJECT_TYPE):
    """
    Create a project structure with a specified name, target path, and optional project type. 
    The project type can be either 'kimball' [default].
    """
    print("#" * 30 + f" Build Project in {project_path}")
    build_project_structure(project_name,project_path, project_type)
    print(f"Project {project_name} created in {project_path}")

@app.command()
def update_all_makefile(pipeline_folder_path: Annotated[str, typer.Argument()]):
        pass

@app.command()
def list_topics(project_path: Annotated[str, typer.Argument()]):
        """
        Get the list of topics for the Kafka Cluster. Be sure to have a conflig.yaml file setup
        """
        print("#" * 30 + f" List topic {project_path}")
        list_of_topics = get_topic_list(project_path + "/topic_list.txt")
        print(list_of_topics)