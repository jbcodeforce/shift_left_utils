"""
Copyright 2024-2025 Confluent, Inc.
"""
import typer
from rich import print
from shift_left.core.utils.app_config import get_config, log_file_path
from shift_left.core.compute_pool_mgr import get_compute_pool_list
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
import shift_left.core.project_manager as project_manager
from shift_left.core.project_manager import (
        DATA_PRODUCT_PROJECT_TYPE, 
        KIMBALL_PROJECT_TYPE)
from typing_extensions import Annotated
from shift_left.core.models.flink_statement_model import Statement 

"""
Manage project foundations
"""
app = typer.Typer(no_args_is_help=True)

@app.command()
def init(project_name: Annotated[str, typer.Argument(help= "Name of project to create")] = "default_data_project", 
            project_path: Annotated[str, typer.Argument(help= "")] = "./tmp", 
            project_type: Annotated[str, typer.Option()] = DATA_PRODUCT_PROJECT_TYPE):
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
        print("#" * 30 + f" List conpute pool {environment_id}")
        list_of_pools = compute_pool_mgr.get_compute_pool_list(environment_id, region)
        print(list_of_pools)

@app.command()
def clear_logs():
       """
       Clear the CLI logs to start from a white page.
       """
       import os
       os.remove(log_file_path)
       print(f"{log_file_path} removed !")

@app.command()
def clean_completed_failed_statements():
        """
        Delete all statements that are failed and completed
        """
        print("#" * 30 + f" Clean statements starting with Workspace in completed and failed state")
        statement_list = statement_mgr.get_statement_list().copy()
        for statement_name in statement_list:
              statement = statement_list[statement_name]
              if "workspace" in statement_name and (statement.status_phase == "COMPLETED" or statement.status_phase == "FAILED"):
                     statement_mgr.delete_statement_if_exists(statement_name)
                     print(f"delete {statement_name}")
        print("Workspace statements cleaned")