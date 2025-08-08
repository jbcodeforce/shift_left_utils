"""
Copyright 2024-2025 Confluent, Inc.
"""
import typer
import subprocess
import os
from pathlib import Path
from rich import print
from shift_left.core.utils.app_config import get_config, validate_config as validate_config_impl
from shift_left.core.compute_pool_mgr import get_compute_pool_list
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.compute_pool_mgr as compute_pool_mgr
import shift_left.core.project_manager as project_manager
from shift_left.core.project_manager import (
        DATA_PRODUCT_PROJECT_TYPE, 
        KIMBALL_PROJECT_TYPE)
from typing_extensions import Annotated


"""
Manage project foundations
"""
app = typer.Typer(no_args_is_help=True)

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
        print("#" * 30 + f" List conpute pool {environment_id}")
        list_of_pools = compute_pool_mgr.get_compute_pool_list(environment_id, region)
        print(list_of_pools)


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
def list_modified_files(
    branch_name: Annotated[str, typer.Argument(help="Git branch name to compare against (e.g., 'main', 'origin/main')")],
    output_file: Annotated[str, typer.Option(help="Output file path to save the list")] = "modified_flink_files.txt",
    project_path: Annotated[str, typer.Option(help="Project path where git repository is located")] = ".",
    file_filter: Annotated[str, typer.Option(help="File extension filter (e.g., '.sql', '.py')")] = ".sql"
):
    """
    Get the list of files modified in the current git branch compared to the specified branch.
    Filters for Flink-related files (by default SQL files) and saves the list to a text file.
    This is useful for identifying which Flink statements need to be redeployed in a blue-green deployment.
    """
    print("#" * 30 + f" List modified files in current branch vs {branch_name}")
    
    try:
        # Change to project directory
        original_cwd = os.getcwd()
        if project_path != ".":
            os.chdir(project_path)
        
        # Get the current branch name
        current_branch_result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        current_branch = current_branch_result.stdout.strip()
        print(f"Current branch: {current_branch}")
        
        # Get list of modified files compared to the specified branch
        git_diff_result = subprocess.run(
            ["git", "diff", "--name-only", f"{branch_name}...HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        
        all_modified_files = git_diff_result.stdout.strip().split('\n')
        all_modified_files = [f for f in all_modified_files if f.strip()]  # Remove empty strings
        
        # Filter for specific file types (default: SQL files)
        filtered_files = []
        for file_path in all_modified_files:
            if file_filter in file_path.lower():
                filtered_files.append(file_path)
        
        print(f"Found {len(all_modified_files)} total modified files")
        print(f"Found {len(filtered_files)} modified files matching filter '{file_filter}'")
        
        # Create output file path
        output_path = Path(output_file)
        if not output_path.is_absolute():
            output_path = Path(original_cwd) / output_path
        
        # Write filtered files to output file
        with open(output_path, 'w') as f:
            f.write(f"# Modified files in branch '{current_branch}' compared to '{branch_name}'\n")
            f.write(f"# Filter applied: {file_filter}\n")
            f.write(f"# Generated on: {subprocess.run(['date'], capture_output=True, text=True).stdout.strip()}\n")
            f.write(f"# Total files: {len(filtered_files)}\n\n")
            
            for file_path in sorted(filtered_files):
                f.write(f"{file_path}\n")
        
        # Display results
        if filtered_files:
            print(f"\nModified {file_filter} files:")
            for file_path in sorted(filtered_files):
                print(f"  {file_path}")
        else:
            print(f"\nNo modified files found matching filter '{file_filter}'")
        
        print(f"\nFile list saved to: {output_path}")
        
        # Additional information for blue-green deployment
        if filtered_files:
            print(f"\n💡 For blue-green deployment:")
            print(f"   - Review the {len(filtered_files)} modified Flink statements")
            print(f"   - Ensure table naming follows versioning strategy (e.g., table_name_v2)")
            print(f"   - Update downstream dependencies as needed")
            print(f"   - Use: shift_left pipeline deploy --table-list-file-name {output_path}")
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Git command failed: {e}")
        print(f"Error output: {e.stderr}")
        raise typer.Exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        raise typer.Exit(1)
    finally:
        # Restore original working directory
        os.chdir(original_cwd)
