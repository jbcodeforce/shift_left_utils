"""
Copyright 2024-2025 Confluent, Inc.
"""
import datetime
import os
from pathlib import Path
import subprocess
import shutil
import importlib.resources 
from datetime import timezone
from typing import Tuple, List
from pydantic import BaseModel, Field
from shift_left.core.table_mgr import get_or_build_inventory
from shift_left.core.utils.file_search import create_folder_if_not_exist
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.pipeline_mgr import FlinkTablePipelineDefinition
from shift_left.core.utils.file_search import PIPELINE_JSON_FILE_NAME, get_table_ref_from_inventory
from shift_left.core.utils.file_search import read_pipeline_definition_from_file
DATA_PRODUCT_PROJECT_TYPE="data_product"
KIMBALL_PROJECT_TYPE="kimball"
TMPL_FOLDER="templates"


class ModifiedFileInfo(BaseModel):
    """Information about a modified file"""
    table_name: str = Field(description="Extracted table name")
    file_modified_url: str = Field(description="File path/URL of the modified file")


class ModifiedFilesResult(BaseModel):
    """Result of list_modified_files operation"""
    description: str = Field(description="Summary information about the operation")
    file_list: List[ModifiedFileInfo] = Field(description="List of modified files with extracted table names")


def _extract_table_name_from_path(file_path: str) -> str:
    """Extract table name from file path.
    
    Attempts to extract a meaningful table name from various file path patterns:
    - For SQL files: extracts filename without extension
    - For pipeline paths: attempts to extract table name from directory structure
    
    Args:
        file_path: The file path to extract table name from
        
    Returns:
        Extracted table name or filename without extension as fallback
    """
    path = Path(file_path)
    
    # Remove file extension
    table_name = path.stem
    
    # Handle common patterns in pipeline directory structures
    if 'pipelines' in path.parts:
        # If it's in a pipelines directory, the parent directory might be the table name
        if len(path.parts) > 1 and path.parent.name != 'pipelines':
            table_name = path.parent.name
    
    # Clean up common prefixes/suffixes
    table_name = table_name.replace('ddl.', '').replace('dml.', '')
    
    return table_name


def build_project_structure(project_name: str, 
                            project_path: str, 
                            project_type: str):
    logger.info(f"build_project_structure({project_name}, {project_path}, {project_type}")
    project_folder=os.path.join(project_path, project_name)
    create_folder_if_not_exist(project_folder)
    create_folder_if_not_exist(os.path.join(project_folder, "pipelines"))
    create_folder_if_not_exist(os.path.join(project_folder, "staging"))
    create_folder_if_not_exist(os.path.join(project_folder, "docs"))
    if project_type == DATA_PRODUCT_PROJECT_TYPE:
        _define_dp_structure(os.path.join(project_folder, "pipelines"))
    else:
        _define_kimball_structure(os.path.join(project_folder, "pipelines"))
    #os.chdir(project_folder)
    _initialize_git_repo(project_folder)
    _add_important_files(project_folder)
        

def get_topic_list(file_name: str):
    ccloud = ConfluentCloudClient(get_config())
    topics = ccloud.list_topics()
    with open(file_name, "w") as f:
        for topic in topics["data"]:
            f.write(topic['cluster_id'] + "," + topic['topic_name'] + "," + str(topic['partitions_count']) + "\n")
    return topics["data"]


def report_table_cross_products(project_path: str):
    """
    Return the lit of table names for tables that are referenced in other products.
    """
    if not project_path:
        project_path = os.getenv("PIPELINES")
    inventory = get_or_build_inventory(project_path, project_path, False)
    risky_tables = []
    for table_name, table_ref in inventory.items():
        table_hierarchy: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref['table_folder_name'] + "/" + PIPELINE_JSON_FILE_NAME)
        if table_hierarchy:
            for child in table_hierarchy.children:
                if child.product_name != table_ref['product_name']:
                    risky_tables.append(table_name)
                    break
    return risky_tables

def list_modified_files(project_path: str, branch_name: str, date_filter: str, file_filter: str, output_file: str = None) -> ModifiedFilesResult:
    """List modified files and return structured result.
    
    Args:
        project_path: Path to the project directory
        branch_name: Git branch name to check
        date_filter: Date filter for git log (YYYY-MM-DD format)
        file_filter: File extension filter (e.g., '.sql')
        output_file: Optional output file path (for backward compatibility)
        
    Returns:
        ModifiedFilesResult: Structured result with description and file list
        
    Raises:
        subprocess.CalledProcessError: If git commands fail
        Exception: For other errors
    """
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
        logger.info(f"Current branch: {current_branch}")
        if current_branch != branch_name:
            print(f"Current branch is not the same as the specified branch {branch_name}, I will automaticall checkout to {branch_name}")
            subprocess.run(
                ["git", "checkout", f"{branch_name}"],
                capture_output=True,
                text=True,
                check=True
            )
        if not date_filter:
            # If no date_filter is provided, set it to yesterday's date in YYYY-MM-DD format
            yesterday = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            date_filter = yesterday
        # Get list of modified files compared to the specified branch
        git_diff_result = subprocess.run(
            ["git", "log", "--name-only", f"--since={date_filter}", '--pretty=format:'],
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
        
        logger.info(f"Found {len(all_modified_files)} total modified files")
        logger.info(f"Found {len(filtered_files)} modified files matching filter '{file_filter}'")
        
        # Generate timestamp
        generated_on = subprocess.run(['date'], capture_output=True, text=True).stdout.strip()
        
        # Create description from lines 123-126 equivalent
        description = (
            f"Modified files in branch '{current_branch}'\n"
            f"Filter applied: {file_filter}\n"
            f"Generated on: {generated_on}\n"
            f"Total files: {len(filtered_files)}"
        )
        
        # Create file list with extracted table names
        file_list = []
        for file_path in sorted(filtered_files):
            table_name = _extract_table_name_from_path(file_path)
            file_list.append(ModifiedFileInfo(
                table_name=table_name,
                file_modified_url=file_path
            ))
        
        # Create result object
        result = ModifiedFilesResult(
            description=description,
            file_list=file_list
        )
        
        # Backward compatibility: write to file if output_file is provided
        if output_file:
            output_path = Path(output_file)
            if not output_path.is_absolute():
                output_path = Path(original_cwd) / output_path
            
            with open(output_path, 'w') as f:
                f.write(result.model_dump_json(indent=2))
            
            print(f"\nFile list saved to: {output_path}")
        
        # Display results
        if filtered_files:
            logger.info(f"\nModified {file_filter} files:")
            for file_info in file_list:
                logger.info(f"  {file_info.table_name}: {file_info.file_modified_url}")
        else:
            logger.info(f"\nNo modified files found matching filter '{file_filter}'")
        
        # Additional information for blue-green deployment
        if filtered_files:
            print(f"\n💡 For blue-green deployment:")
            print(f"   - Review the {len(filtered_files)} modified Flink statements")
            print(f"   - Ensure table naming follows versioning strategy (e.g., table_name_v2)")
            print(f"   - Update downstream dependencies as needed")
            if output_file:
                print(f"   - Use: shift_left pipeline deploy --table-list-file-name {output_file}")
        
        return result
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Git command failed: {e}")
        print(f"Error output: {e.stderr}")
        raise e
    except Exception as e:
        print(f"❌ Error: {e}")
        raise e
    finally:
        # Restore original working directory
        os.chdir(original_cwd)

# --- Private APIs ---

def _initialize_git_repo(project_folder: str):
    print(f"initialize_git_repo({project_folder})")
    try:
        subprocess.run(["git", "init"], check=True, cwd=project_folder)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to initialize git repository in {project_folder}: {e}")

def _define_dp_structure(pipeline_folder: str):
    data_folder=pipeline_folder + "/data_product_1"
    create_folder_if_not_exist(data_folder)
    create_folder_if_not_exist(data_folder + "/intermediates")
    create_folder_if_not_exist(data_folder + "/facts")
    create_folder_if_not_exist(data_folder + "/sources")

def _define_kimball_structure(pipeline_folder: str):
    create_folder_if_not_exist(pipeline_folder + "/intermediates")
    create_folder_if_not_exist(pipeline_folder + "/facts")
    create_folder_if_not_exist(pipeline_folder + "/dimensions")
    create_folder_if_not_exist(pipeline_folder + "/sources")
    create_folder_if_not_exist(pipeline_folder + "/views")

def _add_important_files(project_folder: str):    
    logger.info(f"add_important_files({project_folder}")
    for file in ["common.mk"]:
        template_path = importlib.resources.files("shift_left.core.templates").joinpath(file)
        shutil.copyfile(template_path, os.path.join(project_folder, "pipelines", file))
    template_path = importlib.resources.files("shift_left.core.templates").joinpath(".env_tmpl")
    shutil.copyfile(template_path, os.path.join(project_folder, ".env"))
    # Update FLINK_PROJECT in .env file with project folder path
    env_file = os.path.join(project_folder, ".env")
    with open(env_file, 'r') as f:
        env_content = f.read()
    env_content = env_content.replace("FLINK_PROJECT=", f"FLINK_PROJECT={project_folder}")
    with open(env_file, 'w') as f:
        f.write(env_content)
    template_path = importlib.resources.files("shift_left.core.templates").joinpath(".gitignore_tmpl")  
    shutil.copyfile(template_path, os.path.join(project_folder, ".gitignore"))
    template_path = importlib.resources.files("shift_left.core.templates").joinpath("config_tmpl.yaml")
    shutil.copyfile(template_path, os.path.join(shift_left_dir, "config.yaml"))
