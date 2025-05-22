"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
import subprocess
import shutil
import importlib.resources 
from typing import Tuple, List
from shift_left.core.utils.file_search import create_folder_if_not_exist
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger

DATA_PRODUCT_PROJECT_TYPE="data_product"
KIMBALL_PROJECT_TYPE="kimball"
TMPL_FOLDER="templates"


def build_project_structure(project_name: str, 
                            project_path: str, 
                            project_type: str):
    logger.info(f"build_project_structure({project_name}, {project_path}, {project_type}")
    project_folder=os.path.join(project_path, project_name)
    create_folder_if_not_exist(project_folder)
    create_folder_if_not_exist(os.path.join(project_folder, "pipelines"))
    create_folder_if_not_exist(os.path.join(project_folder, "staging"))
    create_folder_if_not_exist(os.path.join(project_folder, "docs"))
    create_folder_if_not_exist(os.path.join(project_folder, "logs"))
    if project_type == DATA_PRODUCT_PROJECT_TYPE:
        _define_dp_structure(os.path.join(project_folder, "pipelines"))
    else:
        _define_kimball_structure(os.path.join(project_folder, "pipelines"))
    #os.chdir(project_folder)
    _initialize_git_repo(".")
    _add_important_files(project_folder)
        

def get_topic_list(file_name: str):
    ccloud = ConfluentCloudClient(get_config())
    topics = ccloud.list_topics()
    with open(file_name, "w") as f:
        for topic in topics["data"]:
            f.write(topic['cluster_id'] + "," + topic['topic_name'] + "," + str(topic['partitions_count']) + "\n")
    return topics["data"]



# --- Private APIs ---

def _initialize_git_repo(project_folder: str):
    logger.info(f"_initialize_git_repo({project_folder})")
    try:
        subprocess.run(["git", "init"], check=True, cwd=project_folder)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to initialize git repository in {project_folder}: {e}")

def _define_dp_structure(pipeline_folder: str):
    data_folder=pipeline_folder + "/data_product_1"
    create_folder_if_not_exist(data_folder)
    create_folder_if_not_exist(data_folder + "/intermediates")
    create_folder_if_not_exist(data_folder + "/facts")
    create_folder_if_not_exist(data_folder + "/dimensions")
    create_folder_if_not_exist(data_folder + "/sources")

def _define_kimball_structure(pipeline_folder: str):
    create_folder_if_not_exist(pipeline_folder + "/intermediates")
    create_folder_if_not_exist(pipeline_folder + "/facts")
    create_folder_if_not_exist(pipeline_folder + "/dimensions")
    create_folder_if_not_exist(pipeline_folder + "/sources")

def _add_important_files(project_folder: str):    
    logger.info(f"add_important_files({project_folder}")
    for file in ["common.mk", "config_tmpl.yaml"]:
        template_path = importlib.resources.files("shift_left.core.templates").joinpath(file)
        shutil.copyfile(template_path, os.path.join(project_folder, "pipelines", file))
    template_path = importlib.resources.files("shift_left.core.templates").joinpath(".env_tmpl")
    shutil.copyfile(template_path, os.path.join(project_folder, ".env"))
    template_path = importlib.resources.files("shift_left.core.templates").joinpath(".gitignore_tmpl")  
    shutil.copyfile(template_path, os.path.join(project_folder, ".gitignore"))
