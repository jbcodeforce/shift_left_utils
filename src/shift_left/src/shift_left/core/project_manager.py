import os
import logging
from typing import Tuple

DATA_PRODUCT_PROJECT_TYPE="data_product_project"
KIMBALL_PROJECT_TYPE="kimball_project"

def create_folder_if_not_exist(new_path: str):
    if not os.path.exists(new_path):
        os.makedirs(new_path)
        logging.info(f"{new_path} folder created")

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

def build_project_structure(project_name: str, project_path: str, project_type: str):
    logging.info(f"build_project_structure({project_name}, {project_path}, {project_type}")
    project_folder=f"{project_path}/{project_name}"
    create_folder_if_not_exist(project_folder)
    create_folder_if_not_exist(project_folder + "/pipelines")
    create_folder_if_not_exist(project_folder + "/staging")
    create_folder_if_not_exist(project_folder + "/docs")
    if project_type == DATA_PRODUCT_PROJECT_TYPE:
        _define_dp_structure(project_folder + "/pipelines")
    else:
        _define_kimball_structure(project_folder + "/pipelines")
        

def get_ddl_dml_from_folder(root, dir) -> Tuple[str, str]:
    """
    Returns the name of the ddl or dml files
    """
    ddl_file_name = None
    dml_file_name = None
    base_scripts=os.path.join(root,dir)
    for file in os.listdir(base_scripts):
        if file.startswith("ddl"):
            ddl_file_name=os.path.join(base_scripts,file)
        if file.startswith('dml'):
            dml_file_name=os.path.join(base_scripts,file)
    if ddl_file_name is None:
        logging.error(f"No DDL file found in the directory: {base_scripts}")
    if dml_file_name is None:
        logging.error(f"No DML file found in the directory: {base_scripts}")
    return ddl_file_name, dml_file_name