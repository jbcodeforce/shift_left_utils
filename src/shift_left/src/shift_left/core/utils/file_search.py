import os
from pathlib import Path, PosixPath
from typing import Final, Dict, Set, Optional, Any, Tuple
import json
import logging
from functools import lru_cache
from pydantic import BaseModel
from shift_left.core.utils.sql_parser import SQLparser

"""
Provides a set of function to search files from a given folder path for source project or Flink project.
"""

INVENTORY_FILE_NAME: Final[str] = "inventory.json"
SCRIPTS_DIR: Final[str] = "sql-scripts"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"
# ------ Public APIs ------

class FlinkTableReference(BaseModel):
    """Reference to a Flink table including its metadata and location information."""
    table_name: Final[str]
    type: Optional[str]
    dml_ref: Optional[str]
    ddl_ref: Optional[str]
    table_folder_name: str

    def __hash__(self) -> int:
        return hash(self.table_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkTableReference):
            return NotImplemented
        return self.table_name == other.table_name

def build_inventory(pipeline_folder: str) -> Dict:
    """Build inventory from pipeline folder.
    
    Args:
        pipeline_folder: Root folder containing pipeline definitions
        
    Returns:
        Dictionary mapping table names to their FlinkTableReference metadata
    """
    return get_or_build_inventory(pipeline_folder, pipeline_folder, True)

def get_or_build_inventory(
    pipeline_folder: str,
    target_path: str,
    recreate: bool = False
) -> Dict:
    """Get existing inventory or build new one if needed.
    This will parse of file content to get the name of the table and will be a 
    hashmap <table_name> -> FlinkTableReference
    
    Args:
        pipeline_folder: Root folder containing pipeline definitions
        target_path: Path to store inventory file
        recreate: Whether to force recreation of inventory
        
    Returns:
        Dictionary mapping table names to their FlinkTableReference metadata
    """
    create_folder_if_not_exist(target_path)
    inventory_path = os.path.join(target_path, INVENTORY_FILE_NAME)
    
    if not recreate and os.path.exists(inventory_path):
        return load_existing_inventory(target_path)
        
    inventory = {}
    parser = SQLparser()
    
    for root, dirs, _ in os.walk(pipeline_folder):
        for dir in dirs:
            if SCRIPTS_DIR == dir:
                ddl_file_name, dml_file_name = get_ddl_dml_from_folder(root, dir)
                if not dml_file_name:
                    continue
                    
                logging.debug(f"Processing file {dml_file_name}")
                # extract table name from dml filefrom sql script   
                with open(dml_file_name, "r") as f:
                    sql_content = f.read()
                    table_name = parser.extract_table_name_from_insert_into_statement(sql_content)
                    directory = os.path.dirname(dml_file_name)
                    table_folder = from_absolute_to_pipeline(os.path.dirname(directory))
                    table_type = get_table_type_from_file_path(dml_file_name)
                    ref = FlinkTableReference.model_validate({
                        "table_name": table_name,
                        "type": table_type,
                        "ddl_ref": from_absolute_to_pipeline(ddl_file_name),
                        "dml_ref": from_absolute_to_pipeline(dml_file_name),
                        "table_folder_name": table_folder
                    })
                    
                    logging.debug(ref)
                    inventory[ref.table_name] = ref.model_dump()
                    
    with open(inventory_path, "w") as f:
        json.dump(inventory, f, indent=4)
    logging.info(f"Created inventory file {inventory_path}")
    return inventory

def load_existing_inventory(target_path: str) -> Dict:
    """Load existing inventory from file.
    
    Args:
        target_path: Path containing inventory file
        
    Returns:
        Dictionary containing inventory data
    """
    inventory_path = os.path.join(target_path, INVENTORY_FILE_NAME)
    with open(inventory_path, "r") as f:
        return json.load(f)

def get_table_type_from_file_path(file_name: str) -> str:
    """
    Determine the type of table one of fact, intermediate, source, stage or dimension
    """
    if "source" in file_name:
        return "source"
    elif "intermediates" in file_name:
        return "intermediate"
    if "facts" in file_name:
        return "fact"
    elif "dimensions" in file_name:
        return "dimension"
    elif "stage" in file_name:
        return "intermediate"
    elif "mv" in file_name:
        return "view"
    elif "seed" in file_name:
        return "seed"
    elif "dead_letter" in file_name:
        return "dead_letter"
    else:
        return "unknow-type"

def create_folder_if_not_exist(new_path: str) -> str:
    if not os.path.exists(new_path):
        os.makedirs(new_path)
        logging.debug(f"{new_path} folder created")
    return new_path

def from_absolute_to_pipeline(file_or_folder_name) -> str:
    """Convert absolute path to pipeline-relative path.
    
    Args:
        file_or_folder_name: Absolute path to convert
        
    Returns:
        Path relative to pipeline root
    """
    if isinstance(file_or_folder_name,  PosixPath):
        str_file_path=str(file_or_folder_name.resolve())
    else:
        str_file_path = file_or_folder_name

    if not str_file_path or str_file_path.startswith(PIPELINE_FOLDER_NAME):
        return str_file_path
        
    index = str_file_path.find(PIPELINE_FOLDER_NAME)
    return str_file_path[index:] if index != -1 else str_file_path

def from_pipeline_to_absolute(file_or_folder_name: str) -> str:
    """Convert pipeline-relative path to absolute path.
    
    Args:
        file_or_folder_name: Pipeline-relative path
        
    Returns:
        Absolute path
    """
    if not file_or_folder_name or not file_or_folder_name.startswith(PIPELINE_FOLDER_NAME):
        return file_or_folder_name
        
    root = os.path.dirname(os.getenv("PIPELINES"))
    return os.path.join(root, file_or_folder_name)

def get_table_ref_from_inventory(table_name: str, inventory: Dict) -> FlinkTableReference:
    """Get table reference from inventory.
    
    Args:
        table_name: Name of table
        inventory: Dictionary of inventory data
        
    Returns:
        FlinkTableReference for the table
    """
    return FlinkTableReference.model_validate(inventory[table_name])
    
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

@lru_cache
def get_or_build_source_file_inventory(src_path: str) -> Dict[str, str]:
    file_paths=list_src_sql_files(f"{src_path}/intermediates")
    file_paths.update(list_src_sql_files(f"{src_path}/dimensions"))
    file_paths.update(list_src_sql_files(f"{src_path}/stage"))
    file_paths.update(list_src_sql_files(f"{src_path}/facts"))
    file_paths.update(list_src_sql_files(f"{src_path}/sources"))
    file_paths.update(list_src_sql_files(f"{src_path}/intermediates/dedups"))
    file_paths.update(list_src_sql_files(f"{src_path}/stage"))
    return file_paths

def get_ddl_dml_names_from_table(table_name: str, prefix: str, product_name: str) -> Tuple[str,str]:
    print(f"Get dml name from table {table_name} and {product_name}")  
    if product_name:
        prefix= prefix + "-" + product_name
    ddl_n = prefix + "-ddl-" + table_name.replace("_","-"),
    dml_n = prefix + "-dml-" + table_name.replace("_","-")
    return ddl_n, dml_n

def extract_product_name(existing_path: str) -> str:
    """
    Given an existing folder path, get the name of the folder below one of the structural folder as it is the name of the data product.
    """
    parent_folder=os.path.dirname(existing_path).split("/")[-1]
    if parent_folder not in ["facts", "intermediates", "sources", "dimensions", "views"]:
        return parent_folder
    else:
        return ""
    
def list_src_sql_files(folder_path: str) -> Dict[str, str]:
    """
    Given the folder path, list the sql statements and use the name of the file as table name
    return the list of files and table name

    :param folder_path: path to the folder which includes n sql files
    :return: Set of complete file path for the sql file in the folder
    """
    sql_files = {}
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql'):
                key=file[:-4]
                sql_files[key]=os.path.join(root, file)
    return sql_files