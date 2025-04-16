"""
Copyright 2024-2025 Confluent, Inc.
"""
import os
from pathlib import  PosixPath
from typing import Final, Dict, Optional, Any, Tuple, Set, List
import json
from datetime import datetime
from functools import lru_cache
from pydantic import BaseModel, Field
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.app_config import logger, get_config
from shift_left.core.flink_statement_model import StatementInfo
"""
Provides a set of function to search files from a given folder path for source project or Flink project.
"""

INVENTORY_FILE_NAME: Final[str] = "inventory.json"
SCRIPTS_DIR: Final[str] = "sql-scripts"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"
PIPELINE_JSON_FILE_NAME: Final[str] = "pipeline_definition.json"

# ------ Public APIs ------

class InfoNode(BaseModel):
    table_name: Final[str]
    type: Optional[str]
    dml_ref: Optional[str]
    ddl_ref: Optional[str]

class FlinkTableReference(InfoNode):
    """Reference to a Flink table including its metadata and location information."""
    table_folder_name: Optional[str] = Field(default="", description="table_folder_name")
    
    def __hash__(self) -> int:
        return hash(self.table_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkTableReference):
            return NotImplemented
        return self.table_name == other.table_name

class FlinkStatementNode(BaseModel):
    """
    To build an execution plan we need one node for each popential Flink Statement to run.
    A node has 0 to many parents and 0 to many children
    """
    table_name: str
    path:  Optional[str] =  Field(default=None, description="Name of path")
    created_at: Optional[datetime] = Field(default=None)
    existing_statement_info:  Optional[StatementInfo] =  Field(default=None, description="Flink statement status")
    dml_ref: Optional[str] =  Field(default=None, description="DML sql file path")
    dml_statement: Optional[str] =  Field(default=None, description="DML Statement name")
    ddl_ref: Optional[str] =  Field(default=None, description="DDL sql file path")
    ddl_statement: Optional[str] =  Field(default=None, description="DDL Statement name")
    dml_only: Optional[bool] = Field(default=False, description="Used during deployment to enforce DDL and DML deployment or DML only")
    update_children: Optional[bool] = Field(default=False, description="Update children when the table is not a sink table. Used during deployment")
    compute_pool_id:  Optional[str] =  Field(default=None, description="Name of compute pool to use for deployment")
    parents: Set =  Field(default=set(), description="List of parent")
    children: Set = Field(default=set(), description="Child list")
    to_run: bool = Field(default=False, description="statement must be executed")
    to_restart: bool = Field(default=False, description="statement will be restarted, this is to differentiate child treatment from parent")

    def add_child(self, child):
        self.children.add(child)
        child.parents.add(self)

    def add_parent(self, parent):
        self.parents.add(parent)
        parent.children.add(self)

    def is_running(self) -> bool:
        if self.existing_statement_info and self.existing_statement_info.status_phase:
            return (self.existing_statement_info.status_phase == "RUNNING")
        else:
            return False
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkStatementNode):
            return NotImplemented
        return self.table_name == other.table_name
    
    def __hash__(self) -> int:
        return hash(self.table_name)

class FlinkStatementExecutionPlan(BaseModel):
    created_at: datetime = Field(default=None)
    start_table_name: str = Field(default=None)
    nodes: List[FlinkStatementNode] = Field(default=[])

class FlinkTablePipelineDefinition(InfoNode):
    """Metadata definition for a Flink Statement to manage the pipeline hierarchy.
    
    For source tables, parents will be empty.
    For sink tables, children will be empty.
    """
    path: str
    state_form: Optional[str] =  Field(default="Stateful", description="Type of Flink SQL statement. Could be Stateful or Stateless")
    parents: Optional[Set['FlinkTablePipelineDefinition']] = Field(default=set(), description="parents of this flink dml")
    children: Optional[Set['FlinkTablePipelineDefinition']] = Field(default=set(), description="users of the table created by this flink dml")

    def __hash__(self) -> int:
        return hash(self.table_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkTablePipelineDefinition):
            return NotImplemented
        return self.table_name == other.table_name
     
    def to_node(self) -> FlinkStatementNode:
        ddl_statement_name, dml_statement_name = get_ddl_dml_names_from_pipe_def(self, get_config()['kafka']['cluster_type'])
       
        r = FlinkStatementNode(table_name= self.table_name,
                               path= self.path,
                               dml_statement=dml_statement_name,
                               dml_ref=self.dml_ref,
                               ddl_statement=ddl_statement_name,
                               ddl_ref=self.ddl_ref
                               )
        
        for p in self.parents:
            node_p:FlinkStatementNode = p.to_node()
            r.add_parent(node_p)
        
        for c in self.children:
            node_c:FlinkStatementNode = c.to_node()
            r.add_child(node_c)
        return r
    
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
        with open(inventory_path, "r") as f:
            inventory= json.load(f)
            return inventory
        
    inventory = {}
    parser = SQLparser()
    count = 0
    for root, dirs, _ in os.walk(pipeline_folder):
        for dir in dirs:
            if SCRIPTS_DIR == dir:
                ddl_file_name, dml_file_name = get_ddl_dml_from_folder(root, dir)
                logger.debug(f"Processing file {dml_file_name}")
                count+=1
                if not dml_file_name:
                    continue
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
                    logger.debug(ref)
                    if ref.table_name in inventory:
                        logger.error(f"duplicate name {ref.table_name} dml = {dml_file_name}")
                    inventory[ref.table_name] = ref.model_dump()
    logger.info(f"processed {count} files and got {len(inventory)} entries")
                    
    with open(inventory_path, "w") as f:
        json.dump(inventory, f, indent=4)
    logger.info(f"Created inventory file {inventory_path}")
    return inventory



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
        return "unknown-type"

def derive_table_type_product_name_from_path(path: str) -> Tuple[str, str, str]:
    """
    Derive the table type and product name from the path

    """
    table_type = get_table_type_from_file_path(path)
    product_name = extract_product_name(path)
    table_name = os.path.basename(path)
    return table_type, product_name, table_name

def create_folder_if_not_exist(new_path: str) -> str:
    if not os.path.exists(new_path):
        os.makedirs(new_path)
        logger.debug(f"{new_path} folder created")
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
    if table_name not in inventory:
        raise Exception(f"Table {table_name} not in inventory")
    entry = inventory[table_name]
    return FlinkTableReference.model_validate(entry)


def read_pipeline_definition_from_file(relative_path_file_name: str) -> FlinkTablePipelineDefinition:
    """Read pipeline metadata from file.
    
    Args:
        file_name: Path to pipeline metadata file
        
    Returns:
        FlinkTablePipelineDefinition object
    """
    file_name = from_pipeline_to_absolute(relative_path_file_name)
    try:
        with open(file_name, "r") as f:
            content = FlinkTablePipelineDefinition.model_validate_json(f.read())
            return content
    except Exception as e:
        logger.error(f"processing {file_name} got {e}, ... try to continue")
        #return FlinkStatementExecutionPlan()
        return None

def update_pipeline_definition_file(relative_path_file_name: str, data: FlinkTablePipelineDefinition):
    file_name = from_pipeline_to_absolute(relative_path_file_name)
    with open(file_name, "w") as f:
        f.write(data.model_dump_json(indent=3))

def get_ddl_dml_from_folder(root, dir) -> Tuple[str, str]:
    """
    Returns the name of the ddl or dml files
    """
    ddl_file_name = None
    dml_file_name = None
    base_scripts=os.path.join(root, dir)
    for file in os.listdir(base_scripts):
        if file.startswith("ddl."):
            ddl_file_name=os.path.join(base_scripts,file)
        if file.startswith('dml.'):
            dml_file_name=os.path.join(base_scripts,file)
    if ddl_file_name is None:
        logger.error(f"No DDL file found in the directory: {base_scripts}")
        raise Exception(f"No DDL file found in the directory: {base_scripts}")
    if dml_file_name is None:
        logger.error(f"No DML file found in the directory: {base_scripts}")
        raise Exception(f"No DML file found in the directory: {base_scripts}")
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

def get_ddl_dml_names_from_table(table_name: str, prefix: str) -> Tuple[str,str]:
     
    if prefix:
        ddl_n = prefix + "-ddl-" + table_name.replace("_","-")
        dml_n = prefix + "-dml-" + table_name.replace("_","-")
    else:
        ddl_n = "ddl-" + table_name.replace("_","-")
        dml_n = "dml-" + table_name.replace("_","-")
    #logger.debug(f"Get dml name from table {table_name} and {product_name} as {ddl_n} and {dml_n}") 
    return ddl_n, dml_n



def get_ddl_dml_names_from_pipe_def(to_process: FlinkTablePipelineDefinition, 
                                    prefix: str) -> Tuple[str,str]:
    return get_ddl_dml_names_from_table(to_process.table_name, 
                                        prefix)

def get_ddl_file_name(folder_path: str) -> str:
    """
    Return the ddl file name if it exists in the given folder. All DDL file must start with ddl
    or includes a CREATE TABLE statement
    """
    folder_path = from_pipeline_to_absolute(folder_path)
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.startswith('ddl'):
                return from_absolute_to_pipeline(os.path.join(root, file))
    return ""

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


  