"""
Pipeline manager defines functions to build inventory, pipeline definitions, and navigate pipeline trees.

This module provides functionality to:
1. Build and manage pipeline definition inventories
2. Create pipeline definitions for tables
3. Navigate and analyze pipeline hierarchies
"""
from collections import deque
import json
import logging
import os
from pathlib import Path
from typing import Dict, Optional, Final, Any, Set, List, Tuple

from pydantic import BaseModel

from shift_left.core.project_manager import create_folder_if_not_exist, get_ddl_dml_from_folder
from shift_left.core.sql_parser import SQLparser
from shift_left.core.app_config import get_config

# Configure logging
logging.basicConfig(
    filename=os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs', 'pipelines.log'),
    filemode='w',
    level=get_config()["app"]["logging"],
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Constants
SCRIPTS_DIR: Final[str] = "sql-scripts"
INVENTORY_FILE_NAME: Final[str] = "inventory.json"
PIPELINE_FOLDER_NAME: Final[str] = "pipelines"
PIPELINE_JSON_FILE_NAME: Final[str] = "pipeline_definition.json"

# Global queues for processing
files_to_process: deque = deque()  # Files to process when parsing SQL dependencies
node_to_process: deque = deque()   # Nodes to process in pipeline hierarchy

class FlinkTableReference(BaseModel):
    """Reference to a Flink table including its metadata and location information."""
    table_name: Final[str]
    dml_ref: Optional[str]
    ddl_ref: Optional[str]
    table_folder_name: str

    def __hash__(self) -> int:
        return hash(self.table_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkTableReference):
            return NotImplemented
        return self.table_name == other.table_name

class FlinkStatementHierarchy(BaseModel):
    """Metadata definition for a table in the pipeline hierarchy.
    
    For source tables, parents will be empty.
    For sink tables, children will be empty.
    """
    table_name: str
    type: str
    path: str
    ddl_ref: str
    dml_ref: str
    parents: Optional[Set[FlinkTableReference]]
    children: Optional[Set[FlinkTableReference]]

    def __hash__(self) -> int:
        return hash(self.table_name)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FlinkStatementHierarchy):
            return NotImplemented
        return self.table_name == other.table_name

class ReportInfoNode(BaseModel):
    """Node containing reporting information for a table in the pipeline."""
    table_name: str
    base_path: str
    ddl_path: str
    dml_path: str
    parents: Optional[Any]
    children: Optional[Any]

def _build_table_reference(
    table_name: str,
    dml_file_name: str,
    ddl_file_name: str,
    table_folder: Optional[str] = None
) -> FlinkTableReference:
    """Build a FlinkTableReference object from table metadata.
    
    Args:
        table_name: Name of the table
        dml_file_name: Path to DML file
        ddl_file_name: Path to DDL file
        table_folder: Optional folder containing the table files
    
    Returns:
        FlinkTableReference object
    """
    if not table_folder:
        directory = os.path.dirname(dml_file_name)
        table_folder = os.path.dirname(directory)
    
    return FlinkTableReference.model_validate({
        "table_name": table_name,
        "dml_ref": dml_file_name,
        "ddl_ref": ddl_file_name,
        "table_folder_name": table_folder
    })

def build_inventory(pipeline_folder: str) -> Dict:
    """Build inventory from pipeline folder.
    
    Args:
        pipeline_folder: Root folder containing pipeline definitions
        
    Returns:
        Dictionary mapping table names to their FlinkTableReference metadata
    """
    return get_or_build_inventory(pipeline_folder, pipeline_folder, True)

def from_absolute_to_pipeline(file_or_folder_name: str) -> str:
    """Convert absolute path to pipeline-relative path.
    
    Args:
        file_or_folder_name: Absolute path to convert
        
    Returns:
        Path relative to pipeline root
    """
    if not file_or_folder_name or file_or_folder_name.startswith(PIPELINE_FOLDER_NAME):
        return file_or_folder_name
        
    index = file_or_folder_name.find(PIPELINE_FOLDER_NAME)
    return file_or_folder_name[index:] if index != -1 else file_or_folder_name

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

def get_or_build_inventory(
    pipeline_folder: str,
    target_path: str,
    recreate: bool = False
) -> Dict:
    """Get existing inventory or build new one if needed.
    
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
                    
                    ref = FlinkTableReference.model_validate({
                        "table_name": table_name,
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

def build_pipeline_definition_from_table(dml_file_name: str, pipeline_path: str) -> FlinkStatementHierarchy:
    """Build pipeline definition hierarchy starting from given table.
    
    Args:
        dml_file_name: Path to DML file for root table
        pipeline_path: Root pipeline folder path
        
    Returns:
        FlinkStatementHierarchy for the table and its dependencies
    """
    dml_file_name = from_absolute_to_pipeline(dml_file_name)
    all_files = load_existing_inventory(pipeline_path)
    
    table_name, parent_references = get_parent_table_references_from_sql_content(
        dml_file_name, all_files
    )
    
    current_node = _create_table_hierarchy_node(dml_file_name, table_name, parent_references, set())
    node_to_process.append(current_node)
    _process_next_node(node_to_process, dict(), all_files)
    
    return current_node

def delete_metada_files(root_folder: str):
    """
    Delete all the files with the given name in the given root folder tree
    """
    file_to_delete = PIPELINE_JSON_FILE_NAME
    logging.info(f"Delete {file_to_delete} from folder: {root_folder}")
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file_to_delete == file:
                file_path=os.path.join(root, file)
                os.remove(file_path)
                print(f"File '{file_path}' deleted successfully.")


def get_parent_table_references_from_sql_content(
    sql_file_name: str,
    all_files: Dict
) -> Tuple[str, Set[FlinkTableReference]]:
    """Extract parent table references from SQL content.
    
    Args:
        sql_file_name: Path to SQL file
        all_files: Dictionary of all available files
        
    Returns:
        Tuple of (table_name, set of parent FlinkTableReferences)
    """
    try:
        if sql_file_name.startswith(PIPELINE_FOLDER_NAME):
            sql_file_name = os.path.join(os.getenv("PIPELINES"), "..", sql_file_name)
            
        with open(sql_file_name) as f:
            sql_content = f.read()
            parser = SQLparser()
            table_name = parser.extract_table_name_from_insert_into_statement(sql_content)
            dependencies = set()
            
            dependency_names = parser.extract_table_references(sql_content)
            if dependency_names:
                for dependency in dependency_names:
                    logging.info(f"{table_name} - depends on: {dependency}")
                    ddl_file_name, dml_file_name = get_table_ddl_dml_from_inventory(
                        dependency, all_files
                    )
                    dependencies.add(_build_table_reference(
                        dependency, dml_file_name, ddl_file_name
                    ))
                    
            return table_name, dependencies
            
    except Exception as e:
        logging.error(e)
        return "", set()

def _get_table_type_from_file_path(file_name: str) -> str:
    """Determine table type from file path.
    
    Args:
        file_name: Path to table file
        
    Returns:
        One of: fact, intermediate, source, stage, dimension, or unknown
    """
    if "facts" in file_name:
        return "fact"
    elif "intermediates" in file_name:
        return "intermediate" 
    elif "sources" in file_name:
        return "source"
    elif "stage" in file_name:
        return "stage"
    elif "dimensions" in file_name:
        return "dimension"
    return "unknown"

def _create_table_hierarchy_node(
    dml_file_name: str,
    table_name: str, 
    parent_names: Set[FlinkTableReference],
    children: Set[FlinkTableReference]
) -> FlinkStatementHierarchy:
    """Create hierarchy node with table information.
    
    Args:
        dml_file_name: Path to DML file
        table_name: Name of the table
        parent_names: Set of parent table references
        children: Set of child table references
        
    Returns:
        FlinkStatementHierarchy node
    """
    logging.info(f"_create_table_hierarchy_node( {dml_file_name}, {table_name},  {parent_names}, {children})")
   
    table_type = _get_table_type_from_file_path(dml_file_name)
    directory = os.path.dirname(dml_file_name)
    base_path = os.path.dirname(directory)
    
    return FlinkStatementHierarchy.model_validate({
        "table_name": table_name,
        "type": table_type,
        "path": base_path,
        "ddl_ref": get_ddl_file_name(directory),
        "dml_ref": base_path + "/" + SCRIPTS_DIR + "/" + dml_file_name.split("/")[-1],
        "parents": parent_names,
        "children": children
    })


def walk_the_hierarchy_for_report_from_table(
    table_name: str,
    inventory_path: str = None
) -> Dict:
    """Generate report of pipeline hierarchy from table name.
    
    Args:
        table_name: Name of table to start from
        inventory_path: Optional path to inventory file
        
    Returns:
        Dictionary containing hierarchy report
    """
    if not inventory_path:
        inventory_path = os.getenv("PIPELINES")
    inventory = load_existing_inventory(inventory_path)
    
    if table_name not in inventory:
        return None
        
    table_ref = _get_table_ref_from_inventory(table_name, inventory)
    return walk_the_hierarchy_for_report(
        assess_pipeline_definition_exists(table_ref.dml_ref)
    )

def _get_table_ref_from_inventory(table_name: str, inventory: Dict) -> FlinkTableReference:
    """Get table reference from inventory.
    
    Args:
        table_name: Name of table
        inventory: Dictionary of inventory data
        
    Returns:
        FlinkTableReference for the table
    """
    return FlinkTableReference.model_validate(inventory[table_name])


def get_table_ddl_dml_from_inventory(table_name: str, inventory: dict) -> Tuple[str,str]:
    """
    Given the table name, search the matching information from the inventory.
    returns ddl and dml file name, relative to pipelines folder
    """
    if table_name.endswith(".sql"):
        table_name=table_name.split(".sql")[0]    # legacy table name
    table_ref: FlinkTableReference= FlinkTableReference.model_validate(inventory[table_name])
    logging.debug(f"Search {table_name} in inventory of files got {table_ref}")
    if table_ref and table_ref.dml_ref:
        return table_ref.ddl_ref, table_ref.dml_ref
    return "", ""

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

def _get_table_type_from_file_path(file_name: str) -> str:
    """
    Determine the type of table one of fact, intermediate, source, stage or dimension
    """
    if "fact" in file_name:
        return "fact"
    elif "intermediate" in file_name:
        return "intermediate"
    elif "source" in file_name:
        return "source"
    elif "stage" in file_name:
        return "stage"
    else:
        return "dimension"
    

def _process_next_node(nodes_to_process, processed_nodes,  all_files):
    """
    Process the next node from the queue, to walk up the hierarchy
    """
    if len(nodes_to_process) > 0:
        current_node = nodes_to_process.pop()
        logging.debug(f"\n\n\t... processing the node {current_node}")
        nodes_to_process = _add_parents_for_future_process(current_node, nodes_to_process, processed_nodes, all_files)
        _create_or_merge(current_node)
        processed_nodes[current_node.table_name]=current_node
        _process_next_node(nodes_to_process, processed_nodes, all_files)

def _create_or_merge(current: FlinkStatementHierarchy):
    """
    If the pipeline definition exists we may need to merge the parents and children
    """
    pipe_definition_fn = os.path.join(os.getenv("PIPELINES"), "..", current.path, PIPELINE_JSON_FILE_NAME)
    if not os.path.exists(pipe_definition_fn):
        with open(pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))
    else:
        with open(pipe_definition_fn, "r") as f:
            old_definition = FlinkStatementHierarchy.model_validate_json(f.read())
            combined_children = old_definition.children
            combined_parents = old_definition.parents
            for child in current.children:
                if child in old_definition.children:
                    continue
                else:
                    combined_children.add(child)
            for parent in current.parents:
                if parent in old_definition.parents:
                    continue
                else:
                    combined_parents.add(parent)
        current.children = combined_children
        current.parents = combined_parents
        with open(pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))

def read_pipeline_metadata(file_name: str) -> FlinkStatementHierarchy:
    """Read pipeline metadata from file.
    
    Args:
        file_name: Path to pipeline metadata file
        
    Returns:
        FlinkStatementHierarchy object
    """
    file_name = from_pipeline_to_absolute(file_name)
    with open(file_name, "r") as f:
        content = FlinkStatementHierarchy.model_validate_json(f.read())
        return content

def assess_pipeline_definition_exists(file_name: str) -> str:
    """Assess if pipeline definition exists.
    
    Args:
        file_name: Path to pipeline definition file
        
    Returns:
        Path to pipeline definition file if it exists, otherwise None
    """
    try:
        pname = _get_path_to_pipeline_file(file_name)
        if os.path.exists(pname):
            try:
                with open(pname, 'r') as f:
                    json.load(f)
                return pname
            except json.JSONDecodeError:
                logging.error("ERROR {pname} not a json file")
                return None
        else:
            return None
    except Exception as e:
        return None

def _modify_children(current: FlinkStatementHierarchy, parent_ref: FlinkTableReference):
    """
    Verify the current is in the children of the parent.
    It may happend that parent was already processed, but the current is referencing it another time,
    in this case we need to add to the children of the parent
    """
    child = _build_table_reference(current.table_name, current.dml_ref, current.ddl_ref)
    pipe_definition_fn = parent_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME
    parent = read_pipeline_metadata(pipe_definition_fn)
    parent.children.add(child)
    _create_or_merge(parent)


def _add_node_to_process_if_not_present(current_hierarchy, nodes_to_process):
    try: 
        nodes_to_process.index(current_hierarchy)
    except ValueError:
        nodes_to_process.append(current_hierarchy)

def _add_parents_for_future_process(current_node: FlinkStatementHierarchy, 
                                    nodes_to_process: List[FlinkStatementHierarchy], 
                                    processed_nodes: dict, 
                                    all_files: dict) -> List[FlinkStatementHierarchy]:
    """
    loop on the parents of the current node, for each parent, research they own parents to create the FlinkTableReferences for those parents
    add the newly created parent node to the list of node to process.
    e.g. current_node = sink_table,  its parents = int_table_1, and int_table_2. Build the FlinkStatementHierarchy for each parent.
    To do so FlinkStatementHierarchy needs parents and children list too.
    returns the list of nodes to process
    """
    for parent_table_ref in current_node.parents:
        if not parent_table_ref.table_name in processed_nodes:
            table_name, parent_references = get_parent_table_references_from_sql_content(parent_table_ref.dml_ref, all_files)
            if not table_name  in processed_nodes:
                child = _build_table_reference(current_node.table_name, 
                                              current_node.dml_ref, 
                                              current_node.ddl_ref, 
                                              current_node.path)
                parent_node=_create_table_hierarchy_node(parent_table_ref.dml_ref, 
                                         table_name, 
                                         parent_references, 
                                         [child])
                _add_node_to_process_if_not_present(parent_node, nodes_to_process)
            else:
                _modify_children(current_node, parent_table_ref)
        else:
            _modify_children(current_node, parent_table_ref)
    return nodes_to_process

# ---- Reporting and walking up the hierarchy ----

def _get_matching_node_pipeline_info(access_info: FlinkTableReference) -> FlinkStatementHierarchy:
    """
    Given a table reference, get the associated FlinkStatementHierarchy by reading the pipeline definition file.
    This function is used to navigate through the hierarchy
    """
    if access_info.table_folder_name:
        return read_pipeline_metadata(access_info.table_folder_name+ "/" + PIPELINE_JSON_FILE_NAME)

def _visit_parents(current_node: FlinkStatementHierarchy) -> Dict:
    """Visit parents of current node.
    
    Args:
        current_node: Current node
    
    Returns:
        Dictionary containing parent information
    """
    parents = []
    print(f"->> {current_node.table_name}")
    for parent in current_node.parents:
        parent_info = _get_matching_node_pipeline_info(parent)
        rep = _visit_parents(parent_info)
        parents.append(rep)
    return {"table_name": current_node.table_name, "ddl_path": current_node.ddl_ref, "dml_path": current_node.dml_ref, "base_path": current_node.path, "parents": parents }

def _visit_children(current_node: FlinkStatementHierarchy) -> Dict:
    """Visit children of current node.
    
    Args:
        current_node: Current node
    
    Returns:
        Dictionary containing child information
    """
    children = []
    for child in current_node.children:
        children.append(_visit_children(_get_matching_node_pipeline_info(child)))
    return {"table_name": current_node.table_name, "ddl_path": current_node.ddl_ref, "dml_path": current_node.dml_ref, "base_path": current_node.path, "children": children }


def walk_the_hierarchy_for_report(pipeline_definition_fname: str) -> ReportInfoNode:
    """
    Walk the hierarchy of tables given the pipeline definition file name.
    This function is used to generate a report on the pipeline hierarchy for a given table.
    The function returns a dictionnary with the table name, its DDL and DML path, its parents and children.
    The parents are a list of dictionnary with the same structure, and so on.
    """
    current_hierarchy: FlinkStatementHierarchy= read_pipeline_metadata(pipeline_definition_fname)
    parents = _visit_parents(current_hierarchy)["parents"]
    children = _visit_children(current_hierarchy)["children"]
    return ReportInfoNode.model_validate({"table_name" : current_hierarchy.table_name, 
            "base_path": current_hierarchy.path,
            "ddl_path": current_hierarchy.ddl_ref,
            "dml_path": current_hierarchy.dml_ref,
            "parents": parents, 
            "children": children})

def walk_the_hierarchy_for_report_from_table(table_name: str, inventory_path: str) -> ReportInfoNode:
    """
    Walk the hierarchy of tables given the table name. This function is used to generate a report on the pipeline hierarchy for a given table.
    The function returns a dictionnary with the table name, its DDL and DML path, its parents and children.
    The parents are a list of dictionnary with the same structure, and so on.
    """
    inventory = load_existing_inventory(inventory_path)
    table_ref: FlinkTableReference = _get_table_ref_from_inventory(table_name, inventory)
    return walk_the_hierarchy_for_report(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)

if __name__ == "__main__":
    #print(json.dumps(walk_the_hierarchy_for_report_from_table("aqem_dim_tag", os.getenv("PIPELINES") ), indent=4))
    pipeline_path="/Users/jerome//Code/customers/master-control/data-platform-flink/pipelines"
    print(json.dumps(build_pipeline_definition_from_table(pipeline_path + "/dimensions/aqem/dim_user/sql-scripts/dml.aqem_dim_user.sql", pipeline_path ), indent=4))
