"""
Pipeline manager defines a set of functions to build an inventory, pipeline definition for table,
and navigation within the pipeline tree.

"""
import os
import logging
import json
from shift_left.core.project_manager import create_folder_if_not_exist, get_ddl_dml_from_folder
from shift_left.core.sql_parser import SQLparser
from shift_left.core.app_config import get_config
from collections import deque
from typing import Optional, Final, Any, Set, Tuple, List
from pydantic import BaseModel

logging.basicConfig(filename='logs/pipelines.log',  filemode='w', level=get_config()["app"]["logging"], 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

SCRIPTS_DIR="sql-scripts"
INVENTORY_FILE_NAME="inventory.json"
PIPELINE_FOLDER_NAME="pipelines"
PIPELINE_JSON_FILE_NAME="pipeline_definition.json"

# --- GLOBAL variables

files_to_process= deque()   # keep a list file to process, as when parsing a sql we can find a lot of dependencies
node_to_process = deque()

# --- Data Model for the Pipeline Metadata
class FlinkTableReference(BaseModel):
    table_name: Final[str] 
    dml_ref: Optional[str]
    ddl_ref: Optional[str]
    table_folder_name: str
    def __hash__(self):
        return hash((self.table_name))
    def __eq__(self, other):
        if not isinstance(other, FlinkTableReference):
            return NotImplemented
        return self.table_name == other.table_name

class FlinkStatementHierarchy(BaseModel):
    """
    The metadata definition for a given table. As a source it will not have parent, as a sink it will not have child.
    """
    table_name: str 
    type: str
    path: str
    ddl_ref: str
    dml_ref: str
    parents: Optional[Set[FlinkTableReference]]
    children: Optional[Set[FlinkTableReference]]
    def __hash__(self):
        return hash((self.table_name))

    def __eq__(self, other):
        if not isinstance(other, FlinkStatementHierarchy):
            return NotImplemented
        return self.table_name == other.table_name
    
class ReportInfoNode(BaseModel):
    table_name: str
    base_path: str
    ddl_path: str
    dml_path: str
    parents: Optional[Any]
    children: Optional[Any]

# --- Factories to build model
def _build_table_reference(table_name: str, 
                           dml_file_name: str, 
                           ddl_file_name: str, 
                           table_folder: str = None) -> FlinkTableReference:
    if not table_folder:
        directory = os.path.dirname(dml_file_name)
        table_folder=os.path.dirname(directory)
    return FlinkTableReference.model_validate(
                        {"table_name": table_name,
                         "dml_ref": dml_file_name,
                         "ddl_ref": ddl_file_name,
                         "table_folder_name": table_folder})

def build_inventory(pipeline_folder: str) -> dict:
    """
    Entry point for cli command to build inventory
    """
    return get_or_build_inventory(pipeline_folder, pipeline_folder, True)


def from_absolute_to_pipeline(file_or_folder_name) -> str:
    """
    As the tool is persisting only relative path from pipelines, it needs to
    transfrom from absolute path to pipelines based path
    """
    if file_or_folder_name and not file_or_folder_name.startswith(PIPELINE_FOLDER_NAME):
        index = file_or_folder_name.find(PIPELINE_FOLDER_NAME) 
        new_path = file_or_folder_name[index:] if index != -1 else file_or_folder_name
        return new_path
    else:
        return file_or_folder_name

def from_pipeline_to_absolute(file_or_folder_name) -> str:
    if file_or_folder_name and file_or_folder_name.startswith(PIPELINE_FOLDER_NAME):
        root = os.path.dirname(os.getenv("PIPELINES"))
        new_path = os.path.join(root,file_or_folder_name)
        return new_path
    else:
        return file_or_folder_name


def get_or_build_inventory(pipeline_folder: str, target_path: str, recreate: bool= False) -> dict:
    """
    From the root folder where all the ddl are saved. e.g. the pipelines folder navigate to get the dml
    open each dml file to get the table name and build and inventory, table_name, FlinkTableReference
    Root_folder should be absolute path
    """
    create_folder_if_not_exist(target_path)
    inventory_path= os.path.join(target_path, INVENTORY_FILE_NAME)
    print(inventory_path)
    inventory={}
    if recreate or not os.path.exists(inventory_path):
        parser = SQLparser()
        for root, dirs, _ in os.walk(pipeline_folder):
            for dir in dirs:
                if SCRIPTS_DIR == dir:
                    ddl_file_name, dml_file_name = get_ddl_dml_from_folder(root, dir)
                    logging.debug(f"process file {dml_file_name}")
                    if dml_file_name:
                        with open(dml_file_name, "r") as f:
                            sql_content= f.read()
                            table_name=parser.extract_table_name_from_insert_into_statement(sql_content)
                            directory = os.path.dirname(dml_file_name)
                            table_folder = from_absolute_to_pipeline(os.path.dirname(directory))
                            ref = FlinkTableReference.model_validate(
                                        {"table_name": table_name,
                                        "ddl_ref": from_absolute_to_pipeline(ddl_file_name),
                                        "dml_ref": from_absolute_to_pipeline(dml_file_name),
                                        "table_folder_name": table_folder})
                            logging.debug(ref)
                            inventory[ref.table_name]= ref.model_dump()
        with open(inventory_path, "w") as f:
            json.dump(inventory, f, indent=4)
        logging.info(f"Create inventory file {inventory_path}")
    else:
        with open(inventory_path, "r") as f:
            inventory= json.load(f)
    return inventory

def load_existing_inventory(target_path: str):
    inventory_path= os.path.join(target_path, INVENTORY_FILE_NAME)
    with open(inventory_path, "r") as f:
        return json.load(f)


def build_pipeline_definition_from_table(dml_file_name: str, pipeline_path: str):
    """
    1/  From the DML file name, build the hierarchy from sink as the root of the tree, and sources as the leaf
    2/ at each level of the tree is info_node with data to be able to run a statement for the current node
    3/ write at each level the list of parents and children and meta-data. Keep only the name
    At this stage dml_file name as the absolute path
    """
    dml_file_name = from_absolute_to_pipeline(dml_file_name)
    all_files= load_existing_inventory(pipeline_path)
    table_name, parent_references = get_parent_table_references_from_sql_content(dml_file_name, all_files)
    current_node= _create_node(dml_file_name, table_name, parent_references, [])
    node_to_process.append(current_node)
    _process_next_node(node_to_process, dict(), all_files)
    return current_node

def get_parent_table_references_from_sql_content(sql_file_name: str, all_files) -> Set[FlinkTableReference]:
    """
    From the given sql file name, use sql parser to get the tables used by this sql
    content, and return the list of table names
    """
    try:
        if sql_file_name.startswith(PIPELINE_FOLDER_NAME):
            sql_file_name=os.path.join(os.getenv("PIPELINES"),"..", sql_file_name)
        with open(sql_file_name) as f:
            sql_content= f.read()
            parser = SQLparser()
            table_name=parser.extract_table_name_from_insert_into_statement(sql_content)
            dependencies = set()
            dependency_names = parser.extract_table_references(sql_content)
            if (len(dependency_names) > 0):
                for dependency in dependency_names:
                    logging.info(f"{table_name} - depends on : {dependency}")
                    ddl_file_name, dml_file_name= search_table_in_inventory(dependency, all_files)
                    dependencies.add(_build_table_reference(dependency, dml_file_name, ddl_file_name))
            return table_name, dependencies
    except Exception as e:
        logging.error(e)
        return "",  set()

def search_table_in_inventory(table_name: str, inventory: dict) -> Tuple[str,str]:
    """
    Given the table name, search the matching information from the inventory.
    returns ddl and dml file name, relative to pipelines folder
    """
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
    folder_path=from_pipeline_to_absolute(folder_path)
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.startswith('ddl'):
                return from_absolute_to_pipeline(os.path.join(root,file))
            # NOT IMPLEMENTED to read file content
    return ""

def get_table_type_from_file_path(file_name: str) -> str:
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
    
def _create_node(dml_file_name: str,
                table_name: str,
                parent_names: Set[FlinkTableReference],
                children: Set[FlinkTableReference]) -> FlinkStatementHierarchy:
    """
    Create the Hierarchy object with needed information.
    The dml_file_name may be absolute or relative to pipelines. 
    """
    logging.debug(f"_create_node_of_hierarchy( {dml_file_name}, {table_name},  {parent_names}, {children})")
    directory = os.path.dirname(dml_file_name)
    table_folder=os.path.dirname(directory)
    level = get_table_type_from_file_path(dml_file_name)
    ddl_file_name=  get_ddl_file_name(directory)
    dml_file_name = table_folder + "sql-scripts/" + dml_file_name.split("/")[-1]
    return FlinkStatementHierarchy.model_validate({"table_name": table_name, 
                                          "type": level, 
                                          "path": table_folder,
                                          "ddl_ref":  ddl_file_name, 
                                          "dml_ref":  dml_file_name, 
                                          "parents": parent_names, 
                                          "children": children })

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
    pipe_definition_fn=os.path.join(os.getenv("PIPELINES"), "..", current.path, PIPELINE_JSON_FILE_NAME)
    if not os.path.exists(pipe_definition_fn):
        with open( pipe_definition_fn, "w") as f:
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
        current.children=combined_children
        current.parents=combined_parents
        with open( pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))

def _read_pipeline_metadata(file_name: str) -> FlinkStatementHierarchy:
    file_name=from_pipeline_to_absolute(file_name)
    with open(file_name, "r") as f:
        content = FlinkStatementHierarchy.model_validate_json(f.read())
        return content
    

def _modify_children(current: FlinkStatementHierarchy, parent_ref: FlinkTableReference):
    """
    Verify the current is in the children of the parent.
    It may happend that parent was already processed, but the current is referencing it another time,
    in this case we need to add to the children of the parent
    """
    child = _build_table_reference(current.table_name, current.dml_ref, current.ddl_ref)
    pipe_definition_fn = parent_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME
    parent = _read_pipeline_metadata(pipe_definition_fn)
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
                parent_node=_create_node(parent_table_ref.dml_ref, 
                                         table_name, 
                                         parent_references, 
                                         [child])
                _add_node_to_process_if_not_present(parent_node, nodes_to_process)
            else:
                _modify_children(current_node, parent_table_ref)
        else:
            _modify_children(current_node, parent_table_ref)
    return nodes_to_process