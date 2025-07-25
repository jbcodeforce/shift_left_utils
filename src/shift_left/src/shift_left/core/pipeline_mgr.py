"""
Copyright 2024-2025 Confluent, Inc.

Flink Statement pipeline manager defines functions to build inventory, create pipeline definition for table, 
and navigate statement pipeline trees.

This module provides functionality to:
1. Build and manage pipeline definition inventories
2. Create pipeline definitions for tables
3. Navigate and analyze pipeline hierarchies
"""
from collections import deque

import os
from pathlib import Path
from typing import Dict, Optional, Any, Set, Tuple

from pydantic import BaseModel, Field
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.app_config import logger
from shift_left.core.utils.file_search import (
    PIPELINE_JSON_FILE_NAME,
    PIPELINE_FOLDER_NAME,
    from_absolute_to_pipeline,
    FlinkTableReference, 
    FlinkTablePipelineDefinition,
    get_ddl_file_name,
    extract_product_name,
    get_table_ref_from_inventory,
    get_or_build_inventory,
    get_table_type_from_file_path,
    read_pipeline_definition_from_file
)


# Constants

ERROR_TABLE_NAME = "error_table"
# Global queues for processing
files_to_process: deque = deque()  # Files to process when parsing SQL dependencies
node_to_process: deque = deque()   # Nodes to process in pipeline hierarchy


 
class PipelineReport(BaseModel):
    """
    Class to represent a full pipeline tree without recursion
    """
    table_name: str
    path: str
    ddl_ref: Optional[str] = Field(default="", description="DDL path")
    dml_ref: Optional[str] = Field(default="", description="DML path")
    parents: Optional[Set[Any]] = Field(default=set(),   description="parents of this flink dml")
    children: Optional[Set[Any]] = Field(default=set(),  description="users of the table created by this flink dml")


def get_pipeline_definition_for_table(table_name: str, inventory_path: str) -> FlinkTablePipelineDefinition:
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    if not table_ref:
        raise Exception(f"Table {table_name} not found. Stop processing")
    return read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)


def build_pipeline_definition_from_ddl_dml_content(
    dml_file_name: str, 
    ddl_file_name: str,
    pipeline_path: str
) -> FlinkTablePipelineDefinition:
    """Build pipeline definition hierarchy starting from given dml file. This is the exposed API
    so entry point of the processing.
    
    Args:
        dml_file_name: Path to DML file for root table
        pipeline_path: Root pipeline folder path
        
    Returns: FlinkTablePipelineDefinition
        FlinkTablePipelineDefinition for the table and its dependencies
    """
    #dml_file_name = from_absolute_to_pipeline(dml_file_name)
    table_inventory = get_or_build_inventory(pipeline_path, pipeline_path, False)
    
    table_name, parent_references, state_form = _build_pipeline_definitions_from_sql_content(dml_file_name, ddl_file_name, table_inventory)
    logger.debug(f"Build pipeline for table: {table_name} from {dml_file_name} with parents: {parent_references}")
    current_node = _build_pipeline_definition(table_name=table_name,
                                              table_type=None,
                                              state_form=state_form,
                                              table_folder=None,
                                              dml_file_name=from_absolute_to_pipeline(dml_file_name),
                                              ddl_file_name=None, 
                                              parents=parent_references, 
                                              children=set())
    node_to_process.append(current_node)
    _update_hierarchy_of_next_node(node_to_process, dict(), table_inventory)
    return current_node

def build_all_pipeline_definitions(pipeline_path: str):
    count = 0
    for folder in ["dimensions", "facts", "views", "intermediates", "stage", "sources", "seeds"]:
        path = Path(pipeline_path) / folder
        count=_process_one_sink_folder(path, pipeline_path, count)
    logger.info(f"Total number of pipeline definitions created: {count}")
    print(f"Total number of pipeline definitions created: {count}")

    
def get_static_pipeline_report_from_table(
        table_name: str, 
        inventory_path: str,
        parent_only: bool = True, 
        children_only: bool = False
) -> PipelineReport:
    """
    Walk the static hierarchy of tables given the table name. This function is used to generate a report on the pipeline hierarchy for a given table.
    The function returns a dictionnary with the table name, its DDL and DML path, its parents and children.
    The parents are a list of dictionnary with the same structure, and so on.
    """
    logger.info(f"walk_the_hierarchy_for_report_from_table({table_name}, {inventory_path})")
    if not inventory_path:
        inventory_path = os.getenv("PIPELINES")
    inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    if table_name not in inventory:
        raise Exception("Table not found in inventory")
    try:
        table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, inventory)
        current_hierarchy: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
        root_ref= PipelineReport(table_name= table_ref.table_name, 
                                  path= table_ref.table_folder_name, 
                                  ddl_ref= table_ref.ddl_ref, 
                                  dml_ref= table_ref.dml_ref,
                                  parents= set(),
                                  children= set())
        if parent_only:
            root_ref.parents = _visit_parents(current_hierarchy).parents # at this level all the parent elements are FlinkTablePipelineDefinition
        if children_only:
            root_ref.children = _visit_children(current_hierarchy).children
        logger.debug(f"Report built is {root_ref.model_dump_json(indent=3)}")
        return root_ref
    except Exception as e:
        logger.error(f"Error in processing pipeline report {e}")
        raise Exception(f"Error in processing pipeline report for {table_name}")



def delete_all_metada_files(root_folder: str):
    """
    Delete all the files with the given name in the given root folder tree
    """
    count = 0
    file_to_delete = PIPELINE_JSON_FILE_NAME
    logger.info(f"Delete {file_to_delete} from folder: {root_folder}")
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file_to_delete == file:
                file_path=os.path.join(root, file)
                os.remove(file_path)
                logger.info(f"File '{file_path}' deleted successfully.")
                count += 1
    logger.info(f"Total number of files deleted: {count}")
    print(f"Total number of files deleted: {count}")




# ---- Private APIs ---- 


def _build_pipeline_definitions_from_sql_content(
    dml_file_name: str,
    ddl_file_name: str,
    table_inventory: Dict
) -> Tuple[str, Set[FlinkTablePipelineDefinition], str]:
    """Extract parent table references and semantic from SQL content.
    
    Args:
        dml_file_name: Path to SQL file for dml content
        ddl_file_name: Path to SQL file for ddl content
        table_inventory: Dictionary of all available files
        
    Returns:
        Tuple of (current_table_name, set of parent FlinkTablePipelineDefinition)
    """
    try:
        if dml_file_name.startswith(PIPELINE_FOLDER_NAME):
            dml_file_name = os.path.join(os.getenv("PIPELINES"), "..", dml_file_name)
        if ddl_file_name.startswith(PIPELINE_FOLDER_NAME):
            ddl_file_name = os.path.join(os.getenv("PIPELINES"), "..", ddl_file_name)
        dml_sql_content = ""
        ddl_sql_content = ""
        with open(dml_file_name) as f:
            dml_sql_content = f.read()
        with open(ddl_file_name) as f:
            ddl_sql_content = f.read()
        parser = SQLparser()
        current_table_name = parser.extract_table_name_from_insert_into_statement(dml_sql_content)
        dependencies = set()
        referenced_table_names = parser.extract_table_references(dml_sql_content)
        state_form = parser.extract_upgrade_mode(dml_sql_content, ddl_sql_content)
        if referenced_table_names:
            if current_table_name in referenced_table_names:
                referenced_table_names.remove(current_table_name)
            for table_name in referenced_table_names:
                # strangely it is possible that the tablename was a field name because of SQL code like TRIM(BOTH '" ' FROM
                try:
                    table_ref_dict= table_inventory[table_name]
                except Exception as e:
                    logger.warning(f"{table_name} is most likely not a known table name")
                    continue
                table_ref: FlinkTableReference= FlinkTableReference.model_validate(table_ref_dict)
                dependent_state_form = state_form
                if table_ref.dml_ref.startswith(PIPELINE_FOLDER_NAME):
                    table_dml_ref = os.path.join(os.getenv("PIPELINES"), "..", table_ref.dml_ref)
                    _dml_sql_content=""
                    _ddl_sql_content=""
                    with open(table_dml_ref, "r") as g:
                        _dml_sql_content = g.read()
                    table_ddl_ref = os.path.join(os.getenv("PIPELINES"), "..", table_ref.ddl_ref)
                    with open(table_ddl_ref, "r") as g:
                        _ddl_sql_content = g.read()
                    dependent_state_form = parser.extract_upgrade_mode(_dml_sql_content, _ddl_sql_content)
                logger.debug(f"{current_table_name} - depends on: {table_name} which is : {dependent_state_form}") 
                dependencies.add(_build_pipeline_definition(
                    table_name, 
                    table_ref.type,
                    state_form,
                    table_ref.table_folder_name,
                    table_ref.dml_ref,
                    table_ref.ddl_ref,
                    set(),
                    set()
                ))
        else:
            logger.warning(f"No referenced table found in {dml_file_name}")
        return current_table_name, dependencies, state_form
            
    except Exception as e:
        logger.error(f"Error while processing {dml_file_name} or {ddl_file_name} with message: {e} but process continues...")
        return ERROR_TABLE_NAME, set(), None

    
def _process_one_sink_folder(sink_folder_path, pipeline_path, count: int):
    for sql_scripts_path in sink_folder_path.rglob("sql-scripts"): # rglob recursively finds all sql-scripts directories.
        if sql_scripts_path.is_dir():
            dml_file_name = ""
            ddl_file_name = ""
            for file_path in sql_scripts_path.iterdir(): #Iterate through the directory.

                if file_path.is_file() and file_path.name.startswith("dml"):
                    logger.debug(f"Process the dml {file_path}")
                    dml_file_name = str(file_path.resolve())
                if file_path.is_file() and file_path.name.startswith("ddl"):
                    ddl_file_name = str(file_path.resolve())
            count += 1
            build_pipeline_definition_from_ddl_dml_content(dml_file_name, ddl_file_name, pipeline_path)
    return count
    

def _build_pipeline_definition(
            table_name: str,
            table_type: str,
            state_form: str,
            table_folder: str,
            dml_file_name: str,
            ddl_file_name: str,
            parents: Optional[Set[FlinkTablePipelineDefinition]],
            children: Optional[Set[FlinkTablePipelineDefinition]]
            ) -> FlinkTablePipelineDefinition:
    """Create hierarchy node with table information.
    
    Args:
        dml_file_name: Path to DML file
        table_name: Name of the table
        parent_names: Set of parent table references
        children: Set of child table references
        
    Returns:
        FlinkTablePipelineDefinition node
    """
    logger.debug(f"parameters dml: {dml_file_name}, table_name: {table_name},  parents: {parents}, children: {children})")
    if not table_type:
        table_type = get_table_type_from_file_path(dml_file_name)
    sql_scripts_directory = os.path.dirname(dml_file_name)
    if not table_folder:
        table_folder = os.path.dirname(sql_scripts_directory)
    if not ddl_file_name:
        ddl_file_name = get_ddl_file_name(sql_scripts_directory)
    product_name = extract_product_name(table_folder)
    f = FlinkTablePipelineDefinition.model_validate({
        "table_name": table_name,
        "product_name": product_name,
        "state_form": state_form,
        "type": table_type,
        "path": table_folder,
        "ddl_ref": ddl_file_name,
        #"dml_ref": base_path + "/" + SCRIPTS_DIR + "/" + dml_file_name.split("/")[-1],
        "dml_ref" : dml_file_name,
        "parents": parents,
        "children": children
    })
    logger.debug(f" FlinkTablePipelineDefinition created: {f}")
    return f

    
def _update_hierarchy_of_next_node(nodes_to_process, processed_nodes,  table_inventory):
    """
    Process the next node from the queue if not already processed.
    Look at parent of current nodes.
    """
    if len(nodes_to_process) > 0:
        current_node = nodes_to_process.pop()
        logger.info(f"{current_node}")
        if not current_node.table_name in processed_nodes:
            if not current_node.parents:
                table_name, parent_references, state_form = _build_pipeline_definitions_from_sql_content(current_node.dml_ref, current_node.ddl_ref, table_inventory)
                current_node.parents = parent_references   # set of FlinkTablePipelineDefinition
                current_node.state_form = state_form
            tmp_node= current_node.model_copy(deep=True)
            tmp_node.children = set()
            tmp_node.parents = set()
            for parent in current_node.parents:
                if not  current_node in parent.children: # current is a child of its parents        
                    parent.children.add(tmp_node)
                _add_node_to_process_if_not_present(parent, nodes_to_process)
            _create_or_merge_pipeline_definition(current_node)
            processed_nodes[current_node.table_name]=current_node
            _update_hierarchy_of_next_node(nodes_to_process, processed_nodes, table_inventory)



def _create_or_merge_pipeline_definition(current: FlinkTablePipelineDefinition):
    """
    If the pipeline definition exists we may need to merge the parents and children
    """
    pipe_definition_fn = os.path.join(os.getenv("PIPELINES"), "..", current.path, PIPELINE_JSON_FILE_NAME)
    if not os.path.exists(pipe_definition_fn):
        with open(pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))
    else:
        with open(pipe_definition_fn, "r") as f:
            old_definition = FlinkTablePipelineDefinition.model_validate_json(f.read())
            combined_children = old_definition.children
            combined_parents = old_definition.parents
            for child in current.children:
                # Remove any existing child with same table_name before adding new one
                combined_children = {c for c in combined_children if c.table_name != child.table_name}
                combined_children.add(child)
            for parent in current.parents:
                combined_parents = {p for p in combined_parents if p.table_name != parent.table_name}
                combined_parents.add(parent)
        current.children = combined_children
        current.parents = combined_parents
        with open(pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))


def _add_node_to_process_if_not_present(current_hierarchy, nodes_to_process):
    try: 
        nodes_to_process.index(current_hierarchy)
    except ValueError:
        nodes_to_process.append(current_hierarchy)


# ---- Reporting and walking up the hierarchy ----

def _get_statement_hierarchy_from_table_ref(access_info: FlinkTablePipelineDefinition) -> FlinkTablePipelineDefinition:
    """
    Given a table reference, get the associated FlinkTablePipelineDefinition by reading the pipeline definition file.
    This function is used to navigate through the hierarchy
    """
    if access_info.path:
        return read_pipeline_definition_from_file(access_info.path+ "/" + PIPELINE_JSON_FILE_NAME)

def _visit_parents(current_node: FlinkTablePipelineDefinition) -> FlinkTablePipelineDefinition:
    """Visit parents of current node.
    The goal is for the current node which does not have a parents or children populated with FlinkTablePipelineDefinition objects to populate those
    sets. 
    Args:
        current_node: Current node
    
    Returns:
        FlinkTablePipelineDefinition containing parents information as FlinkTablePipelineDefinition
    """
    parents = set()
    
    for parent in current_node.parents:
        parent_info = _get_statement_hierarchy_from_table_ref(parent)
        rep = _visit_parents(parent_info)
        parents.add(rep)
    current_node.parents = parents
    logger.info(f"The parents of {current_node.table_name} are {', '.join([p.table_name for p in parents])}")
    return current_node

def _visit_children(current_node: FlinkTablePipelineDefinition) -> FlinkTablePipelineDefinition:
    """Visit children of current node.
    
    Args:
        current_node: Current node
    
    Returns:
        FlinkTablePipelineDefinition containing parents and childrens information
    """
    children = set()
    logger.info(f"child of -> {current_node.table_name}")
    for child in current_node.children:
        child_info = _get_statement_hierarchy_from_table_ref(child)
        children.add(_visit_children(child_info))
    current_node.children = children
    return current_node





