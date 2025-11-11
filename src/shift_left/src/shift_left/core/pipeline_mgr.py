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
import time
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
    FlinkStatementComplexity,
    get_ddl_file_name,
    extract_product_name,
    get_table_ref_from_inventory,
    get_or_build_inventory,
    get_table_type_from_file_path,
    create_folder_if_not_exist,
    read_pipeline_definition_from_file,
    get_ddl_dml_names_from_pipe_def
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

class PipelineStatusTree:
    """
    Class to provide the status of each statement in a pipeline given the child table.
    """
    def __init__(self, statement_info: Dict[str, Dict[str, Any]], inventory_path: str):
        self.statement_info = statement_info
        self.inventory_path = inventory_path
        self.summary = {}

    def pipeline_status(self, child_table, node_data, tree_node):

        logger.info(f"child_table: {child_table}")
        if node_data.parents:
            for parent in node_data.parents:
                logger.info(f"Parent: {parent.table_name}")
                c_pipeline_def = get_pipeline_definition_for_table(child_table, self.inventory_path)
                c_ddl_n, c_dml_n = get_ddl_dml_names_from_pipe_def(c_pipeline_def)
                p_ddl_n, p_dml_n = get_ddl_dml_names_from_pipe_def(parent)
                product_name = c_pipeline_def.product_name
                if c_dml_n in self.statement_info:
                    status_phase = self.statement_info[c_dml_n]['status_phase']
                    compute_pool_id = self.statement_info[c_dml_n]['compute_pool_id']
                    #compute_pool_name = self.statement_info[c_dml_n]['compute_pool_name']
                else:
                    status_phase = 'UNKNOWN'
                    compute_pool_id = None

                # Initialize the status counters for the product
                logger.info(f"Product: {product_name}")
                if product_name not in self.summary:
                    self.summary[product_name] = {}
                    self.summary[product_name]["OUT_OF_ORDER"] = {}
                    self.summary[product_name]["POOLS"] = []

                logger.info(f"compute_pool_id: {compute_pool_id} status_phase: {status_phase}")
                if compute_pool_id and compute_pool_id not in self.summary[product_name]["POOLS"]:
                    self.summary[product_name]["POOLS"].append(compute_pool_id)
                if status_phase not in self.summary[product_name]:
                    self.summary[product_name][status_phase] = {}
                #if child_table not in self.summary[product_name][status_phase]:
                #    self.summary[product_name][status_phase][child_table] = 1
                if c_dml_n not in self.summary[product_name][status_phase]:
                    self.summary[product_name][status_phase][c_dml_n] = 1
                if  c_dml_n in self.statement_info and p_dml_n in self.statement_info:   #-- Not RUNNING statements
                    if self.statement_info[c_dml_n]['created_at'] < self.statement_info[p_dml_n]['created_at']:
                        if not parent.table_name in self.summary[product_name]["OUT_OF_ORDER"]:
                            self.summary[product_name]["OUT_OF_ORDER"][parent.table_name] = f"{p_dml_n},{self.statement_info[p_dml_n]['created_at']},{c_dml_n},{self.statement_info[c_dml_n]['created_at']}"

                parent_node = tree_node.add(f"[green]{parent.table_name}[/green]")
                parent_node.add(f"[dim]Type: {parent.type}[/dim]")
                parent_node.add(f"[dim]Product: {parent.product_name}[/dim]")
                child=parent
                # Recursive call
                logger.info(f"recursive call : {child.table_name}")
                self.pipeline_status(child.table_name, parent, parent_node)
        else:
            # this is the case of a source table with no parents
            logger.info(f"Source: {node_data.table_name}")
            s_pipeline_def = get_pipeline_definition_for_table(node_data.table_name, self.inventory_path)
            s_ddl_n, s_dml_n = get_ddl_dml_names_from_pipe_def(s_pipeline_def)
            product_name = s_pipeline_def.product_name
            if s_dml_n in self.statement_info:
                status_phase = self.statement_info[s_dml_n]['status_phase']
                compute_pool_id = self.statement_info[s_dml_n]['compute_pool_id']
            else:
                status_phase = 'UNKNOWN'
                compute_pool_id = None

            if product_name not in self.summary:
                self.summary[product_name] = {}
                self.summary[product_name]["OUT_OF_ORDER"] = {}
                self.summary[product_name]["POOLS"] = []

            if compute_pool_id and compute_pool_id not in self.summary[product_name]["POOLS"]:
                self.summary[product_name]["POOLS"].append(compute_pool_id)
            if status_phase not in self.summary[product_name]:
                self.summary[product_name][status_phase] = {}
            #if not node_data.table_name in self.summary[product_name][status_phase]:
            #    self.summary[product_name][status_phase][node_data.table_name] = 1
            if s_dml_n not in self.summary[product_name][status_phase]:
                self.summary[product_name][status_phase][s_dml_n] = 1
        return self.summary

def get_pipeline_definition_for_table(table_name: str, inventory_path: str) -> FlinkTablePipelineDefinition:
    table_inventory = get_or_build_inventory(inventory_path, inventory_path, False)
    table_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, table_inventory)
    return read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)


def build_pipeline_definition_from_ddl_dml_content(
    dml_file_name: str,
    ddl_file_name: str,
    pipeline_path: str
) -> FlinkTablePipelineDefinition:
    """Build pipeline definition hierarchy starting from given dml file. This is the exposed API
    so entry point of the processing.

    Args:
        ddl_file_name: Path to DDL file for root table
        dml_file_name: Path to DML file for root table
        pipeline_path: Root pipeline folder path

    Returns: FlinkTablePipelineDefinition
        FlinkTablePipelineDefinition for the table and its dependencies
    """
    #dml_file_name = from_absolute_to_pipeline(dml_file_name)
    table_inventory = get_or_build_inventory(pipeline_path, pipeline_path, False)

    table_name, parent_references, complexity = _build_pipeline_definitions_from_sql_content(dml_file_name, ddl_file_name, table_inventory)
    logger.debug(f"Build pipeline for table: {table_name} from {dml_file_name} with parents: {parent_references}")
    current_node = _build_pipeline_definition(table_name=table_name,
                                              table_type=None,
                                              complexity=complexity,
                                              table_folder=None,
                                              dml_file_name=from_absolute_to_pipeline(dml_file_name),
                                              ddl_file_name=from_absolute_to_pipeline(ddl_file_name),
                                              parents=parent_references,
                                              children=set())
    node_to_process= deque()
    node_to_process.append(current_node)
    _update_hierarchy_of_next_node(node_to_process, {}, table_inventory)
    return current_node

def build_all_pipeline_definitions(pipeline_path: str):
    count = 0
    for folder in ["dimensions", "facts", "views", "intermediates", "stage", "sources", "seeds"]:
        path = Path(pipeline_path) / folder
        if path.exists():
            count=_process_table_folder_build_pipeline_def(path, pipeline_path, count)
    if count == 0:
        # it is possible the pipeline path is not a kimball structure but a flat structure
        count=_process_table_folder_build_pipeline_def(Path(pipeline_path), pipeline_path, count)
    logger.info(f"Total number of pipeline definitions created: {count}")
    print(f"{time.strftime('%Y%m%d_%H:%M:%S')} Total number of pipeline definitions created: {count}")


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

def _process_table_for_integration_test(table_def: FlinkTablePipelineDefinition, where_to_save_test_files: str):
    if len(table_def.parents) == 0:
        # insert statement
        print(f"Insert statement for {table_def.table_name}")
        pass
    else:
        # generate a validation statement
        print(f"Validation statement for {table_def.table_name}")
        # recursively call the function for the parent
        for parent in table_def.parents:
            _process_table_for_integration_test(parent, where_to_save_test_files)


def _build_pipeline_definitions_from_sql_content(
    dml_file_name: str,
    ddl_file_name: str,
    table_inventory: Dict
) -> Tuple[str, Set[FlinkTablePipelineDefinition], FlinkStatementComplexity]:
    """Extract parent table references and semantic from SQL content plus some
    complexity metrics.

    Args:
        dml_file_name: Path to SQL file for dml content
        ddl_file_name: Path to SQL file for ddl content
        table_inventory: Dictionary of all available files

    Returns:
        Tuple of (current_table_name, set of parent FlinkTablePipelineDefinition, complexity of current table)
    """
    try:
        dml_sql_content = ""
        ddl_sql_content = ""
        referenced_table_names = set()
        parser = SQLparser()
        current_table_name = None
        complexity = FlinkStatementComplexity()
        if dml_file_name:
            if dml_file_name.startswith(PIPELINE_FOLDER_NAME):
                dml_file_name = os.path.join(os.getenv("PIPELINES"), "..", dml_file_name)
            with open(dml_file_name) as f:
                dml_sql_content = f.read()
            current_table_name = parser.extract_table_name_from_insert_into_statement(dml_sql_content)
            referenced_table_names = parser.extract_table_references(dml_sql_content)

        if  not current_table_name and ddl_file_name:
            if ddl_file_name.startswith(PIPELINE_FOLDER_NAME):
                ddl_file_name = os.path.join(os.getenv("PIPELINES"), "..", ddl_file_name)
            with open(ddl_file_name) as f:
                ddl_sql_content = f.read()
            current_table_name = parser.extract_table_name_from_create_statement(ddl_sql_content)
        dependencies = set()
        state_form = parser.extract_upgrade_mode(dml_sql_content, ddl_sql_content)
        complexity = parser.extract_statement_complexity(dml_sql_content,state_form)
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
                dep_complexity = FlinkStatementComplexity(state_form=state_form)
                if table_ref.dml_ref and table_ref.dml_ref.startswith(PIPELINE_FOLDER_NAME):
                    table_dml_ref = os.path.join(os.getenv("PIPELINES"), "..", table_ref.dml_ref)
                    _dml_sql_content=""
                    _ddl_sql_content=""
                    with open(table_dml_ref, "r") as g:
                        _dml_sql_content = g.read()
                    table_ddl_ref = os.path.join(os.getenv("PIPELINES"), "..", table_ref.ddl_ref)
                    with open(table_ddl_ref, "r") as g:
                        _ddl_sql_content = g.read()
                    dependent_state_form = parser.extract_upgrade_mode(_dml_sql_content, _ddl_sql_content)
                    dep_complexity = parser.extract_statement_complexity(_dml_sql_content, dependent_state_form)
                logger.debug(f"{current_table_name} - depends on: {table_name} which is : {dependent_state_form}")
                bpd = _build_pipeline_definition(
                    table_name,
                    table_ref.type,
                    dep_complexity,
                    table_ref.table_folder_name,
                    table_ref.dml_ref,
                    table_ref.ddl_ref,
                    set(),
                    set()
                )
                dependencies.add(bpd)
        else:
            logger.warning(f"No referenced table found in {dml_file_name}")
        return current_table_name, dependencies, complexity

    except Exception as e:
        logger.error(f"Error while processing {dml_file_name} or {ddl_file_name} with message: {e} but process continues...")
        return ERROR_TABLE_NAME, set(), None


def _process_table_folder_build_pipeline_def(parent_folder_path, pipeline_path, count: int) -> int:
    for sql_scripts_path in parent_folder_path.rglob("sql-scripts"): # rglob recursively finds all sql-scripts directories.
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
            complexity: FlinkStatementComplexity,
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
    sql_scripts_directory = os.path.dirname(dml_file_name if dml_file_name else ddl_file_name)
    if not table_folder:
        table_folder = os.path.dirname(sql_scripts_directory)
    if not ddl_file_name:
        ddl_file_name = get_ddl_file_name(sql_scripts_directory)
    product_name = extract_product_name(table_folder)
    f = FlinkTablePipelineDefinition.model_validate({
        "table_name": table_name,
        "product_name": product_name,
        "complexity": complexity,
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
    Look at parents of current node, and add them to the queue if not already present, add current node to children of its parents.
    """
    if len(nodes_to_process) > 0:
        current_node = nodes_to_process.pop()
        logger.info(f"Work on hierarchy for {current_node.table_name}")
        if not current_node.table_name in processed_nodes:
            if not current_node.parents: # the current node may not be fully built yet
                table_name, parent_references, complexity = _build_pipeline_definitions_from_sql_content(current_node.dml_ref, current_node.ddl_ref, table_inventory)
                current_node.parents = parent_references   # parents is a set of FlinkTablePipelineDefinition
                current_node.complexity = complexity
            tmp_node= current_node.model_copy(deep=True)  # make a copy with parent and children to avoid huge/recurring pipedef.
            tmp_node.children = set()
            tmp_node.parents = set()
            for parent in current_node.parents:
                if not  current_node in parent.children: # current is a child of its parents
                    parent.children.add(tmp_node)
                    _create_or_merge_pipeline_definition(parent)
                nodes_to_process=_add_node_to_process_if_not_present(parent, nodes_to_process)
            _create_or_merge_pipeline_definition(current_node)
            processed_nodes[current_node.table_name]=current_node
            _update_hierarchy_of_next_node(nodes_to_process, processed_nodes, table_inventory)



def _create_or_merge_pipeline_definition(current: FlinkTablePipelineDefinition):
    """
    If the pipeline definition exists we may need to merge the parents and children
    """
    def merge_table_sets(old_set, new_set):
        """Merge sets, with new items overriding old ones by table_name"""
        # Convert to dict by table_name for easy merging
        merged = {item.table_name: item for item in old_set}
        merged.update({item.table_name: item for item in new_set})
        return set(merged.values())

    pipe_definition_fn = os.path.join(os.getenv("PIPELINES"), "..", current.path, PIPELINE_JSON_FILE_NAME)
    if not os.path.exists(pipe_definition_fn):
        with open(pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))
    else:
        with open(pipe_definition_fn, "r") as f:
            old_definition = FlinkTablePipelineDefinition.model_validate_json(f.read())
            combined_children = merge_table_sets(old_definition.children, current.children)
            combined_parents = merge_table_sets(old_definition.parents, current.parents)
        current.children = combined_children
        current.parents = combined_parents
        with open(pipe_definition_fn, "w") as f:
            f.write(current.model_dump_json(indent=3))


def _add_node_to_process_if_not_present(current_hierarchy, nodes_to_process):
    try:
        nodes_to_process.index(current_hierarchy)
    except ValueError:
        nodes_to_process.append(current_hierarchy)
    return nodes_to_process


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





