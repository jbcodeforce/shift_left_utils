
from functools import lru_cache
from pydantic import BaseModel, ValidationError, ConfigDict
from typing import List, Optional, Any, Tuple
import os, argparse
from collections import deque
from pathlib import Path
from create_table_folder_structure import extract_table_name
from sql_parser import SQLparser
import json
import logging

"""
Provides a set of functions to search for table dependencies from one Flink table up to the sources from the SQL project
or from a migrated Flink SQL project. The structure of the project is important and should be built with 
shift_left_project_setup.py.
"""
PIPELINE_JSON_FILE_NAME="pipeline_definition.json"

files_to_process= deque()   # keep a list file to process, as when parsing a sql we can find a lot of dependencies
dependency_list = set()
node_to_process = deque()

"""
Program arguments definition
"""
parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source, build pipeline definition metadata file.'
)

parser.add_argument('-i', '--inventory', required=False, help="name of the folder used to get all the context for the search, the inventory of table")
parser.add_argument('-f', '--file_name', required=True, help="name of the file of the SQL table, cloud Flink or dbt")
parser.add_argument('-s', '--save_file_name', required=False, help="name of the file to save the tracking content")
parser.add_argument('-t', '--table_name', required=False, help="Table name to build a pipeline or to process a pipeline on.")


class FlinkStatementHierarchy(BaseModel):
    """
    The metadata definition for a given table. As a source it will not have parent, as a sink it will not have child.
    """
    table_name: str 
    type: str
    pipe_definition: str
    ddl_ref: str
    dml_ref: str
    parents: Optional[List[Any]]
    children: Optional[List[Any]]


def _is_ddl_exists(folder_path: str) -> str:
    """
    Return the ddl file name if it exists in the given folder. All DDL file must start with ddl
    or includes a CREATE TABLE statement
    """
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.startswith('ddl'):
                return file
            # NOT IMPLEMENTED to read file content
    return None


def list_sql_files(folder_path: str) -> set[str]:
    """
    Given the folder path, list the sql statements and use the name of the file as table name
    return the list of files and table name

    :param folder_path: path to the folder which includes n sql files
    :return: Set of complete file path for the sql file in the folder
    """
    sql_files = set()
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.sql') and file.startswith("dml"):
                sql_files.add(os.path.join(root, file))   
    return sql_files


@lru_cache
def build_all_file_inventory(src_path= os.getenv("SRC_FOLDER","../dbt-src/models")) -> set[str]:
    """
    Given the path where all the sql files are persisted, build an in-memory list of paths.
    The list of folders is for a classical dbt project, but works for Flink project too
    :return: a set of sql file paths
    """
    file_paths=list_sql_files(f"{src_path}/intermediates")
    file_paths.update(list_sql_files(f"{src_path}/dimensions"))
    file_paths.update(list_sql_files(f"{src_path}/stage"))
    file_paths.update(list_sql_files(f"{src_path}/facts"))
    file_paths.update(list_sql_files(f"{src_path}/sources"))
    file_paths.update(list_sql_files(f"{src_path}/dedups"))
    logging.info("Done building file inventory")
    return file_paths

def search_table_in_inventory(table_name: str, inventory: set[str]) -> str | None:
    """
    :return: the path to access the sql file for the matching table: the filename has to include the table name
    """
    for apath in inventory:
        if table_name+'.' in apath:
            return apath
    else:
        return None
    

def get_dependencies(table_name: str, dbt_script_content: str) -> list[str]:
    """
    For a given table and dbt script content, get the dependent tables using the dbt { ref: } template
    """
    parser = SQLparser()
    dependencies = []
    dependency_names = parser.extract_table_references(dbt_script_content)

    if (len(dependency_names) > 0):
        print("Dependencies found:")
        for dependency in dependency_names:
            print(f"- depends on : {dependency}")
            dependencies.append(dependency)
    else:
        print("  - No dependency")
    return dependencies

def list_dependencies(file_or_folder: str, persist_dependencies: bool = False):
    """
    List the dependencies for the given file, or all the dependencies of all tables in the given folder
    """
    if file_or_folder.endswith(".sql"):
        table_name = extract_table_name(file_or_folder)
        with open(file_or_folder) as f:
            sql_content= f.read()
            l=get_dependencies(table_name, sql_content)
            return l
    else:
        # loop over the files in the folder
        for file in list_sql_files(file_or_folder):
            list_dependencies(file,persist_dependencies)



def generic_search_in_processed_tables(table_name: str, root_folder: str) -> bool:
    """
    It assumes that the table_name will be a folder name in the tree from the root folder
    """
    for root, dirs, files in os.walk(root_folder):
        for dir in dirs:
            if table_name == dir:
                return True
    else:
        return False
    
    
def search_table_in_processed_tables(table_name: str) -> bool:
    """
    Search in pipeline  and staging folders
    """
    pipeline_path=os.getenv("PIPELINE_FOLDER","../pipelines")
    if not generic_search_in_processed_tables(table_name,pipeline_path):
        staging_path=os.getenv("STAGING","../staging")
        return generic_search_in_processed_tables(table_name,staging_path)
    else:
      return True
    
def generate_tracking_output(file_name: str, dep_list) -> str:
    the_path= Path(file_name)

    table_name = the_path.stem
    output=f"""## Tracking the pipeline implementation for table: {table_name}
    
    -- Processed file: {file_name}
    --- Final result is a list of tables in the pipeline:
    """
    output+="\n"
    output+="\n".join(f"NOT_TESTED || OK | Table: {str(d[0])},\tSrc: {str(d[1])}" for d in dep_list)
    output+="\n\n## Data\n"
    output+="Created with tool and updated to make the final join working on the merge conditions:\n"
    return output

def get_dependent_tables(file_name: str) -> List[str]:
    """
    From the given sql file name, use sql parser to get the tables used by this sql
    content, and return the list of table names
    """
    try:
        with open(file_name) as f:
            sql_content= f.read()
            parser = SQLparser()
            table_name=parser.extract_table_name_from_insert(sql_content)
            dependencies = []
            dependency_names = parser.extract_table_references(sql_content)
            if (len(dependency_names) > 0):
                for dependency in dependency_names:
                    logging.info(f"{table_name} - depends on : {dependency}")
                    dependencies.append(dependency)
            return table_name, dependencies
    except Exception as e:
        print(e)
        return "", []
    
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


def _create_node_of_hierarchy(dml_file_name: str, table_name: str, parent_names: List[str], children: List[str]) -> FlinkStatementHierarchy:
    directory = os.path.dirname(dml_file_name)
    d2=os.path.dirname(directory)
    level = get_table_type_from_file_path(dml_file_name)
    ddl_file_name = _is_ddl_exists(directory)
    return FlinkStatementHierarchy.model_validate({"table_name": table_name, 
                                          "type": level, 
                                          "pipe_definition": d2+"/pipeline_definition.json",
                                          "ddl_ref": directory+"/"+ddl_file_name, 
                                          "dml_ref":  dml_file_name, 
                                          "parents": parent_names, 
                                          "children": children })

def _create_or_merge(current: FlinkStatementHierarchy):
    if not os.path.exists(current.pipe_definition):
        with open( current.pipe_definition, "w") as f:
            f.write(current.model_dump_json(indent=3))
    else:
        with open(current.pipe_definition, "r") as f:
            old_definition = FlinkStatementHierarchy.model_validate_json(f.read())
            combined_children=set(old_definition.children).union(set(current.children))
            combined_parents=set(old_definition.parents).union(set(current.parents))
        current.children=combined_children
        current.parents=combined_parents
        with open( current.pipe_definition, "w") as f:
            f.write(current.model_dump_json(indent=3))

def _process_next_node(node_to_process, all_files):
    if len(node_to_process) > 0:
        current_hierarchy = node_to_process.popleft()
        for parent_table_name in current_hierarchy.parents:
            parent_dml_file= search_table_in_inventory(parent_table_name, all_files)
            table_name, parent_names = get_dependent_tables(parent_dml_file)
            parent_hierarchy=_create_node_of_hierarchy(parent_dml_file, table_name, parent_names, [current_hierarchy.table_name])
            node_to_process.append(parent_hierarchy)
        _create_or_merge(current_hierarchy)
        _process_next_node(node_to_process, all_files)

def build_pipeline_definition_from_table(dml_file_name: str, children: List[str], all_files):
    """
    1/  From the DML file name build the hierarchy from sink as the root of the tree, and sources as the leaf
    2/ at each level of the tree is info_node with data to be able to run a statement for the current node
    3/ write at each level the list of parents and children and meta-data. Keep only the name
    """
    table_name, parent_names = get_dependent_tables(dml_file_name)
    current_node= _create_node_of_hierarchy(dml_file_name, table_name, parent_names, children)
    node_to_process.append(current_node)
    _process_next_node(node_to_process, all_files)
    return current_node




def _get_path_to_pipeline_file(file_name: str) -> str:
    directory = os.path.dirname(file_name)
    if "sql-scripts" in directory:
        pname = f"{directory}/../{PIPELINE_JSON_FILE_NAME}"
    else:
        pname = f"{directory}/{PIPELINE_JSON_FILE_NAME}"
    return pname   

def assess_pipeline_definition_exists(file_name: str) -> str:
    """
    From the given sql file path that includes the table we want to have the pipeline
    extract the file_path, and looks at the metadata file
    """
    try:
        pname = _get_path_to_pipeline_file(file_name)
        if os.path.exists(pname):
            try:
                with open(pname, 'r') as f:
                    json.load(f)
                return pname
            except json.JSONDecodeError:
                print("ERROR {pname} not a json file")
                return None
        else:
            return None
    except Exception as e:
        return None
    
def process_files_from_queue(files_to_process, all_files):
    """
    For each file in the queue get the parents (dependencies) of the table declared in the file. 
    Get the matching file name in the dbt or Flink project of each of those parent table,
    when found add the filename to the queue so this code can build the dependency pipeline.

    :parameter: files to process
    :parameter: all_files: an inventory of all sql file in a project.
    """
    if (len(files_to_process) > 0):
        fn = files_to_process.popleft()
        logging.info(f"\n\n-- Process file: {fn}")
        #if not assess_pipeline_definition_exists(fn):
        #    table_name, hierarchy=build_pipeline_definition_from_table(args.file_name,None,all_files)
   
        all_dependencies=list_dependencies(fn)
        if all_dependencies:
            current_dependencies=set(all_dependencies)
            for dep in current_dependencies:
                matching_sql_file=search_table_in_inventory(dep, all_files)
                if matching_sql_file:
                    dependency_list.add((dep, matching_sql_file))
                    files_to_process.append(matching_sql_file)
                else:
                    dependency_list.add((dep,None))
        return process_files_from_queue(files_to_process, all_files)
    else:
        return dependency_list
    
if __name__ == "__main__":
    """
    Build the inventory of flink sql files, then search the given table name or
    the tables referenced in the sql file specified in parameter
    """
    args = parser.parse_args()
    if args.inventory:
        all_files= build_all_file_inventory(args.inventory)
    else:
        all_files= build_all_file_inventory()
    if args.file_name:
        files_to_process.append(args.file_name)
        dependencies=process_files_from_queue(files_to_process, all_files)
        output=generate_tracking_output(args.file_name, dependencies)
    
        if args.save_file_name:
            with open(args.save_file_name, "w") as f:
                f.write(output)
                print(f"\n Save result to {args.save_file_name}")
        print(output)

        