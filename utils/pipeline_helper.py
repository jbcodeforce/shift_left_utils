
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Optional, Set, Tuple
import os, argparse
from collections import deque
from pathlib import Path
from sql_parser import SQLparser
import json
import logging
from kafka.app_config import get_config
from create_table_folder_structure import get_or_build_inventory, FlinkTableReference, from_absolute_to_pipeline, from_pipeline_to_absolute

"""
Provides a set of functions to search for table dependencies from one Flink table up to the sources from the SQL project
or from a migrated Flink SQL project.
"""
PIPELINE_JSON_FILE_NAME="pipeline_definition.json"

logging.basicConfig(filename='logs/pipelines.log',  filemode='a', level=get_config()["app"]["logging"], 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

#logging.basicConfig(level=get_config()["app"]["logging"],format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


files_to_process= deque()   # keep a list file to process, as when parsing a sql we can find a lot of dependencies
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


@lru_cache
def build_all_file_inventory(root_folder: str) -> set[str]:
    """
    There are two folders to keep the flink sql, the final pipelines, and the staging
    where flink sql are under development.
    """
    print(root_folder)
    if root_folder:
        file_paths=list_sql_files(f"{root_folder}")
        return file_paths
    else:
        return set()



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


def list_sql_files(folder_path: str) -> set[str]:
    """
    Given the folder path, list the sql statements and use the name of the file as table name
    return the list of files and table name

    :param folder_path: path to the folder which includes n sql files
    :return: Set of complete file path for the sql file in the folder
    """
    sql_files = set()
    for root, dirs, files in os.walk(folder_path):
        if "tests" not in root:
            for file in files:
                if file.endswith('.sql'):
                    if not file.startswith("ddl"):
                        sql_files.add(os.path.join(root, file))   
    return sql_files



def search_table_in_inventory(table_name: str, inventory: dict) -> Tuple[str,str]:
    
    table_name=table_name.split(".sql")[0]    # legacy table name
    table_ref: FlinkTableReference= FlinkTableReference.model_validate(inventory[table_name])
    logging.debug(f"Search {table_name} in inventory of files got {table_ref}")
    if table_ref and table_ref.dml_ref:
        return table_ref.ddl_ref, table_ref.dml_ref
    return "", ""

def search_table_in_inventory_all_files(table_name: str, inventory: set[str]) -> str | None:
    """
    :return: the path to access the sql file for the matching table: 
    the dml script filename has to include the table name
    """
    logging.debug(f"Search {table_name} in inventory of files")
    table_name=table_name.split(".sql")[0]    # legacy table name
    list_potential=[]
    for apath in inventory:
        if table_name in apath:
            list_potential.append(apath)
    logging.debug(f"Found potential results: {list_potential}")
    if len(list_potential) == 0:
        if table_name.startswith("src"):
            table_name=table_name.replace("src_","",1)
            for apath in inventory:
                if table_name in apath and "source" in apath:
                    return apath
            return None
        return None
    # BIG HACK
    for sql in list_potential:
        if not "/mx/" in sql:
            return sql
    return list_potential[0]
    

def get_dependencies(dbt_script_content: str) -> list[str]:
    """
    For a given table and dbt script content, get the dependent tables using the dbt { ref: } or FROM or JOINS
    """
    parser = SQLparser()
    dependencies = set()
    dependency_names = parser.extract_table_references(dbt_script_content)

    if (len(dependency_names) > 0):
        logging.debug("Dependencies found:")
        for dependency in dependency_names:
            logging.debug(f"- depends on : {dependency}")
            dependencies.add(dependency)
    else:
        logging.debug("  - No dependency")
    return dependencies

def list_parents_from_sql_content(file_or_folder: str):
    """
    List the dependencies for the given file, or all the dependencies of all tables in the given folder
    """
    if file_or_folder.endswith(".sql"):
        with open(file_or_folder) as f:
            sql_content= f.read()
            l=get_dependencies(sql_content)
            return l
    else:
        # loop over the files in the folder
        for file in list_sql_files(file_or_folder):
            list_parents_from_sql_content(file)



def generic_search_in_processed_tables(table_name: str, root_folder: str) -> bool:
    """
    As a first heuristic, it assumes that the table_name will be a folder name in the tree from the root folder,
    but we cannot be 100% sure so 
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
    pipeline_path=os.getenv("PIPELINES","../pipelines")
    if not generic_search_in_processed_tables(table_name, pipeline_path):
        staging_path=os.getenv("STAGING","../staging")
        return generic_search_in_processed_tables(table_name, staging_path)
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

def build_table_reference(table_name: str, dml_file_name: str, ddl_file_name: str, table_folder: str = None) -> FlinkTableReference:
    if not table_folder:
        directory = os.path.dirname(dml_file_name)
        table_folder=os.path.dirname(directory)
    return FlinkTableReference.model_validate(
                        {"table_name": table_name,
                         "dml_ref": dml_file_name,
                         "ddl_ref": ddl_file_name,
                         "table_folder_name": table_folder})

def get_parent_table_references_from_sql_content(sql_file_name: str, all_files) -> Set[FlinkTableReference]:
    """
    From the given sql file name, use sql parser to get the tables used by this sql
    content, and return the list of table names
    """
    try:
        if sql_file_name.startswith("pipelines"):
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
                    dependencies.add(build_table_reference(dependency, dml_file_name, ddl_file_name))
            return table_name, dependencies
    except Exception as e:
        logging.error(e)
        return "",  set()
    
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

def read_pipeline_metadata(file_name: str) -> FlinkStatementHierarchy:
    file_name=from_pipeline_to_absolute(file_name)
    with open(file_name, "r") as f:
        content = FlinkStatementHierarchy.model_validate_json(f.read())
        return content
    
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

def _modify_children(current: FlinkStatementHierarchy, parent_ref: FlinkTableReference):
    """
    Verify the current is in the children of the parent.
    It may happend that parent was already processed, but the current is referencing it another time,
    in this case we need to add to the children of the parent
    """
    child= build_table_reference(current.table_name, current.dml_ref, current.ddl_ref)
    pipe_definition_fn=parent_ref.table_folder_name+"/"+PIPELINE_JSON_FILE_NAME
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
                child = build_table_reference(current_node.table_name, 
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

def _process_next_node(nodes_to_process, processed_nodes,  all_files):
    if len(nodes_to_process) > 0:
        current_node = nodes_to_process.pop()
        logging.debug(f"\n\n\t... processing the node {current_node}")
        nodes_to_process = _add_parents_for_future_process(current_node, nodes_to_process, processed_nodes, all_files)
        _create_or_merge(current_node)
        processed_nodes[current_node.table_name]=current_node
        _process_next_node(nodes_to_process, processed_nodes, all_files)

def build_pipeline_definition_from_table(dml_file_name: str, children: List[str], all_files):
    """
    1/  From the DML file name, build the hierarchy from sink as the root of the tree, and sources as the leaf
    2/ at each level of the tree is info_node with data to be able to run a statement for the current node
    3/ write at each level the list of parents and children and meta-data. Keep only the name
    At this stage dml_file name as the absolute path
    """
    dml_file_name = from_absolute_to_pipeline(dml_file_name)
    table_name, parent_references = get_parent_table_references_from_sql_content(dml_file_name, all_files)
    current_node= _create_node(dml_file_name, table_name, parent_references, children)
    node_to_process.append(current_node)
    _process_next_node(node_to_process, dict(), all_files)
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
                logging.error("ERROR {pname} not a json file")
                return None
        else:
            return None
    except Exception as e:
        return None
    
def process_files_from_queue(files_to_process, all_files, dependency_list):
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
   
        current_parents=list_parents_from_sql_content(fn)
        if current_parents:
            current_dependencies=set(current_parents)
            for dep in current_dependencies:
                matching_sql_filename=search_table_in_inventory(dep, all_files)
                if matching_sql_filename:
                    if not (dep,matching_sql_filename) in dependency_list:
                        dependency_list.add((dep, matching_sql_filename))
                        files_to_process.append(matching_sql_filename)
                else:
                    dependency_list.add((dep,None))
        return process_files_from_queue(files_to_process, all_files, dependency_list)
    else:
        return dependency_list
    
if __name__ == "__main__":
    """
    Build the inventory of flink sql files, then search the given table name or
    the tables referenced in the sql file specified in parameter
    """
    args = parser.parse_args()
    if not args.inventory:
        print("ERROR you need an inventory folder name")
        exit(1)

    if args.file_name:
        files_to_process.append(args.file_name)
        all_files= get_or_build_inventory(args.inventory, args.inventory, False)
        dependency_list = set()
        dependencies=process_files_from_queue(files_to_process, all_files, dependency_list)
        output=generate_tracking_output(args.file_name, dependencies)
    
        if args.save_file_name:
            with open(args.save_file_name, "w") as f:
                f.write(output)
                print(f"\n Save result to {args.save_file_name}")
        print(output)

        