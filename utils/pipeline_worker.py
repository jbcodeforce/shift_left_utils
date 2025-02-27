"""
A tool to read a pipeline dependencies and process each ddl and dml 
deploment in the expected order using the Flink Gateway REST API.
The tool also includes function to create pipeline metadata file
"""
import os, argparse
from pathlib import Path
import json
from sql_parser import SQLparser
from kafka.app_config import get_config
from pipeline_helper import FlinkStatementHierarchy, FlinkTableReference,  read_pipeline_metadata, assess_pipeline_definition_exists, PIPELINE_JSON_FILE_NAME, build_pipeline_definition_from_table, build_all_file_inventory
import logging

logging.basicConfig(filename='pipelines.log',  filemode='w', level=get_config()["app"]["logging"], 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file_name', required=False, help="File name for the pipeline metadata in json format. This will give the parents and children tree")
parser.add_argument('-d', '--delete_file_from', required=False, help="Delete all the metadata files from the root folder given as argument")
parser.add_argument('-t', '--table_name', required=False, help="Table name to build a pipeline or to process a pipeline on.")
parser.add_argument('-i', '--inventory', required=False, help="name of the folder used to get all the context for the search, the inventory of table")
parser.add_argument('-report', action=argparse.BooleanOptionalAction, default= False, help="Run a static report for given table")





def _get_matching_node_pipeline_info(access_info: FlinkTableReference) -> FlinkStatementHierarchy:
    if access_info.table_folder_name:
        return read_pipeline_metadata(access_info.table_folder_name+ "/" + PIPELINE_JSON_FILE_NAME)

def _visit_parents(current_hierarchy: FlinkStatementHierarchy) -> dict:
    parents = []
    print(f"->> {current_hierarchy.table_name}")
    for parent in current_hierarchy.parents:
        parent_info = _get_matching_node_pipeline_info(parent)
        rep=_visit_parents(parent_info)
        parents.append(rep)
    return {"table_name": current_hierarchy.table_name, "path": current_hierarchy.path, "parents": parents }


def _visit_children(current_hierarchy: FlinkStatementHierarchy) -> dict:
    children = []
    for child in current_hierarchy.children:
        children.append(_visit_children(_get_matching_node_pipeline_info(child)))
    return {"table_name": current_hierarchy.table_name, "path": current_hierarchy.path , "children": children}

def get_path_to_pipeline_file(file_name: str) -> str:
    directory = os.path.dirname(file_name)
    if "sql-scripts" in directory:
        pname = f"{directory}/../{PIPELINE_JSON_FILE_NAME}"
    else:
        pname = f"{directory}/{PIPELINE_JSON_FILE_NAME}"
    return pname 

def walk_the_hierarchy_for_report(pipeline_definition_fname: str) -> dict:
    current_hierarchy= _read_pipeline_metadata(pipeline_definition_fname)
    parents = _visit_parents(current_hierarchy)["parents"]
    children = _visit_children(current_hierarchy)["children"]
    return {"table_name" : current_hierarchy.table_name, 
            "path": current_hierarchy.path,
            "parents": parents, 
            "children": children}

def delete_metada_file(root_folder: str, file_to_delete: str):
    """
    Delete all the files with the given name in the given root folder tree
    """
    logging.info(f"Delete {file_to_delete} from folder: {root_folder}")
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            if file_to_delete == file:
                file_path=os.path.join(root, file)
                os.remove(file_path)
                logging.info(f"File '{file_path}' deleted successfully.")

def debug():
    all_files=build_all_file_inventory("../../data-platform-flink/staging/../pipelines")
    hierarchy=build_pipeline_definition_from_table("../../data-platform-flink/staging/../pipelines/dimensions/aqem/dim_tag/sql-scripts/dml.aqem_dim_tag.sql", [], all_files)
    print(hierarchy.model_dump_json(indent=3))

def run():
    """
    """
    args = parser.parse_args()
    if args.delete_file_from:
        print(f"Delete {PIPELINE_JSON_FILE_NAME}")
        delete_metada_file(args.delete_file_from, PIPELINE_JSON_FILE_NAME)
        print("\n\tDone!")
        exit()

    if not args.file_name:
        print("\nERROR -f file_name is mandatory")
        exit()

    metadata_file_name=assess_pipeline_definition_exists(args.file_name)

    if metadata_file_name:
        logging.info(f"Found {PIPELINE_JSON_FILE_NAME}")
        if args.report:
            print(json.dumps(walk_the_hierarchy_for_report(metadata_file_name), indent=3))
    else:
        print(f"{PIPELINE_JSON_FILE_NAME} not found")
        all_files=build_all_file_inventory(args.inventory)
        hierarchy=build_pipeline_definition_from_table(args.file_name, [], all_files)
        metadata_file_name=get_path_to_pipeline_file(args.file_name)
        print(hierarchy.model_dump_json(indent=3))
        with open( metadata_file_name, "w") as f:
            f.write(hierarchy.model_dump_json())
        print(hierarchy)

if __name__ == "__main__":
    #run()
    debug()

 
   
    
