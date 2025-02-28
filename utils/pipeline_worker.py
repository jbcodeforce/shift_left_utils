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
from create_table_folder_structure import get_or_build_inventory_from_ddl
from pipeline_helper import FlinkStatementHierarchy, FlinkTableReference,  read_pipeline_metadata, assess_pipeline_definition_exists, PIPELINE_JSON_FILE_NAME, build_pipeline_definition_from_table
import logging

logging.basicConfig(filename='logs/pipelines.log',  filemode='w', level=get_config()["app"]["logging"], 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file-name', required=False, help="File name for the pipeline metadata in json format. This will give the parents and children tree")
parser.add_argument('-d', '--delete-file-from', required=False, help="Delete all the metadata files from the root folder given as argument")
parser.add_argument('-t', '--table-name', required=False, help="Table name to build a pipeline or to process a pipeline on.")
parser.add_argument('-i', '--inventory', required=False, help="name of the folder used to get all the context for the search, the inventory of table")
parser.add_argument('--report', action=argparse.BooleanOptionalAction, default= False, help="Run a static report for given table")
parser.add_argument('--build', action=argparse.BooleanOptionalAction, default= False, help="Build the pipeline metadata files from sink to source")





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
    current_hierarchy= read_pipeline_metadata(pipeline_definition_fname)
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
                print(f"File '{file_path}' deleted successfully.")


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
        print("\nERROR -f DML sql file_name is mandatory")
        exit()
    
    if args.report:
        metadata_file_name=assess_pipeline_definition_exists(args.file_name)
        print(json.dumps(walk_the_hierarchy_for_report(metadata_file_name), indent=3))
        exit()

    if args.build:
        if not args.inventory:
            print("\nERROR need to provide the path where the inventory is saved via: -i <folder_for_file_inventory>")
            print("\te.g.: python pipeline_worker.py -f $PIPELINES/dimensions/p1/dim_name/sql-scripts/dml.p1_dim_name.sql --build -i $PIPELINES")
            exit()
        inventory=get_or_build_inventory_from_ddl(args.inventory, args.inventory, False)
        hierarchy=build_pipeline_definition_from_table(args.file_name, [], inventory)
        print(hierarchy.model_dump_json(indent=3))
        print("Done !")


def _debug():
    pipeline_folder= os.getenv("PIPELINES")
    all_files=get_or_build_inventory_from_ddl(root_folder="", target_path=pipeline_folder)
    hierarchy=build_pipeline_definition_from_table(pipeline_folder + "/dimensions/aqem/dim_tag/sql-scripts/dml.aqem_dim_tag.sql", [], all_files)
    print(hierarchy.model_dump_json(indent=3))

if __name__ == "__main__":
    run()
    #_debug()

 
   
    
