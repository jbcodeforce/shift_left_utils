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
from pipeline_helper import FlinkStatementHierarchy, FlinkTableReference,  search_table_in_inventory, assess_pipeline_definition_exists, PIPELINE_JSON_FILE_NAME, build_pipeline_definition_from_table, build_all_file_inventory
import logging

logging.basicConfig(filename='pipelines.log',  filemode='w', level=get_config()["app"]["logging"], 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file_name', required=False, help="File name for the pipeline metadata in json format. This will give the parents and children tree")
parser.add_argument('-d', '--delete_file_from', required=False, help="Delete all the file from the root folder given as argument")
parser.add_argument('-t', '--table_name', required=False, help="Table name to build a pipeline or to process a pipeline on.")
parser.add_argument('-i', '--inventory', required=False, help="name of the folder used to get all the context for the search, the inventory of table")
parser.add_argument('-r', action=argparse.BooleanOptionalAction, default= False, help="Run a static report for given table")



def _read_pipeline_metadata(file_name: str) -> FlinkStatementHierarchy:
    with open(file_name, "r") as f:
        return FlinkStatementHierarchy.model_validate_json(f.read())

def _visit_parents(current_hierarchy, all_files) -> str:
    report = []
    for parent in current_hierarchy.parents:
        report.append({"table_name": parent.table_name, "path": parent.table_folder_name })
    return report

def _visit_children(current_hierarchy, all_files) -> str:
    report = []
    for child in current_hierarchy.children:
        report.append({"table_name": child.table_name, "path": child.table_folder_name })
    return report

def get_path_to_pipeline_file(file_name: str) -> str:
    directory = os.path.dirname(file_name)
    if "sql-scripts" in directory:
        pname = f"{directory}/../{PIPELINE_JSON_FILE_NAME}"
    else:
        pname = f"{directory}/{PIPELINE_JSON_FILE_NAME}"
    return pname 

def _walk_the_hierarchy_for_report(pipeline_def_fname: str, all_files) -> dict:
    current_hierarchy= _read_pipeline_metadata(pipeline_def_fname)
    parents = _visit_parents(current_hierarchy, all_files)
    children = _visit_children(current_hierarchy, all_files)
    return {"table_name" : current_hierarchy.table_name, 
            "path": current_hierarchy.path,
            "parents": parents, 
            "children": children}

def _delete_metada_file(root_folder: str, file_to_delete: str):
    """
    Delete all the files with the given name in the given root folder tree
    """
    for root, dirs, files in os.walk(root_folder):
        print(f"Current folder: {root}")
        for file in files:
            if file_to_delete == file:
                file_path=os.path.join(root, file)
                os.remove(file_path)
                print(f"File '{file_path}' deleted successfully.")

def test_debug():
    all_files=build_all_file_inventory("../examples")
    hierarchy=build_pipeline_definition_from_table("../examples/facts/p1/fct_order/sql-scripts/dml.fct_order.sql", [], all_files)
    print(hierarchy.model_dump_json(indent=3))

def test_debug2():
    all_files=build_all_file_inventory("../../data-platform-flink/staging/../pipelines")
    hierarchy=build_pipeline_definition_from_table("../../data-platform-flink/staging/../pipelines/dimensions/aqem/dim_tag/sql-scripts/dml.aqem_dim_tag.sql", [], all_files)
    print(hierarchy.model_dump_json(indent=3))

def run():
    """
    """
    args = parser.parse_args()
    if args.delete_file_from:
        if not args.file_name:
            print("\nERROR: when deleting in a hierarchy you need to specify a filename to delete.")
            print(f"\tExample: --delete_file_from $pipeline_folder --file_name {PIPELINE_JSON_FILE_NAME}")
            exit(1)
        else:
            _delete_metada_file(args.delete_file_from, args.file_name)
            print("\n\tDone!")
            exit()

    if not args.file_name:
        print("\nERROR -f file_name is mandatory")
        exit()

    metadata_file_name=assess_pipeline_definition_exists(args.file_name)
    all_files=build_all_file_inventory(args.inventory)
    if metadata_file_name:
        logging.info(f"Found {PIPELINE_JSON_FILE_NAME}")
        if args.r:
            print(json.dumps(_walk_the_hierarchy_for_report(metadata_file_name, all_files), indent=3))
    else:
        print(f"{PIPELINE_JSON_FILE_NAME} not found")
        hierarchy=build_pipeline_definition_from_table(args.file_name, [], all_files)
        metadata_file_name=get_path_to_pipeline_file(args.file_name)
        print(hierarchy.model_dump_json(indent=3))
        with open( metadata_file_name, "w") as f:
            f.write(hierarchy.model_dump_json())
        print(hierarchy)

if __name__ == "__main__":
    run()
    #test_debug2()
 
   
    
