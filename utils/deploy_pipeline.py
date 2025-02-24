"""
A tool to read a pipeline dependencies and process each ddl and dml 
deploment in the expected order using the Flink Gateway REST API.
The tool also includes function to create pipeline metadata file
"""
import os, argparse
from pathlib import Path
import json
from sql_parser import SQLparser
from pipeline_helper import FlinkStatementHierarchy, assess_pipeline_definition_exists, PIPELINE_JSON_FILE_NAME,build_pipeline_definition_from_table, build_all_file_inventory



parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Get the hierarchy of tables from sink to source - different options'
)

parser.add_argument('-f', '--file_name', required=False, help="File name for the pipeline metadata in json format.")
parser.add_argument('-t', '--table_name', required=False, help="Table name to build a pipeline or to process a pipeline on.")
parser.add_argument('-i', '--inventory', required=False, help="name of the folder used to get all the context for the search, the inventory of table")



def read_pipeline_metadata(file_name: str) -> FlinkStatementHierarchy:
    pass



def get_path_to_pipeline_file(file_name: str) -> str:
    directory = os.path.dirname(file_name)
    if "sql-scripts" in directory:
        pname = f"{directory}/../{PIPELINE_JSON_FILE_NAME}"
    else:
        pname = f"{directory}/{PIPELINE_JSON_FILE_NAME}"
    return pname 


def test_debug():
    all_files=build_all_file_inventory("../examples")
    table_name, hierarchy=build_pipeline_definition_from_table("../examples/facts/fct_order/sql-scripts/dml.fct_order.sql", None, all_files)
    print(hierarchy.model_dump_json(indent=3))

def run():
    """
    """
    args = parser.parse_args()
    metadata_file_name=assess_pipeline_definition_exists(args.file_name)
    if metadata_file_name:
        print(f"Found {PIPELINE_JSON_FILE_NAME}")
    else:
        print(f"{PIPELINE_JSON_FILE_NAME} not found")
        all_files=build_all_file_inventory( args.inventory)
        table_name, hierarchy=build_pipeline_definition_from_table(args.file_name, None, all_files)
        metadata_file_name=get_path_to_pipeline_file(args.file_name)
        print(hierarchy.model_dump_json(indent=3))
        with open( metadata_file_name, "w") as f:
            f.write(hierarchy.model_dump_json())
        print(hierarchy)

if __name__ == "__main__":
    #run()
    test_debug()
 
   
    
