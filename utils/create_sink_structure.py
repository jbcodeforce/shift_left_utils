"""
Set of function and main to create the sink table folder structure and makefile

For a given table name create under pipelines
"""

import argparse
import os
from jinja2 import Environment, FileSystemLoader
from pathlib import Path

TMPL_FOLDER="./templates"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate flink project for a given dbt starting code - different options'
)
parser.add_argument('-o', '--pipeline_folder_path', required=True, help="name of the folder output of the pipelines")
parser.add_argument('-t', '--table_name', required=True, help="name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file.")
parser.add_argument('-m', action=argparse.BooleanOptionalAction, default= False, required=False, help="Flag to generate a makefile")

def create_folder_if_not_exist(new_path):
    if not os.path.exists(new_path):
        os.makedirs(new_path)

def extract_table_name(src_file_name: str) -> str:
    """
    Extract the name of the table given a src file name
    """
    the_path= Path(src_file_name)
    table_name = the_path.stem
    print(f"Process the table: {table_name}")
    return table_name

def create_folder_structure(src_file_name: str, sql_folder_name:str, target_path: str):
    """
    create a folder structure like for example:
    target_path/table_name/sql-scripts/ddl. and dml.
    target_path/table_name/tests
    target_path/table_name/Makefile
    """
    table_name = extract_table_name(src_file_name)  
    table_folder, table_name = create_sink_table_structure(target_path, sql_folder_name, table_name)
    create_tracking_doc(table_name,src_file_name,table_folder)
    return table_folder, table_name


def create_sink_table_structure(target_path: str, sql_folder_name:str, table_name: str):
    table_folder=f"{target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/{sql_folder_name}")
    create_folder_if_not_exist(f"{table_folder}/tests")
    create_makefile(table_name, sql_folder_name, sql_folder_name, table_folder)
    create_tracking_doc(table_name,"",table_folder)
    return table_folder, table_name

def create_makefile(table_name: str, ddl_folder: str, dml_folder: str, out_dir: str):
    """
    Create a makefile to help deploy Flink statements for the given table name
    When the dml folder is called dedup the ddl should include a table name that is `_raw` suffix.
    """
    env = Environment(loader=FileSystemLoader('.'))
    makefile_template = env.get_template(f"{TMPL_FOLDER}/makefile_ddl_dml_tmpl.jinja")
    
    context = {
        'table_name': table_name,
        'statement_name': table_name.replace("_","-"),
        'ddl_folder': ddl_folder,
        'dml_folder': dml_folder
    }
    rendered_makefile = makefile_template.render(context)
    # Write the rendered Makefile to a file
    with open(out_dir + '/Makefile', 'w') as f:
        f.write(rendered_makefile)

def create_tracking_doc(table_name: str, src_file_name: str,  out_dir: str):
    env = Environment(loader=FileSystemLoader('.'))
    tracking_tmpl = env.get_template(f"{TMPL_FOLDER}/tracking_tmpl.jinja")
    context = {
        'table_name': table_name,
        'src_file_name': src_file_name,
    }
    rendered_tracking_md = tracking_tmpl.render(context)
    with open(out_dir + '/tracking.md', 'w') as f:
        f.write(rendered_tracking_md)

if __name__ == "__main__":
    args = parser.parse_args()
    if args.m:
        existing_path=args.pipeline_folder_path + "/" + args.table_name
        create_makefile( args.table_name, "sql-scripts", "sql-scripts", existing_path)
    else:    
        create_sink_table_structure(args.pipeline_folder_path, "sql-scripts", args.table_name)
    print("\n\nDone !")
