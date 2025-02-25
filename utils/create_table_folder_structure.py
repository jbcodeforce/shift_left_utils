"""
Set of functions and a main() to create the table folder structure and makefile for a sink, an intermediate or a source table.
"""

import argparse
import os
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from kafka.app_config import get_config
import logging

logging.basicConfig(level=get_config()["app"]["logging"], format='%(levelname)s: %(message)s')
TMPL_FOLDER="./templates"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate flink project for a given dbt starting code - different options'
)
parser.add_argument('-o', '--pipeline_folder_path', required=True, help="name of the folder output of the pipelines")
parser.add_argument('-t', '--table_name', required=True, help="name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file.")
parser.add_argument('-m', action=argparse.BooleanOptionalAction, default= False, required=False, help="Flag to generate a makefile only")

def create_folder_if_not_exist(new_path):
    if not os.path.exists(new_path):
        os.makedirs(new_path)

def extract_table_name(src_file_name: str) -> str:
    """
    Extract the name of the table given a src file name
    """
    the_path= Path(src_file_name)
    table_name = the_path.stem
    if table_name.startswith("src_"):
        table_name = table_name.replace("src_","",1)
    return table_name

def extract_product_name(existing_path: str) -> str:
    parent_folder=os.path.dirname(existing_path).split("/")[-1]
    if parent_folder not in ["facts", "intermediates", "sources"]:
        return parent_folder
    else:
        return None
    
def create_folder_structure_for_table(src_file_name: str, sql_folder_name:str, target_path: str, config):
    """
    Create the folder structure for the given table name, under the target path. The structure looks like:
    
    * `target_path/table_name/sql-scripts/`:  with two template file, one for ddl. and one for dml.
    * `target_path/table_name/tests`: for test harness content
    * `target_path/table_name/Makefile`: a makefile to do Flink SQL statement life cycle management
    """
    table_name = extract_table_name(src_file_name)  
    table_folder=f"{target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/{sql_folder_name}")
    create_folder_if_not_exist(f"{table_folder}/tests")
    if "source" in target_path:
        internal_table_name=config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    product_name = extract_product_name(table_folder)
    create_makefile(internal_table_name, sql_folder_name, sql_folder_name, table_folder, config["kafka"]["cluster_type"], product_name)
    _create_tracking_doc(internal_table_name, "", table_folder)
    _create_ddl_skeleton(internal_table_name, table_folder)
    _create_dml_skeleton(internal_table_name, table_folder)
    return table_folder, table_name

def create_makefile(table_name: str, ddl_folder: str, dml_folder: str, out_dir: str, cluster_type: str, product_name: str):
    """
    Create a makefile to help deploy Flink statements for the given table name
    When the dml folder is called dedup the ddl should include a table name that is `_raw` suffix.
    """
    logging.debug(f"Create makefile for {table_name} in {out_dir}")
    env = Environment(loader=FileSystemLoader('.'))
    makefile_template = env.get_template(f"{TMPL_FOLDER}/makefile_ddl_dml_tmpl.jinja")
    if product_name:
        prefix=cluster_type + "-" + product_name
    else:
        prefix=cluster_type
    context = {
        'table_name': table_name,
        'ddl_statement_name': prefix+ "-ddl-" + table_name.replace("_","-"),
        'dml_statement_name': prefix + "-dml-" + table_name.replace("_","-"),
        'ddl_folder': ddl_folder,
        'dml_folder': dml_folder
    }
    rendered_makefile = makefile_template.render(context)
    # Write the rendered Makefile to a file
    with open(out_dir + '/Makefile', 'w') as f:
        f.write(rendered_makefile)

def _create_tracking_doc(table_name: str, src_file_name: str,  out_dir: str):
    env = Environment(loader=FileSystemLoader('.'))
    tracking_tmpl = env.get_template(f"{TMPL_FOLDER}/tracking_tmpl.jinja")
    context = {
        'table_name': table_name,
        'src_file_name': src_file_name,
    }
    rendered_tracking_md = tracking_tmpl.render(context)
    with open(out_dir + '/tracking.md', 'w') as f:
        f.write(rendered_tracking_md)

def _create_ddl_skeleton(table_name: str,out_dir: str):
    env = Environment(loader=FileSystemLoader('.'))
    ddl_tmpl = env.get_template(f"{TMPL_FOLDER}/create_table_squeleton.jinja")
    context = {
        'table_name': table_name,
        'default_PK': 'default_key',
        'column_definitions': '-- put here column definitions'
    }
    rendered_ddl = ddl_tmpl.render(context)
    with open(out_dir + '/sql-scripts/ddl.' + table_name + ".sql", 'w') as f:
        f.write(rendered_ddl)

def _create_dml_skeleton(table_name: str,out_dir: str):
    env = Environment(loader=FileSystemLoader('.'))
    dml_tmpl = env.get_template(f"{TMPL_FOLDER}/dml_src_tmpl.jinja")
    context = {
        'table_name': table_name,
        'sql_part': '-- part to select stuff',
        'where_part': '-- where condition or remove it',
        'src_table': 'src_table'
    }
    rendered_dml = dml_tmpl.render(context)
    with open(out_dir + '/sql-scripts/dml.' + table_name + ".sql", 'w') as f:
        f.write(rendered_dml)

def main(arguments):
    """
    Process the creation of the folder structure given the table name as part of the program arguments.

    **Arguments:**

    * `-t or --table_name` name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file
    * `-o or --pipeline_folder_path`, required=True, help="name of the folder output of the pipelines"
    * `-m `: Flag to generate a makefile only
    """
    if arguments.m:
        if args.pipeline_folder_path and arguments.table_name:
            existing_path=args.pipeline_folder_path
            product_name=extract_product_name(existing_path)
            create_makefile( arguments.table_name, "sql-scripts", "sql-scripts", existing_path, get_config()["kafka"]["cluster_type"], product_name)
        else:
            print("\nERROR: you need to specify the pipeline_folder_path and table_name when redoing a makefile")
            exit()
    else:    
        create_folder_structure_for_table(args.pipeline_folder_path, "sql-scripts", arguments.table_name, get_config())
    print("\n\nDone !")

if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
    
