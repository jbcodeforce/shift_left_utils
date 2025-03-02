"""
Set of functions and a main() to create the table folder structure and makefile for a sink, an intermediate or a source table.
"""

import argparse
import os
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from kafka.app_config import get_config
import logging
from sql_parser import SQLparser
import json
from pydantic import BaseModel
from typing import Optional, Final, Tuple

logging.basicConfig(filename='logs/pipelines.log',  filemode='a', level=get_config()["app"]["logging"], 
                    format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')



TMPL_FOLDER="./templates"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate flink project for a given dbt starting code - different options'
)

parser.add_argument('-o', '--pipeline_folder_path', required=False, help="name of the folder output of the pipelines")
parser.add_argument('-t', '--table_name', required=False, help="name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file.")
parser.add_argument('--build-makefile', action=argparse.BooleanOptionalAction, default= False, required=False, help="Flag to generate a makefile only")
parser.add_argument('--update-makefiles', action=argparse.BooleanOptionalAction, default= False, required=False, help="Flag to recursively modify the makefile from the -o folder")
parser.add_argument('--build-inventory', action=argparse.BooleanOptionalAction, default= False, required=False, help="Flag to recursively modify the makefile from the -o folder")
parser.add_argument('--root-folder', required=False, help="Folder to search the Flink SQL statements")
parser.add_argument('-if', '--inventory-folder', required=False, help="Folder to persist repository information.")


def main(arguments):
    """
    Process the creation of the folder structure given the table name as part of the program arguments.

    **Arguments:**

    * `-t or --table_name` name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file
    * `-o or --pipeline_folder_path`, required=True, help="name of the folder output of the pipelines"
    * `-m `: Flag to generate a makefile only
    * --build-inventory
    """
    if arguments.build_inventory:
        if args.root_folder and args.inventory_folder:
            print("-"*20 + "\nPrepare an image of current pipeline content for inventory")
            get_or_build_inventory(args.root_folder,args.inventory_folder,True)
            print("Done !")
            exit(0)
        else:
            print("ERROR for building inventory, specify the folder where the Flink sql statements are")
            print("   and the folder name for where to persist the inventory. It is used by other tools.")
            exit(1)

    if arguments.build_makefile:
        if args.pipeline_folder_path and arguments.table_name:
            existing_path=args.pipeline_folder_path
            product_name=extract_product_name(existing_path)
            create_makefile( arguments.table_name, "sql-scripts", "sql-scripts", existing_path, get_config()["kafka"]["cluster_type"], product_name)
            exit(0)
        else:
            print("\nERROR: you need to specify the pipeline_folder_path and table_name when redoing a makefile")
            exit(1)
    elif arguments.update_makefiles:
        if args.pipeline_folder_path:
            _change_all_makefiles_from(args.pipeline_folder_path, get_config())
            print("Done processing the makefiles")
            exit(1)
        else:
            print("\nERROR: you need to specify the pipeline_folder_path and updating all the makefiles for a given folder tree")
            exit(1)
    elif arguments.pipeline_folder_path:
        if arguments.table_name:
            fn=os.path.join(arguments.pipeline_folder_path,arguments.table_name)
            create_folder_structure_for_table(fn, "sql-scripts", arguments.pipeline_folder_path, get_config())
            print("\n\nDone !")
            exit(0)
        else:
            print("\nERROR: you need to specify the table_name to create a folder structure")
            exit(1)

class FlinkTableReference(BaseModel):
    table_name: Final[str] 
    dml_ref: Optional[str]
    ddl_ref: Optional[str]
    table_folder_name: str
    def __hash__(self):
        return hash((self.table_name))
    def __eq__(self, other):
        if not isinstance(other, FlinkTableReference):
            return NotImplemented
        return self.table_name == other.table_name
    
def from_absolute_to_pipeline(file_or_folder_name) -> str:
    if not file_or_folder_name.startswith("pipeline"):
        index = file_or_folder_name.find("pipelines") 
        new_path = file_or_folder_name[index:] if index != -1 else file_or_folder_name
        return new_path
    else:
        return file_or_folder_name

def from_pipeline_to_absolute(file_or_folder_name) -> str:
    if file_or_folder_name.startswith("pipeline"):
        root = os.path.dirname(os.getenv("PIPELINES"))
        new_path = os.path.join(root,file_or_folder_name)
        return new_path
    else:
        return file_or_folder_name
    
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
    """
    Given an existing folder path, get the name of the folder below one of the structural folder as it is the name of the data product.
    """
    parent_folder=os.path.dirname(existing_path).split("/")[-1]
    if parent_folder not in ["facts", "intermediates", "sources", "dimensions"]:
        return parent_folder
    else:
        return None
    
def create_folder_structure_for_table(src_file_name: str, sql_folder_name:str, target_path: str, config):
    """
    Create the folder structure for the given table name, under the target path. The structure looks like:
    
    * `target_path/table_name/sql-scripts/`:  with two template files, one for ddl. and one for dml.
    * `target_path/table_name/tests`: for test harness content
    * `target_path/table_name/Makefile`: a makefile to do Flink SQL statement life cycle management
    """
    logging.info(f"Create folder {src_file_name}")
    table_name = extract_table_name(src_file_name)  
    table_folder=f"{target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/{sql_folder_name}")
    create_folder_if_not_exist(f"{table_folder}/tests")
    if "source" in target_path:
        internal_table_name=config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    else:
        internal_table_name=table_name
    product_name = extract_product_name(table_folder)
    create_makefile(internal_table_name, sql_folder_name, sql_folder_name, table_folder, config["kafka"]["cluster_type"], product_name)
    _create_tracking_doc(internal_table_name, "", table_folder)
    _create_ddl_skeleton(internal_table_name, table_folder)
    _create_dml_skeleton(internal_table_name, table_folder)
    logging.info(f"Create folder {table_folder} for the table {table_name}")
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

def _read_table_name_from_ddl(table_folder: str) -> str:
    sql_dir=table_folder + "/sql-scripts"
    for file in os.listdir(sql_dir):
        if file.startswith("dml"):
            parser = SQLparser()
            with open(sql_dir + "/" + file, "r") as f:
                sql_content= f.read()
                table_name=parser.extract_table_name_from_insert_into_statement(sql_content)
                return table_name
    logging.warning(f"table name not found in dml file in {table_folder}")
    return extract_table_name(table_folder)


def _change_all_makefiles_from(root_folder: str, config):
    """
    For each folder under this folder_root, each table folder, rebuild the makefile
    """
    for root, dirs, files in os.walk(root_folder):
        for file in files:
            file_path=os.path.join(root, file)
            if "Makefile" == file:
                table_folder = os.path.dirname(file_path)
                product_name = extract_product_name(table_folder)
                internal_table_name = _read_table_name_from_ddl(table_folder)
                sql_folder_name="sql-scripts"
                print(f"create_makefile({internal_table_name}, {sql_folder_name}, {sql_folder_name}, {table_folder}, {config["kafka"]["cluster_type"]}, {product_name})")
                create_makefile(internal_table_name, sql_folder_name, sql_folder_name, table_folder, config["kafka"]["cluster_type"], product_name)

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


def get_ddl_dml_from_folder(root, dir) -> Tuple[str, str]:
    ddl_file_name = None
    dml_file_name = None
    base_scripts=os.path.join(root,dir)
    for file in os.listdir(base_scripts):
        if file.startswith("ddl"):
            ddl_file_name=os.path.join(base_scripts,file)
        if file.startswith('dml'):
            dml_file_name=os.path.join(base_scripts,file)
    if ddl_file_name is None:
        logging.error(f"No DDL file found in the directory: {base_scripts}")
    if dml_file_name is None:
        logging.error(f"No DML file found in the directory: {base_scripts}")
    return ddl_file_name, dml_file_name

def remove_folder_prefix(folder_or_file_name: str):
    if folder_or_file_name:
        return folder_or_file_name.replace(os.getenv("PIPELINES"),"pipelines")
    return ""

def get_or_build_inventory(root_folder: str, target_path: str, recreate: bool= False) -> dict:
    """
    From the root folder where all the ddl are saved. e.g. the pipelines folder navigate to get the dml
    open each dml file to get the table name and build and inventory, table_name, FlinkTableReference
    Root_folder should be absolute path
    """
    create_folder_if_not_exist(target_path)
    inventory_path= os.path.join(target_path,"inventory.json")
    inventory={}
    if recreate or not os.path.exists(inventory_path):
        parser = SQLparser()
        for root, dirs, files in os.walk(root_folder):
            for dir in dirs:
                if "sql-scripts" == dir:
                    ddl_file_name, dml_file_name = get_ddl_dml_from_folder(root, dir)
                    logging.debug(f"process file {dml_file_name}")
                    if dml_file_name:
                        with open(dml_file_name, "r") as f:
                            sql_content= f.read()
                            table_name=parser.extract_table_name_from_insert_into_statement(sql_content)
                            directory = os.path.dirname(dml_file_name)
                            table_folder=remove_folder_prefix(os.path.dirname(directory))
                            ref= FlinkTableReference.model_validate(
                                        {"table_name": table_name,
                                        "ddl_ref": remove_folder_prefix(ddl_file_name),
                                        "dml_ref": remove_folder_prefix(dml_file_name),
                                        "table_folder_name": table_folder})
                            logging.debug(ref)
                            inventory[ref.table_name]= ref.model_dump()
        with open(inventory_path, "w") as f:
            json.dump(inventory, f, indent=4)
        logging.info(f"Create inventory file {inventory_path}")
    else:
        with open(inventory_path, "r") as f:
            inventory= json.load(f)
    return inventory
           



if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
    
