
import logging
import os
from pathlib import Path
from shift_left.core.project_manager import create_folder_if_not_exist
from shift_left.core.pipeline_mgr import ( 
    read_pipeline_metadata, 
    PIPELINE_JSON_FILE_NAME,
    PIPELINE_FOLDER_NAME)

from shift_left.core.utils.app_config import get_config
from jinja2 import Environment, PackageLoader
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.file_search import (
    FlinkTableReference, 
    get_or_build_source_file_inventory, 
    load_existing_inventory,
    SCRIPTS_DIR, 
    get_table_ref_from_inventory,
    extract_product_name,
    get_ddl_dml_names_from_table,
    build_inventory)
from typing import Set, Dict


"""
Table management is for managing table folder content
"""

# --------- Public APIs ---------------
def build_folder_structure_for_table(table_name: str, 
                                    target_path: str):
    """
    Create the folder structure for the given table name, under the target path. The structure looks like:
    
    * `target_path/table_name/sql-scripts/`:  with two template files, one for ddl. and one for dml.
    * `target_path/table_name/tests`: for test harness content
    * `target_path/table_name/Makefile`: a makefile to do Flink SQL statement life cycle management
    """
    logging.info(f"Create folder {table_name} in {target_path}")
    config = get_config()
    if table_name.startswith("src_"):
        table_name = table_name.replace("src_", "")
    table_folder = f"{target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/" +  SCRIPTS_DIR)
    create_folder_if_not_exist(f"{table_folder}/tests")
    if "source" in target_path:
        internal_table_name = config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    else:
        internal_table_name=table_name
    product_name = extract_product_name(table_folder)
    _create_makefile(internal_table_name, SCRIPTS_DIR, SCRIPTS_DIR, table_folder, config["kafka"]["cluster_type"], product_name)
    _create_tracking_doc(internal_table_name, "", table_folder)
    _create_ddl_skeleton(internal_table_name, table_folder)
    _create_dml_skeleton(internal_table_name, table_folder)
    logging.debug(f"Created folder {table_folder} for the table {table_name}")
    return table_folder, table_name

def search_source_dependencies_for_dbt_table(sql_file_name: str, src_project_folder: str) -> str:
    """
    Search from the source project the dependencies for a given table.
    """
    logging.info(f"Search source dependencies for table {sql_file_name} in {src_project_folder}")
    with open(sql_file_name, "r") as f:
        sql_content = f.read()
        parser = SQLparser()
        table_names = parser.extract_table_references(sql_content)
        if table_names:
            all_src_files = get_or_build_source_file_inventory(src_project_folder)
            dependencies = []
            for table in table_names:
                if not table in all_src_files:
                    logging.error(f"Table {table} not found in the source project")
                    continue
                dependencies.append({"table": table, "src_dbt": all_src_files[table]})
            return dependencies
    return []

def extract_table_name(src_file_name: str) -> str:
    """
    Extract the name of the table given a src file name
    """
    the_path= Path(src_file_name)
    table_name = the_path.stem
    if table_name.startswith("src_"):
        table_name = table_name.replace("src_","",1)
    return table_name

def build_update_makefile(pipeline_folder: str, table_name: str):
    inventory = load_existing_inventory(pipeline_folder)
    if table_name not in inventory:
        logging.error(f"Table {table_name} not found in the pipeline inventory {pipeline_folder}")
        return
    existing_path = inventory[table_name]["table_folder_name"]
    table_folder = pipeline_folder.replace(PIPELINE_FOLDER_NAME,"",1) + "/" +  existing_path
    _create_makefile( table_name, 
                    SCRIPTS_DIR, 
                    SCRIPTS_DIR, 
                    table_folder, 
                    get_config()["kafka"]["cluster_type"], None)

def search_users_of_table(table_name: str, pipeline_folder: str) -> str:
    """
    When pipeline definitions is present for this table, return the list of children
    """
    inventory = load_existing_inventory(pipeline_folder)
    tab_ref: FlinkTableReference = get_table_ref_from_inventory(table_name, inventory)
    if tab_ref is None:
        logging.error(f"Table {table_name} not found in the pipeline inventory {pipeline_folder}")
    else:
        results =  read_pipeline_metadata(tab_ref.table_folder_name+ "/" + PIPELINE_JSON_FILE_NAME).children
        output=f"## `{table_name}` is referenced in {len(results)} Flink SQL statements:\n"
        if len(results) == 0:
            output+="\n\t no table ... yet"
        else:
             for t in results:
                output+=f"\n* `{t.table_name}` with the DML: {t.dml_ref}\n"
        return output

def get_or_create_inventory(pipeline_folder: str):
    """
    Build the table inventory from the PIPELINES path. This is a service API for the CLI, so it is kept here even
    if it delegates to file_search.build_inventory
    """
    return build_inventory(pipeline_folder)

# --------- Private APIs ---------------
def _create_tracking_doc(table_name: str, src_file_name: str,  out_dir: str):
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    tracking_tmpl = env.get_template(f"tracking_tmpl.jinja")
    context = {
        'table_name': table_name,
        'src_file_name': src_file_name,
    }
    rendered_tracking_md = tracking_tmpl.render(context)
    with open(out_dir + '/tracking.md', 'w') as f:
        f.write(rendered_tracking_md)

def _create_ddl_skeleton(table_name: str,out_dir: str):
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    ddl_tmpl = env.get_template(f"create_table_skeleton.jinja")
    context = {
        'table_name': table_name,
        'default_PK': 'default_key',
        'column_definitions': '-- put here column definitions'
    }
    rendered_ddl = ddl_tmpl.render(context)
    with open(out_dir + '/sql-scripts/ddl.' + table_name + ".sql", 'w') as f:
        f.write(rendered_ddl)

def _create_dml_skeleton(table_name: str,out_dir: str):
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    dml_tmpl = env.get_template(f"dml_src_tmpl.jinja")
    context = {
        'table_name': table_name,
        'sql_part': '-- part to select stuff',
        'where_part': '-- where condition or remove it',
        'src_table': 'src_table'
    }
    rendered_dml = dml_tmpl.render(context)
    with open(out_dir + '/sql-scripts/dml.' + table_name + ".sql", 'w') as f:
        f.write(rendered_dml)

    
def _create_makefile(table_name: str, 
                     ddl_folder: str,
                     dml_folder: str, 
                     out_dir: str, 
                     cluster_type: str, 
                     product_name: str):
    """
    Create a makefile to help deploy Flink statements for the given table name
    When the dml folder is called dedup the ddl should include a table name that is `_raw` suffix.
    """
    logging.debug(f"Create makefile for {table_name} in {out_dir}")
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    makefile_template = env.get_template(f"makefile_ddl_dml_tmpl.jinja")

    ddl_name, dml_name = get_ddl_dml_names_from_table(table_name, cluster_type, product_name)
    context = {
        'table_name': table_name,
        'ddl_statement_name': ddl_name, 
        'dml_statement_name': dml_name,
        'drop_statement_name': table_name.replace("_","-"),
        'ddl_folder': ddl_folder,
        'dml_folder': dml_folder
    }
    rendered_makefile = makefile_template.render(context)
    # Write the rendered Makefile to a file
    with open(out_dir + '/Makefile', 'w') as f:
        f.write(rendered_makefile)

def get_column_definitions(table_name: str, config) -> tuple[str,str]:
    return "-- put here column definitions", "-- put here column definitions"