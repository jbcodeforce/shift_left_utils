
import logging
import os
from pathlib import Path
from shift_left.core.project_manager import create_folder_if_not_exist
from shift_left.core.app_config import get_config
from jinja2 import Environment, PackageLoader

"""
Table management is for managing table folder content
"""

SCRIPTS_FOLDER="sql-scripts"

# --------- Public APIs ---------------
def build_folder_structure_for_table(table_name: str, target_path: str):
    """
    Create the folder structure for the given table name, under the target path. The structure looks like:
    
    * `target_path/table_name/sql-scripts/`:  with two template files, one for ddl. and one for dml.
    * `target_path/table_name/tests`: for test harness content
    * `target_path/table_name/Makefile`: a makefile to do Flink SQL statement life cycle management
    """
    logging.info(f"Create folder {table_name}")
    config = get_config()
    table_name = _extract_table_name(table_name)  
    table_folder = f"{target_path}/{table_name}"
    create_folder_if_not_exist(f"{table_folder}/"+ SCRIPTS_FOLDER)
    create_folder_if_not_exist(f"{table_folder}/tests")
    if "source" in target_path:
        internal_table_name = config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    else:
        internal_table_name=table_name
    product_name = _extract_product_name(table_folder)
    _create_makefile(internal_table_name, SCRIPTS_FOLDER, SCRIPTS_FOLDER, table_folder, config["kafka"]["cluster_type"], product_name)
    _create_tracking_doc(internal_table_name, "", table_folder)
    _create_ddl_skeleton(internal_table_name, table_folder)
    _create_dml_skeleton(internal_table_name, table_folder)
    logging.info(f"Created folder {table_folder} for the table {table_name}")
    return table_folder, table_name


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
    ddl_tmpl = env.get_template(f"create_table_squeleton.jinja")
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


def _extract_table_name(src_file_name: str) -> str:
    """
    Extract the name of the table given a src file name
    """
    the_path= Path(src_file_name)
    table_name = the_path.stem
    if table_name.startswith("src_"):
        table_name = table_name.replace("src_","",1)
    return table_name

def _extract_product_name(existing_path: str) -> str:
    """
    Given an existing folder path, get the name of the folder below one of the structural folder as it is the name of the data product.
    """
    parent_folder=os.path.dirname(existing_path).split("/")[-1]
    if parent_folder not in ["facts", "intermediates", "sources", "dimensions"]:
        return parent_folder
    else:
        return None
    
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
    if product_name:
        prefix=cluster_type + "-" + product_name
    else:
        prefix=cluster_type
    context = {
        'table_name': table_name,
        'ddl_statement_name': prefix+ "-ddl-" + table_name.replace("_","-"),
        'dml_statement_name': prefix + "-dml-" + table_name.replace("_","-"),
        'drop_statement_name': table_name.replace("_","-"),
        'ddl_folder': ddl_folder,
        'dml_folder': dml_folder
    }
    rendered_makefile = makefile_template.render(context)
    # Write the rendered Makefile to a file
    with open(out_dir + '/Makefile', 'w') as f:
        f.write(rendered_makefile)