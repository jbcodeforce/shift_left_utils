"""

The processing of dbt sql statements for defining sources adopt a different approach than sink tables.

Processes per application, and generates the sql in the existing pipelines. 
So this script needs to be created after the pipeline folder structure is created from the sink, as 
there is one pipeline per sink table.

"""
import os, argparse, sys

from jinja2 import Environment, FileSystemLoader
from flink_sql_code_agent_lg import translate_to_flink_sqls
from utils.create_table_folder_structure import create_folder_structure, create_folder_if_not_exist
from get_schema_for_src_table import search_matching_topic, get_column_definitions
from pipeline_helper import build_all_file_inventory, search_table_in_inventory, list_sql_files, search_table_in_processed_tables, get_dependencies
from clean_sql import process_ddl_file
from kafka.app_config import get_config


TMPL_FOLDER="./templates"
TABLES_TO_PROCESS="./reports/tables_to_process.txt"
TABLES_DONE="./reports/tables_done.txt"
CREATE_TABLE_TMPL="create_table_squeleton.jinja"
#DML_DEDUP_TMPL="dedup_dml_squeleton.jinja"
DML_DEDUP_TMPL="dedup_dml_squeleton.jinja"
TEST_DEDUL_TMPL="test_dedup_statement.jinja"
INPUT_TOPIC_LIST=os.getenv("STAGING") + "/src_topic_list.txt"

parser = argparse.ArgumentParser(
    prog=os.path.basename(__file__),
    description='Generate flink project for a given dbt starting code - different options'
)
parser.add_argument('-f', '--folder_path', required=False, help="name of the folder including the sink tables, could be a .sql file only")
parser.add_argument('-o', '--pipeline_folder_path', required=True, help="name of the folder output of the pipelines")
parser.add_argument('-t', '--table_name', required=False, help="name of the table to process - as dependencies are derived it is useful to give the table name as parameter and search for the file.")
parser.add_argument('-ld', action=argparse.BooleanOptionalAction, default= False, help="For the given file or folder list for each table their table dependancies")
parser.add_argument('-pd', action=argparse.BooleanOptionalAction, default= False, help="For the given file, process also its dependencies so a full graph is created. NOT SUPPORTED")
# Attention pd is for taking a sink and process up to all sources.

# --- utilities functions


def create_file_if_not_exist(new_file):
    if not os.path.exists(new_file):
        with open(new_file,"w") as f:
           f.write("")


def create_ddl_squeleton(table_name:str, config: dict, target_folder: str):
    """
    Create in the target folder a ddl sql squeleton file for the given table name

    :param table_name: The table name for the ddl sql to create
    :param target_folder: The folder where to create the ddl statement as sql file
    :return: create a file in the target folder
    """
    if config["app"]["default_PK"]:
        pk_to_use= config["app"]["default_PK"]
    else:
        pk_to_use="__pd"
    fname=config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    file_name=f"{target_folder}/ddl.{fname}.sql" 

    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{CREATE_TABLE_TMPL}")
    column_definitions, fields=get_column_definitions(table_name, config)
    context = {
        'table_name': fname,
        'column_definitions': column_definitions,
        'default_PK': pk_to_use
    }
    rendered_sql = sql_template.render(context)
    print(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)
    return fields

def create_dedup_dml_squeleton(table_name:str, target_folder: str, fields: str):
    """
    Create in the target folder a dml sql squeleton file for the given table name

    :param table_name: The table name for the dml sql to create
    :param target_folder: The folder where to create the dml statement as sql file
    :return: create a file in the target folder
    """
    file_name=f"{target_folder}/dml.{table_name}.sql" 
    fname=config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{DML_DEDUP_TMPL}")
    topic_name=search_matching_topic(table_name)
    context = {
        'table_name': fname,
        'topic_name': f"`{topic_name}`",
        'fields': fields
    }
    rendered_sql = sql_template.render(context)
    print(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)

def create_test_dedup(table_name: str, target_folder: str):
    file_name=f"{target_folder}/validate_no_duplicate.sql" 
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{TEST_DEDUL_TMPL}")
    context = {
        'table_name': table_name,
    }
    rendered_sql = sql_template.render(context)
    print(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)
    

def save_one_file(fname: str, content: str):
    if "dml" in fname:
        print(f"DML write: {fname}")
    else:
        print(f"DDL write: {fname}")
    with open(fname,"w") as f:
        f.write(content)

def save_dml_ddl(content_path: str, table_name: str, dml: str, ddl: str):
    """
    creates two files, prefixed by "ddl." and "dml." from the dml and ddl SQL statements
    """
    ddl_fn=f"{content_path}/sql-scripts/ddl.{table_name}.sql"
    save_one_file(ddl_fn,ddl)
    process_ddl_file(f"{content_path}/sql-scripts/",ddl_fn)
    save_one_file(f"{content_path}/sql-scripts/dml.{table_name}.sql",dml)


def remove_already_processed_table(parents: list[str]) -> list[str]:
    """
    If a table in the provided list of parents table is already processed, remove it from the list.
    A processed table is one already define in the pipelines folder hierarchy.
    """
    newParents=[]
    for parent in parents:
        table_name=parent.replace("src_","").strip()
        if not search_table_in_processed_tables(table_name):
            newParents.append(parent)
        else:
            print(f"{table_name} already processed")
    return newParents


# ----- more specific functions
def process_src_sql_file(src_file_name: str, generated_code_folder_name: str, config: dict):
    """
    Transform the source file content to the new Flink SQL format within the source_target_path.
    It creates a tests folder.

    The source file is added to the table to process tracking file

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file

    """
    table_folder, table_name=create_folder_structure(src_file_name,"sql-scripts", generated_code_folder_name)
    fields=create_ddl_squeleton(table_name,config, f"{table_folder}/sql-scripts")
    create_dedup_dml_squeleton(f"{table_name}",f"{table_folder}/sql-scripts", fields)   
    create_test_dedup(f"int_{table_name}_deduped",f"{table_folder}/tests") 
    # merge_items_in_reporting_file([table_name], TABLES_TO_PROCESS)


def process_fact_dim_sql_file(src_file_name: str, source_target_path: str, walk_parent: bool = False, config: dict= {}):
    """
    Transform stage, fact or dimension sql file to Flink SQL. 
    The folder created are <table_name>/sql_scripts and <table_name>/tests + a makefile to facilitate Confluent cloud deployment.

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file
    :param walk_parent: Assess if it needs to process the dependencies
    """
    
    table_folder, table_name=create_folder_structure(src_file_name,"sql-scripts",source_target_path)
    parents=[]
    with open(src_file_name) as f:
        sql_content= f.read()
        parents=get_dependencies(table_name, sql_content)
        parents=remove_already_processed_table(parents)
        merge_items_in_reporting_file(parents,TABLES_TO_PROCESS)
        dml, ddl = translate_to_flink_sqls(table_name, sql_content)
        save_dml_ddl(table_folder, table_name, dml, ddl)
        merge_items_in_reporting_file([table_name],TABLES_DONE)   # add the current table to the processed tables
    if walk_parent:
        for parent_table_name in parents:
            process_from_table_name(parent_table_name, source_target_path, walk_parent, config)


def create_target_folder(target_root_folder: str) -> str:
    """
    For the given target root folder, creates the structure for the 
    sources subfolder 
    """
    sources_path=f"{target_root_folder}"
    create_folder_if_not_exist(sources_path)
    return sources_path


def select_src_sql_file_processing(sql_file_path: str, generated_code_folder_name: str, walk_parent: bool = False, config: dict = {}):
    """
    Routing fct to select the relevant processing for the given SQL source file.
    :param: the sql file to process
    :param: the generated code folder name e.g. $STAGING/ab 
    :param: the flag to process the parent hierarchy or not
    """
    print(f"\t PROCESS file: {sql_file_path}")
    if sql_file_path.find("source") > 0:
        process_src_sql_file(sql_file_path, generated_code_folder_name, config)
    else:
        process_fact_dim_sql_file(sql_file_path, generated_code_folder_name, walk_parent, config)


def process_files_in_folder(args):
    """
    List all sql files within the source folder, for each of them
    prepare the target Flink ddls and makefile
    """
    sql_files = list_sql_files(args.folder_path)
    source_target_path= create_target_folder(args.pipeline_folder_path)
    for sql_file in sql_files:
        select_src_sql_file_processing(sql_file, source_target_path, args.pd)
        print("\n\n----------")


def process_one_file(src_file: str, target_folder: str, process_dependency: bool = False, config: dict = {}):
    """ Process one sql file """
    generated_code_folder_name=create_target_folder(target_folder)
    select_src_sql_file_processing(src_file, generated_code_folder_name, process_dependency, config)


def merge_items_in_reporting_file(dependencies: list[str], persistent_file):
    """
    using the existing persistent_file, save the dependency in the list of dependencies if not present in the file
    The file contains as a first column the name of the dependency
    """
    existing_lines = set()
    with open(persistent_file, 'r') as file:
        for line in file:
            if line.find(",") > 0:
                element=line.split(",")[0]
            else:
                element=line.strip()
            existing_lines.add(element)
    elements_to_add = [elem for elem in dependencies if elem not in existing_lines]

    # Append new unique elements
    if elements_to_add:
        with open(persistent_file, 'a') as file:
            for element in elements_to_add:
                file.write(f"{element}\n")



def process_from_table_name(table_name: str, pipeline_folder_path: str, walk_parent: bool, config: dict):
    """
    Load matching sql file given the table name as input.
    This method may be useful when we get the table name from the dependencies list of another table.

    :param: the table name
    """
    all_files= build_all_file_inventory()

    matching_sql_file=search_table_in_inventory(table_name, all_files)
    if matching_sql_file:
        print(f"\n\n------------------------------------------")
        print(f"\tStart processing the table: {table_name} from the dbt file: {matching_sql_file}")
        process_one_file(matching_sql_file, pipeline_folder_path, walk_parent, config)
    else:
        print("Matching sql file not found !")
    
if __name__ == "__main__":
    args = parser.parse_args()
    config=get_config()
    REPORT_DIR: str=os.getenv("REPORT_FOLDER", config["app"]["report_output_dir"])
    create_folder_if_not_exist(REPORT_DIR)
    TABLES_TO_PROCESS=f"{REPORT_DIR}/tables_to_process.txt"
    create_file_if_not_exist(TABLES_TO_PROCESS)
    TABLES_DONE=f"{REPORT_DIR}/tables_done.txt"
    create_file_if_not_exist(TABLES_DONE)
    if args.ld:
        list_dependencies(args.folder_path, True)
        sys.exit()
    if args.folder_path:
        if args.folder_path.endswith(".sql"):
            process_one_file(args.folder_path, args.pipeline_folder_path, args.pd, config)
        else:    
            process_files_in_folder(args)
    elif args.table_name:
        process_from_table_name(args.table_name, args.pipeline_folder_path, args.pd, config)

    print("\n\nDone !")
    
