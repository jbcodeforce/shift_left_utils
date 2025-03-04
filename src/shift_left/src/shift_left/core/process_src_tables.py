"""

The processing of dbt sql statements for defining sources adopt a different approach than sink tables.

Processes per application, and generates the sql in the existing pipelines. 
So this script needs to be created after the pipeline folder structure is created from the sink, as 
there is one pipeline per sink table.

"""
import os
import logging
from jinja2 import Environment, FileSystemLoader
from shift_left.core.flink_sql_code_agent_lg import translate_to_flink_sqls
from shift_left.core.project_manager import create_folder_if_not_exist

from create_table_folder_structure import create_folder_structure_for_table
from get_schema_for_src_table import search_matching_topic, get_column_definitions
from pipeline_helper import build_all_file_inventory, search_table_in_inventory, search_table_in_processed_tables, get_dependencies

from shift_left.core.app_config import get_config

logging.basicConfig(level=get_config()["app"]["logging"], format='%(levelname)s: %(message)s')
TMPL_FOLDER="./templates"
TABLES_TO_PROCESS="./reports/tables_to_process.txt"
TABLES_DONE="./reports/tables_done.txt"
CREATE_TABLE_TMPL="create_table_squeleton.jinja"
#DML_DEDUP_TMPL="dedup_dml_squeleton.jinja"
DML_DEDUP_TMPL="dedup_dml_squeleton.jinja"
TEST_DEDUL_TMPL="test_dedup_statement.jinja"
INPUT_TOPIC_LIST=os.getenv("STAGING") + "/src_topic_list.txt"


# --- utilities functions

def _create_src_ddl_statement(table_name:str, config: dict, target_folder: str):
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
    logging.info(f"Create DDL Skeleton for {fname}")
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{CREATE_TABLE_TMPL}")
    try:
        logging.info("try to get the column definitions by calling Confluent Cloud REST API")
        column_definitions, fields=get_column_definitions(table_name, config)
    except Exception as e:
        logging.error(e)
        column_definitions = "-- add columns"
        fields=""
    context = {
        'table_name': fname,
        'column_definitions': column_definitions,
        'default_PK': pk_to_use
    }
    rendered_sql = sql_template.render(context)
    logging.info(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)
    return fields

def _create_dml_statement(table_name:str, target_folder: str, fields: str, config):
    """
    Create in the target folder a dml sql squeleton file for the given table name

    :param table_name: The table name for the dml sql to create
    :param target_folder: The folder where to create the dml statement as sql file
    :return: create a file in the target folder
    """
    if "source" in target_folder:
        table_fname=config["app"]["src_table_name_prefix"] + table_name + config["app"]["src_table_name_suffix"]
    file_name=f"{target_folder}/dml.{table_fname}.sql" 
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{DML_DEDUP_TMPL}")
    topic_name=search_matching_topic(table_name, config["kafka"]["reject_topics_prefixes"])
    context = {
        'table_name': table_fname,
        'topic_name': f"`{topic_name}`",
        'fields': fields
    }
    rendered_sql = sql_template.render(context)
    logging.info(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)

def _create_test_dedup(table_name: str, target_folder: str):
    file_name=f"{target_folder}/validate_no_duplicate.sql" 
    env = Environment(loader=FileSystemLoader('.'))
    sql_template = env.get_template(f"{TMPL_FOLDER}/{TEST_DEDUL_TMPL}")
    context = {
        'table_name': table_name,
    }
    rendered_sql = sql_template.render(context)
    logging.info(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)
    
def process_ddl_file(file_path: str, sql_file: str):
    print(f"Process {sql_file}")
    with fileinput.input(sql_file, inplace=True) as f:
        for line in f:
            if "```" in line or "final AS (" in line or "SELECT * FROM final" in line:
                pass
            else:
                print(line,end='')

def save_one_file(fname: str, content: str):
    logging.debug(f"Write: {fname}")
    with open(fname,"w") as f:
        f.write(content)

def save_dml_ddl(content_path: str, table_name: str, dml: str, ddl: str):
    """
    creates two files, prefixed by "ddl." and "dml." from the dml and ddl SQL statements
    """
    ddl_fn=f"{content_path}/sql-scripts/ddl.{table_name}.sql"
    save_one_file(ddl_fn, ddl)
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
            logging.info(f"{table_name} already processed")
    return newParents


# ----- more specific functions
def _process_src_sql_file(src_file_name: str, generated_code_folder_name: str, config: dict):
    """
    Transform the source file content to the new Flink SQL format within the source_target_path.
    It creates a tests folder.

    The source file is added to the table to process tracking file

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file

    """
    logging.debug(f"process src SQL file {src_file_name}")
    table_folder, table_name = create_folder_structure_for_table(src_file_name, "sql-scripts", generated_code_folder_name, config)
    fields = _create_src_ddl_statement(table_name, config, f"{table_folder}/sql-scripts")
    _create_dml_statement(f"{table_name}", f"{table_folder}/sql-scripts", fields, config)   
    _create_test_dedup(f"int_{table_name}_deduped",f"{table_folder}/tests") 
    # merge_items_in_reporting_file([table_name], TABLES_TO_PROCESS)


def _process_non_source_sql_file(src_file_name: str, 
                                 source_target_path: str, 
                                 walk_parent: bool = False):
    """
    Transform intermediate or fact or dimension sql file to Flink SQL. 
    The folder created are <table_name>/sql_scripts and <table_name>/tests + a makefile to facilitate Confluent cloud deployment.

    :param src_file_name: the file name of the dbt or SQL source file
    :param source_target_path: the path for the newly created Flink sql file
    :param walk_parent: Assess if it needs to process the dependencies
    """
    
    table_folder, table_name=create_folder_structure_for_table(src_file_name, "sql-scripts", source_target_path, config)
    parents=[]
    with open(src_file_name) as f:
        sql_content= f.read()
        parents=get_dependencies(sql_content)
        parents=remove_already_processed_table(parents)
        merge_items_in_reporting_file(parents,TABLES_TO_PROCESS)
        dml, ddl = translate_to_flink_sqls(table_name, sql_content)
        save_dml_ddl(table_folder, table_name, dml, ddl)
        merge_items_in_reporting_file([table_name],TABLES_DONE)   # add the current table to the processed tables
    if walk_parent:
        for parent_table_name in parents:
            process_from_table_name(parent_table_name, source_target_path, walk_parent, config)


# ---- PUBLIC APIs ----

def process_one_file(src_file: str, 
                    target_folder: str, 
                    process_dependency: bool = False):
    """ Process one sql file """
    logging.info(f"process_one_file: {src_file} - {target_folder}")
    generated_code_folder_name = create_folder_if_not_exist(target_folder)
    if src_file.find("source") > 0:
        _process_src_sql_file(src_file, generated_code_folder_name, config)
    else:
        _process_non_source_sql_file(src_file, generated_code_folder_name, walk_parent, config)


def process_from_table_name(table_name: str, pipeline_folder_path: str, walk_parent: bool):
    """
    Load matching sql file given the table name as input.
    This method may be useful when we get the table name from the dependencies list of another table.

    :param: the table name
    """
    all_files= build_all_file_inventory()

    matching_sql_file=search_table_in_inventory(table_name, all_files)
    if matching_sql_file:
        logging.info(f"\n\n------------------------------------------")
        logging.info(f"\tStart processing the table: {table_name} from the dbt file: {matching_sql_file}")
        process_one_file(matching_sql_file, pipeline_folder_path, walk_parent)
    else:
        logging.info("Matching sql file not found !")