"""

The processing of dbt sql statements for defining sources adopt a different approach than sink tables.

Processes per application, and generates the sql in the existing pipelines. 
So this script needs to be created after the pipeline folder structure is created from the sink, as 
there is one pipeline per sink table.

"""
import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from jinja2 import Environment, PackageLoader
from shift_left.core.utils.flink_sql_code_agent_lg import translate_to_flink_sqls
from shift_left.core.utils.file_search import create_folder_if_not_exist, SCRIPTS_DIR, get_or_build_source_file_inventory
from shift_left.core.utils.app_config import get_config
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.ccloud_client import search_matching_topic
from shift_left.core.table_mgr import build_folder_structure_for_table, get_column_definitions


log_dir = os.path.join(os.getcwd(), 'logs')
logger = logging.getLogger("deployment")
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(get_config()["app"]["logging"])
log_file_path = os.path.join(log_dir, "process-src-table.log")
file_handler = RotatingFileHandler(
    log_file_path, 
    maxBytes=1024*1024,  # 1MB
    backupCount=3        # Keep up to 3 backup files
)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
logger.addHandler(console_handler)

TMPL_FOLDER="templates"
CREATE_TABLE_TMPL="create_table_skeleton.jinja"
DML_DEDUP_TMPL="dedup_dml_skeleton.jinja"
TEST_DEDUL_TMPL="test_dedup_statement.jinja"

# ---- PUBLIC APIs ----

def process_one_file(table_name: str,
                     sql_src_file: str, 
                    staging_target_folder: str, 
                    src_folder_path: str,
                    process_parents: bool = False):
    """ Process one source sql file to extract code and migrate to Flink SQL """
    logger.debug(f"Migration process_one_file: {sql_src_file} - {staging_target_folder} as {table_name}")
    if sql_src_file.endswith(".sql"):
        create_folder_if_not_exist(staging_target_folder)
        if sql_src_file.find("source") > 0:
            _process_source_sql_file(table_name,sql_src_file, staging_target_folder)
        else:
            _process_non_source_sql_file(table_name, sql_src_file, staging_target_folder, src_folder_path, process_parents)
    else:
        raise Exception("Error: the first parameter needs to be a sql file")
        

def process_from_table_name(table_name: str, staging_folder: str, src_folder_path: str, walk_parent: bool):
    """
    Load matching sql file given the table name as input.
    This method may be useful when we get the table name from the dependencies list of another table.

    :param: the table name
    """
    all_files= get_or_build_source_file_inventory(src_folder_path)
    print(all_files)
    matching_sql_file=all_files[table_name]
    if matching_sql_file:
        logger.info(f"\n\n------------------------------------------")
        logger.info(f"\tStart processing the table: {table_name} from the dbt file: {matching_sql_file}")
        process_one_file(table_name, matching_sql_file, staging_folder, src_folder_path, walk_parent)
    else:
        logger.info("Matching sql file not found !")

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
    logger.info(f"Create DDL Skeleton for {fname}")
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    sql_template = env.get_template(f"{CREATE_TABLE_TMPL}")
    try:
        logger.info("try to get the column definitions by calling Confluent Cloud REST API")
        column_definitions, fields=get_column_definitions(table_name, config)
    except Exception as e:
        logger.error(e)
        column_definitions = "-- add columns"
        fields=""
    context = {
        'table_name': fname,
        'column_definitions': column_definitions,
        'default_PK': pk_to_use
    }
    rendered_sql = sql_template.render(context)
    logger.info(f"writing file {file_name}")
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
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    sql_template = env.get_template(f"{DML_DEDUP_TMPL}")
    topic_name=search_matching_topic(table_name)
    context = {
        'table_name': table_fname,
        'topic_name': f"`{topic_name}`",
        'fields': fields
    }
    rendered_sql = sql_template.render(context)
    logger.info(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)

def _create_test_dedup(table_name: str, target_folder: str):
    file_name=f"{target_folder}/validate_no_duplicate.sql" 
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    sql_template = env.get_template(f"{TEST_DEDUL_TMPL}")
    context = {
        'table_name': table_name,
    }
    rendered_sql = sql_template.render(context)
    logger.info(f"writing file {file_name}")
    with open(file_name, 'w') as f:
        f.write(rendered_sql)
    
def _process_ddl_file(file_path: str, sql_file: str):
    """
    Process a ddl file, replacing the ``` final AS (``` and SELECT * FROM final 
    """
    print(f"Process {sql_file} in {file_path}")
    file_name=Path(sql_file)
    content = file_name.read_text()
    update_content = content.replace("``` final AS (","```").replace("SELECT * FROM final","")
    file_name.write_text(update_content)


def _save_one_file(fname: str, content: str):
    logger.debug(f"Write: {fname}")
    with open(fname,"w") as f:
        f.write(content)

def _save_dml_ddl(content_path: str, table_name: str, dml: str, ddl: str):
    """
    creates two files, prefixed by "ddl." and "dml." from the dml and ddl SQL statements
    """
    ddl_fn=f"{content_path}/{SCRIPTS_DIR}/ddl.{table_name}.sql"
    _save_one_file(ddl_fn, ddl)
    _process_ddl_file(f"{content_path}/{SCRIPTS_DIR}/",ddl_fn)
    _save_one_file(f"{content_path}/{SCRIPTS_DIR}/dml.{table_name}.sql",dml)


def _remove_already_processed_table(parents: list[str]) -> list[str]:
    """
    If a table in the provided list of parents is already processed, remove it from the list.
    A processed table is one already define in the pipelines folder hierarchy.
    """
    newParents=[]
    for parent in parents:
        table_name=parent.replace("src_","").strip()
        if not _search_table_in_processed_tables(table_name):
            newParents.append(parent)
        else:
            logger.info(f"{table_name} already processed")
    return newParents



def _search_table_in_processed_tables(table_name: str) -> bool:
    pipeline_path=os.getenv("PIPELINES","../pipelines")
    for _, dirs, _ in os.walk(pipeline_path):
        for dir in dirs:
            if table_name == dir:
                return True
    else:
        return False


def _process_source_sql_file(table_name: str,
                            src_file_name: str, 
                             target_path: str):
    """
    Transform the source file content to the new Flink SQL format within the source_target_path.
    It creates a tests folder.

    The source file is added to the table to process tracking file

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file

    """
    print(f"process src SQL file {src_file_name} from {target_path}")
    table_folder, table_name = build_folder_structure_for_table(table_name,  target_path + "/sources")
    config = get_config()
    fields = _create_src_ddl_statement(table_name, config, f"{table_folder}/{SCRIPTS_DIR}")
    _create_dml_statement(f"{table_name}", f"{table_folder}/{SCRIPTS_DIR}", fields, config)   
    _create_test_dedup(f"int_{table_name}_deduped",f"{table_folder}/tests") 


def _process_non_source_sql_file(table_name: str, 
                                sql_src_file_name: str, 
                                target_path: str, 
                                src_folder_path: str,
                                walk_parent: bool = False):
    """
    Transform intermediate or fact or dimension sql file to Flink SQL. 
    The folder created are <table_name>/sql_scripts and <table_name>/tests + a makefile to facilitate Confluent cloud deployment.

    :param src_file_name: the file name of the dbt or SQL source file
    :param source_target_path: the path for the newly created Flink sql file
    :param walk_parent: Assess if it needs to process the dependencies
    """
    logger.debug(f"process SQL file {sql_src_file_name}")
    product_path = os.path.dirname(sql_src_file_name).replace(src_folder_path, "",1)[1:]
    where_to_write_path = os.path.join(target_path, product_path)
    table_folder, _ = build_folder_structure_for_table(table_name, where_to_write_path)
    parents=[]
    with open(sql_src_file_name, "r") as f:
        sql_content= f.read()
        parser = SQLparser()
        parents=parser.extract_table_references(sql_content)
        dml, ddl = translate_to_flink_sqls(table_name, sql_content)
        _save_dml_ddl(table_folder, table_name, dml, ddl)
    if walk_parent:
        parents=_remove_already_processed_table(parents)
        for parent_table_name in parents:
            process_from_table_name(parent_table_name, target_path, src_folder_path, walk_parent)


