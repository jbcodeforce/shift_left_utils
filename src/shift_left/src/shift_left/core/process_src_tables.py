"""
Copyright 2024-2025 Confluent, Inc.

The processing of dbt sql statements for defining sources adopt a different approach than sink tables.

Processes per application, and generates the sql in the existing pipelines. 
So this script needs to be created after the pipeline folder structure is created from the sink, as 
there is one pipeline per sink table.

"""
import os
from pathlib import Path
from jinja2 import Environment, PackageLoader

from shift_left.core.utils.translator_to_flink_sql import get_or_build_sql_translator_agent, KsqlTranslatorToFlinkSqlAgent

from shift_left.core.utils.file_search import (
    create_folder_if_not_exist, 
    SCRIPTS_DIR,
    get_or_build_source_file_inventory, 
    extract_product_name
)
from shift_left.core.utils.app_config import get_config, logger
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.table_mgr import build_folder_structure_for_table, get_column_definitions, get_long_table_name
from typing import List, Tuple


TMPL_FOLDER="templates"
CREATE_TABLE_TMPL="create_table_skeleton.jinja"
DML_DEDUP_TMPL="dedup_dml_skeleton.jinja"
TEST_DEDUL_TMPL="test_dedup_statement.jinja"

TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')

# ---- PUBLIC APIs ----

def migrate_one_file(table_name: str,
                    sql_src_file: str, 
                    staging_target_folder: str, 
                    src_folder_path: str,
                    process_parents: bool = False,
                    source_type: str = "spark"):
    """ Process one source sql file to extract code from and migrate to Flink SQL """
    logger.info(f"Migration process_one_file: {sql_src_file} to {staging_target_folder} as {table_name}")
    create_folder_if_not_exist(staging_target_folder)
    if source_type in ['dbt', 'spark']:
        if sql_src_file.endswith(".sql"):
            product_name= extract_product_name(sql_src_file)
            if sql_src_file.find("source") > 0:
                _process_source_sql_file(table_name, sql_src_file, staging_target_folder, product_name)
            else:
                _process_non_source_sql_file(table_name, sql_src_file, staging_target_folder, src_folder_path, process_parents)
        else:
            raise Exception("Error: the sql_src_file parameter needs to be a sql or ksqlfile")
    elif source_type == "ksql":
        _process_ksql_sql_file(table_name, sql_src_file, staging_target_folder)
    else:
        raise Exception(f"Error: the source_type parameter needs to be one of ['dbt', 'spark', 'ksql']")
        

def process_from_table_name(table_name: str, staging_folder: str, src_folder_path: str, walk_parent: bool):
    """
    Load matching sql file given the table name as input.
    This method may be useful when we get the table name from the dependencies list of another table.

    :param: the table name
    """
    all_files= get_or_build_source_file_inventory(src_folder_path)
    if table_name in all_files:
        matching_sql_file=all_files[table_name]
        logger.info(f"\tStart processing the table: {table_name} from the dbt file: {matching_sql_file}")
        migrate_one_file(table_name, matching_sql_file, staging_folder, src_folder_path, walk_parent)
    else:
        logger.error(f"Matching sql file {table_name} not found in {all_files}!")

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
   
    file_name=f"{target_folder}/ddl.{table_name}.sql" 
    logger.info(f"Create DDL Skeleton for {table_name}")
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
        'table_name': table_name,
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

    file_name=f"{target_folder}/dml.{table_name}.sql" 
    env = Environment(loader=PackageLoader("shift_left.core","templates"))
    sql_template = env.get_template(f"{DML_DEDUP_TMPL}")
    topic_name=_search_matching_topic(table_name, config['kafka']['reject_topics_prefixes'])
    context = {
        'table_name': table_name,
        'topic_name': f"`{topic_name}`",
        'fields': fields
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

def _save_dml_ddl(content_path: str, 
                  internal_table_name: str, 
                  dml: str, 
                  ddl: str):
    """
    creates two files, prefixed by "ddl." and "dml." from the dml and ddl SQL statements
    """
    ddl_fn=f"{content_path}/{SCRIPTS_DIR}/ddl.{internal_table_name}.sql"
    _save_one_file(ddl_fn, ddl)
    _process_ddl_file(f"{content_path}/{SCRIPTS_DIR}/",ddl_fn)
    dml_fn=f"{content_path}/{SCRIPTS_DIR}/dml.{internal_table_name}.sql"
    _save_one_file(dml_fn,dml)


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
                             target_path: str, 
                             product_name: str):
    """
    Transform the source file content to the new Flink SQL format within the source_target_path.
    It creates a tests folder.

    The source file is added to the table to process tracking file

    :param src_file_name: the file name of the dbt source file
    :param source_target_path: the path for the newly created Flink sql file

    """
    print(f"process src SQL file {src_file_name} from {target_path}")
    table_folder, internal_table_name = build_folder_structure_for_table(table_name,  target_path + "/sources", product_name)
    config = get_config()
    fields = _create_src_ddl_statement(internal_table_name, config, f"{table_folder}/{SCRIPTS_DIR}")
    _create_dml_statement(f"{internal_table_name}", f"{table_folder}/{SCRIPTS_DIR}", fields, config)   
    

def _process_non_source_sql_file(table_name: str, 
                                sql_src_file_name: str, 
                                target_path: str, 
                                src_folder_path: str,
                                walk_parent: bool = False):
    """
    Transform intermediate or fact or dimension sql file to Flink SQL. 
    The folder created are <table_name>/sql_scripts and <table_name>/tests + a makefile to facilitate Confluent cloud deployment.

    :param src_file_name: the file name of the dbt or SQL source file
    :param target_path 
    :param source_target_path: the path for the newly created Flink sql file
    :param walk_parent: Assess if it needs to process the dependencies
    """
    logger.debug(f"process SQL file {sql_src_file_name}")
    product_path = os.path.dirname(sql_src_file_name).replace(src_folder_path, "",1)[1:]
    where_to_write_path = os.path.join(target_path, product_path)
    table_folder, internal_table_name = build_folder_structure_for_table(table_name, where_to_write_path, None)
    parents=[]
    translator_agent = get_or_build_sql_translator_agent()
    with open(sql_src_file_name, "r") as f:
        sql_content= f.read()
        parser = SQLparser()
        parents=parser.extract_table_references(sql_content)
        if table_name in parents:
            parents.remove(table_name)
        dml, ddl = translator_agent.translate_to_flink_sqls(table_name, sql_content)
        _save_dml_ddl(table_folder, internal_table_name, dml, ddl)
    if walk_parent:
        parents=_remove_already_processed_table(parents)
        for parent_table_name in parents:
            process_from_table_name(parent_table_name, target_path, src_folder_path, walk_parent)


def _search_matching_topic(table_name: str, rejected_prefixes: List[str]) -> str:
    """
    Given the table name search in the list of topics the potential matching topics.
    return the topic name if found otherwise return the table name
    rejected_prefixes list the prefixes the topic name should not start with
    """
    potential_matches=[]
    logger.debug(f"Search {table_name} in the list of topics, avoiding the ones starting by {rejected_prefixes}")
    with open(TOPIC_LIST_FILE,"r") as f:
        for line in f:
            line=line.strip()
            if ',' in line:
                keyname=line.split(',')[0]
                line=line.split(',')[1].strip()
            else:
                keyname=line
            if table_name == keyname:
                potential_matches.append(line)
            elif table_name in keyname:
                potential_matches.append(line)
            elif _find_sub_string(table_name, keyname):
                potential_matches.append(line)
    if len(potential_matches) == 1:
        return potential_matches[0]
    else:
        logger.warning(f"Found multiple potential matching topics: {potential_matches}, removing the ones that may be not start with {rejected_prefixes}")
        narrow_list=[]
        for topic in potential_matches:
            found = False
            for prefix in rejected_prefixes:
                if topic.startswith(prefix):
                    found = True
            if not found:
                narrow_list.append(topic)
        if len(narrow_list) > 1:
            logger.error(f"Still found more topic than expected {narrow_list}\n\t--> Need to abort")
            exit()
        elif len(narrow_list) == 0:
            logger.warning(f"Found no more topic {narrow_list}")
            return ""
        logger.debug(f"Take the following topic: {narrow_list[0]}")
        return narrow_list[0]


def _find_sub_string(table_name, topic_name) -> bool:
    """
    Topic name may includes words separated by ., and table may have words
    separated by _, so try to find all the words defining the name of the table
    to be in the topic name
    """
    words=table_name.split("_")
    subparts=topic_name.split(".")
    all_present = True
    for w in words:
        if w not in subparts:
            all_present=False
            break
    return all_present

def _process_ksql_sql_file(table_name: str, 
                           ksql_src_file: str, 
                           staging_target_folder: str
                           ) -> Tuple[str, str]:
    """
    Process a ksql sql file to Flink SQL.
    """
    logger.info(f"Process ksql SQL file {ksql_src_file} to {staging_target_folder}")
    table_folder = table_name.lower()
    table_folder, internal_table_name = build_folder_structure_for_table(table_folder, staging_target_folder, None)
    agent = KsqlTranslatorToFlinkSqlAgent()
    with open(ksql_src_file, "r") as f:
        ksql_content = f.read()
        ddl_content, dml_content  = agent.translate_to_flink_sqls(table_name, ksql_content, validate=True)
        _save_dml_ddl(table_folder, internal_table_name, dml_content, ddl_content)
        return ddl_content, dml_content