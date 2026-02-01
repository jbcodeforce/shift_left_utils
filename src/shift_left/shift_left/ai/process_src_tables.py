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

from shift_left.ai.agent_factory import AgentFactory
from shift_left.ai.translator_to_flink_sql import TranslatorToFlinkSqlAgent
from shift_left.core.utils.file_search import (
    create_folder_if_not_exist,
    SCRIPTS_DIR
)
from shift_left.core.utils.app_config import logger
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.table_mgr import build_folder_structure_for_table, get_column_definitions
from typing import Any, List, Tuple, Optional


TMPL_FOLDER="templates"
CREATE_TABLE_TMPL="create_table_skeleton.jinja"
DML_DEDUP_TMPL="dedup_dml_skeleton.jinja"
TEST_DEDUL_TMPL="test_dedup_statement.jinja"

TOPIC_LIST_FILE=os.getenv("TOPIC_LIST_FILE",'src_topic_list.txt')

# ---- PUBLIC APIs ----

def migrate_one_file(table_name: str,
                    sql_src_file: str,
                    staging_target_folder: str,
                    source_type: str = "spark",
                    product_name: str = "",
                    validate: bool = False):
    """ Process one source sql file to extract code from and migrate to Flink SQL.
    This is the enry point of migration, and routes to the different type of migration.
    """
    logger.info(f"Migration process_one_file: {sql_src_file} to {staging_target_folder} as {table_name}")
    create_folder_if_not_exist(staging_target_folder)
    table_folder = table_name.lower()
    table_folder, internal_table_name = build_folder_structure_for_table(table_folder, staging_target_folder, product_name)

    if source_type in ['dbt', 'spark']:
        if sql_src_file.endswith(".sql"):
            ddl_contents, dml_contents = _process_spark_sql_file(table_name=table_name,
                                    sql_src_file_name=sql_src_file,
                                    validate=validate)
            logger.info(f"Processed {table_name} to {staging_target_folder}")
        else:
            raise Exception("Error: the sql_src_file parameter needs to be a sql file")
    elif source_type == "ksql":
        ddl_contents, dml_contents = _process_ksql_sql_file(table_name=table_name,
                               ksql_src_file=sql_src_file,
                               validate=validate)
    else:
        raise Exception(f"Error: the source_type parameter needs to be one of ['dbt', 'spark', 'ksql']")
    _save_dmls_ddls(table_folder, internal_table_name, dml_contents, ddl_contents)
# ------------------------------------------------------------
# --- private functions
# ------------------------------------------------------------

def _process_ksql_sql_file(table_name: str,
                           ksql_src_file: str,
                           validate: bool = False,
                           ) -> Tuple[List[str], List[str]]:
    """
    Process a ksql sql file to Flink SQL.
    """
    agent = AgentFactory().get_or_build_sql_translator_agent("ksql")
    with open(ksql_src_file, "r") as f:
        ksql_content = f.read()
        ddl_contents, dml_contents = agent.translate_to_flink_sqls(table_name, ksql_content, validate=validate)
        return ddl_contents, dml_contents


def _process_spark_sql_file(table_name: str,
                            sql_src_file_name: str,
                            validate: bool = False):
    """
    From Spark SQL to Flink SQL
    :param src_file_name: the file name of the dbt, spark SQL source file
    :param target_path
    :param source_target_path: the path for the newly created Flink sql file
    :param walk_parent: Assess if it needs to process the dependencies
    :param validate: Assess if it needs to validate the sql using Confluent Cloud for Flink
    """
    parents=set[str]()
    translator_agent = AgentFactory().get_or_build_sql_translator_agent("spark")
    with open(sql_src_file_name, "r") as f:
        sql_content= f.read()
        parser = SQLparser()
        parents=parser.extract_table_references(sql_content)
        if table_name in parents:
            parents.remove(table_name)
        ddls, dmls = translator_agent.translate_to_flink_sqls(table_name=table_name, sql=sql_content, validate=validate)
    return ddls, dmls


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
        column_definitions, fields=get_column_definitions(table_name)
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

def _save_dmls_ddls(content_path: str,
                  internal_table_name: str,
                  dmls: List[str],
                  ddls: List[str]):
    """
    save each ddl and dml in files, prefixed by "ddl." and "dml."

    """
    logger.info(f"Save DML and DDL statements to {content_path} for {internal_table_name} with {len(ddls)} DDLs and {len(dmls)} DMLs")
    # Ensure we have lists, not strings (to avoid character iteration)
    if isinstance(ddls, str):
        ddls = [ddls] if ddls.strip() else []
    if isinstance(dmls, str):
        dmls = [dmls] if dmls.strip() else []

    sql_parser = SQLparser()
    idx=0
    for ddl in ddls:
        if ddl and ddl.strip():  # Only process non-empty DDLs
            parsed_table_name = sql_parser.extract_table_name_from_create_statement(ddl)
            table_name = parsed_table_name if parsed_table_name else internal_table_name if idx == 0 else f"{internal_table_name}_{idx}"
            ddl_fn=f"{content_path}/{SCRIPTS_DIR}/ddl.{table_name}.sql"
            _save_one_file(ddl_fn, ddl)
            # remove templated file if needed:
            if table_name != internal_table_name and os.path.exists(f"{content_path}/{SCRIPTS_DIR}/ddl.{internal_table_name}.sql"):
                os.remove(f"{content_path}/{SCRIPTS_DIR}/ddl.{internal_table_name}.sql")
            _process_ddl_file(f"{content_path}/{SCRIPTS_DIR}/",ddl_fn)
            idx+=1
    idx=0
    for dml in dmls:
        if dml and dml.strip():  # Only process non-empty DMLs
            parsed_table_name = sql_parser.extract_table_name_from_insert_into_statement(dml)
            table_name = parsed_table_name if parsed_table_name else internal_table_name if idx == 0 else f"{internal_table_name}_{idx}"
            dml_fn=f"{content_path}/{SCRIPTS_DIR}/dml.{table_name}.sql"
            _save_one_file(dml_fn,dml)
            if table_name != internal_table_name and os.path.exists(f"{content_path}/{SCRIPTS_DIR}/dml.{internal_table_name}.sql"):
                os.remove(f"{content_path}/{SCRIPTS_DIR}/dml.{internal_table_name}.sql")
            idx+=1

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


