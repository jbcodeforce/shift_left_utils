"""
Copyright 2024-2025 Confluent, Inc.
"""
import datetime
import os
from pathlib import Path
import re
import hashlib
import subprocess
import shutil
import importlib.resources
from datetime import timezone
from typing import Tuple, List, Any
from pydantic import BaseModel, Field
from shift_left.core.table_mgr import get_or_build_inventory
from shift_left.core.utils.ccloud_client import ConfluentCloudClient
from shift_left.core.utils.app_config import get_config, logger, shift_left_dir
from shift_left.core.pipeline_mgr import FlinkTablePipelineDefinition
from shift_left.core.utils.file_search import (
    PIPELINE_JSON_FILE_NAME,
    PIPELINE_FOLDER_NAME,
    get_table_ref_from_inventory,
    read_pipeline_definition_from_file,
    from_pipeline_to_absolute,
    create_folder_if_not_exist,
)
from shift_left.core.utils.sql_parser import SQLparser
import shift_left.core.statement_mgr as statement_mgr
from shift_left.core.models.flink_statement_model import Statement
from shift_left.core.utils.table_worker import ReplaceVersionInSqlContent
DATA_PRODUCT_PROJECT_TYPE="data_product"
KIMBALL_PROJECT_TYPE="kimball"
TMPL_FOLDER="templates"


class ModifiedFileInfo(BaseModel):
    table_name: str = Field(description="Extracted table name")
    file_modified_url: str = Field(description="File path/URL of the modified file")
    same_sql_content: bool = Field(description="Whether the SQL is the same as the running statement")
    running: bool = Field(description="Whether the statement is running")
    new_table_name: str = Field(description="New table name after versioning")

    def __hash__(self) -> int:
        return hash(self.table_name)
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ModifiedFileInfo):
            return NotImplemented
        return self.table_name == other.table_name and self.file_modified_url == other.file_modified_url


class ModifiedFilesResult(BaseModel):
    """Result of list_modified_files operation"""
    description: str = Field(description="Summary information about the operation")
    file_list: set[ModifiedFileInfo] = Field(description="Set of modified files with extracted table names")

# ------- Public APIS ----------

def build_project_structure(project_name: str,
                            project_path: str,
                            project_type: str):
    logger.info(f"build_project_structure({project_name}, {project_path}, {project_type}")
    project_folder=os.path.join(project_path, project_name)
    create_folder_if_not_exist(project_folder)
    create_folder_if_not_exist(os.path.join(project_folder, "pipelines"))
    create_folder_if_not_exist(os.path.join(project_folder, "staging"))
    create_folder_if_not_exist(os.path.join(project_folder, "docs"))
    if project_type == DATA_PRODUCT_PROJECT_TYPE:
        _define_dp_structure(os.path.join(project_folder, "pipelines"))
    else:
        _define_kimball_structure(os.path.join(project_folder, "pipelines"))
    _initialize_git_repo(project_folder)
    _add_important_files(project_folder)


def get_topic_list(file_name: str) -> list[dict]:
    """
    Get the list of topics from the Confluent Cloud and save it to a file.
    """
    ccloud = ConfluentCloudClient(get_config())
    topics = ccloud.list_topics()
    if topics and topics.get('data'):
        with open(file_name, "w") as f:
            for topic in topics["data"]:
                f.write(f"{topic['cluster_id']},{topic['topic_name']},{topic['partitions_count']}\n")
        return topics["data"]
    else:
        return []


def report_table_cross_products(project_path: str):
    """
    Return the lit of table names for tables that are referenced in other products.
    """
    if not project_path:
        project_path = os.getenv("PIPELINES")
    inventory = get_or_build_inventory(project_path, project_path, False)
    risky_tables = []
    for table_name, table_ref in inventory.items():
        try:
            table_hierarchy: FlinkTablePipelineDefinition= read_pipeline_definition_from_file(table_ref['table_folder_name'] + "/" + PIPELINE_JSON_FILE_NAME)
            if table_hierarchy:
                for child in table_hierarchy.children:
                    if child.product_name != table_ref['product_name']:
                        risky_tables.append(table_name)
                        break
        except Exception as e:
            logger.error(f"Error in reporting table cross products: {e}")
            continue
    return risky_tables


def list_tables_with_one_child(project_path: str):
    """
    Return the list of table names for tables that have exactly one child table.

    Args:
        project_path: Path to the pipeline folder. If not provided, uses $PIPELINES environment variable.

    Returns:
        List of table names that have exactly one child
    """
    if not project_path:
        project_path = os.getenv("PIPELINES")
    inventory = get_or_build_inventory(project_path, project_path, False)
    tables_with_one_child = []
    for table_name, table_ref in inventory.items():
        table_hierarchy: FlinkTablePipelineDefinition = read_pipeline_definition_from_file(
            table_ref['table_folder_name'] + "/" + PIPELINE_JSON_FILE_NAME
        )
        if table_hierarchy and table_hierarchy.children and len(table_hierarchy.children) <= 1:
            tables_with_one_child.append(table_name)
    return tables_with_one_child

def list_modified_files(project_path: str,
                        branch_name: str,
                        since: str,
                        file_filter: str,
                        output_file: str) -> ModifiedFilesResult:
    """List modified files and return structured result.

    Args:
        project_path: Path to the project directory
        branch_name: Git branch name to check
        since: Date filter for git log (YYYY-MM-DD format)
        file_filter: File extension filter (e.g., '.sql')
        output_file: Optional output file path (for backward compatibility)

    Returns:
        ModifiedFilesResult: Structured result with description and file list

    Raises:
        subprocess.CalledProcessError: If git commands fail
        Exception: For other errors
    """
    try:
        # Change to project directory
        original_cwd = os.getcwd()
        if project_path and project_path != ".":
            os.chdir(project_path)

        # Get the current branch name
        current_branch_result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True
        )
        current_branch = current_branch_result.stdout.strip()
        logger.info(f"Current branch: {current_branch}")
        if current_branch != branch_name:
            print(f"Current branch is not the same as the specified branch {branch_name}, I will automaticall checkout to {branch_name}")
            subprocess.run(
                ["git", "checkout", f"{branch_name}"],
                capture_output=True,
                text=True,
                check=True
            )
        if not since:
            # If no date_filter is provided, set it to yesterday's date in YYYY-MM-DD format
            yesterday = (datetime.datetime.now(timezone.utc) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            since = yesterday
        # Get list of modified files compared to the specified branch
        git_diff_result = subprocess.run(
            ["git", "log", "--name-only", f"--since={since}", '--pretty=format:'],
            capture_output=True,
            text=True,
            check=True
        )

        all_modified_files = git_diff_result.stdout.strip().split('\n')
        all_modified_files = [f for f in all_modified_files if f.strip()]  # Remove empty strings
        # Filter for specific file types (default: SQL files)
        filtered_files = set[str]()
        for file_path in all_modified_files:
            lowered_file_path = file_path.lower()
            if file_filter in lowered_file_path and PIPELINE_FOLDER_NAME in lowered_file_path:
                logger.info(f"Checking file: {file_path}")
                # Exclude files that have a parent folder named 'tests'
                # (i.e., a path segment '/tests/' or startswith 'tests/')
                normalized_path = os.path.normpath(file_path)
                path_parts = normalized_path.split(os.sep)
                # Check if any parent directory is 'tests'
                if "tests" in path_parts[-2]:
                    logger.info(f"Skipping file: {file_path} because it has a parent folder named 'tests'")
                    continue
                absolute_file_path = project_path + "/" + file_path
                if not os.path.exists(absolute_file_path):
                    logger.warning(f"File {absolute_file_path} does not currently exist, skipping")
                    continue
                filtered_files.add(absolute_file_path)

        print(f"Found {len(all_modified_files)} total modified files")

        print(f"Found {len(filtered_files)} modified files matching filter '{file_filter}'")

        generated_on = datetime.datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        description = (
            f"Modified files in branch:'{current_branch}'\n"
            f"Filter applied: {file_filter}\n"
            f"Generated on: {generated_on}\n"
            f"Total files: {len(filtered_files)}"
        )
        logger.info(description)
        # Create file list with extracted table names
        file_list = set()
        parser = SQLparser()
        for absolute_file_path in sorted(filtered_files):
            with open(absolute_file_path, 'r') as file:
                sql_content = file.read()
                if "create table" in sql_content.lower() or "create or replace table" in sql_content.lower():
                    table_name = parser.extract_table_name_from_create_statement(sql_content)
                    same_sql, running = False, False
                else:
                    table_name = parser.extract_table_name_from_insert_into_statement(sql_content)
                    if table_name == "No-Table":
                        logger.warning(f"No table name found in file {sql_content}, skipping")
                        continue
                    same_sql, running = _assess_flink_statement_state(table_name, absolute_file_path, sql_content)
            file_list.add(ModifiedFileInfo(
                table_name=table_name,
                file_modified_url=absolute_file_path,
                same_sql_content=same_sql,
                running=running,
                new_table_name=table_name
            ))

        # Create result object
        result = ModifiedFilesResult(
            description=description,
            file_list=file_list
        )

        # Backward compatibility: write to file if output_file is provided
        if output_file:
            output_path = Path(output_file)
            if not output_path.is_absolute():
                output_path = Path(original_cwd) / output_path

            with open(output_path, 'w') as f:
                f.write(result.model_dump_json(indent=2))

            print(f"\nFile list saved to: {output_path}")

        # Display results
        if filtered_files:
            logger.info(f"\nModified {file_filter} files:")
            for file_info in file_list:
                logger.info(f"  {file_info.table_name}: {file_info.file_modified_url}")
        else:
            logger.info(f"\nNo modified files found matching filter '{file_filter}'")
        return result

    except subprocess.CalledProcessError as e:
        print(f"❌ Git command failed: {e}")
        print(f"Error output: {e.stderr}")
        raise e
    except Exception as e:
        print(f"❌ Error: {e}")
        raise e
    finally:
        # Restore original working directory
        os.chdir(original_cwd)


def update_tables_version(to_process_tables: list[ModifiedFileInfo], default_version: str):
    """
    Update the table version within the DDL and DML SQL content for the given list of table names
    """
    inventory = get_or_build_inventory(os.getenv("PIPELINES"), os.getenv("PIPELINES"), False)
    processed_files = set[ModifiedFileInfo]()
    for file_info in to_process_tables:
        _update_version_in_current_ddl_dml(file_info, processed_files, default_version, inventory)
        _add_children_to_process_files(file_info, to_process_tables, inventory)
    return processed_files

def _add_children_to_process_files(file_info: ModifiedFileInfo, to_process_tables: list[ModifiedFileInfo], inventory: dict):
    """
    Add the children of the table to the list of tables to process
    """
    table_ref = inventory[file_info.table_name]
    if not table_ref:
        logger.error(f"Error: table {file_info.table_name} not found in inventory")
        return
    pipeline_definition = read_pipeline_definition_from_file(table_ref['table_folder_name'] + "/" + PIPELINE_JSON_FILE_NAME)
    if not pipeline_definition:
        logger.error(f"Error: pipeline definition not found for table {file_info.table_name}")
        return
    for child in pipeline_definition.children:
        to_process_tables.append(ModifiedFileInfo(table_name=child.table_name,
                                                  file_modified_url=child.dml_ref,
                                                  same_sql_content=False,
                                                  running=False,
                                                  new_table_name=child.table_name))
def _change_version_in_ddl(fname: str,
                           old_table_name: str,
                           default_version: str):
    parser = SQLparser()
    fname = from_pipeline_to_absolute(fname)
    new_table_name = _build_new_table_name(old_table_name, default_version)
    with open(fname, 'r') as f:
        sql_content = f.read()
        parsed_table_name = parser.extract_table_name_from_create_statement(sql_content)
        if parsed_table_name == old_table_name:
            sql_content = sql_content.replace(parsed_table_name,new_table_name,1) # first occurence is in CREATE TABLE line
        else:
            print(f"❌ Error: table name {parsed_table_name} does not match {old_table_name} in {fname}")
            logger.error(f"❌ Error: table name {parsed_table_name} does not match {old_table_name} in {fname}")
            return
        with open(fname, 'w') as f2:
            f2.write(sql_content)
        logger.info("-"*10 +f"\n{sql_content}\n" + "-"*10)

def _replace_table_name_in_sql_clauses(sql_content: str, old_table_name: str, new_table_name: str) -> str:
    """
    Replace table name in FROM, JOIN, and INSERT INTO clauses.
    Also replaces implicit table aliases (tablename.column) when no explicit AS alias is provided.

    Args:
        sql_content: The SQL content to modify
        old_table_name: The table name to replace
        new_table_name: The new table name

    Returns:
        Modified SQL content with table name replaced in appropriate contexts
    """
    result = sql_content

    # Step 1: Find all FROM/JOIN occurrences and check if they have explicit aliases
    # Pattern to match table references: FROM/JOIN tablename [AS alias]
    table_ref_pattern = rf'\b(FROM|(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN)\s+`?{re.escape(old_table_name)}`?\s+(?:AS\s+(\w+))?'

    # Check if there's an explicit alias defined
    has_explicit_alias = False
    matches = re.finditer(table_ref_pattern, sql_content, re.IGNORECASE)
    for match in matches:
        if match.group(2):  # Group 2 is the alias name after AS
            has_explicit_alias = True
            break

    # Step 2: Replace table name in FROM clause
    result = re.sub(
        rf'(\bFROM\s+)(`?{re.escape(old_table_name)}`?)(\b)',
        rf'\g<1>{new_table_name}\g<3>',
        result,
        flags=re.IGNORECASE
    )

    # Step 3: Replace table name in JOIN clauses
    result = re.sub(
        rf'(\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+|CROSS\s+)?JOIN\s+)(`?{re.escape(old_table_name)}`?)(\b)',
        rf'\g<1>{new_table_name}\g<3>',
        result,
        flags=re.IGNORECASE
    )

    # Step 4: Replace table name in INSERT INTO clause
    result = re.sub(
        rf'(\bINSERT\s+INTO\s+)(`?{re.escape(old_table_name)}`?)(\b)',
        rf'\g<1>{new_table_name}\g<3>',
        result,
        flags=re.IGNORECASE
    )

    # Step 5: If no explicit alias, replace implicit table alias references (tablename.column)
    if not has_explicit_alias:
        # Replace tablename. with newtablename. in column references
        # Use word boundary before table name and require a dot after it
        result = re.sub(
            rf'\b{re.escape(old_table_name)}\.',
            f'{new_table_name}.',
            result,
            flags=re.IGNORECASE
        )

    return result


def _change_version_in_dml(processed_files: set[ModifiedFileInfo], fname, table_name: str, version: str):
    """
    Change the version of the sql content
    """

    parser = SQLparser()
    fname = from_pipeline_to_absolute(fname)
    updated = False
    processed_table_names = [file_info.table_name for file_info in processed_files]
    with open(fname, 'r') as f:
        sql_content = f.read()
        referenced_table_names = parser.extract_table_references(sql_content)
        for referenced_table_name in referenced_table_names:
            if referenced_table_name in processed_table_names:
                new_referenced_table_name = _build_new_table_name(referenced_table_name, version)
                # Use smart replacement that only replaces in FROM/JOIN clauses
                sql_content = _replace_table_name_in_sql_clauses(sql_content, referenced_table_name, new_referenced_table_name)
                updated = True
    if updated:
        with open(fname, 'w') as f:
            f.write(sql_content)
    logger.info("-"*10 +f"\n{sql_content}\n" + "-"*10)

def _update_version_in_current_ddl_dml(currentfile_info: ModifiedFileInfo,
                               processed_files: set[ModifiedFileInfo],
                               default_version: str,
                               inventory: dict):
    """
    Update the version of the current ddl and dml files for the given table name
    """
    try:
        table_ref = inventory[currentfile_info.table_name]
    except KeyError:
        logger.error(f"Error: table {currentfile_info.table_name} not found in inventory")
        return
    pipeline_definition = read_pipeline_definition_from_file(table_ref['table_folder_name'] + "/" + PIPELINE_JSON_FILE_NAME)
    if not pipeline_definition:
        logger.error(f"Error: pipeline definition not found for table {currentfile_info.table_name}")
        return
    if currentfile_info not in processed_files:
        processed_files.add(currentfile_info)
        _change_version_in_ddl(pipeline_definition.ddl_ref, currentfile_info.table_name, default_version)
        _change_version_in_dml(processed_files, pipeline_definition.dml_ref, currentfile_info.table_name, default_version)
        logger.info(f"Updated version in {pipeline_definition.ddl_ref} and {pipeline_definition.dml_ref}")


def isolate_data_product(product_name: str, source_folder: str, target_folder: str):
    logger.info(f"isolate_data_product({product_name}, {source_folder}, {target_folder})")
    """
    isolate a data product table hierarchy for a given product name to be copied to a target folder.
    go to the facts and build a list of tables for this product name.
    add any children of the tables in the list of facts, recursively.
    build an integrated execution plan from the list of tables.
    move all the folder to the target folder.
    """
    inventory = get_or_build_inventory(source_folder, source_folder, False)
    tables = [table for table in inventory if inventory[table]['product_name'] == product_name]
    tables_to_process = {}
    visited = set()

    # Process each table and recursively find all its parents
    for table in tables:
        logger.info(f"Processing table {table} and finding all its dependencies")
        _find_all_parent_tables_recursive(table, inventory, visited, tables_to_process)

    logger.info(f"Found {len(tables_to_process)} total tables to process (including all dependencies)")

    # Copy all tables (original + all dependencies) to target folder

    for table, table_folder_name in tables_to_process.items():
        logger.info(f"Copying table: {table}, from {table_folder_name} to {target_folder}")

        # Keep the hierarchy of folder in the table_folder_name
        print(f"Copying table: {table}, from {table_folder_name} to {target_folder}")
        shutil.copytree(
            os.path.join(source_folder, '..', table_folder_name),
            os.path.join(target_folder, table_folder_name),
            dirs_exist_ok=True
        )
    with open(os.path.join(shift_left_dir, "tables_to_process.txt"), "w") as f:
        for table, table_folder_name in tables_to_process.items():
            f.write(f"{table},{table_folder_name}\n")

# ---------------------------------
# --- Private APIs ---
# ---------------------------------

def _extract_table_name_from_path(file_path: str) -> str:
    """Extract table name from file path.

    Attempts to extract a meaningful table name from various file path patterns:
    - For SQL files: extracts filename without extension
    - For pipeline paths: attempts to extract table name from directory structure

    Args:
        file_path: The file path to extract table name from

    Returns:
        Extracted table name or filename without extension as fallback
    """
    path = Path(file_path)

    # Remove file extension
    table_name = path.stem

    # Handle common patterns in pipeline directory structures
    if 'pipelines' in path.parts:
        # If it's in a pipelines directory, the parent directory might be the table name
        if len(path.parts) > 1 and path.parent.name != 'pipelines':
            table_name = path.parent.name

    # Clean up common prefixes/suffixes
    table_name = table_name.replace('ddl.', '').replace('dml.', '')

    return table_name

def _initialize_git_repo(project_folder: str):
    print(f"initialize_git_repo({project_folder})")
    try:
        subprocess.run(["git", "init"], check=True, cwd=project_folder)
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to initialize git repository in {project_folder}: {e}")

def _define_dp_structure(pipeline_folder: str):
    data_folder=pipeline_folder + "/data_product_1"
    create_folder_if_not_exist(data_folder)
    create_folder_if_not_exist(data_folder + "/intermediates")
    create_folder_if_not_exist(data_folder + "/facts")
    create_folder_if_not_exist(data_folder + "/sources")

def _define_kimball_structure(pipeline_folder: str):
    create_folder_if_not_exist(pipeline_folder + "/intermediates")
    create_folder_if_not_exist(pipeline_folder + "/facts")
    create_folder_if_not_exist(pipeline_folder + "/dimensions")
    create_folder_if_not_exist(pipeline_folder + "/sources")
    create_folder_if_not_exist(pipeline_folder + "/views")

def _add_important_files(project_folder: str):
    logger.info(f"add_important_files({project_folder}")
    for file in ["common.mk"]:
        template_path = importlib.resources.files("shift_left.core.templates").joinpath(file)
        shutil.copyfile(template_path, os.path.join(project_folder, "pipelines", file))
    template_path = importlib.resources.files("shift_left.core.templates").joinpath(".env_tmpl")
    shutil.copyfile(template_path, os.path.join(project_folder, ".env"))
    # Update FLINK_PROJECT in .env file with project folder path
    env_file = os.path.join(project_folder, ".env")
    with open(env_file, 'r') as f:
        env_content = f.read()
    env_content = env_content.replace("FLINK_PROJECT=", f"FLINK_PROJECT={project_folder}")
    with open(env_file, 'w') as f:
        f.write(env_content)
    template_path = importlib.resources.files("shift_left.core.templates").joinpath(".gitignore_tmpl")
    shutil.copyfile(template_path, os.path.join(project_folder, ".gitignore"))
    template_path = importlib.resources.files("shift_left.core.templates").joinpath("config_tmpl.yaml")
    shutil.copyfile(template_path, os.path.join(shift_left_dir, "config.yaml"))


def _assess_flink_statement_state(table_name: str, file_path: str, sql_content: str) -> Tuple[bool, bool]:
    """
    Assess the state of a Flink statement for the given table.
    Returns (same_sql, running) where same_sql indicates if the SQL is the same as the running statement and running indicates if the statement is running.
    """

    inventory = get_or_build_inventory(os.getenv("PIPELINES"), os.getenv("PIPELINES"), True)
    table_ref = get_table_ref_from_inventory(table_name, inventory)
    if not table_ref:
        print(f"Error: Table {table_name} not found in inventory")
        return False, False
    pipeline_definition = read_pipeline_definition_from_file(table_ref.table_folder_name + "/" + PIPELINE_JSON_FILE_NAME)
    if not pipeline_definition:
        print(f"Error: pipeline definition not found for table {table_name}")
        return False, False
    statement_node = pipeline_definition.to_node()
    flink_statement = statement_mgr.get_statement(statement_node.dml_statement_name)
    if not flink_statement:
        print(f"Error: statement {statement_node.dml_statement_name} not found")
        return True, False
    if isinstance(flink_statement, Statement):
        same_sql = _assess_sql_difference(table_name, sql_content, flink_statement.spec.statement)
        return same_sql, flink_statement.status.phase == "RUNNING"
    else:
        return False, False

def _assess_sql_difference(table_name: str, file_sql_content: str, running_sql_content: str) -> bool:
    """
    Assess the difference between the SQL content on disk and the running statement.
    Returns True if the normalized SQL content is the same after removing comments and normalizing whitespace.
    """
    # Normalize both SQL content strings
    normalized_file_sql = _normalize_sql_content(file_sql_content)
    normalized_running_sql = _normalize_sql_content(running_sql_content)

    logger.info(f"Normalized FILE SQL: \n {normalized_file_sql}")
    logger.info(f"Normalized RUNNING SQL: \n {normalized_running_sql}")

    hash_normalized_file_sql = hashlib.md5(normalized_file_sql.encode('utf-8')).hexdigest()
    hash_normalized_running_sql = hashlib.md5(normalized_running_sql.encode('utf-8')).hexdigest()

    logger.info(f"Hash normalized file SQL: {hash_normalized_file_sql}")
    logger.info(f"Hash normalized running SQL: {hash_normalized_running_sql}")

    return hash_normalized_file_sql == hash_normalized_running_sql


def _normalize_sql_content(sql_content: str) -> str:
    """
    Normalize SQL content by removing comments and normalizing whitespace.

    Args:
        sql_content: Raw SQL content string

    Returns:
        Normalized SQL content string
    """
    if not sql_content:
        return ""

    # Remove SQL comments
    sql_without_comments = _remove_sql_comments(sql_content)

    # Normalize whitespace
    normalized_sql = _normalize_whitespace(sql_without_comments)

    return normalized_sql


def _remove_sql_comments(sql_content: str) -> str:
    """
    Remove SQL comments from the content.
    Handles both single-line comments (--) and multi-line comments (/* */).

    Args:
        sql_content: SQL content with potential comments

    Returns:
        SQL content with comments removed
    """
    # Remove multi-line comments /* ... */
    # Use re.DOTALL to make . match newlines
    sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

    # Remove single-line comments -- ...
    # Match -- followed by anything until end of line
    sql_content = re.sub(r'--.*?$', '', sql_content, flags=re.MULTILINE)

    return sql_content


def _normalize_whitespace(sql_content: str) -> str:
    """
    Normalize whitespace in SQL content.

    Args:
        sql_content: SQL content string

    Returns:
        SQL content with normalized whitespace
    """
    # Replace multiple whitespace characters (spaces, tabs, newlines) with single space
    normalized = re.sub(r'\s+', ' ', sql_content)

    # Strip leading and trailing whitespace
    normalized = normalized.strip()

    # Convert to uppercase for case-insensitive comparison
    normalized = normalized.upper()

    return normalized

def _find_all_parent_tables_recursive(table_name: str, inventory: dict, visited: set, tables_to_process: dict):
    """
    Recursively find all parent tables for a given table.

    Args:
        table_name: The table to find parents for
        inventory: The complete inventory of tables
        visited: Set of already visited tables to avoid circular dependencies
        tables_to_process: Dictionary to accumulate all tables that need processing
    """
    if table_name in visited:
        # Avoid circular dependencies
        logger.debug(f"Table {table_name} already visited, skipping to avoid circular dependency")
        return

    visited.add(table_name)

    # Get table reference from inventory
    if table_name not in inventory:
        logger.warning(f"Table {table_name} not found in inventory")
        return

    tableRef = inventory[table_name]
    tables_to_process[table_name] = tableRef['table_folder_name']

    # Read pipeline definition to find parents
    pipeline_definition = read_pipeline_definition_from_file(
        os.path.join(tableRef['table_folder_name'], PIPELINE_JSON_FILE_NAME)
    )

    if pipeline_definition and pipeline_definition.parents:
        logger.debug(f"Found {len(pipeline_definition.parents)} parents for table {table_name}")
        for parent in pipeline_definition.parents:
            logger.debug(f"Processing parent {parent.table_name} for table {table_name}")
            # Recursively process each parent
            _find_all_parent_tables_recursive(parent.table_name, inventory, visited, tables_to_process)


def _build_new_table_name(old_table_name: str, version: str) -> str:
    version_pattern = re.compile(r'_v(\d+)$')
    match = version_pattern.search(old_table_name)
    if match:
        existing_version = match.group(1)  # Extract the numerical value
        # Remove the existing version suffix before adding the new version
        base_table_name = version_pattern.sub('', old_table_name)
        return f"{base_table_name}_v{str(int(existing_version)+1)}"
    else:
        return f"{old_table_name}{version}"
