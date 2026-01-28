"""
Copyright 2024-2026 Confluent, Inc.

Module to analyze unused Flink tables by comparing inventory against running statements.
"""
from typing import Dict, Set, List, Optional
from shift_left.core.utils.sql_parser import SQLparser
from shift_left.core.utils.file_search import (
    get_or_build_inventory,
    get_table_ref_from_inventory,
    read_pipeline_definition_from_file,
    PIPELINE_JSON_FILE_NAME,
    FlinkTableReference,
    FlinkTablePipelineDefinition,
    get_ddl_dml_names_from_pipe_def
)
from shift_left.core.pipeline_mgr import get_pipeline_definition_for_table
from shift_left.core.utils.app_config import logger
import shift_left.core.statement_mgr as statement_mgr
import shift_left.core.project_manager as project_manager




def get_tables_referenced_by_running_statements(
    statement_list: Dict[str, any],
    all_tables: Dict[str, FlinkTableReference],
    inventory_path: str
) -> Set[str]:
    """
    Get all tables referenced by running Flink statements.
    Includes the table itself and all its parent tables from pipeline.json.

    Args:
        statement_list: Dictionary of statement_name -> StatementInfo
        inventory_path: Path to the pipeline folder

    Returns:
        Set of table names that are referenced by running statements
    """
    referenced_tables = set()

    # Filter to only RUNNING statements
    running_statements = {
        name: info for name, info in statement_list.items()
        if hasattr(info, 'status_phase') and info.status_phase in ['RUNNING', 'PENDING', 'DEGRADED']
    }

    logger.info(f"Found {len(running_statements)} running statements out of {len(statement_list)} total")
    parser = SQLparser()
    for statement_name, statement_info in running_statements.items():
        # Map statement name to table name
        table_name = parser.extract_table_name_from_insert_into_statement(statement_info.sql_content)
        if table_name:
            referenced_tables.add(table_name)

            # Read pipeline.json to get parent tables
            try:
                pipeline_def = get_pipeline_definition_for_table(table_name, inventory_path)

                # Add all parent tables
                for parent in pipeline_def.parents:
                    referenced_tables.add(parent.table_name)
                    logger.debug(f"Added parent table {parent.table_name} for {table_name}")

            except Exception as e:
                logger.warning(f"Could not read pipeline.json for table {table_name}: {e}")
        else:
            logger.debug(f"Could not map statement {statement_name} to a table")

    return referenced_tables


def get_topics_for_tables(tables: Set[str], inventory_path: str) -> Set[str]:
    """
    Get expected Kafka topic names for a set of tables.

    Args:
        tables: Set of table names
        inventory_path: Path to the pipeline folder

    Returns:
        Set of topic names
    """
    topics = set()

    # For now, assume topic name matches table name
    # In the future, this could read from DDL files to get actual topic names
    for table_name in tables:
        # Simple mapping: table name = topic name (common convention)
        topics.add(table_name)

        # Could also check DDL files for explicit topic configuration
        # This would require parsing DDL files for 'topic' = '...' in connector config

    return topics


def assess_unused_tables(
    inventory_path: str,
    include_topics: bool = False
) -> Dict:
    """
    Assess which tables are unused (not referenced by any running Flink statements).

    Args:
        inventory_path: Path to the pipeline folder
        include_topics: Whether to also check for unused Kafka topics

    Returns:
        Dictionary with:
        - unused_tables: List of table names not referenced by running statements
        - unused_topics: List of topics not used (if include_topics=True)
        - table_details: Dict mapping table_name -> metadata (type, product, has_children, etc.)
    """
    logger.info(f"Assessing unused tables in {inventory_path}")

    # Get all tables from inventory
    all_tables = get_or_build_inventory(inventory_path, inventory_path, False)
    all_table_names = set(all_tables.keys())
    logger.info(f"Found {len(all_table_names)} tables in inventory")

    # Get Flink statements
    statement_list = statement_mgr.get_statement_list()
    logger.info(f"Found {len(statement_list)} total statements")

    referenced_tables = get_tables_referenced_by_running_statements(statement_list,all_tables, inventory_path)
    logger.info(f"Found {len(referenced_tables)} tables referenced by running statements")

    # Find unused tables
    unused_table_names = all_table_names - referenced_tables

    # Build table details
    table_details = {}
    for table_name in unused_table_names:
        table_info = all_tables[table_name]

        # Check if table has children (might be used indirectly)
        has_children = False
        try:
            pipeline_def = get_pipeline_definition_for_table(table_name, inventory_path)
            has_children = len(pipeline_def.children) > 0
        except Exception as e:
            logger.debug(f"Could not check children for {table_name}: {e}")

        table_details[table_name] = {
            'type': table_info.type,
            'product_name': table_info.product_name,
            'path': table_info.table_folder_name,
            'has_children': has_children
        }

    result = {
        'unused_tables': list(unused_table_names),
        'table_details': table_details
    }

    # Check topics if requested
    if include_topics:
        try:
            # Get topics from Kafka
            # Note: get_topic_list expects a file path, but we just need the list
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
                temp_file = f.name

            kafka_topics_list = project_manager.get_topic_list(temp_file)
            kafka_topics = {topic['topic_name'] for topic in kafka_topics_list} if kafka_topics_list else set()

            # Get expected topics for referenced tables
            expected_topics = get_topics_for_tables(referenced_tables, inventory_path)

            # Find unused topics
            unused_topics = kafka_topics - expected_topics

            result['unused_topics'] = list(unused_topics)
            result['total_kafka_topics'] = len(kafka_topics)
            result['expected_topics'] = len(expected_topics)

            logger.info(f"Found {len(unused_topics)} unused topics out of {len(kafka_topics)} total")

            # Clean up temp file
            import os
            try:
                os.unlink(temp_file)
            except:
                pass

        except Exception as e:
            logger.warning(f"Could not analyze topics: {e}")
            result['unused_topics'] = []
            result['topic_analysis_error'] = str(e)

    logger.info(f"Assessment complete: {len(unused_table_names)} unused tables found")
    return result
