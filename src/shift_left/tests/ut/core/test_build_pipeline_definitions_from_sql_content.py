"""
Copyright 2024-2025 Confluent, Inc.

Comprehensive unit tests for _build_pipeline_definitions_from_sql_content function
"""
import unittest
import os
import tempfile
import pathlib
from unittest.mock import patch, mock_open, MagicMock
from typing import Dict, Set

# Set up test environment before importing modules
os.environ["SL_CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")

from shift_left.core.pipeline_mgr import (
    _build_pipeline_definitions_from_sql_content,
    ERROR_TABLE_NAME,
    _build_pipeline_definition
)
from shift_left.core.utils.file_search import (
    EXTERNAL_KAFKA_TYPE,
    FlinkTableReference,
    FlinkTablePipelineDefinition,
    PIPELINE_FOLDER_NAME,
)


class TestBuildPipelineDefinitionsFromSqlContent(unittest.TestCase):

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        os.environ["PIPELINES"] = os.path.join(self.temp_dir, "pipelines")

        # Sample table inventory
        self.table_inventory = {
            "source_table": {
                "table_name": "source_table",
                "type": "source",
                "table_folder_name": "sources/source_table",
                "dml_ref": "sources/source_table/sql-scripts/dml.source_table.sql",
                "ddl_ref": "sources/source_table/sql-scripts/ddl.source_table.sql"
            },
            "dim_table": {
                "table_name": "dim_table",
                "type": "dimensions",
                "table_folder_name": "dimensions/dim_table",
                "dml_ref": "dimensions/dim_table/sql-scripts/dml.dim_table.sql",
                "ddl_ref": "dimensions/dim_table/sql-scripts/ddl.dim_table.sql"
            },
            "fact_table": {
                "table_name": "fact_table",
                "type": "fact",
                "table_folder_name": "facts/fact_table",
                "dml_ref": "facts/fact_table/sql-scripts/dml.fact_table.sql",
                "ddl_ref": "facts/fact_table/sql-scripts/ddl.fact_table.sql"
            }
        }

    @patch("builtins.open")
    def test_1_happy_path(self, mock_open_file):
        """Test happy path"""

        mock_open_file.return_value.__enter__.return_value.read.side_effect= [
            "CREATE TABLE fact_table (id INT, name VARCHAR(50))",
            "INSERT INTO fact_table SELECT * FROM dim_table",
            "CREATE TABLE dim_table (id INT, name VARCHAR(50))",
            "INSERT INTO dim_table SELECT * FROM source_table",
            "CREATE TABLE source_table (id INT, name VARCHAR(50))",
            "INSERT INTO source_table VALUES (1, 'test')"
        ]
        result_table, parent_references, complexity = _build_pipeline_definitions_from_sql_content(
            "facts/fact_table/sql-scripts/dml.fact_table.sql",
            "facts/fact_table/sql-scripts/ddl.fact_table.sql",
            self.table_inventory
        )
        self.assertEqual(result_table, "fact_table")
        assert complexity
        assert complexity.state_form == "Stateless"
        assert len(parent_references) == 1
        assert next(iter(parent_references)).table_name == "dim_table"


    @patch("builtins.open")
    def test_2_parent_not_found_but_external_kafka(self, mock_open_file):
        """Test parent not found in inventory"""
        os.environ["SL_KEEP_UNKNOWN_SQL_REFS_AS_EXTERNAL_KAFKA"] = "1"
        mock_open_file.return_value.__enter__.return_value.read.side_effect= [
            "CREATE TABLE fact_table (id INT, name VARCHAR(50))",
            "INSERT INTO fact_table SELECT * FROM unknown_dim_table",
            "CREATE TABLE dim_table (id INT, name VARCHAR(50))",
            "INSERT INTO dim_table SELECT * FROM source_table",
            "CREATE TABLE source_table (id INT, name VARCHAR(50))",
            "INSERT INTO source_table VALUES (1, 'test')"
        ]
        result_table, parent_references, complexity = _build_pipeline_definitions_from_sql_content(
            "facts/fact_table/sql-scripts/dml.fact_table.sql",
            "facts/fact_table/sql-scripts/ddl.fact_table.sql",
            self.table_inventory
        )
        self.assertEqual(result_table, "fact_table")
        assert complexity
        assert complexity.state_form == "Stateless"
        assert len(parent_references) == 1
        assert next(iter(parent_references)).table_name == "unknown_dim_table"
        assert next(iter(parent_references)).type == EXTERNAL_KAFKA_TYPE
        assert next(iter(parent_references)).kafka_topic == "unknown_dim_table"


    @patch("builtins.open")
    def test_3_parent_not_found(self, mock_open_file):
        """Test parent not found in inventory"""
        os.environ["SL_KEEP_UNKNOWN_SQL_REFS_AS_EXTERNAL_KAFKA"] = "0"
        mock_open_file.return_value.__enter__.return_value.read.side_effect= [
            "CREATE TABLE fact_table (id INT, name VARCHAR(50))",
            "INSERT INTO fact_table SELECT * FROM unknown_dim_table",
            "CREATE TABLE dim_table (id INT, name VARCHAR(50))",
            "INSERT INTO dim_table SELECT * FROM source_table",
            "CREATE TABLE source_table (id INT, name VARCHAR(50))",
            "INSERT INTO source_table VALUES (1, 'test')"
        ]
        result_table, parent_references, complexity = _build_pipeline_definitions_from_sql_content(
            "facts/fact_table/sql-scripts/dml.fact_table.sql",
            "facts/fact_table/sql-scripts/ddl.fact_table.sql",
            self.table_inventory
        )
        self.assertEqual(result_table, "fact_table")
        assert complexity
        assert complexity.state_form == "Stateless"
        assert len(parent_references) == 0


    @patch("builtins.open")
    def test_4_join_should_lead_to_two_parents(self, mock_open_file):
        """Test parent not found in inventory"""
        os.environ["SL_KEEP_UNKNOWN_SQL_REFS_AS_EXTERNAL_KAFKA"] = "0"
        mock_open_file.return_value.__enter__.return_value.read.side_effect= [
            "CREATE TABLE fact_table (id INT, name VARCHAR(50))",
            "INSERT INTO fact_table SELECT * FROM source_table JOIN dim_table on source_table.id = dim_table.id",
            "CREATE TABLE dim_table (id INT, name VARCHAR(50))",
            "INSERT INTO dim_table SELECT * FROM source_table",
            "CREATE TABLE source_table (id INT, name VARCHAR(50))",
            "INSERT INTO source_table VALUES (1, 'test')"
        ]
        result_table, parent_references, complexity = _build_pipeline_definitions_from_sql_content(
            "facts/fact_table/sql-scripts/dml.fact_table.sql",
            "facts/fact_table/sql-scripts/ddl.fact_table.sql",
            self.table_inventory
        )
        self.assertEqual(result_table, "fact_table")
        assert complexity
        assert complexity.state_form == "Stateful"
        assert len(parent_references) == 2

if __name__ == '__main__':
    unittest.main()
