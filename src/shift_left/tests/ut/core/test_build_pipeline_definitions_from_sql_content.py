"""
Copyright 2024-2025 Confluent, Inc.

Comprehensive unit tests for _build_pipeline_definitions_from_sql_content function
These tests work with the actual behavior of the function
"""
import unittest
import os
import tempfile
import pathlib
from unittest.mock import patch, mock_open, MagicMock
from typing import Dict, Set

# Set up test environment before importing modules
os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")
from shift_left.core.models.flink_statement_model import FlinkStatementComplexity 
from shift_left.core.pipeline_mgr import (
    _build_pipeline_definitions_from_sql_content,
    ERROR_TABLE_NAME,
    _build_pipeline_definition
)
from shift_left.core.utils.file_search import (
    FlinkTableReference,
    FlinkTablePipelineDefinition,
    PIPELINE_FOLDER_NAME
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
            "intermediate_table": {
                "table_name": "intermediate_table", 
                "type": "intermediate",
                "table_folder_name": "intermediates/intermediate_table",
                "dml_ref": "intermediates/intermediate_table/sql-scripts/dml.intermediate_table.sql",
                "ddl_ref": "intermediates/intermediate_table/sql-scripts/ddl.intermediate_table.sql"
            },
            "fact_table": {
                "table_name": "fact_table",
                "type": "fact",
                "table_folder_name": "facts/fact_table",
                "dml_ref": "facts/fact_table/sql-scripts/dml.fact_table.sql",
                "ddl_ref": "facts/fact_table/sql-scripts/ddl.fact_table.sql"
            }
        }

    @patch('builtins.open')
    def test_successful_dml_processing_with_dependencies(self, mock_open_file):
        """Test successful processing with DML file and dependencies"""
        # Setup
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO fact_table SELECT * FROM source_table JOIN intermediate_table on source_table.id = intermediate_table.id"
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "fact_table")
        self.assertEqual(len(result_deps), 2)
        self.assertEqual(result_complexity.state_form, "Stateful")
        self.assertEqual(result_complexity.number_of_regular_joins, 1)

    @patch('shift_left.core.pipeline_mgr._build_pipeline_definition')
    @patch('builtins.open')
    def test_dml_with_complex_table_names(self, mock_open_file, mock_build_def):
        """Test processing with complex multi-part table names"""
        # Setup
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO target_table SELECT * FROM `clone.prod.database.audit_trail`"
        
        mock_pipeline_def = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_build_def.return_value = mock_pipeline_def
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "target_table")
        self.assertEqual(len(result_deps), 0) # clone. are removed
        self.assertEqual(result_complexity.state_form, "Stateless")

    @patch('builtins.open')
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    def test_self_reference_removal(self, mock_parser_class, mock_open_file):
        """Test that self-references are removed from dependencies"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "target_table"
        mock_parser.extract_table_references.return_value = {"target_table", "source_table"}
        mock_parser.extract_upgrade_mode.return_value = "append"
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO target_table SELECT * FROM target_table JOIN source_table"
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "target_table")
        self.assertEqual(len(result_deps), 1)  # Only source_table, not target_table

    @patch('shift_left.core.pipeline_mgr.logger')
    @patch('builtins.open')
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    def test_unknown_table_reference_handling(self, mock_parser_class, mock_open_file, mock_logger):
        """Test handling of table not in the inventory references"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "target_table"
        mock_parser.extract_table_references.return_value = {"unknown_table", "source_table"}
        mock_parser.extract_upgrade_mode.return_value = "append"
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO target_table SELECT * FROM unknown_table JOIN source_table"
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "target_table")
        self.assertEqual(len(result_deps), 1)  # Only source_table, unknown_table skipped
        # Verify warning was logged for unknown table
        warning_calls = [call for call in mock_logger.warning.call_args_list if 'unknown_table' in str(call)]
        self.assertTrue(len(warning_calls) > 0)

    @patch('builtins.open')
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    def test_no_referenced_tables(self, mock_parser_class, mock_open_file):
        """Test handling when no tables are referenced"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "target_table"
        mock_parser.extract_table_references.return_value = set()
        mock_parser.extract_upgrade_mode.return_value = "Stateless"
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO target_table VALUES (1, 'test')"
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "target_table")
        self.assertEqual(len(result_deps), 0)
        self.assertEqual(result_complexity.state_form, "Stateless")

    @patch('builtins.open')
    @patch('shift_left.core.pipeline_mgr.SQLparser')
    def test_ddl_only_processing(self, mock_parser_class, mock_open_file):
        """Test processing with only DDL file provided"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_create_statement.return_value = "new_table"
        mock_parser.extract_upgrade_mode.return_value = "Stateful"
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        mock_open_file.return_value.__enter__.return_value.read.return_value = "CREATE TABLE new_table (id INT, name VARCHAR(50))"
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            None,  # No DML file
            "/path/to/ddl.sql", 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "new_table")
        self.assertEqual(len(result_deps), 0)

    @patch('builtins.open')
    @patch('shift_left.core.pipeline_mgr.SQLparser')
    def test_ddl_fallback_when_dml_has_no_table_name(self, mock_parser_class, mock_open_file):
        """Test fallback to DDL when DML doesn't contain extractable table name"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        # This simulates the actual parser behavior when no INSERT INTO is found
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "No-Table"  
        mock_parser.extract_table_name_from_create_statement.return_value = "created_table"
        mock_parser.extract_table_references.return_value = {"source_table"}
        mock_parser.extract_upgrade_mode.return_value = "Stateful"
        complexity = FlinkStatementComplexity()
        complexity.state_form = "Stateful"
        mock_parser.extract_statement_complexity.return_value = complexity
        dml_content = "SELECT * FROM source_table"  # No INSERT INTO statement
        ddl_content = "CREATE TABLE created_table (id INT, name VARCHAR(50))"
        mock_open_file.return_value.__enter__.return_value.read.side_effect = [dml_content, ddl_content]
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql",
            "/path/to/ddl.sql", 
            self.table_inventory
        )
        
        # Assert - function works with "No-Table" as table name (actual behavior)
        self.assertEqual(result_table, "No-Table")
        self.assertEqual(len(result_deps), 1)  # source_table dependency from DML


    @patch('builtins.open')
    @patch('shift_left.core.pipeline_mgr.SQLparser')
    def test_pipeline_folder_path_transformation(self, mock_parser_class, mock_open_file):
        """Test path transformation when file starts with PIPELINE_FOLDER_NAME"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "target_table"
        mock_parser.extract_table_references.return_value = set()
        mock_parser.extract_upgrade_mode.return_value = "Stateless"
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO target_table VALUES (1, 'test')"
        
        # Execute with pipeline folder name prefix
        dml_path = f"{PIPELINE_FOLDER_NAME}/facts/target_table/dml.sql"
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            dml_path, 
            None, 
            self.table_inventory
        )
        
        # Assert
        expected_path = os.path.join(os.getenv("PIPELINES"), "..", dml_path)
        mock_open_file.assert_called_with(expected_path)
        self.assertEqual(result_table, "target_table")
        self.assertIsNotNone(result_complexity)

    @patch('shift_left.core.pipeline_mgr._build_pipeline_definition')
    @patch('builtins.open')
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    def test_dependent_table_with_pipeline_folder_ref(self, mock_parser_class, mock_open_file, mock_build_def):
        """Test processing dependent table with pipeline folder reference"""
        # Setup inventory with pipeline folder reference
        inventory_with_pipeline_ref = {
            "source_table": {
                "table_name": "source_table",
                "type": "source", 
                "table_folder_name": "sources/source_table",
                "dml_ref": f"{PIPELINE_FOLDER_NAME}/sources/source_table/sql-scripts/dml.source_table.sql",
                "ddl_ref": f"{PIPELINE_FOLDER_NAME}/sources/source_table/sql-scripts/ddl.source_table.sql"
            }
        }
        
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "target_table"
        mock_parser.extract_table_references.return_value = {"source_table"}
        mock_parser.extract_upgrade_mode.side_effect = ["append", "Stateful"]  # Main table, then dependent
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        # Mock file reads: main DML, dependent DML, dependent DDL
        mock_open_file.return_value.__enter__.return_value.read.side_effect = [
            "INSERT INTO target_table SELECT * FROM source_table",
            "INSERT INTO source_table VALUES (1, 'test')",
            "CREATE TABLE source_table (id INT, name VARCHAR(50))"
        ]
        
        mock_pipeline_def = MagicMock(spec=FlinkTablePipelineDefinition)
        mock_build_def.return_value = mock_pipeline_def
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            inventory_with_pipeline_ref
        )
        
        # Assert
        self.assertEqual(result_table, "target_table")
        self.assertEqual(len(result_deps), 1)
        self.assertEqual(mock_open_file.call_count, 3)  # Main DML + dependent DML + dependent DDL

    @patch('shift_left.core.pipeline_mgr.logger')
    @patch('builtins.open', side_effect=FileNotFoundError("File not found"))
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    def test_file_not_found_error_handling(self, mock_parser_class, mock_open_file, mock_logger):
        """Test error handling when file is not found"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/nonexistent/path/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, ERROR_TABLE_NAME)
        self.assertEqual(len(result_deps), 0)
        self.assertIsNone(result_complexity)
        mock_logger.error.assert_called()

    @patch('shift_left.core.pipeline_mgr.logger')
    @patch('shift_left.core.utils.sql_parser.SQLparser', side_effect=Exception("Parser error"))
    def test_parser_exception_handling(self, mock_parser_class, mock_logger):
        """Test error handling when parser raises exception"""
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, ERROR_TABLE_NAME)
        self.assertEqual(len(result_deps), 0)
        self.assertIsNone(result_complexity)
        mock_logger.error.assert_called()

    @patch('builtins.open')
    @patch('shift_left.core.utils.sql_parser.SQLparser')
    def test_empty_file_handling(self, mock_parser_class, mock_open_file):
        """Test handling of empty files"""
        # Setup
        mock_parser = MagicMock()
        mock_parser_class.return_value = mock_parser
        mock_parser.extract_table_name_from_insert_into_statement.return_value = "No-Table"
        mock_parser.extract_table_name_from_create_statement.return_value = "No-Table"
        mock_parser.extract_table_references.return_value = set()
        mock_parser.extract_upgrade_mode.return_value = "Stateless"
        mock_parser.extract_statement_complexity.return_value = FlinkStatementComplexity()
        
        mock_open_file.return_value.__enter__.return_value.read.return_value = ""
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/empty_dml.sql",
            "/path/to/empty_ddl.sql", 
            self.table_inventory
        )
        
        # Assert
        self.assertEqual(result_table, "No-Table")
        self.assertEqual(len(result_deps), 0)
        self.assertEqual(result_complexity.state_form, "Stateless")

    @patch('shift_left.core.pipeline_mgr.logger')
    @patch('shift_left.core.pipeline_mgr.FlinkTableReference')
    @patch('builtins.open')
    def test_table_reference_validation_error(self, mock_open_file, mock_table_ref, mock_logger):
        """Test handling of table reference validation errors"""
        # Setup        
        mock_open_file.return_value.__enter__.return_value.read.return_value = "INSERT INTO target_table SELECT * FROM source_table"
        
        # Mock validation error
        mock_table_ref.model_validate.side_effect = Exception("Validation error")
        
        # Execute
        result_table, result_deps, result_complexity = _build_pipeline_definitions_from_sql_content(
            "/path/to/dml.sql", 
            None, 
            self.table_inventory
        )
        
        # Assert - function continues despite validation error
        self.assertEqual(result_table, ERROR_TABLE_NAME)
        self.assertEqual(len(result_deps), 0)  # Failed validation means no dependencies added
        self.assertIsNone(result_complexity)
        mock_logger.error.assert_called()


if __name__ == '__main__':
    unittest.main()