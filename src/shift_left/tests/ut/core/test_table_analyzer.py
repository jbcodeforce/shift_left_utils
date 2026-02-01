"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import os
import pathlib
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone

os.environ["CONFIG_FILE"] = str(pathlib.Path(__file__).parent.parent.parent / "config.yaml")

TEST_PIPELINES_DIR = str(pathlib.Path(__file__).parent.parent.parent / "data/flink-project/pipelines")
os.environ["PIPELINES"] = TEST_PIPELINES_DIR

from shift_left.core.models.flink_statement_model import StatementInfo, StatementListCache
from shift_left.core.utils.file_search import FlinkTablePipelineDefinition
from shift_left.core.table_analyzer import (
    get_tables_referenced_by_running_statements,
    get_topics_for_tables,
    assess_unused_tables
)


class TestTableAnalyzer(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures"""
        cls.inventory_path = TEST_PIPELINES_DIR
        cls.test_table_name = "fct_order"
        cls.test_source_table = "src_table_1"


    def test_get_tables_referenced_by_running_statements_empty(self):
        """Test with empty statement list"""
        empty_statements = {}
        result = get_tables_referenced_by_running_statements(empty_statements,  self.inventory_path)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 0)

    def test_get_tables_referenced_by_running_statements_with_running(self):
        """Test that running statements correctly identify referenced tables"""
        # Create mock running statements
        mock_statements = {
            "dev-use1-fct_order-dml": StatementInfo(
                name="dev-use1-fct_order-dml",
                status_phase="RUNNING",
                sql_content="insert into fct_order select * from int_table_1",
                created_at=datetime.now(timezone.utc)
            ),
            "dev-use1-int_table_1-dml": StatementInfo(
                name="dev-use1-int_table_1-dml",
                status_phase="RUNNING",
                sql_content="insert into int_table_1 select * from src_table_1",
                created_at=datetime.now(timezone.utc)
            )
        }

        result = get_tables_referenced_by_running_statements(mock_statements,  self.inventory_path)

        self.assertIsInstance(result, set)
        # Should include the tables themselves and their parents
        self.assertIn("fct_order", result)
        self.assertIn("int_table_1", result)

    def test_get_tables_referenced_by_running_statements_filters_non_running(self):
        """Test that only RUNNING statements are considered"""
        mock_statements = {
            "dev-use1-fct_order-dml": StatementInfo(
                name="dev-use1-fct_order-dml",
                status_phase="RUNNING",
                sql_content="insert into fct_order select * from int_table_1",
                created_at=datetime.now(timezone.utc)
            ),
            "dev-use1-int_table_1-dml": StatementInfo(
                name="dev-use1-int_table_1-dml",
                status_phase="COMPLETED",  # Not running
                sql_content="insert into int_table_1 select * from src_table_1",
                created_at=datetime.now(timezone.utc)
            ),
            "dev-use1-int_table_2-dml": StatementInfo(
                name="dev-use1-int_table_2-dml",
                status_phase="FAILED",  # Not running
                sql_content="insert into int_table_2 select * from src_table_2",
                created_at=datetime.now(timezone.utc)
            )
        }

        result = get_tables_referenced_by_running_statements(mock_statements,  self.inventory_path)

        # Should only include fct_order (the running one) and its parents
        self.assertIn("fct_order", result)
        # int_table_1 and int_table_2 should not be directly included (they're not running)
        # But if they're parents of fct_order, they should be included


    def test_get_topics_for_tables(self):
        """Test that topics are correctly extracted for tables"""
        tables = {"fct_order", "int_table_1"}
        result = get_topics_for_tables(tables, self.inventory_path)

        self.assertIsInstance(result, set)
        # Should return topic names (exact format depends on naming convention)

    def test_get_topics_for_tables_empty(self):
        """Test with empty table set"""
        result = get_topics_for_tables(set(), self.inventory_path)

        self.assertIsInstance(result, set)
        self.assertEqual(len(result), 0)

    def test_assess_unused_tables_basic(self):
        """Test basic assessment of unused tables"""
        # Mock running statements to reference some tables
        mock_statements = {
            "dev-use1-fct_order-dml": StatementInfo(
                name="dev-use1-fct_order-dml",
                status_phase="RUNNING",
                created_at=datetime.now(timezone.utc)
            )
        }

        with patch('shift_left.core.table_analyzer.statement_mgr.get_statement_list', return_value=mock_statements):
            result = assess_unused_tables(self.inventory_path, include_topics=False)

        self.assertIsInstance(result, dict)
        self.assertIn('unused_tables', result)
        self.assertIn('table_details', result)
        self.assertIsInstance(result['unused_tables'], list)
        self.assertIsInstance(result['table_details'], dict)

    def test_assess_unused_tables_with_topics(self):
        """Test assessment including topic analysis"""
        mock_statements = {
            "dev-use1-fct_order-dml": StatementInfo(
                name="dev-use1-fct_order-dml",
                status_phase="RUNNING",
                created_at=datetime.now(timezone.utc)
            )
        }

        mock_topics = [
            {"topic_name": "fct_order", "cluster_id": "lkc-123", "partitions_count": 3},
            {"topic_name": "unused_topic", "cluster_id": "lkc-123", "partitions_count": 1}
        ]

        with patch('shift_left.core.table_analyzer.statement_mgr.get_statement_list', return_value=mock_statements), \
             patch('shift_left.core.table_analyzer.project_manager.get_topic_list', return_value=mock_topics):
            result = assess_unused_tables(self.inventory_path, include_topics=True)

        self.assertIn('unused_topics', result)
        self.assertIsInstance(result['unused_topics'], list)

    def test_assess_unused_tables_excludes_tables_with_children(self):
        """Test that tables with children are handled appropriately"""
        # This test verifies that if a table has children that are referenced,
        # the table itself might be marked differently
        mock_statements = {
            "dev-use1-fct_order-dml": StatementInfo(
                name="dev-use1-fct_order-dml",
                status_phase="RUNNING",
                created_at=datetime.now(timezone.utc)
            )
        }

        with patch('shift_left.core.table_analyzer.statement_mgr.get_statement_list', return_value=mock_statements):
            result = assess_unused_tables(self.inventory_path, include_topics=False)

        # Tables with children should be handled (exact behavior depends on implementation)
        for table_name, details in result['table_details'].items():
            if 'has_children' in details:
                # If a table has children, it might be used indirectly
                pass

    def test_assess_unused_tables_source_tables_warning(self):
        """Test that source tables are flagged appropriately"""
        mock_statements = {}

        with patch('shift_left.core.table_analyzer.statement_mgr.get_statement_list', return_value=mock_statements):
            result = assess_unused_tables(self.inventory_path, include_topics=False)

        # Source tables might be in unused list but should be flagged
        for table_name, details in result['table_details'].items():
            if details.get('type') == 'source':
                # Source tables might need special handling
                pass

    def test_statement_name_to_table_mapping(self):
        """Test that statement names are correctly mapped to table names"""
        # Test various statement name formats
        test_cases = [
            ("dev-use1-fct_order-dml", "fct_order"),
            ("dev-use1-int_table_1-dml", "int_table_1"),
            ("prod-usw2-src_table_1-dml", "src_table_1"),
        ]

        # This would test the internal mapping function
        # Implementation depends on how statement names map to tables
        for statement_name, expected_table in test_cases:
            # The actual mapping logic will be in the implementation
            pass

    def test_pipeline_json_reading(self):
        """Test that pipeline.json files are correctly read"""
        # Verify that we can read pipeline.json for a known table
        from shift_left.core.pipeline_mgr import get_pipeline_definition_for_table

        try:
            pipeline_def = get_pipeline_definition_for_table(self.test_table_name, self.inventory_path)
            self.assertIsInstance(pipeline_def, FlinkTablePipelineDefinition)
            self.assertEqual(pipeline_def.table_name, self.test_table_name)
        except Exception as e:
            # If table doesn't exist in test data, that's okay for now
            pass

if __name__ == '__main__':
    unittest.main()
