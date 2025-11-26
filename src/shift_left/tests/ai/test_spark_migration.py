"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
from unittest.mock import patch

from shift_left.ai.translator_to_flink_sql import get_or_build_sql_translator_agent
from shift_left.core.utils.app_config import get_config

"""
Taking a complex SQL statement migrates to Flink SQL.
"""

class TestDbtMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        os.environ["STAGING"] = str(cls.data_dir / "flink-project/staging")
        os.environ["SRC_FOLDER"] = str(cls.data_dir / "spark-project")
        os.makedirs(os.environ["STAGING"], exist_ok=True)
        os.makedirs(os.environ["STAGING"] + "/data_product", exist_ok=True)

    def _process_one_spark_file(self, dbt_file: str):
        config = get_config()
        config['app']['translator_to_flink_sql_agent']='shift_left.core.utils.translator_to_flink_sql.DbtTranslatorToFlinkSqlAgent'
        src_folder = os.environ["SRC_FOLDER"]
        dbt_src_file = src_folder + "/" + dbt_file
        with open(dbt_src_file, "r") as f:
            dbt_content = f.read()
        translator_agent = get_or_build_sql_translator_agent()
        dml, ddl = translator_agent.translate_to_flink_sqls(dbt_file, dbt_content, validate=True)
        print(f"\n=== {dbt_file} ===")
        print("DML Output:")
        print(dml)
        print("\nDDL Output:")
        print(ddl)
        assert dml is not None
        assert ddl is not None

    def setUp(self):
        pass

    def tearDown(self):
        pass

# -- test methods --
    @patch('builtins.input')
    def test_1_spark_basic_table(self, mock_input):
        """
        Test a basic table spark fact users table migration.
        """
        dbt_file = "facts/p5/fct_users.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_2_product_analytics_window_functions(self, mock_input):
        """
        Test product analytics query with window functions and time-based aggregations.
        """
        dbt_file = "sources/src_product_analytics.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_3_customer_journey_complex_ctes(self, mock_input):
        """
        Test customer journey analysis with multiple CTEs and complex joins.
        """
        dbt_file = "sources/src_customer_journey.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_4_event_processing_arrays_structs(self, mock_input):
        """
        Test event processing with array and struct operations commonly used in Spark.
        """
        dbt_file = "sources/src_event_processing.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_5_sales_pivot_advanced_analytics(self, mock_input):
        """
        Test sales analysis with pivot operations and advanced analytics.
        """
        dbt_file = "sources/src_sales_pivot.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_6_streaming_aggregations_time_windows(self, mock_input):
        """
        Test streaming aggregations with time windows and real-time analytics.
        """
        dbt_file = "sources/src_streaming_aggregations.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_7_set_operations_subqueries(self, mock_input):
        """
        Test set operations and complex subqueries with user segmentation.
        """
        dbt_file = "sources/src_set_operations.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_8_temporal_analytics_time_series(self, mock_input):
        """
        Test complex temporal analytics and time series analysis.
        """
        dbt_file = "sources/src_temporal_analytics.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    @patch('builtins.input')
    def test_9_advanced_transformations_udfs(self, mock_input):
        """
        Test advanced transformations with UDFs and complex data manipulations.
        """
        dbt_file = "sources/src_advanced_transformations.sql"
        mock_input.return_value = "n"
        self._process_one_spark_file(dbt_file)

    # Integration test to run all examples
    @patch('builtins.input')
    def test_all_spark_examples_integration(self, mock_input):
        """
        Integration test to validate all Spark SQL examples can be processed.
        """
        mock_input.return_value = "n"

        spark_examples = [
            "facts/p5/fct_users.sql",
            "sources/src_product_analytics.sql",
            "sources/src_customer_journey.sql",
            "sources/src_event_processing.sql",
            "sources/src_sales_pivot.sql",
            "sources/src_streaming_aggregations.sql",
            "sources/src_set_operations.sql",
            "sources/src_temporal_analytics.sql",
            "sources/src_advanced_transformations.sql"
        ]

        success_count = 0
        total_count = len(spark_examples)

        for example in spark_examples:
            try:
                self._process_one_spark_file(example)
                success_count += 1
                print(f"✓ Successfully processed: {example}")
            except Exception as e:
                print(f"✗ Failed to process {example}: {e}")

        print(f"\nIntegration Test Results: {success_count}/{total_count} examples processed successfully")

        # Allow some failures but expect most to work
        success_rate = success_count / total_count
        assert success_rate >= 0.7, f"Integration test failed: only {success_rate:.1%} of examples processed successfully"

if __name__ == '__main__':
    unittest.main()
