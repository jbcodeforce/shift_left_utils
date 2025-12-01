"""
Copyright 2024-2025 Confluent, Inc.
"""
import unittest
import pathlib
import os
import shutil
import json
from unittest.mock import patch, MagicMock
from shift_left.ai.process_src_tables import migrate_one_file
from ai.utilities import compare_files_unordered

data_dir = pathlib.Path(__file__).parent.parent / "data"
os.environ["CONFIG_FILE"] = str(data_dir.parent / "config-ccloud.yaml")
from shift_left.ai.process_src_tables import migrate_one_file
import shift_left.core.utils.app_config as app_config
# Mock validate_config to avoid SystemExit
app_config.validate_config = MagicMock()

"""
Taking a complex Spark SQL statement migrates to Flink SQL.
"""
class TestSparkMigration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.data_dir = pathlib.Path(__file__).parent.parent / "data"  # Path to the data directory
        cls.src_folder = str(cls.data_dir / "spark-project/")
        cls.staging = str(cls.data_dir / "flink-project/staging/ut")
        cls.product_name = "basic"
        os.environ["STAGING"] = cls.staging
        shutil.rmtree(os.environ["STAGING"], ignore_errors=True)
        os.makedirs(os.environ["STAGING"], exist_ok=True)

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
        spark_src_file = "/facts/p5/fct_users.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="fct_users",
                        sql_src_file=self.src_folder + spark_src_file,
                        staging_target_folder=self.staging,
                        product_name="p5",
                        source_type="spark",
                        validate=False)
        assert os.path.exists(self.staging + "/p5/fct_users")
        assert os.path.exists(self.staging + "/p5/fct_users/sql-scripts/ddl.fct_users.sql")
        assert os.path.exists(self.staging + "/p5/fct_users/sql-scripts/dml.fct_users.sql")
        reference_file = self.src_folder + "/flink-references/fct_users/sql-scripts/ddl.fct_users.sql"
        created_file = self.staging + "/p5/fct_users/sql-scripts/ddl.fct_users.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(result)
        print(f"dml result: {json.dumps(result, indent=4)}")
        assert result['match_percentage'] >= 80
        reference_file = self.src_folder + "/flink-references/fct_users/sql-scripts/dml.fct_users.sql"
        created_file = self.staging + "/p5/fct_users/sql-scripts/dml.fct_users.sql"
        result = compare_files_unordered(reference_file, created_file)
        print(result)
        assert result['match_percentage'] >= 80


    @patch('builtins.input')
    def test_2_product_analytics_window_functions(self, mock_input):
        """
        Test product analytics query with window functions and time-based aggregations.
        """
        spark_src_file = "/sources/src_product_analytics.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_product_analytics",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)
        assert os.path.exists(self.staging + "/p5/src_product_analytics")
        assert os.path.exists(self.staging + "/p5/src_product_analytics/sql-scripts/ddl.raw_product_events.sql")
        # Verify there are two 'ddl.' files in the sql-scripts folder
        sql_scripts_folder = os.path.join(self.staging, "p5", "src_product_analytics", "sql-scripts")
        ddl_files = [f for f in os.listdir(sql_scripts_folder) if f.startswith("ddl.") and f.endswith(".sql")]
        assert len(ddl_files) == 2, f"Expected 2 ddl.*.sql files, found {len(ddl_files)} in {sql_scripts_folder}: {ddl_files}"
        for ddl_file in ddl_files:
            created_file = self.staging + "/p5/src_product_analytics/sql-scripts/" + ddl_file
            if ddl_file == "ddl.raw_product_events.sql":
                reference_file = self.src_folder + "/flink-references/src_product_analytics/sql-scripts/ddl.raw_product_events.sql"
            else:
                reference_file = self.src_folder + "/flink-references/src_product_analytics/sql-scripts/ddl.enriched_product_events.sql"
            result = compare_files_unordered(reference_file, created_file)
            print(result)
            print(f"Result: {json.dumps(result, indent=4)}")
            assert result['match_percentage'] >= 80

    @patch('builtins.input')
    def test_3_customer_journey_complex_ctes(self, mock_input):
        """
        Test customer journey analysis with multiple CTEs and complex joins.
        """
        spark_src_file = "/sources/src_customer_journey.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_customer_journey",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)
        sql_scripts_folder = os.path.join(self.staging, "p5", "src_customer_journey", "sql-scripts")
        ddl_files = [f for f in os.listdir(sql_scripts_folder) if f.startswith("ddl.") and f.endswith(".sql")]
        assert len(ddl_files) >= 1


    @patch('builtins.input')
    def test_4_event_processing_arrays_structs(self, mock_input):
        """
        Test event processing with array and struct operations commonly used in Spark.
        """
        spark_src_file = "/sources/src_event_processing.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_event_processing",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)
        sql_scripts_folder = os.path.join(self.staging, "p5", "src_event_processing", "sql-scripts")
        ddl_files = [f for f in os.listdir(sql_scripts_folder) if f.startswith("ddl.") and f.endswith(".sql")]
        assert len(ddl_files) >= 1

    @patch('builtins.input')
    def test_5_sales_pivot_advanced_analytics(self, mock_input):
        """
        Test sales analysis with pivot operations and advanced analytics.
        """
        spark_src_file = "/sources/src_sales_pivot.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_sales_pivot",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)

    @patch('builtins.input')
    def test_6_streaming_aggregations_time_windows(self, mock_input):
        """
        Test streaming aggregations with time windows and real-time analytics.
        """
        spark_src_file = "/sources/src_streaming_aggregations.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_streaming_aggregations",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)

    @patch('builtins.input')
    def test_7_set_operations_subqueries(self, mock_input):
        """
        Test set operations and complex subqueries with user segmentation.
        """
        spark_src_file = "/sources/src_set_operations.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_set_operations",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)

    @patch('builtins.input')
    def test_8_temporal_analytics_time_series(self, mock_input):
        """
        Test complex temporal analytics and time series analysis.
        """
        spark_src_file = "/sources/src_temporal_analytics.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_temporal_analytics",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)

    @patch('builtins.input')
    def test_9_advanced_transformations_udfs(self, mock_input):
        """
        Test advanced transformations with UDFs and complex data manipulations.
        """
        spark_src_file = "/sources/src_advanced_transformations.sql"
        mock_input.return_value = "n"
        migrate_one_file(table_name="src_advanced_transformations",
                sql_src_file=self.src_folder + spark_src_file,
                staging_target_folder=self.staging,
                product_name="p5",
                source_type="spark",
                validate=False)


if __name__ == '__main__':
    unittest.main()
