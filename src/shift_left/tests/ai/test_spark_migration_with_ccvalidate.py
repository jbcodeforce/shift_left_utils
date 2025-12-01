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
        mock_input.return_value = "y"
        migrate_one_file(table_name="fct_users",
                        sql_src_file=self.src_folder + spark_src_file,
                        staging_target_folder=self.staging,
                        product_name="p5",
                        source_type="spark",
                        validate=True)
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

if __name__ == '__main__':
    unittest.main()
